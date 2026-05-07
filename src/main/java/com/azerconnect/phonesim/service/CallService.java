package com.azerconnect.phonesim.service;

import com.azerconnect.phonesim.adapter.kafka.CallEventPublisher;
import com.azerconnect.phonesim.adapter.kafka.FireEvent;
import com.azerconnect.phonesim.adapter.kafka.HangupEvent;
import com.azerconnect.phonesim.adapter.kafka.HangupEventPublisher;
import com.azerconnect.phonesim.adapter.redis.CallRepository;
import com.azerconnect.phonesim.adapter.scheduler.SchedulerClient;
import com.azerconnect.phonesim.adapter.webhook.CallEvent;
import com.azerconnect.phonesim.adapter.webhook.WebhookDispatcher;
import com.azerconnect.phonesim.config.CallProps;
import com.azerconnect.phonesim.domain.Call;
import com.azerconnect.phonesim.domain.CallKind;
import com.azerconnect.phonesim.domain.CallNotFoundException;
import com.azerconnect.phonesim.domain.CallStateMachine;
import com.azerconnect.phonesim.domain.CallStatus;
import com.azerconnect.phonesim.domain.Direction;
import com.azerconnect.phonesim.domain.DuplicateTestIdException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Voice call lifecycle (answer-driven):
 * <ol>
 *   <li>{@code POST /api/v1/calls/voice/...} arrives → {@code PENDING → DIALING}</li>
 *   <li>Publish {@code INITIAL} CallRecord on the call-event Kafka topic → {@code RINGING}</li>
 *   <li>Enqueue a <em>no-answer</em> guard timer with the scheduler. Wait for an
 *       {@link com.azerconnect.phonesim.adapter.kafka.AnswerEvent} on the answer-event topic.</li>
 *   <li>{@link #onAnswer(String, String)} cancels the guard timer, transitions
 *       {@code RINGING → ANSWERED}, then enqueues the duration timer.</li>
 *   <li>{@link #onTimerFire(String, String)} with {@code RELEASE} publishes a
 *       {@link com.azerconnect.phonesim.adapter.kafka.HangupEvent} (subscriber pressed "End call")
 *       and transitions to {@code RELEASED}. The CAP simulator owns ApplyChargingReport
 *       chunking and the final disconnect toward the SCP — phone-simulator does not chunk.</li>
 *   <li>If the no-answer guard fires before the AnswerEvent, the call is moved
 *       to {@code FAILED} with reason {@code no_answer_timeout}.</li>
 * </ol>
 *
 * <p>SMS skips DIALING/RINGING and the timer paths entirely:
 * {@code PENDING → ANSWERED → RELEASED} in one synchronous step after a single publish.
 */
@Service
public class CallService {

    private static final Logger log = LoggerFactory.getLogger(CallService.class);

    private final CallRepository repo;
    private final CallRecordMapper mapper;
    private final CallEventPublisher publisher;
    private final HangupEventPublisher hangupPublisher;
    private final SchedulerClient scheduler;
    private final WebhookDispatcher webhooks;
    private final CallProps callProps;
    private final MeterRegistry meters;

    public CallService(CallRepository repo,
                       CallRecordMapper mapper,
                       CallEventPublisher publisher,
                       HangupEventPublisher hangupPublisher,
                       SchedulerClient scheduler,
                       WebhookDispatcher webhooks,
                       CallProps callProps,
                       MeterRegistry meters) {
        this.repo = repo;
        this.mapper = mapper;
        this.publisher = publisher;
        this.hangupPublisher = hangupPublisher;
        this.scheduler = scheduler;
        this.webhooks = webhooks;
        this.callProps = callProps;
        this.meters = meters;
    }

    public Call placeVoice(String testId, Direction direction,
                            String callingParty, String calledParty, String imsi,
                            String mscNumber, String vlrAddress, int lac, int cellId,
                            int durationSeconds, boolean roaming, Integer serviceKeyOverride,
                            String callbackUrl) {
        MDC.put("testId", testId);
        try {
            int serviceKey = mapper.resolveServiceKey(CallKind.VOICE, direction, roaming, serviceKeyOverride);
            Instant now = Instant.now();
            Call call = new Call(testId, CallKind.VOICE, direction, CallStatus.PENDING,
                    callingParty, calledParty, imsi, mscNumber, vlrAddress, lac, cellId,
                    durationSeconds, serviceKey, roaming, callbackUrl, now, now, null);
            if (!repo.saveIfAbsent(call)) {
                throw new DuplicateTestIdException(testId);
            }
            Counter.builder("phonesim.calls.created")
                    .tag("kind", "VOICE").tag("direction", direction.name())
                    .register(meters).increment();

            try {
                call = transition(call, CallStatus.DIALING);
                publisher.publish(testId, mapper.toInitial(call));
                call = transition(call, CallStatus.RINGING);
                // Arm the no-answer guard. The duration timer is NOT enqueued here —
                // it's enqueued in onAnswer() once the CAP simulator confirms the answer.
                UUID guardTimerId = UUID.randomUUID();
                scheduler.enqueueNoAnswerTimeout(guardTimerId, testId,
                        callProps.noAnswerTimeout().toMillis());
                repo.saveNoAnswerTimerId(testId, guardTimerId);
            } catch (RuntimeException e) {
                call = fail(call, e.getMessage());
                webhooks.dispatch(callbackUrl, CallEvent.of("CALL_FAILED", call));
            }
            return call;
        } finally {
            MDC.remove("testId");
        }
    }

    public Call sendSms(String testId, Direction direction,
                        String callingParty, String calledParty, String imsi,
                        String mscNumber, String vlrAddress, int lac, int cellId,
                        Integer serviceKeyOverride, String callbackUrl) {
        MDC.put("testId", testId);
        try {
            int serviceKey = mapper.resolveServiceKey(CallKind.SMS, direction, false, serviceKeyOverride);
            Instant now = Instant.now();
            Call sms = new Call(testId, CallKind.SMS, direction, CallStatus.PENDING,
                    callingParty, calledParty, imsi, mscNumber, vlrAddress, lac, cellId,
                    0, serviceKey, false, callbackUrl, now, now, null);
            if (!repo.saveIfAbsent(sms)) {
                throw new DuplicateTestIdException(testId);
            }
            Counter.builder("phonesim.sms.sent")
                    .tag("direction", direction.name()).register(meters).increment();

            try {
                publisher.publish(testId, mapper.toInitial(sms));
                sms = transition(sms, CallStatus.ANSWERED);
                sms = transition(sms, CallStatus.RELEASED);
                webhooks.dispatch(callbackUrl, CallEvent.of("SMS_DELIVERED", sms));
            } catch (RuntimeException e) {
                sms = fail(sms, e.getMessage());
                webhooks.dispatch(callbackUrl, CallEvent.of("SMS_FAILED", sms));
            }
            return sms;
        } finally {
            MDC.remove("testId");
        }
    }

    /**
     * Driven by {@link com.azerconnect.phonesim.adapter.kafka.AnswerEventConsumer}.
     * Cancels the no-answer guard, transitions to ANSWERED, and enqueues the duration timer.
     * Idempotent: if the call is already past RINGING, the event is silently dropped.
     */
    public void onAnswer(String testId, String answerType) {
        Optional<Call> maybe = repo.findById(testId);
        if (maybe.isEmpty()) {
            log.warn("AnswerEvent for unknown testId={} — ignoring", testId);
            return;
        }
        Call call = maybe.get();
        if (call.status() != CallStatus.RINGING) {
            log.debug("AnswerEvent ignored for testId={} — current state is {} (already past RINGING)",
                    testId, call.status());
            return;
        }
        // Best-effort cancel of the no-answer guard. A 404 is normal if it already fired
        // moments before — the fire path is idempotent and will see ANSWERED state.
        repo.findNoAnswerTimerId(testId).ifPresent(id -> {
            try {
                scheduler.cancel(id);
            } catch (RuntimeException e) {
                log.warn("Failed to cancel no-answer timer {} for testId={}: {}",
                        id, testId, e.getMessage());
            }
        });
        repo.deleteNoAnswerTimerId(testId);

        try {
            call = transition(call, CallStatus.ANSWERED);
            UUID durationTimerId = UUID.randomUUID();
            scheduler.enqueueRelease(durationTimerId, testId, call.durationSeconds() * 1000L);
            repo.saveDurationTimerId(testId, durationTimerId);
            Counter.builder("phonesim.calls.answered")
                    .tag("answerType", answerType == null ? "UNKNOWN" : answerType)
                    .register(meters).increment();
            webhooks.dispatch(call.callbackUrl(), CallEvent.of("CALL_ANSWERED", call));
        } catch (RuntimeException e) {
            Call failed = fail(call, "answer-handling-failed: " + e.getMessage());
            webhooks.dispatch(failed.callbackUrl(), CallEvent.of("CALL_FAILED", failed));
            throw e;
        }
    }

    public void onTimerFire(String testId, String eventType) {
        if (FireEvent.EVENT_RELEASE.equals(eventType)) {
            handleRelease(testId);
        } else if (FireEvent.EVENT_NO_ANSWER.equals(eventType)) {
            handleNoAnswer(testId);
        } else {
            log.warn("Unknown FireEvent type {} for testId={} — ignoring", eventType, testId);
        }
    }

    private void handleRelease(String testId) {
        Optional<Call> maybe = repo.findById(testId);
        if (maybe.isEmpty()) {
            log.warn("RELEASE timer fired for unknown testId={} — ignoring", testId);
            return;
        }
        Call call = maybe.get();
        if (call.status().isTerminal()) {
            log.debug("RELEASE timer fired for terminal call {} (state={}) — idempotent skip",
                    testId, call.status());
            return;
        }
        if (call.status() != CallStatus.ANSWERED) {
            log.warn("RELEASE timer fired for testId={} but state is {} (expected ANSWERED) — failing call",
                    testId, call.status());
            Call failed = fail(call, "release-before-answer");
            webhooks.dispatch(failed.callbackUrl(), CallEvent.of("CALL_FAILED", failed));
            return;
        }
        try {
            // Phone-simulator hangs up — CAP owns chunking and final ACR/DISCONNECT toward SCP.
            hangupPublisher.publish(testId, HangupEvent.userHangup(testId));
            call = transition(call, CallStatus.RELEASED);
            repo.deleteDurationTimerId(testId);
            Counter.builder("phonesim.calls.released")
                    .tag("reason", "duration_elapsed")
                    .register(meters).increment();
            webhooks.dispatch(call.callbackUrl(), CallEvent.of("CALL_RELEASED", call));
        } catch (RuntimeException e) {
            Call failed = fail(call, "release-failed: " + e.getMessage());
            webhooks.dispatch(failed.callbackUrl(), CallEvent.of("CALL_FAILED", failed));
            throw e;
        }
    }

    private void handleNoAnswer(String testId) {
        Optional<Call> maybe = repo.findById(testId);
        if (maybe.isEmpty()) {
            log.debug("NO_ANSWER timer fired for unknown testId={} — ignoring", testId);
            return;
        }
        Call call = maybe.get();
        if (call.status() != CallStatus.RINGING) {
            // Either we already got the answer (ANSWERED) or the call is terminal.
            log.debug("NO_ANSWER timer fired for testId={} but state is {} — idempotent skip",
                    testId, call.status());
            repo.deleteNoAnswerTimerId(testId);
            return;
        }
        Call failed = fail(call, "no_answer_timeout");
        repo.deleteNoAnswerTimerId(testId);
        Counter.builder("phonesim.calls.released")
                .tag("reason", "no_answer_timeout")
                .register(meters).increment();
        webhooks.dispatch(failed.callbackUrl(), CallEvent.of("CALL_FAILED", failed));
    }

    public Call findOrThrow(String testId) {
        return repo.findById(testId).orElseThrow(() -> new CallNotFoundException(testId));
    }

    public List<Call> listByStatus(CallStatus status) {
        return repo.listByStatus(status);
    }

    private Call transition(Call call, CallStatus next) {
        CallStateMachine.require(call.kind(), call.status(), next);
        Call updated = call.withStatus(next, Instant.now());
        repo.save(updated);
        return updated;
    }

    private Call fail(Call call, String reason) {
        Call failed = call.withFailure(reason, Instant.now());
        repo.save(failed);
        return failed;
    }
}
