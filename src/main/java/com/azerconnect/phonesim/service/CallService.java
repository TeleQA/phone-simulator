package com.azerconnect.phonesim.service;

import com.azerconnect.phonesim.adapter.amqp.CallRecordPublisher;
import com.azerconnect.phonesim.adapter.kafka.FireEvent;
import com.azerconnect.phonesim.adapter.redis.CallRepository;
import com.azerconnect.phonesim.adapter.scheduler.SchedulerClient;
import com.azerconnect.phonesim.adapter.webhook.CallEvent;
import com.azerconnect.phonesim.adapter.webhook.WebhookDispatcher;
import com.azerconnect.phonesim.domain.Call;
import com.azerconnect.phonesim.domain.CallKind;
import com.azerconnect.phonesim.domain.CallNotFoundException;
import com.azerconnect.phonesim.domain.CallStateMachine;
import com.azerconnect.phonesim.domain.CallStatus;
import com.azerconnect.phonesim.domain.Direction;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class CallService {

    private static final Logger log = LoggerFactory.getLogger(CallService.class);

    private final CallRepository repo;
    private final CallRecordMapper mapper;
    private final CallRecordPublisher publisher;
    private final SchedulerClient scheduler;
    private final WebhookDispatcher webhooks;
    private final MeterRegistry meters;

    public CallService(CallRepository repo,
                       CallRecordMapper mapper,
                       CallRecordPublisher publisher,
                       SchedulerClient scheduler,
                       WebhookDispatcher webhooks,
                       MeterRegistry meters) {
        this.repo = repo;
        this.mapper = mapper;
        this.publisher = publisher;
        this.scheduler = scheduler;
        this.webhooks = webhooks;
        this.meters = meters;
    }

    public Call placeVoice(Direction direction,
                            String callingParty, String calledParty, String imsi,
                            String mscNumber, String vlrAddress, int lac, int cellId,
                            int durationSeconds, boolean roaming, Integer serviceKeyOverride,
                            String callbackUrl) {
        UUID callId = UUID.randomUUID();
        int serviceKey = mapper.resolveServiceKey(CallKind.VOICE, direction, roaming, serviceKeyOverride);
        Instant now = Instant.now();
        Call call = new Call(callId, CallKind.VOICE, direction, CallStatus.PENDING,
                callingParty, calledParty, imsi, mscNumber, vlrAddress, lac, cellId,
                durationSeconds, serviceKey, roaming, callbackUrl, now, now, null);
        repo.save(call);
        Counter.builder("phonesim.calls.created")
                .tag("kind", "VOICE").tag("direction", direction.name())
                .register(meters).increment();

        try {
            call = transition(call, CallStatus.DIALING);
            publisher.publish(callId, mapper.toInitial(call));
            call = transition(call, CallStatus.RINGING);
            UUID timerId = UUID.randomUUID();
            scheduler.enqueueRelease(timerId, callId, durationSeconds * 1000L);
            repo.saveTimerId(callId, timerId);
            call = transition(call, CallStatus.ANSWERED);
        } catch (RuntimeException e) {
            call = fail(call, e.getMessage());
        }
        return call;
    }

    public Call sendSms(Direction direction,
                        String callingParty, String calledParty, String imsi,
                        String mscNumber, String vlrAddress, int lac, int cellId,
                        Integer serviceKeyOverride, String callbackUrl) {
        UUID callId = UUID.randomUUID();
        int serviceKey = mapper.resolveServiceKey(CallKind.SMS, direction, false, serviceKeyOverride);
        Instant now = Instant.now();
        Call sms = new Call(callId, CallKind.SMS, direction, CallStatus.PENDING,
                callingParty, calledParty, imsi, mscNumber, vlrAddress, lac, cellId,
                0, serviceKey, false, callbackUrl, now, now, null);
        repo.save(sms);
        Counter.builder("phonesim.sms.sent")
                .tag("direction", direction.name()).register(meters).increment();

        try {
            publisher.publish(callId, mapper.toInitial(sms));
            sms = transition(sms, CallStatus.ANSWERED);
            sms = transition(sms, CallStatus.RELEASED);
            webhooks.dispatch(callbackUrl, CallEvent.of("SMS_DELIVERED", sms));
        } catch (RuntimeException e) {
            sms = fail(sms, e.getMessage());
            webhooks.dispatch(callbackUrl, CallEvent.of("SMS_FAILED", sms));
        }
        return sms;
    }

    public void onTimerFire(UUID callId, String eventType) {
        if (!FireEvent.EVENT_RELEASE.equals(eventType)) {
            log.warn("Unknown FireEvent type {} for callId={} — ignoring", eventType, callId);
            return;
        }
        Optional<Call> maybe = repo.findById(callId);
        if (maybe.isEmpty()) {
            log.warn("Timer fired for unknown callId={} — ignoring", callId);
            return;
        }
        Call call = maybe.get();
        if (call.status().isTerminal()) {
            log.debug("Timer fired for already-terminal call {} (state={}) — idempotent skip",
                    callId, call.status());
            return;
        }
        try {
            publisher.publish(callId, mapper.toLastChunk(call));
            call = transition(call, CallStatus.RELEASED);
            repo.deleteTimerId(callId);
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

    public Call findOrThrow(UUID callId) {
        return repo.findById(callId).orElseThrow(() -> new CallNotFoundException(callId));
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
        Counter.builder("phonesim.calls.released")
                .tag("reason", "failed").register(meters).increment();
        return failed;
    }
}
