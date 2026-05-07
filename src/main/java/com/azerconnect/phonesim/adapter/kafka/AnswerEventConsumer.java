package com.azerconnect.phonesim.adapter.kafka;

import com.azerconnect.phonesim.service.CallService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class AnswerEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(AnswerEventConsumer.class);

    private final CallService callService;

    public AnswerEventConsumer(CallService callService) {
        this.callService = callService;
    }

    @KafkaListener(
            topics = "${phonesim.kafka.answer-event-topic}",
            containerFactory = "answerEventListenerFactory"
    )
    public void onAnswer(AnswerEvent event, Acknowledgment ack) {
        if (event == null || event.testId() == null || event.testId().isBlank()) {
            log.warn("Received empty/invalid AnswerEvent — discarding");
            ack.acknowledge();
            return;
        }
        MDC.put("testId", event.testId());
        try {
            callService.onAnswer(event.testId(), event.answerType());
            ack.acknowledge();
        } catch (RuntimeException e) {
            log.error("Failed processing answer event for testId={}: {}",
                    event.testId(), e.getMessage(), e);
            // Ack to avoid poison-pill loop. Re-driving an answer for a stale call
            // is handled idempotently downstream.
            ack.acknowledge();
        } finally {
            MDC.remove("testId");
        }
    }
}
