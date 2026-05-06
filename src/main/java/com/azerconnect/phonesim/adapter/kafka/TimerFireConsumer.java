package com.azerconnect.phonesim.adapter.kafka;

import com.azerconnect.phonesim.service.CallService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class TimerFireConsumer {

    private static final Logger log = LoggerFactory.getLogger(TimerFireConsumer.class);

    private final CallService callService;

    public TimerFireConsumer(CallService callService) {
        this.callService = callService;
    }

    @KafkaListener(
            topics = "${phonesim.kafka.timer-topic}",
            containerFactory = "fireEventListenerFactory"
    )
    public void onFire(FireEvent event, Acknowledgment ack) {
        if (event == null || event.callId() == null) {
            log.warn("Received empty/invalid FireEvent — discarding");
            ack.acknowledge();
            return;
        }
        MDC.put("callId", event.callId().toString());
        try {
            callService.onTimerFire(event.callId(), event.eventType());
            ack.acknowledge();
        } catch (RuntimeException e) {
            log.error("Failed processing timer fire for callId={}: {}", event.callId(), e.getMessage(), e);
            // ack regardless to avoid poison-pill loop; transitional state failures are
            // surfaced via metrics + webhook FAILED event
            ack.acknowledge();
        } finally {
            MDC.remove("callId");
        }
    }
}
