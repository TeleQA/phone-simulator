package com.azerconnect.phonesim.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "phonesim.kafka")
public record KafkaProps(
        String timerTopic,
        String callEventTopic,
        int listenerConcurrency
) {
    public KafkaProps {
        if (timerTopic == null || timerTopic.isBlank()) {
            throw new IllegalArgumentException("phonesim.kafka.timer-topic must be set");
        }
        if (callEventTopic == null || callEventTopic.isBlank()) {
            throw new IllegalArgumentException("phonesim.kafka.call-event-topic must be set");
        }
        if (listenerConcurrency <= 0) listenerConcurrency = 4;
    }
}
