package com.azerconnect.phonesim.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "phonesim.kafka")
public record KafkaProps(
        String timerTopic,
        int listenerConcurrency
) {
    public KafkaProps {
        if (timerTopic == null || timerTopic.isBlank()) {
            throw new IllegalArgumentException("phonesim.kafka.timer-topic must be set");
        }
        if (listenerConcurrency <= 0) listenerConcurrency = 4;
    }
}
