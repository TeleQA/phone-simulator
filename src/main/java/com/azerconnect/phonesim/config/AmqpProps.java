package com.azerconnect.phonesim.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "phonesim.amqp")
public record AmqpProps(
        String queue,
        String exchange,
        long confirmTimeoutMs
) {
    public AmqpProps {
        if (queue == null || queue.isBlank()) {
            throw new IllegalArgumentException("phonesim.amqp.queue must be set");
        }
        if (exchange == null) exchange = "";
        if (confirmTimeoutMs <= 0) confirmTimeoutMs = 5000;
    }
}
