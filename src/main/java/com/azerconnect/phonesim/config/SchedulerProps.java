package com.azerconnect.phonesim.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "phonesim.scheduler")
public record SchedulerProps(
        String baseUrl,
        int connectTimeoutMs,
        int readTimeoutMs
) {
    public SchedulerProps {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException("phonesim.scheduler.base-url must be set");
        }
        if (connectTimeoutMs <= 0) connectTimeoutMs = 1000;
        if (readTimeoutMs <= 0) readTimeoutMs = 3000;
    }
}
