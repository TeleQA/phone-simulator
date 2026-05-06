package com.azerconnect.phonesim.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "phonesim.webhook")
public record WebhookProps(
        String fallbackUrl,
        int maxRetries,
        long initialBackoffMs,
        long maxBackoffMs,
        int maxConcurrent,
        int requestTimeoutMs
) {
    public WebhookProps {
        if (maxRetries < 0) maxRetries = 4;
        if (initialBackoffMs <= 0) initialBackoffMs = 500;
        if (maxBackoffMs <= 0) maxBackoffMs = 8000;
        if (maxConcurrent <= 0) maxConcurrent = 256;
        if (requestTimeoutMs <= 0) requestTimeoutMs = 3000;
    }
}
