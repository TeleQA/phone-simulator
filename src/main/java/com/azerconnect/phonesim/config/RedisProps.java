package com.azerconnect.phonesim.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "phonesim.redis")
public record RedisProps(
        Duration callTtl
) {
    public RedisProps {
        if (callTtl == null || callTtl.isZero() || callTtl.isNegative()) {
            callTtl = Duration.ofHours(2);
        }
    }
}
