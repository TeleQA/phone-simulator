package com.azerconnect.phonesim.adapter.redis;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public class WebhookRepository {

    private static final String FALLBACK_KEY = "webhook:fallback";

    private final StringRedisTemplate redis;

    public WebhookRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public UUID register(String url) {
        UUID id = UUID.randomUUID();
        redis.opsForHash().put("webhook:" + id, "url", url);
        redis.opsForValue().set(FALLBACK_KEY, id.toString());
        return id;
    }

    public void unregister(UUID id) {
        redis.delete("webhook:" + id);
        String current = redis.opsForValue().get(FALLBACK_KEY);
        if (current != null && current.equals(id.toString())) {
            redis.delete(FALLBACK_KEY);
        }
    }

    public Optional<String> currentFallbackUrl() {
        String id = redis.opsForValue().get(FALLBACK_KEY);
        if (id == null) return Optional.empty();
        Object url = redis.opsForHash().get("webhook:" + id, "url");
        return url == null ? Optional.empty() : Optional.of(url.toString());
    }
}
