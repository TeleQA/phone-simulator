package com.azerconnect.phonesim.adapter.redis;

import com.azerconnect.phonesim.config.RedisProps;
import com.azerconnect.phonesim.domain.Call;
import com.azerconnect.phonesim.domain.CallStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Repository
public class CallRepository {

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final RedisProps props;

    public CallRepository(StringRedisTemplate redis, ObjectMapper redisObjectMapper, RedisProps props) {
        this.redis = redis;
        this.mapper = redisObjectMapper;
        this.props = props;
    }

    public void save(Call call) {
        try {
            String json = mapper.writeValueAsString(call);
            String key = key(call.callId());
            Duration ttl = props.callTtl().plusSeconds(call.durationSeconds());
            redis.opsForValue().set(key, json, ttl);
            redis.opsForZSet().add("calls:active", call.callId().toString(),
                    call.lastTransitionAt() != null ? call.lastTransitionAt().toEpochMilli() : 0);
            // remove from any prior state index, add to current
            for (CallStatus s : CallStatus.values()) {
                if (s != call.status()) {
                    redis.opsForSet().remove("calls:by-state:" + s, call.callId().toString());
                }
            }
            redis.opsForSet().add("calls:by-state:" + call.status(), call.callId().toString());
            if (call.status().isTerminal()) {
                redis.opsForZSet().remove("calls:active", call.callId().toString());
            }
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize call " + call.callId(), e);
        }
    }

    public Optional<Call> findById(UUID callId) {
        String json = redis.opsForValue().get(key(callId));
        if (json == null) return Optional.empty();
        try {
            return Optional.of(mapper.readValue(json, Call.class));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize call " + callId, e);
        }
    }

    public List<Call> listByStatus(CallStatus status) {
        Set<String> ids = redis.opsForSet().members("calls:by-state:" + status);
        if (ids == null || ids.isEmpty()) return List.of();
        return ids.stream()
                .map(id -> findById(UUID.fromString(id)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();
    }

    public long countActive() {
        Long count = redis.opsForZSet().zCard("calls:active");
        return count == null ? 0L : count;
    }

    public void saveTimerId(UUID callId, UUID timerId) {
        redis.opsForValue().set("timer:" + callId, timerId.toString(),
                props.callTtl());
    }

    public Optional<UUID> findTimerId(UUID callId) {
        String value = redis.opsForValue().get("timer:" + callId);
        return value == null ? Optional.empty() : Optional.of(UUID.fromString(value));
    }

    public void deleteTimerId(UUID callId) {
        redis.delete("timer:" + callId);
    }

    private static String key(UUID callId) {
        return "call:" + callId;
    }
}
