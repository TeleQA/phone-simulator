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

    /** Atomically creates the call key — returns false if testId already exists. */
    public boolean saveIfAbsent(Call call) {
        try {
            String json = mapper.writeValueAsString(call);
            String key = key(call.testId());
            Duration ttl = props.callTtl().plusSeconds(call.durationSeconds());
            Boolean created = redis.opsForValue().setIfAbsent(key, json, ttl);
            if (Boolean.FALSE.equals(created)) return false;
            indexCall(call);
            return true;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize call " + call.testId(), e);
        }
    }

    public void save(Call call) {
        try {
            String json = mapper.writeValueAsString(call);
            String key = key(call.testId());
            Duration ttl = props.callTtl().plusSeconds(call.durationSeconds());
            redis.opsForValue().set(key, json, ttl);
            indexCall(call);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize call " + call.testId(), e);
        }
    }

    public Optional<Call> findById(String testId) {
        String json = redis.opsForValue().get(key(testId));
        if (json == null) return Optional.empty();
        try {
            return Optional.of(mapper.readValue(json, Call.class));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize call " + testId, e);
        }
    }

    public List<Call> listByStatus(CallStatus status) {
        Set<String> ids = redis.opsForSet().members("calls:by-state:" + status);
        if (ids == null || ids.isEmpty()) return List.of();
        return ids.stream()
                .map(this::findById)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();
    }

    public long countActive() {
        Long count = redis.opsForZSet().zCard("calls:active");
        return count == null ? 0L : count;
    }

    /* Duration timer (fires when the configured callDuration has elapsed). */

    public void saveDurationTimerId(String testId, UUID timerId) {
        redis.opsForValue().set(durationTimerKey(testId), timerId.toString(), props.callTtl());
    }

    public Optional<UUID> findDurationTimerId(String testId) {
        String value = redis.opsForValue().get(durationTimerKey(testId));
        return value == null ? Optional.empty() : Optional.of(UUID.fromString(value));
    }

    public void deleteDurationTimerId(String testId) {
        redis.delete(durationTimerKey(testId));
    }

    /* No-answer guard timer (fires only if no AnswerEvent arrives in time). */

    public void saveNoAnswerTimerId(String testId, UUID timerId) {
        redis.opsForValue().set(noAnswerTimerKey(testId), timerId.toString(), props.callTtl());
    }

    public Optional<UUID> findNoAnswerTimerId(String testId) {
        String value = redis.opsForValue().get(noAnswerTimerKey(testId));
        return value == null ? Optional.empty() : Optional.of(UUID.fromString(value));
    }

    public void deleteNoAnswerTimerId(String testId) {
        redis.delete(noAnswerTimerKey(testId));
    }

    private static String durationTimerKey(String testId) {
        return "timer:duration:" + testId;
    }

    private static String noAnswerTimerKey(String testId) {
        return "timer:no-answer:" + testId;
    }

    private void indexCall(Call call) {
        redis.opsForZSet().add("calls:active", call.testId(),
                call.lastTransitionAt() != null ? call.lastTransitionAt().toEpochMilli() : 0);
        for (CallStatus s : CallStatus.values()) {
            if (s != call.status()) {
                redis.opsForSet().remove("calls:by-state:" + s, call.testId());
            }
        }
        redis.opsForSet().add("calls:by-state:" + call.status(), call.testId());
        if (call.status().isTerminal()) {
            redis.opsForZSet().remove("calls:active", call.testId());
        }
    }

    private static String key(String testId) {
        return "call:" + testId;
    }
}
