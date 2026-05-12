package com.azerconnect.phonesim.adapter.redis;

import com.azerconnect.phonesim.domain.Subscriber;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.Set;

@Repository
public class SubscriberRepository {

    private static final String INDEX_KEY = "subscribers:all";

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;

    public SubscriberRepository(StringRedisTemplate redis, ObjectMapper redisObjectMapper) {
        this.redis = redis;
        this.mapper = redisObjectMapper;
    }

    public boolean saveIfAbsent(Subscriber subscriber) {
        String json = serialize(subscriber);
        Boolean created = redis.opsForValue().setIfAbsent(key(subscriber.msisdn()), json);
        if (Boolean.FALSE.equals(created)) return false;
        redis.opsForSet().add(INDEX_KEY, subscriber.msisdn());
        return true;
    }

    public void save(Subscriber subscriber) {
        redis.opsForValue().set(key(subscriber.msisdn()), serialize(subscriber));
        redis.opsForSet().add(INDEX_KEY, subscriber.msisdn());
    }

    public Optional<Subscriber> findByMsisdn(String msisdn) {
        String json = redis.opsForValue().get(key(msisdn));
        return json == null ? Optional.empty() : Optional.of(deserialize(msisdn, json));
    }

    public List<Subscriber> findAll() {
        Set<String> msisdns = redis.opsForSet().members(INDEX_KEY);
        if (msisdns == null || msisdns.isEmpty()) return List.of();
        return msisdns.stream()
                .map(this::findByMsisdn)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();
    }

    public boolean delete(String msisdn) {
        Boolean removed = redis.delete(key(msisdn));
        redis.opsForSet().remove(INDEX_KEY, msisdn);
        return Boolean.TRUE.equals(removed);
    }

    private String serialize(Subscriber subscriber) {
        try {
            return mapper.writeValueAsString(subscriber);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize subscriber " + subscriber.msisdn(), e);
        }
    }

    private Subscriber deserialize(String msisdn, String json) {
        try {
            return mapper.readValue(json, Subscriber.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize subscriber " + msisdn, e);
        }
    }

    private static String key(String msisdn) {
        return "subscriber:" + msisdn;
    }
}
