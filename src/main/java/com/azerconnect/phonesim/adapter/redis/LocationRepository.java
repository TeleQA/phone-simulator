package com.azerconnect.phonesim.adapter.redis;

import com.azerconnect.phonesim.domain.Location;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.Set;

@Repository
public class LocationRepository {

    private static final String INDEX_KEY = "locations:all";

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;

    public LocationRepository(StringRedisTemplate redis, ObjectMapper redisObjectMapper) {
        this.redis = redis;
        this.mapper = redisObjectMapper;
    }

    public boolean saveIfAbsent(Location location) {
        String json = serialize(location);
        Boolean created = redis.opsForValue().setIfAbsent(key(location.id()), json);
        if (Boolean.FALSE.equals(created)) return false;
        redis.opsForSet().add(INDEX_KEY, location.id());
        return true;
    }

    public void save(Location location) {
        redis.opsForValue().set(key(location.id()), serialize(location));
        redis.opsForSet().add(INDEX_KEY, location.id());
    }

    public Optional<Location> findById(String id) {
        String json = redis.opsForValue().get(key(id));
        return json == null ? Optional.empty() : Optional.of(deserialize(id, json));
    }

    public List<Location> findAll() {
        Set<String> ids = redis.opsForSet().members(INDEX_KEY);
        if (ids == null || ids.isEmpty()) return List.of();
        return ids.stream()
                .map(this::findById)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();
    }

    public boolean delete(String id) {
        Boolean removed = redis.delete(key(id));
        redis.opsForSet().remove(INDEX_KEY, id);
        return Boolean.TRUE.equals(removed);
    }

    private String serialize(Location location) {
        try {
            return mapper.writeValueAsString(location);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize location " + location.id(), e);
        }
    }

    private Location deserialize(String id, String json) {
        try {
            return mapper.readValue(json, Location.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize location " + id, e);
        }
    }

    private static String key(String id) {
        return "location:" + id;
    }
}
