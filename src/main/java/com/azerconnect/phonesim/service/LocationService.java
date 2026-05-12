package com.azerconnect.phonesim.service;

import com.azerconnect.phonesim.adapter.redis.LocationRepository;
import com.azerconnect.phonesim.domain.DuplicateLocationIdException;
import com.azerconnect.phonesim.domain.Location;
import com.azerconnect.phonesim.domain.LocationNotFoundException;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
public class LocationService {

    private final LocationRepository repo;

    public LocationService(LocationRepository repo) {
        this.repo = repo;
    }

    public Location create(String id, int lac, int cellId, String vlrAddress, String mscNumber,
                           String mcc, String mnc, boolean roaming) {
        Instant now = Instant.now();
        Location location = new Location(id, lac, cellId, vlrAddress, mscNumber, mcc, mnc, roaming, now, now);
        if (!repo.saveIfAbsent(location)) {
            throw new DuplicateLocationIdException(id);
        }
        return location;
    }

    public Location upsert(String id, int lac, int cellId, String vlrAddress, String mscNumber,
                           String mcc, String mnc, boolean roaming) {
        Instant now = Instant.now();
        Instant createdAt = repo.findById(id).map(Location::createdAt).orElse(now);
        Location location = new Location(id, lac, cellId, vlrAddress, mscNumber, mcc, mnc, roaming, createdAt, now);
        repo.save(location);
        return location;
    }

    public Location findOrThrow(String id) {
        return repo.findById(id).orElseThrow(() -> new LocationNotFoundException(id));
    }

    public List<Location> findAll() {
        return repo.findAll();
    }

    public void delete(String id) {
        if (!repo.delete(id)) {
            throw new LocationNotFoundException(id);
        }
    }
}
