package com.azerconnect.phonesim.service;

import com.azerconnect.phonesim.adapter.redis.SubscriberRepository;
import com.azerconnect.phonesim.domain.DuplicateMsisdnException;
import com.azerconnect.phonesim.domain.Location;
import com.azerconnect.phonesim.domain.Subscriber;
import com.azerconnect.phonesim.domain.SubscriberNotFoundException;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
public class SubscriberService {

    private final SubscriberRepository repo;
    private final LocationService locations;

    public SubscriberService(SubscriberRepository repo, LocationService locations) {
        this.repo = repo;
        this.locations = locations;
    }

    public Subscriber register(String msisdn, String imsi, String homeLocationId, String label) {
        locations.findOrThrow(homeLocationId);
        Instant now = Instant.now();
        Subscriber subscriber = new Subscriber(msisdn, imsi, homeLocationId, homeLocationId, label, now, now);
        if (!repo.saveIfAbsent(subscriber)) {
            throw new DuplicateMsisdnException(msisdn);
        }
        return subscriber;
    }

    public Subscriber findOrThrow(String msisdn) {
        return repo.findByMsisdn(msisdn).orElseThrow(() -> new SubscriberNotFoundException(msisdn));
    }

    public List<Subscriber> findAll() {
        return repo.findAll();
    }

    public Subscriber moveTo(String msisdn, String locationId) {
        Subscriber existing = findOrThrow(msisdn);
        locations.findOrThrow(locationId);
        Subscriber moved = existing.movedTo(locationId, Instant.now());
        repo.save(moved);
        return moved;
    }

    public Subscriber goHome(String msisdn) {
        Subscriber existing = findOrThrow(msisdn);
        return moveTo(msisdn, existing.homeLocationId());
    }

    public void delete(String msisdn) {
        if (!repo.delete(msisdn)) {
            throw new SubscriberNotFoundException(msisdn);
        }
    }

    public ResolvedSubscriber resolve(String msisdn) {
        Subscriber subscriber = findOrThrow(msisdn);
        Location current = locations.findOrThrow(subscriber.currentLocationId());
        return new ResolvedSubscriber(subscriber, current);
    }
}
