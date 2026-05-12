package com.azerconnect.phonesim.domain;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record Subscriber(
        String msisdn,
        String imsi,
        String homeLocationId,
        String currentLocationId,
        String label,
        Instant createdAt,
        Instant updatedAt
) {
    public Subscriber movedTo(String newLocationId, Instant when) {
        return new Subscriber(msisdn, imsi, homeLocationId, newLocationId, label, createdAt, when);
    }
}
