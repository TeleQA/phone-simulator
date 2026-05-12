package com.azerconnect.phonesim.api.dto;

import com.azerconnect.phonesim.domain.Subscriber;

import java.time.Instant;

public record SubscriberResponse(
        String msisdn,
        String imsi,
        String homeLocationId,
        String currentLocationId,
        String label,
        Instant createdAt,
        Instant updatedAt
) {
    public static SubscriberResponse from(Subscriber s) {
        return new SubscriberResponse(
                s.msisdn(), s.imsi(),
                s.homeLocationId(), s.currentLocationId(),
                s.label(), s.createdAt(), s.updatedAt()
        );
    }
}
