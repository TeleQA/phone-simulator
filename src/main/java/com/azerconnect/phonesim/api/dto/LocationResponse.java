package com.azerconnect.phonesim.api.dto;

import com.azerconnect.phonesim.domain.Location;

import java.time.Instant;

public record LocationResponse(
        String id,
        int lac,
        int cellId,
        String vlrAddress,
        String mscNumber,
        String mcc,
        String mnc,
        boolean roaming,
        Instant createdAt,
        Instant updatedAt
) {
    public static LocationResponse from(Location location) {
        return new LocationResponse(
                location.id(), location.lac(), location.cellId(),
                location.vlrAddress(), location.mscNumber(),
                location.mcc(), location.mnc(), location.roaming(),
                location.createdAt(), location.updatedAt()
        );
    }
}
