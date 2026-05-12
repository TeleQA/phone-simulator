package com.azerconnect.phonesim.domain;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record Location(
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
}
