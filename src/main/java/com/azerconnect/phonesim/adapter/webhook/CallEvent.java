package com.azerconnect.phonesim.adapter.webhook;

import com.azerconnect.phonesim.domain.Call;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CallEvent(
        UUID eventId,
        String testId,
        String eventType,
        Instant occurredAt,
        String state,
        Call data
) {
    public static CallEvent of(String eventType, Call call) {
        return new CallEvent(
                UUID.randomUUID(),
                call.testId(),
                eventType,
                Instant.now(),
                call.status().name(),
                call
        );
    }
}
