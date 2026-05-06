package com.azerconnect.phonesim.api.dto;

import com.azerconnect.phonesim.domain.Call;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CallSnapshotResponse(
        UUID callId,
        String kind,
        String direction,
        String state,
        String callingParty,
        String calledParty,
        String imsi,
        int serviceKey,
        int durationSeconds,
        Instant createdAt,
        Instant lastTransitionAt,
        String failureReason
) {
    public static CallSnapshotResponse from(Call call) {
        return new CallSnapshotResponse(
                call.callId(),
                call.kind().name(),
                call.direction().name(),
                call.status().name(),
                call.callingParty(),
                call.calledParty(),
                call.imsi(),
                call.serviceKey(),
                call.durationSeconds(),
                call.createdAt(),
                call.lastTransitionAt(),
                call.failureReason()
        );
    }
}
