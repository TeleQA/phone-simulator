package com.azerconnect.phonesim.api.dto;

import com.azerconnect.phonesim.domain.Call;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record CallAcceptedResponse(
        UUID callId,
        String state,
        Instant acceptedAt,
        Map<String, String> links
) {
    public static CallAcceptedResponse from(Call call, String selfUri) {
        return new CallAcceptedResponse(
                call.callId(),
                call.status().name(),
                call.createdAt(),
                Map.of("self", selfUri)
        );
    }
}
