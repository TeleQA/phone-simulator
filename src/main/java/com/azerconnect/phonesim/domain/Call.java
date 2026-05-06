package com.azerconnect.phonesim.domain;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record Call(
        String testId,
        CallKind kind,
        Direction direction,
        CallStatus status,
        String callingParty,
        String calledParty,
        String imsi,
        String mscNumber,
        String vlrAddress,
        int lac,
        int cellId,
        int durationSeconds,
        int serviceKey,
        boolean roaming,
        String callbackUrl,
        Instant createdAt,
        Instant lastTransitionAt,
        String failureReason
) {
    public Call withStatus(CallStatus next, Instant when) {
        return new Call(
                testId, kind, direction, next,
                callingParty, calledParty, imsi,
                mscNumber, vlrAddress, lac, cellId,
                durationSeconds, serviceKey, roaming, callbackUrl,
                createdAt, when, failureReason
        );
    }

    public Call withFailure(String reason, Instant when) {
        return new Call(
                testId, kind, direction, CallStatus.FAILED,
                callingParty, calledParty, imsi,
                mscNumber, vlrAddress, lac, cellId,
                durationSeconds, serviceKey, roaming, callbackUrl,
                createdAt, when, reason
        );
    }
}
