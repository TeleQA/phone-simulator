package com.azerconnect.phonesim.adapter.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

/**
 * Event phone-simulator publishes on the {@code phonesim.kafka.hangup-event-topic} topic when the
 * simulated subscriber ends the call (the local duration timer fires). It is the wire equivalent
 * of the user pressing "End call" on a real handset — the CAP simulator translates this into the
 * final ApplyChargingReport + DISCONNECT event toward the SCP. Phone-simulator does NOT emit any
 * billing chunks itself; chunking lives entirely on the CAP side.
 *
 * <p>Wire shape (Kafka key MUST be the {@code testId} so partitioning lines up with the
 * call-event topic):
 * <pre>
 * { "testId": "voice-mo-...", "reason": "USER_HANGUP",
 *   "hungUpAt": "2026-05-06T12:00:00Z", "schemaVersion": 1 }
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record HangupEvent(
        String testId,
        String reason,
        Instant hungUpAt,
        int schemaVersion
) {
    /** The simulated subscriber pressed "End call" after the configured duration elapsed. */
    public static final String REASON_USER_HANGUP = "USER_HANGUP";

    public static HangupEvent userHangup(String testId) {
        return new HangupEvent(testId, REASON_USER_HANGUP, Instant.now(), 1);
    }
}
