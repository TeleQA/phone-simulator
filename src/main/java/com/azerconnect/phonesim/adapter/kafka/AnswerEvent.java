package com.azerconnect.phonesim.adapter.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

/**
 * Event published by the GSM CAP simulator on the {@code phonesim.kafka.answer-event-topic} topic
 * when an answer event (O_ANSWER or T_ANSWER) is acknowledged toward the SCP. The phone simulator
 * uses this signal to transition the call to {@code ANSWERED} and start the call-duration timer.
 *
 * <p>Wire contract (the message key on Kafka must be the {@code testId} so partitioning lines up
 * with the call-event topic):
 * <pre>
 * { "testId": "voice-mo-...", "answerType": "O_ANSWER" | "T_ANSWER",
 *   "answeredAt": "2026-05-06T12:00:00Z", "schemaVersion": 1 }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AnswerEvent(
        String testId,
        String answerType,
        Instant answeredAt,
        int schemaVersion
) {
    public static final String TYPE_O_ANSWER = "O_ANSWER";
    public static final String TYPE_T_ANSWER = "T_ANSWER";
}
