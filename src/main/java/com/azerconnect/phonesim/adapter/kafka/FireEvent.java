package com.azerconnect.phonesim.adapter.kafka;

public record FireEvent(
        String testId,
        String eventType,
        int schemaVersion
) {
    /** Duration timer fired — release the call (publish LAST_CHUNK to CAP). */
    public static final String EVENT_RELEASE = "RELEASE";
    /** No-answer guard timer fired before any AnswerEvent arrived — fail the call. */
    public static final String EVENT_NO_ANSWER = "NO_ANSWER";
}
