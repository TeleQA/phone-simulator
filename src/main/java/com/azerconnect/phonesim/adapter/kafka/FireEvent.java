package com.azerconnect.phonesim.adapter.kafka;

public record FireEvent(
        String testId,
        String eventType,
        int schemaVersion
) {
    public static final String EVENT_RELEASE = "RELEASE";
}
