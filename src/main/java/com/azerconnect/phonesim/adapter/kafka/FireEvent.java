package com.azerconnect.phonesim.adapter.kafka;

import java.util.UUID;

public record FireEvent(
        UUID callId,
        String eventType,
        int schemaVersion
) {
    public static final String EVENT_RELEASE = "RELEASE";
}
