package com.azerconnect.phonesim.domain;

public enum CallStatus {
    PENDING,
    DIALING,
    RINGING,
    ANSWERED,
    RELEASED,
    FAILED;

    public boolean isTerminal() {
        return this == RELEASED || this == FAILED;
    }
}
