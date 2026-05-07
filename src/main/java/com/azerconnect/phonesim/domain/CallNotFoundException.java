package com.azerconnect.phonesim.domain;

public class CallNotFoundException extends RuntimeException {
    public CallNotFoundException(String testId) {
        super("Call not found: " + testId);
    }
}
