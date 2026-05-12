package com.azerconnect.phonesim.domain;

public class SubscriberNotFoundException extends RuntimeException {
    public SubscriberNotFoundException(String msisdn) {
        super("Subscriber not found: " + msisdn);
    }
}
