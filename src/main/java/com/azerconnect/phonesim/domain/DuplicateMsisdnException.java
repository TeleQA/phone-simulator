package com.azerconnect.phonesim.domain;

public class DuplicateMsisdnException extends RuntimeException {
    public DuplicateMsisdnException(String msisdn) {
        super("Subscriber already registered: " + msisdn);
    }
}
