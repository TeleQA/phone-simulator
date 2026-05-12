package com.azerconnect.phonesim.domain;

public class LocationNotFoundException extends RuntimeException {
    public LocationNotFoundException(String locationId) {
        super("Location not found: " + locationId);
    }
}
