package com.azerconnect.phonesim.domain;

public class DuplicateLocationIdException extends RuntimeException {
    public DuplicateLocationIdException(String locationId) {
        super("Location id already in use: " + locationId);
    }
}
