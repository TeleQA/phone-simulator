package com.azerconnect.phonesim.domain;

public class DuplicateTestIdException extends RuntimeException {
    public DuplicateTestIdException(String testId) {
        super("testId already in use: " + testId);
    }
}
