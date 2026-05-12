package com.azerconnect.phonesim.service;

import com.azerconnect.phonesim.domain.Location;
import com.azerconnect.phonesim.domain.Subscriber;

public record ResolvedSubscriber(Subscriber subscriber, Location currentLocation) {

    public String msisdn() {
        return subscriber.msisdn();
    }

    public String imsi() {
        return subscriber.imsi();
    }

    public String mscNumber() {
        return currentLocation.mscNumber();
    }

    public String vlrAddress() {
        return currentLocation.vlrAddress();
    }

    public int lac() {
        return currentLocation.lac();
    }

    public int cellId() {
        return currentLocation.cellId();
    }

    public boolean roaming() {
        return currentLocation.roaming();
    }
}
