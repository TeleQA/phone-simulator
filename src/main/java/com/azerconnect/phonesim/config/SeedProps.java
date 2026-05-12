package com.azerconnect.phonesim.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "phonesim.seed")
public record SeedProps(List<LocationSeed> locations) {

    public record LocationSeed(
            String id,
            int lac,
            int cellId,
            String vlrAddress,
            String mscNumber,
            String mcc,
            String mnc,
            boolean roaming
    ) {
    }
}
