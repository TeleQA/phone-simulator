package com.azerconnect.phonesim.service;

import com.azerconnect.phonesim.config.SeedProps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class LocationSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(LocationSeeder.class);

    private final SeedProps seedProps;
    private final LocationService locations;

    public LocationSeeder(SeedProps seedProps, LocationService locations) {
        this.seedProps = seedProps;
        this.locations = locations;
    }

    @Override
    public void run(ApplicationArguments args) {
        List<SeedProps.LocationSeed> seeds = seedProps.locations();
        if (seeds == null || seeds.isEmpty()) {
            log.info("No seed locations configured under phonesim.seed.locations — skipping");
            return;
        }
        for (SeedProps.LocationSeed s : seeds) {
            locations.upsert(s.id(), s.lac(), s.cellId(), s.vlrAddress(), s.mscNumber(),
                    s.mcc(), s.mnc(), s.roaming());
        }
        log.info("Seeded {} locations into registry", seeds.size());
    }
}
