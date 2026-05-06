package com.azerconnect.phonesim.config;

import com.azerconnect.phonesim.adapter.redis.CallRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.boot.context.event.ApplicationReadyEvent;

@Configuration
public class MetricsConfig {

    private final CallRepository callRepository;
    private final MeterRegistry meters;

    public MetricsConfig(CallRepository callRepository, MeterRegistry meters) {
        this.callRepository = callRepository;
        this.meters = meters;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void bindGauges() {
        meters.gauge("phonesim.calls.active", callRepository, CallRepository::countActive);
    }
}
