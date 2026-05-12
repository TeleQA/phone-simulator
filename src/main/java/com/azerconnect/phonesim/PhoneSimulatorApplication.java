package com.azerconnect.phonesim;

import com.azerconnect.phonesim.config.CallProps;
import com.azerconnect.phonesim.config.DefaultsProps;
import com.azerconnect.phonesim.config.KafkaProps;
import com.azerconnect.phonesim.config.RedisProps;
import com.azerconnect.phonesim.config.SchedulerProps;
import com.azerconnect.phonesim.config.SeedProps;
import com.azerconnect.phonesim.config.WebhookProps;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.retry.annotation.EnableRetry;

@SpringBootApplication
@EnableRetry
@EnableConfigurationProperties({
        SchedulerProps.class,
        KafkaProps.class,
        DefaultsProps.class,
        RedisProps.class,
        WebhookProps.class,
        CallProps.class,
        SeedProps.class
})
public class PhoneSimulatorApplication {
    public static void main(String[] args) {
        SpringApplication.run(PhoneSimulatorApplication.class, args);
    }
}
