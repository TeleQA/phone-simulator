package com.azerconnect.phonesim.adapter.scheduler;

import com.azerconnect.phonesim.config.SchedulerProps;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.net.http.HttpClient;
import java.time.Duration;

@Component
public class SchedulerHealthIndicator implements HealthIndicator {

    private final RestClient client;

    public SchedulerHealthIndicator(SchedulerProps props) {
        HttpClient http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(props.connectTimeoutMs()))
                .build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(Duration.ofMillis(props.readTimeoutMs()));
        this.client = RestClient.builder()
                .requestFactory(factory)
                .baseUrl(props.baseUrl())
                .build();
    }

    @Override
    public Health health() {
        try {
            client.get().uri("/healthz").retrieve().toBodilessEntity();
            return Health.up().build();
        } catch (RestClientResponseException e) {
            // Any HTTP response — even non-2xx — proves connectivity
            return Health.up().withDetail("status", e.getStatusCode().value()).build();
        } catch (Exception e) {
            return Health.down().withException(e).build();
        }
    }
}
