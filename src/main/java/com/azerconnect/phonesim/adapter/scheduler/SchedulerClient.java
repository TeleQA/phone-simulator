package com.azerconnect.phonesim.adapter.scheduler;

import com.azerconnect.phonesim.adapter.kafka.FireEvent;
import com.azerconnect.phonesim.config.KafkaProps;
import com.azerconnect.phonesim.config.SchedulerProps;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

@Component
public class SchedulerClient {

    private static final Logger log = LoggerFactory.getLogger(SchedulerClient.class);

    private final RestClient client;
    private final ObjectMapper mapper;
    private final SchedulerProps schedulerProps;
    private final KafkaProps kafkaProps;
    private final Timer enqueueTimer;

    public SchedulerClient(RestClient phoneSimSchedulerRestClient,
                            ObjectMapper objectMapper,
                            SchedulerProps schedulerProps,
                            KafkaProps kafkaProps,
                            MeterRegistry meters) {
        this.client = phoneSimSchedulerRestClient;
        this.mapper = objectMapper;
        this.schedulerProps = schedulerProps;
        this.kafkaProps = kafkaProps;
        this.enqueueTimer = Timer.builder("phonesim.scheduler.enqueue")
                .description("Time spent enqueueing a timer in the scheduler")
                .register(meters);
    }

    public TimerResponse enqueueRelease(UUID timerId, String testId, long delayMs) {
        return enqueue(timerId, testId, delayMs, FireEvent.EVENT_RELEASE);
    }

    public TimerResponse enqueueNoAnswerTimeout(UUID timerId, String testId, long delayMs) {
        return enqueue(timerId, testId, delayMs, FireEvent.EVENT_NO_ANSWER);
    }

    private TimerResponse enqueue(UUID timerId, String testId, long delayMs, String eventType) {
        Timer.Sample sample = Timer.start();
        try {
            FireEvent fire = new FireEvent(testId, eventType, 1);
            String payloadB64 = Base64.getEncoder().encodeToString(toJsonBytes(fire));

            Map<String, Object> body = Map.of(
                    "id", timerId.toString(),
                    "delay_ms", delayMs,
                    "kafka_topic", kafkaProps.timerTopic(),
                    "partition_key", testId,
                    "payload", payloadB64
            );

            return client.post()
                    .uri("/v1/timers")
                    .body(body)
                    .retrieve()
                    .body(TimerResponse.class);
        } catch (RestClientException e) {
            log.error("Scheduler enqueue failed testId={} eventType={} delayMs={}: {}",
                    testId, eventType, delayMs, e.getMessage());
            throw e;
        } finally {
            sample.stop(enqueueTimer);
        }
    }

    public void cancel(UUID timerId) {
        try {
            client.delete()
                    .uri("/v1/timers/{id}", timerId)
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientResponseException e) {
            HttpStatusCode status = e.getStatusCode();
            if (status.value() == 404) {
                log.debug("Scheduler timer {} already gone (404)", timerId);
                return;
            }
            throw e;
        }
    }

    private byte[] toJsonBytes(FireEvent fire) {
        try {
            return mapper.writeValueAsBytes(fire);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize FireEvent", e);
        }
    }

    @org.springframework.context.annotation.Configuration
    static class RestClientBeans {
        @Bean
        RestClient phoneSimSchedulerRestClient(SchedulerProps props) {
            HttpClient http = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .connectTimeout(Duration.ofMillis(props.connectTimeoutMs()))
                    .executor(Executors.newVirtualThreadPerTaskExecutor())
                    .build();
            JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(http);
            factory.setReadTimeout(Duration.ofMillis(props.readTimeoutMs()));
            return RestClient.builder()
                    .requestFactory(factory)
                    .baseUrl(props.baseUrl())
                    .build();
        }
    }
}
