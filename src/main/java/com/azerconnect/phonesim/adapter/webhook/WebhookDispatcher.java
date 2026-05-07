package com.azerconnect.phonesim.adapter.webhook;

import com.azerconnect.phonesim.adapter.redis.WebhookRepository;
import com.azerconnect.phonesim.config.WebhookProps;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@Component
public class WebhookDispatcher {

    private static final Logger log = LoggerFactory.getLogger(WebhookDispatcher.class);

    private final RestClient client;
    private final WebhookRepository webhookRepo;
    private final WebhookProps props;
    private final Executor virtualThreads;
    private final Semaphore inflight;
    private final Timer deliverTimer;
    private final Counter failuresCounter;

    public WebhookDispatcher(RestClient phoneSimWebhookRestClient,
                              WebhookRepository webhookRepo,
                              WebhookProps props,
                              MeterRegistry meters) {
        this.client = phoneSimWebhookRestClient;
        this.webhookRepo = webhookRepo;
        this.props = props;
        this.virtualThreads = Executors.newVirtualThreadPerTaskExecutor();
        this.inflight = new Semaphore(props.maxConcurrent());
        this.deliverTimer = Timer.builder("phonesim.webhook.deliver")
                .description("Time spent delivering a webhook event")
                .register(meters);
        this.failuresCounter = Counter.builder("phonesim.webhook.failures")
                .description("Webhook deliveries that exhausted retries")
                .register(meters);
    }

    public void dispatch(String perCallUrl, CallEvent event) {
        Optional<String> targetUrl = chooseUrl(perCallUrl);
        if (targetUrl.isEmpty()) {
            log.debug("No webhook URL configured for callId={} eventType={} — skipping",
                    event.testId(), event.eventType());
            return;
        }
        virtualThreads.execute(() -> deliver(targetUrl.get(), event));
    }

    private Optional<String> chooseUrl(String perCallUrl) {
        if (perCallUrl != null && !perCallUrl.isBlank()) return Optional.of(perCallUrl);
        if (props.fallbackUrl() != null && !props.fallbackUrl().isBlank()) {
            return Optional.of(props.fallbackUrl());
        }
        return webhookRepo.currentFallbackUrl();
    }

    private void deliver(String url, CallEvent event) {
        boolean acquired = false;
        try {
            acquired = inflight.tryAcquire(2, TimeUnit.SECONDS);
            if (!acquired) {
                failuresCounter.increment();
                log.error("webhook.failed reason=semaphore-timeout callId={} eventType={} url={}",
                        event.testId(), event.eventType(), url);
                return;
            }
            doDeliverWithRetry(url, event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (acquired) inflight.release();
        }
    }

    private void doDeliverWithRetry(String url, CallEvent event) {
        long backoff = props.initialBackoffMs();
        int attempt = 0;
        Timer.Sample sample = Timer.start();
        while (true) {
            attempt++;
            try {
                client.post()
                        .uri(url)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(event)
                        .retrieve()
                        .toBodilessEntity();
                sample.stop(deliverTimer);
                return;
            } catch (RestClientResponseException e) {
                int status = e.getStatusCode().value();
                if (status >= 400 && status < 500 && status != 429) {
                    failuresCounter.increment();
                    log.error("webhook.failed reason=client-{} callId={} eventType={} url={}",
                            status, event.testId(), event.eventType(), url);
                    sample.stop(deliverTimer);
                    return;
                }
                if (attempt > props.maxRetries()) {
                    failuresCounter.increment();
                    log.error("webhook.failed reason=retries-exhausted attempts={} callId={} eventType={} url={}",
                            attempt, event.testId(), event.eventType(), url);
                    sample.stop(deliverTimer);
                    return;
                }
            } catch (Exception e) {
                if (attempt > props.maxRetries()) {
                    failuresCounter.increment();
                    log.error("webhook.failed reason=io attempts={} callId={} eventType={} url={} err={}",
                            attempt, event.testId(), event.eventType(), url, e.getMessage());
                    sample.stop(deliverTimer);
                    return;
                }
            }
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
            backoff = Math.min(backoff * 2, props.maxBackoffMs());
        }
    }

    @Configuration
    static class WebhookRestClientConfig {
        @Bean
        RestClient phoneSimWebhookRestClient(WebhookProps props) {
            HttpClient http = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(2))
                    .executor(Executors.newVirtualThreadPerTaskExecutor())
                    .build();
            JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(http);
            factory.setReadTimeout(Duration.ofMillis(props.requestTimeoutMs()));
            return RestClient.builder().requestFactory(factory).build();
        }
    }
}
