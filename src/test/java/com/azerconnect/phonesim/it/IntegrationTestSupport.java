package com.azerconnect.phonesim.it;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

public final class IntegrationTestSupport {

    public static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("harbor.azerconnect.az/infra/redis:7-alpine"))
                    .withExposedPorts(6379);

    public static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("harbor.azerconnect.az/infra/apache/kafka:4.1.0")
                    .asCompatibleSubstituteFor("apache/kafka"));

    public static final WireMockServer WIREMOCK = new WireMockServer(wireMockConfig().dynamicPort());

    static {
        REDIS.start();
        KAFKA.start();
        WIREMOCK.start();
    }

    private IntegrationTestSupport() {}

    /**
     * Registers a subscriber via the admin REST API, tolerating a 409 if the MSISDN
     * is already on file (tests in the same Redis-backed run can share subscribers).
     */
    public static void ensureSubscriber(RestClient http, String msisdn, String imsi, String homeLocationId) {
        try {
            http.post().uri("/api/v1/subscribers")
                    .body(Map.of(
                            "msisdn", msisdn,
                            "imsi", imsi,
                            "homeLocationId", homeLocationId,
                            "label", "it-test"))
                    .retrieve().toBodilessEntity();
        } catch (HttpClientErrorException e) {
            HttpStatusCode status = e.getStatusCode();
            if (status.value() != 409) {
                throw e;
            }
        }
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext ctx) {
            TestPropertyValues.of(
                    "spring.data.redis.host=" + REDIS.getHost(),
                    "spring.data.redis.port=" + REDIS.getMappedPort(6379),
                    "spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                    "phonesim.scheduler.base-url=" + WIREMOCK.baseUrl()
            ).applyTo(ctx.getEnvironment());
        }
    }
}
