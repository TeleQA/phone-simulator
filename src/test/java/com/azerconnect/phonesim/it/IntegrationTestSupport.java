package com.azerconnect.phonesim.it;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

public final class IntegrationTestSupport {

    public static final RabbitMQContainer RABBIT =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-management"))
                    .withQueue("/", "call-event-queue", true, false, java.util.Map.of());

    public static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379);

    public static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    public static final WireMockServer WIREMOCK = new WireMockServer(wireMockConfig().dynamicPort());

    static {
        RABBIT.start();
        REDIS.start();
        KAFKA.start();
        WIREMOCK.start();
    }

    private IntegrationTestSupport() {}

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext ctx) {
            TestPropertyValues.of(
                    "spring.rabbitmq.host=" + RABBIT.getHost(),
                    "spring.rabbitmq.port=" + RABBIT.getAmqpPort(),
                    "spring.rabbitmq.username=" + RABBIT.getAdminUsername(),
                    "spring.rabbitmq.password=" + RABBIT.getAdminPassword(),
                    "spring.rabbitmq.virtual-host=/",
                    "spring.data.redis.host=" + REDIS.getHost(),
                    "spring.data.redis.port=" + REDIS.getMappedPort(6379),
                    "spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                    "phonesim.scheduler.base-url=" + WIREMOCK.baseUrl()
            ).applyTo(ctx.getEnvironment());
        }
    }
}
