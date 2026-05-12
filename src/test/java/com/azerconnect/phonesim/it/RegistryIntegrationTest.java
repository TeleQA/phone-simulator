package com.azerconnect.phonesim.it;

import com.azerconnect.phonesim.adapter.kafka.CallRecordPayload;
import com.azerconnect.phonesim.config.KafkaProps;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatusCode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = IntegrationTestSupport.Initializer.class)
class RegistryIntegrationTest {

    @LocalServerPort int port;
    @Autowired KafkaProps kafkaProps;
    @Autowired ObjectMapper mapper;

    @Test
    void locationsAreSeededOnStartup() {
        RestClient http = http();
        Map<?, ?> baku = http.get().uri("/api/v1/locations/BAKU_CENTER").retrieve().body(Map.class);
        assertThat(baku.get("lac")).isEqualTo(1001);
        assertThat(baku.get("roaming")).isEqualTo(false);

        Map<?, ?> istanbul = http.get().uri("/api/v1/locations/ISTANBUL_TURKCELL").retrieve().body(Map.class);
        assertThat(istanbul.get("roaming")).isEqualTo(true);
    }

    @Test
    void registerMoveAndGoHome() {
        RestClient http = http();
        String msisdn = "994509990001";
        IntegrationTestSupport.ensureSubscriber(http, msisdn, "400040000099001", "BAKU_CENTER");

        Map<?, ?> initial = http.get().uri("/api/v1/subscribers/" + msisdn).retrieve().body(Map.class);
        assertThat(initial.get("currentLocationId")).isEqualTo("BAKU_CENTER");
        assertThat(initial.get("homeLocationId")).isEqualTo("BAKU_CENTER");

        Map<?, ?> moved = http.post().uri("/api/v1/subscribers/" + msisdn + "/move")
                .body(Map.of("locationId", "ISTANBUL_TURKCELL"))
                .retrieve().body(Map.class);
        assertThat(moved.get("currentLocationId")).isEqualTo("ISTANBUL_TURKCELL");
        assertThat(moved.get("homeLocationId")).isEqualTo("BAKU_CENTER");

        Map<?, ?> home = http.post().uri("/api/v1/subscribers/" + msisdn + "/home")
                .retrieve().body(Map.class);
        assertThat(home.get("currentLocationId")).isEqualTo("BAKU_CENTER");
    }

    @Test
    void unknownMsisdnHardFailsOnPlaceCall() {
        RestClient http = http();
        String testId = "no-registry-" + UUID.randomUUID();
        assertThatThrownBy(() -> http.post().uri("/api/v1/calls/voice/mo")
                .body(Map.of(
                        "testId", testId,
                        "callingParty", "999000000000",
                        "calledParty", "994504445566",
                        "durationSeconds", 5))
                .retrieve().toBodilessEntity())
                .isInstanceOfSatisfying(HttpClientErrorException.class,
                        e -> assertThat(e.getStatusCode()).isEqualTo(HttpStatusCode.valueOf(404)));
    }

    @Test
    void roamingMoUsesRoamingServiceKey() throws Exception {
        IntegrationTestSupport.WIREMOCK.stubFor(post(urlMatching("/v1/timers"))
                .willReturn(aResponse().withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"id\":\"" + UUID.randomUUID()
                                + "\",\"shard_id\":1,\"kafka_topic\":\""
                                + kafkaProps.timerTopic()
                                + "\",\"partition_key\":\"x\",\"state\":0,\"attempts\":0}")));
        IntegrationTestSupport.WIREMOCK.stubFor(delete(urlPathMatching("/v1/timers/.*"))
                .willReturn(aResponse().withStatus(204)));

        RestClient http = http();
        String msisdn = "994509990002";
        IntegrationTestSupport.ensureSubscriber(http, msisdn, "400040000099002", "BAKU_CENTER");
        http.post().uri("/api/v1/subscribers/" + msisdn + "/move")
                .body(Map.of("locationId", "ISTANBUL_TURKCELL"))
                .retrieve().toBodilessEntity();

        String testId = "voice-roam-" + UUID.randomUUID();
        try (KafkaConsumer<String, byte[]> consumer = newCallEventConsumer()) {
            consumer.subscribe(List.of(kafkaProps.callEventTopic()));
            consumer.poll(Duration.ofMillis(500));

            http.post().uri("/api/v1/calls/voice/mo").body(Map.of(
                    "testId", testId,
                    "callingParty", msisdn,
                    "calledParty", "994504445566",
                    "durationSeconds", 5
            )).retrieve().toBodilessEntity();

            JsonNode initial = pollOne(consumer, testId);
            assertThat(initial.get("callState").asText()).isEqualTo(CallRecordPayload.STATE_INITIAL);
            assertThat(initial.get("serviceKey").asInt()).isEqualTo(200);
            assertThat(initial.get("lac").asInt()).isEqualTo(2001);
            assertThat(initial.get("cellId").asInt()).isEqualTo(31);
            assertThat(initial.get("vlrAddress").asText()).isEqualTo("905300000002");
        }
    }

    private RestClient http() {
        return RestClient.builder().baseUrl("http://localhost:" + port).build();
    }

    private JsonNode pollOne(KafkaConsumer<String, byte[]> consumer, String expectedKey) throws Exception {
        long deadline = System.currentTimeMillis() + 8_000;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, byte[]> r : records) {
                if (expectedKey.equals(r.key())) {
                    return mapper.readTree(r.value());
                }
            }
        }
        throw new AssertionError("No call-event Kafka message arrived for testId=" + expectedKey);
    }

    private KafkaConsumer<String, byte[]> newCallEventConsumer() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IntegrationTestSupport.KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "phonesim-test-registry-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(p);
    }
}
