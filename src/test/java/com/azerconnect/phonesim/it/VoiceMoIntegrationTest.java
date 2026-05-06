package com.azerconnect.phonesim.it;

import com.azerconnect.phonesim.adapter.kafka.CallRecordPayload;
import com.azerconnect.phonesim.adapter.kafka.FireEvent;
import com.azerconnect.phonesim.adapter.redis.CallRepository;
import com.azerconnect.phonesim.config.KafkaProps;
import com.azerconnect.phonesim.domain.CallStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = IntegrationTestSupport.Initializer.class)
class VoiceMoIntegrationTest {

    @LocalServerPort int port;
    @Autowired KafkaProps kafkaProps;
    @Autowired CallRepository callRepo;
    @Autowired ObjectMapper mapper;

    @Test
    void voiceMoEndToEnd() throws Exception {
        IntegrationTestSupport.WIREMOCK.stubFor(post(urlMatching("/v1/timers"))
                .willReturn(aResponse().withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"id\":\"" + UUID.randomUUID()
                                + "\",\"shard_id\":1,\"kafka_topic\":\""
                                + kafkaProps.timerTopic()
                                + "\",\"partition_key\":\"x\",\"state\":0,\"attempts\":0}")));
        IntegrationTestSupport.WIREMOCK.stubFor(delete(urlPathMatching("/v1/timers/.*"))
                .willReturn(aResponse().withStatus(204)));

        String testId = "voice-mo-" + UUID.randomUUID();
        try (KafkaConsumer<String, byte[]> callEventConsumer = newCallEventConsumer()) {
            callEventConsumer.subscribe(List.of(kafkaProps.callEventTopic()));
            callEventConsumer.poll(Duration.ofMillis(500));

            RestClient http = RestClient.builder().baseUrl("http://localhost:" + port).build();
            Map<String, Object> body = Map.of(
                    "testId", testId,
                    "callingParty", "994501112233",
                    "calledParty", "994504445566",
                    "imsi", "400040000000001",
                    "mscNumber", "994700000001",
                    "vlrAddress", "994700000002",
                    "lac", 1,
                    "cellId", 1,
                    "durationSeconds", 5
            );
            Map<?, ?> resp = http.post().uri("/api/v1/calls/voice/mo").body(body)
                    .retrieve().body(Map.class);
            assertThat(resp.get("testId")).isEqualTo(testId);

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(callRepo.findById(testId).orElseThrow().status())
                            .isEqualTo(CallStatus.ANSWERED));

            JsonNode initialJson = pollOne(callEventConsumer, testId);
            assertThat(initialJson.get("serviceKey").asInt()).isEqualTo(201);
            assertThat(initialJson.get("smsRecord").asBoolean()).isFalse();
            assertThat(initialJson.get("mtCall").asBoolean()).isFalse();
            assertThat(initialJson.get("callState").asText()).isEqualTo(CallRecordPayload.STATE_INITIAL);
            assertThat(initialJson.get("callDuration").asInt()).isEqualTo(5);

            // Simulate scheduler firing the timer by publishing FireEvent to Kafka
            FireEvent fire = new FireEvent(testId, FireEvent.EVENT_RELEASE, 1);
            try (KafkaProducer<String, byte[]> producer = newProducer()) {
                producer.send(new ProducerRecord<>(
                        kafkaProps.timerTopic(),
                        testId,
                        mapper.writeValueAsBytes(fire))).get(5, TimeUnit.SECONDS);
            }

            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(callRepo.findById(testId).orElseThrow().status())
                            .isEqualTo(CallStatus.RELEASED));

            JsonNode lastChunkJson = pollOne(callEventConsumer, testId);
            assertThat(lastChunkJson.get("callState").asText())
                    .isEqualTo(CallRecordPayload.STATE_LAST_CHUNK);
        }
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

    private KafkaProducer<String, byte[]> newProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IntegrationTestSupport.KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(p);
    }

    private KafkaConsumer<String, byte[]> newCallEventConsumer() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IntegrationTestSupport.KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "phonesim-test-call-event-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(p);
    }
}
