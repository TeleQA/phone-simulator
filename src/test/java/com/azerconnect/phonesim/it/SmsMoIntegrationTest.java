package com.azerconnect.phonesim.it;

import com.azerconnect.phonesim.adapter.kafka.CallRecordPayload;
import com.azerconnect.phonesim.adapter.redis.CallRepository;
import com.azerconnect.phonesim.config.KafkaProps;
import com.azerconnect.phonesim.domain.CallStatus;
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
import org.springframework.test.context.ContextConfiguration;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = IntegrationTestSupport.Initializer.class)
class SmsMoIntegrationTest {

    @LocalServerPort int port;
    @Autowired KafkaProps kafkaProps;
    @Autowired CallRepository callRepo;
    @Autowired ObjectMapper mapper;

    @Test
    void smsMoEndToEnd() throws Exception {
        String testId = "sms-mo-" + UUID.randomUUID();
        try (KafkaConsumer<String, byte[]> consumer = newConsumer()) {
            consumer.subscribe(List.of(kafkaProps.callEventTopic()));
            consumer.poll(Duration.ofMillis(500));

            RestClient http = RestClient.builder().baseUrl("http://localhost:" + port).build();
            Map<String, Object> body = Map.of(
                    "testId", testId,
                    "callingParty", "994501112233",
                    "calledParty", "994504445566",
                    "imsi", "400040000000001",
                    "mscNumber", "994700000001",
                    "vlrAddress", "994700000002",
                    "lac", 1,
                    "cellId", 1
            );
            Map<?, ?> resp = http.post().uri("/api/v1/sms/mo").body(body)
                    .retrieve().body(Map.class);
            assertThat(resp.get("testId")).isEqualTo(testId);

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(callRepo.findById(testId).orElseThrow().status())
                            .isEqualTo(CallStatus.RELEASED));

            JsonNode json = pollOne(consumer, testId);
            assertThat(json.get("serviceKey").asInt()).isEqualTo(205);
            assertThat(json.get("smsRecord").asBoolean()).isTrue();
            assertThat(json.get("callState").asText()).isEqualTo(CallRecordPayload.STATE_INITIAL);

            // SMS should not produce a second call-event message
            ConsumerRecords<String, byte[]> none = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, byte[]> r : none) {
                assertThat(r.key()).isNotEqualTo(testId);
            }
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

    private KafkaConsumer<String, byte[]> newConsumer() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IntegrationTestSupport.KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "phonesim-test-call-event-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(p);
    }
}
