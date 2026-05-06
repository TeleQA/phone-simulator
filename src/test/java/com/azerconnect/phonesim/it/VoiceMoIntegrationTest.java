package com.azerconnect.phonesim.it;

import com.azerconnect.phonesim.adapter.amqp.CallRecordPayload;
import com.azerconnect.phonesim.adapter.kafka.FireEvent;
import com.azerconnect.phonesim.adapter.redis.CallRepository;
import com.azerconnect.phonesim.config.AmqpProps;
import com.azerconnect.phonesim.config.KafkaProps;
import com.azerconnect.phonesim.domain.CallStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.web.client.RestClient;

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
    @Autowired RabbitTemplate rabbitTemplate;
    @Autowired AmqpProps amqpProps;
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

        RestClient http = RestClient.builder().baseUrl("http://localhost:" + port).build();
        Map<String, Object> body = Map.of(
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
        UUID callId = UUID.fromString(resp.get("callId").toString());

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(callRepo.findById(callId).orElseThrow().status())
                        .isEqualTo(CallStatus.ANSWERED));

        Object initial = rabbitTemplate.receiveAndConvert(amqpProps.queue(), 5_000);
        assertThat(initial).isNotNull();
        JsonNode initialJson = mapper.valueToTree(initial);
        assertThat(initialJson.get("serviceKey").asInt()).isEqualTo(201);
        assertThat(initialJson.get("smsRecord").asBoolean()).isFalse();
        assertThat(initialJson.get("mtCall").asBoolean()).isFalse();
        assertThat(initialJson.get("callState").asText()).isEqualTo(CallRecordPayload.STATE_INITIAL);
        assertThat(initialJson.get("callDuration").asInt()).isEqualTo(5);

        // Simulate scheduler firing the timer by publishing FireEvent to Kafka
        FireEvent fire = new FireEvent(callId, FireEvent.EVENT_RELEASE, 1);
        try (KafkaProducer<String, byte[]> producer = newProducer()) {
            producer.send(new ProducerRecord<>(
                    kafkaProps.timerTopic(),
                    callId.toString(),
                    mapper.writeValueAsBytes(fire))).get(5, TimeUnit.SECONDS);
        }

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(callRepo.findById(callId).orElseThrow().status())
                        .isEqualTo(CallStatus.RELEASED));

        Object lastChunk = rabbitTemplate.receiveAndConvert(amqpProps.queue(), 5_000);
        assertThat(lastChunk).isNotNull();
        JsonNode lastChunkJson = mapper.valueToTree(lastChunk);
        assertThat(lastChunkJson.get("callState").asText()).isEqualTo(CallRecordPayload.STATE_LAST_CHUNK);
    }

    private KafkaProducer<String, byte[]> newProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IntegrationTestSupport.KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(p);
    }
}
