package com.azerconnect.phonesim.it;

import com.azerconnect.phonesim.adapter.amqp.CallRecordPayload;
import com.azerconnect.phonesim.adapter.redis.CallRepository;
import com.azerconnect.phonesim.config.AmqpProps;
import com.azerconnect.phonesim.domain.CallStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.web.client.RestClient;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = IntegrationTestSupport.Initializer.class)
class SmsMoIntegrationTest {

    @LocalServerPort int port;
    @Autowired RabbitTemplate rabbitTemplate;
    @Autowired AmqpProps amqpProps;
    @Autowired CallRepository callRepo;
    @Autowired ObjectMapper mapper;

    @Test
    void smsMoEndToEnd() {
        RestClient http = RestClient.builder().baseUrl("http://localhost:" + port).build();
        Map<String, Object> body = Map.of(
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
        UUID smsId = UUID.fromString(resp.get("callId").toString());

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(callRepo.findById(smsId).orElseThrow().status())
                        .isEqualTo(CallStatus.RELEASED));

        Object onlyMsg = rabbitTemplate.receiveAndConvert(amqpProps.queue(), 5_000);
        assertThat(onlyMsg).isNotNull();
        JsonNode json = mapper.valueToTree(onlyMsg);
        assertThat(json.get("serviceKey").asInt()).isEqualTo(205);
        assertThat(json.get("smsRecord").asBoolean()).isTrue();
        assertThat(json.get("callState").asText()).isEqualTo(CallRecordPayload.STATE_INITIAL);
        // SMS should not have a second message
        Object none = rabbitTemplate.receiveAndConvert(amqpProps.queue(), 500);
        assertThat(none).isNull();
    }
}
