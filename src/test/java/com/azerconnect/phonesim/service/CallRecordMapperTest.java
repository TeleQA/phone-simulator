package com.azerconnect.phonesim.service;

import com.azerconnect.phonesim.adapter.amqp.CallRecordPayload;
import com.azerconnect.phonesim.config.DefaultsProps;
import com.azerconnect.phonesim.domain.Call;
import com.azerconnect.phonesim.domain.CallKind;
import com.azerconnect.phonesim.domain.CallStatus;
import com.azerconnect.phonesim.domain.Direction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class CallRecordMapperTest {

    private CallRecordMapper mapper;

    @BeforeEach
    void setUp() {
        DefaultsProps defaults = new DefaultsProps(201, 200, 101, 205, "994550000342");
        mapper = new CallRecordMapper(defaults);
    }

    @Test
    void serviceKeyVoiceMoLocal() {
        assertThat(mapper.resolveServiceKey(CallKind.VOICE, Direction.MO, false, null)).isEqualTo(201);
    }

    @Test
    void serviceKeyVoiceMoRoaming() {
        assertThat(mapper.resolveServiceKey(CallKind.VOICE, Direction.MO, true, null)).isEqualTo(200);
    }

    @Test
    void serviceKeyVoiceMt() {
        assertThat(mapper.resolveServiceKey(CallKind.VOICE, Direction.MT, false, null)).isEqualTo(101);
    }

    @Test
    void serviceKeySms() {
        assertThat(mapper.resolveServiceKey(CallKind.SMS, Direction.MO, false, null)).isEqualTo(205);
        assertThat(mapper.resolveServiceKey(CallKind.SMS, Direction.MT, false, null)).isEqualTo(205);
    }

    @Test
    void overrideTakesPrecedence() {
        assertThat(mapper.resolveServiceKey(CallKind.VOICE, Direction.MO, false, 999)).isEqualTo(999);
    }

    @Test
    void initialPayloadHasInitialState() {
        Call call = sampleCall(CallKind.VOICE, Direction.MO);
        CallRecordPayload payload = mapper.toInitial(call);
        assertThat(payload.callState()).isEqualTo("INITIAL");
        assertThat(payload.smsRecord()).isFalse();
        assertThat(payload.mtCall()).isFalse();
        assertThat(payload.callDuration()).isEqualTo(15);
    }

    @Test
    void lastChunkPayloadHasLastChunkState() {
        Call call = sampleCall(CallKind.VOICE, Direction.MT);
        CallRecordPayload payload = mapper.toLastChunk(call);
        assertThat(payload.callState()).isEqualTo("LAST_CHUNK");
        assertThat(payload.mtCall()).isTrue();
    }

    @Test
    void smsPayloadFlagsAreSet() {
        Call call = sampleCall(CallKind.SMS, Direction.MO);
        CallRecordPayload payload = mapper.toInitial(call);
        assertThat(payload.smsRecord()).isTrue();
    }

    private Call sampleCall(CallKind kind, Direction direction) {
        Instant now = Instant.now();
        return new Call(
                UUID.randomUUID(),
                kind,
                direction,
                CallStatus.PENDING,
                "994501112233",
                "994504445566",
                "400040000000001",
                "994700000001",
                "994700000002",
                1,
                1,
                15,
                kind == CallKind.SMS ? 205 : (direction == Direction.MT ? 101 : 201),
                false,
                null,
                now,
                now,
                null
        );
    }
}
