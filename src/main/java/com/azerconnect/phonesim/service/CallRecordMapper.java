package com.azerconnect.phonesim.service;

import com.azerconnect.phonesim.adapter.kafka.CallRecordPayload;
import com.azerconnect.phonesim.config.DefaultsProps;
import com.azerconnect.phonesim.domain.Call;
import com.azerconnect.phonesim.domain.CallKind;
import com.azerconnect.phonesim.domain.Direction;
import org.springframework.stereotype.Component;

@Component
public class CallRecordMapper {

    private final DefaultsProps defaults;

    public CallRecordMapper(DefaultsProps defaults) {
        this.defaults = defaults;
    }

    public int resolveServiceKey(CallKind kind, Direction direction, boolean roaming, Integer override) {
        if (override != null && override > 0) return override;
        if (kind == CallKind.SMS) return defaults.smsServiceKey();
        if (direction == Direction.MT) return defaults.voiceMtServiceKey();
        return roaming ? defaults.voiceMoRoamingServiceKey() : defaults.voiceMoServiceKey();
    }

    public CallRecordPayload toInitial(Call call) {
        return new CallRecordPayload(
                call.serviceKey(),
                call.kind() == CallKind.SMS,
                call.direction() == Direction.MT,
                call.callingParty(),
                call.calledParty(),
                call.imsi(),
                call.mscNumber(),
                call.vlrAddress(),
                call.lac(),
                call.cellId(),
                call.durationSeconds(),
                CallRecordPayload.STATE_INITIAL
        );
    }

    public CallRecordPayload toLastChunk(Call call) {
        return new CallRecordPayload(
                call.serviceKey(),
                call.kind() == CallKind.SMS,
                call.direction() == Direction.MT,
                call.callingParty(),
                call.calledParty(),
                call.imsi(),
                call.mscNumber(),
                call.vlrAddress(),
                call.lac(),
                call.cellId(),
                call.durationSeconds(),
                CallRecordPayload.STATE_LAST_CHUNK
        );
    }
}
