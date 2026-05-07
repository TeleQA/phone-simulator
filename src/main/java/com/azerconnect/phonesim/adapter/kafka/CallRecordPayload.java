package com.azerconnect.phonesim.adapter.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CallRecordPayload(
        @JsonProperty("serviceKey") int serviceKey,
        @JsonProperty("smsRecord") boolean smsRecord,
        @JsonProperty("mtCall") boolean mtCall,
        @JsonProperty("callingParty") String callingParty,
        @JsonProperty("calledParty") String calledParty,
        @JsonProperty("imsi") String imsi,
        @JsonProperty("mscNumber") String mscNumber,
        @JsonProperty("vlrAddress") String vlrAddress,
        @JsonProperty("lac") int lac,
        @JsonProperty("cellId") int cellId,
        @JsonProperty("callDuration") int callDuration,
        @JsonProperty("callState") String callState
) {
    /**
     * Phone-simulator only ever publishes records in the {@code INITIAL} state — i.e. it tells
     * the CAP simulator "the subscriber is dialling". CAP owns ANSWER, ApplyChargingReport
     * chunking, and LAST_CHUNK accounting on its side; phone-simulator signals call termination
     * via a separate {@link HangupEvent}, not by re-publishing this payload.
     */
    public static final String STATE_INITIAL = "INITIAL";
}
