package com.azerconnect.phonesim.adapter.amqp;

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
    public static final String STATE_INITIAL = "INITIAL";
    public static final String STATE_LAST_CHUNK = "LAST_CHUNK";
}
