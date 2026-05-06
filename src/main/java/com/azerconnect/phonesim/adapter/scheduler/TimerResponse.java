package com.azerconnect.phonesim.adapter.scheduler;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TimerResponse(
        UUID id,
        @JsonProperty("shard_id") int shardId,
        @JsonProperty("kafka_topic") String kafkaTopic,
        @JsonProperty("partition_key") String partitionKey,
        int state,
        int attempts
) {
}
