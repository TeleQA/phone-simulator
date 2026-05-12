package com.azerconnect.phonesim.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record RegisterSubscriberRequest(
        @NotBlank @Pattern(regexp = "\\d{6,15}") String msisdn,
        @NotBlank @Pattern(regexp = "\\d{6,15}") String imsi,
        @NotBlank @Size(max = 64) @Pattern(regexp = "[A-Za-z0-9_-]+") String homeLocationId,
        @Size(max = 128) String label
) {
}
