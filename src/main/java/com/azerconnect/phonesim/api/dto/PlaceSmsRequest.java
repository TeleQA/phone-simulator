package com.azerconnect.phonesim.api.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.hibernate.validator.constraints.URL;

public record PlaceSmsRequest(
        @NotBlank @Pattern(regexp = "\\d{6,15}") String callingParty,
        @NotBlank @Pattern(regexp = "\\d{6,15}") String calledParty,
        @NotBlank String imsi,
        @NotBlank String mscNumber,
        @NotBlank String vlrAddress,
        @Min(0) int lac,
        @Min(0) int cellId,
        @URL String callbackUrl,
        Integer serviceKey
) {
}
