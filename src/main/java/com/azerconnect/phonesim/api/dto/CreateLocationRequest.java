package com.azerconnect.phonesim.api.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record CreateLocationRequest(
        @NotBlank @Size(max = 64) @Pattern(regexp = "[A-Za-z0-9_-]+") String id,
        @Min(0) int lac,
        @Min(0) int cellId,
        @NotBlank String vlrAddress,
        @NotBlank String mscNumber,
        String mcc,
        String mnc,
        boolean roaming
) {
}
