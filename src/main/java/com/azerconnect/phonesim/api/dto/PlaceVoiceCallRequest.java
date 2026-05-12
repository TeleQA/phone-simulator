package com.azerconnect.phonesim.api.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.hibernate.validator.constraints.URL;

public record PlaceVoiceCallRequest(
        @NotBlank @Size(max = 64) @Pattern(regexp = "[A-Za-z0-9_-]+") String testId,
        @NotBlank @Pattern(regexp = "\\d{6,15}") String callingParty,
        @NotBlank @Pattern(regexp = "\\d{6,15}") String calledParty,
        @Min(1) @Max(3600) int durationSeconds,
        @URL String callbackUrl,
        Integer serviceKey
) {
}
