package com.azerconnect.phonesim.api.dto;

import jakarta.validation.constraints.NotBlank;
import org.hibernate.validator.constraints.URL;

public record WebhookRegistrationRequest(
        @NotBlank @URL String url
) {
}
