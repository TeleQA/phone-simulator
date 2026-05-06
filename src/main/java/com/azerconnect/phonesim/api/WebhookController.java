package com.azerconnect.phonesim.api;

import com.azerconnect.phonesim.adapter.redis.WebhookRepository;
import com.azerconnect.phonesim.api.dto.WebhookRegistrationRequest;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/webhooks")
public class WebhookController {

    private final WebhookRepository webhookRepo;

    public WebhookController(WebhookRepository webhookRepo) {
        this.webhookRepo = webhookRepo;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> register(@Valid @RequestBody WebhookRegistrationRequest req) {
        UUID id = webhookRepo.register(req.url());
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("id", id.toString(), "url", req.url()));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> unregister(@PathVariable UUID id) {
        webhookRepo.unregister(id);
        return ResponseEntity.noContent().build();
    }
}
