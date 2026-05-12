package com.azerconnect.phonesim.api;

import com.azerconnect.phonesim.api.dto.MoveSubscriberRequest;
import com.azerconnect.phonesim.api.dto.RegisterSubscriberRequest;
import com.azerconnect.phonesim.api.dto.SubscriberResponse;
import com.azerconnect.phonesim.domain.Subscriber;
import com.azerconnect.phonesim.service.SubscriberService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/subscribers")
public class SubscriberAdminController {

    private final SubscriberService service;

    public SubscriberAdminController(SubscriberService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<SubscriberResponse> register(@Valid @RequestBody RegisterSubscriberRequest req) {
        Subscriber s = service.register(req.msisdn(), req.imsi(), req.homeLocationId(), req.label());
        return ResponseEntity.status(HttpStatus.CREATED).body(SubscriberResponse.from(s));
    }

    @GetMapping("/{msisdn}")
    public SubscriberResponse get(@PathVariable String msisdn) {
        return SubscriberResponse.from(service.findOrThrow(msisdn));
    }

    @GetMapping
    public List<SubscriberResponse> list() {
        return service.findAll().stream().map(SubscriberResponse::from).toList();
    }

    @PostMapping("/{msisdn}/move")
    public SubscriberResponse move(@PathVariable String msisdn, @Valid @RequestBody MoveSubscriberRequest req) {
        return SubscriberResponse.from(service.moveTo(msisdn, req.locationId()));
    }

    @PostMapping("/{msisdn}/home")
    public SubscriberResponse home(@PathVariable String msisdn) {
        return SubscriberResponse.from(service.goHome(msisdn));
    }

    @DeleteMapping("/{msisdn}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String msisdn) {
        service.delete(msisdn);
    }
}
