package com.azerconnect.phonesim.api;

import com.azerconnect.phonesim.api.dto.CreateLocationRequest;
import com.azerconnect.phonesim.api.dto.LocationResponse;
import com.azerconnect.phonesim.domain.Location;
import com.azerconnect.phonesim.service.LocationService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/locations")
public class LocationAdminController {

    private final LocationService service;

    public LocationAdminController(LocationService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<LocationResponse> create(@Valid @RequestBody CreateLocationRequest req) {
        Location location = service.create(req.id(), req.lac(), req.cellId(),
                req.vlrAddress(), req.mscNumber(), req.mcc(), req.mnc(), req.roaming());
        return ResponseEntity.status(HttpStatus.CREATED).body(LocationResponse.from(location));
    }

    @PutMapping("/{id}")
    public LocationResponse upsert(@PathVariable String id, @Valid @RequestBody CreateLocationRequest req) {
        Location location = service.upsert(id, req.lac(), req.cellId(),
                req.vlrAddress(), req.mscNumber(), req.mcc(), req.mnc(), req.roaming());
        return LocationResponse.from(location);
    }

    @GetMapping("/{id}")
    public LocationResponse get(@PathVariable String id) {
        return LocationResponse.from(service.findOrThrow(id));
    }

    @GetMapping
    public List<LocationResponse> list() {
        return service.findAll().stream().map(LocationResponse::from).toList();
    }

    @DeleteMapping("/{id}")
    @org.springframework.web.bind.annotation.ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String id) {
        service.delete(id);
    }
}
