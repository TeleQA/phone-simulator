package com.azerconnect.phonesim.api;

import com.azerconnect.phonesim.api.dto.CallAcceptedResponse;
import com.azerconnect.phonesim.api.dto.CallSnapshotResponse;
import com.azerconnect.phonesim.api.dto.PlaceVoiceCallRequest;
import com.azerconnect.phonesim.domain.Call;
import com.azerconnect.phonesim.domain.CallStatus;
import com.azerconnect.phonesim.domain.Direction;
import com.azerconnect.phonesim.service.CallService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/calls")
public class VoiceCallController {

    private final CallService callService;

    public VoiceCallController(CallService callService) {
        this.callService = callService;
    }

    @PostMapping("/voice/mo")
    public ResponseEntity<CallAcceptedResponse> placeMo(@Valid @RequestBody PlaceVoiceCallRequest req) {
        return accept(placeVoice(Direction.MO, req));
    }

    @PostMapping("/voice/mt")
    public ResponseEntity<CallAcceptedResponse> placeMt(@Valid @RequestBody PlaceVoiceCallRequest req) {
        return accept(placeVoice(Direction.MT, req));
    }

    @GetMapping("/{callId}")
    public CallSnapshotResponse snapshot(@PathVariable UUID callId) {
        return CallSnapshotResponse.from(callService.findOrThrow(callId));
    }

    @GetMapping
    public List<CallSnapshotResponse> list(@RequestParam(defaultValue = "ANSWERED") CallStatus state) {
        return callService.listByStatus(state).stream()
                .map(CallSnapshotResponse::from)
                .toList();
    }

    private Call placeVoice(Direction direction, PlaceVoiceCallRequest req) {
        boolean roaming = req.roaming() != null && req.roaming();
        return callService.placeVoice(
                direction,
                req.callingParty(), req.calledParty(), req.imsi(),
                req.mscNumber(), req.vlrAddress(), req.lac(), req.cellId(),
                req.durationSeconds(), roaming, req.serviceKey(), req.callbackUrl()
        );
    }

    private ResponseEntity<CallAcceptedResponse> accept(Call call) {
        String self = "/api/v1/calls/" + call.callId();
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(CallAcceptedResponse.from(call, self));
    }
}
