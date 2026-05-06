package com.azerconnect.phonesim.api;

import com.azerconnect.phonesim.api.dto.CallAcceptedResponse;
import com.azerconnect.phonesim.api.dto.PlaceSmsRequest;
import com.azerconnect.phonesim.domain.Call;
import com.azerconnect.phonesim.domain.Direction;
import com.azerconnect.phonesim.service.CallService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/sms")
public class SmsController {

    private final CallService callService;

    public SmsController(CallService callService) {
        this.callService = callService;
    }

    @PostMapping("/mo")
    public ResponseEntity<CallAcceptedResponse> sendMo(@Valid @RequestBody PlaceSmsRequest req) {
        return accept(send(Direction.MO, req));
    }

    @PostMapping("/mt")
    public ResponseEntity<CallAcceptedResponse> sendMt(@Valid @RequestBody PlaceSmsRequest req) {
        return accept(send(Direction.MT, req));
    }

    private Call send(Direction direction, PlaceSmsRequest req) {
        return callService.sendSms(
                direction,
                req.callingParty(), req.calledParty(), req.imsi(),
                req.mscNumber(), req.vlrAddress(), req.lac(), req.cellId(),
                req.serviceKey(), req.callbackUrl()
        );
    }

    private ResponseEntity<CallAcceptedResponse> accept(Call sms) {
        String self = "/api/v1/calls/" + sms.callId();
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(CallAcceptedResponse.from(sms, self));
    }
}
