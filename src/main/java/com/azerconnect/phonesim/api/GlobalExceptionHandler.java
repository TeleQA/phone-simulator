package com.azerconnect.phonesim.api;

import com.azerconnect.phonesim.domain.CallNotFoundException;
import com.azerconnect.phonesim.domain.CallStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(CallNotFoundException.class)
    public ResponseEntity<Map<String, Object>> notFound(CallNotFoundException e) {
        return error(HttpStatus.NOT_FOUND, "call_not_found", e.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> badRequest(IllegalArgumentException e) {
        return error(HttpStatus.BAD_REQUEST, "bad_request", e.getMessage());
    }

    @ExceptionHandler(CallStateMachine.IllegalStateTransitionException.class)
    public ResponseEntity<Map<String, Object>> illegalTransition(
            CallStateMachine.IllegalStateTransitionException e) {
        return error(HttpStatus.CONFLICT, "illegal_state_transition", e.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> validation(MethodArgumentNotValidException e) {
        String detail = e.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return error(HttpStatus.BAD_REQUEST, "validation_error", detail);
    }

    @ExceptionHandler(AmqpException.class)
    public ResponseEntity<Map<String, Object>> amqp(AmqpException e) {
        log.error("AMQP failure: {}", e.getMessage());
        return error(HttpStatus.SERVICE_UNAVAILABLE, "amqp_unavailable", e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> unexpected(Exception e) {
        log.error("Unexpected failure", e);
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "internal_error", e.getMessage());
    }

    private ResponseEntity<Map<String, Object>> error(HttpStatus status, String code, String detail) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now().toString());
        body.put("status", status.value());
        body.put("error", code);
        body.put("detail", detail);
        return ResponseEntity.status(status).body(body);
    }
}
