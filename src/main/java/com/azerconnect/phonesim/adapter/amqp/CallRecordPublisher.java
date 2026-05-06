package com.azerconnect.phonesim.adapter.amqp;

import com.azerconnect.phonesim.config.AmqpProps;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class CallRecordPublisher {

    private static final Logger log = LoggerFactory.getLogger(CallRecordPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final AmqpProps props;
    private final Timer publishTimer;

    public CallRecordPublisher(RabbitTemplate rabbitTemplate, AmqpProps props, MeterRegistry meters) {
        this.rabbitTemplate = rabbitTemplate;
        this.props = props;
        this.publishTimer = Timer.builder("phonesim.amqp.publish")
                .description("Time spent publishing CallRecord to CAP simulator queue")
                .register(meters);
    }

    public void publish(UUID callId, CallRecordPayload payload) {
        Timer.Sample sample = Timer.start();
        try {
            MessagePostProcessor mpp = msg -> {
                msg.getMessageProperties().setMessageId(callId.toString());
                msg.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                msg.getMessageProperties().setContentType("application/json");
                return msg;
            };
            rabbitTemplate.convertAndSend(props.exchange(), props.queue(), payload, mpp);
            log.debug("Published CallRecord callId={} state={} serviceKey={}",
                    callId, payload.callState(), payload.serviceKey());
        } catch (AmqpException e) {
            log.error("Failed to publish CallRecord callId={}: {}", callId, e.getMessage());
            throw e;
        } finally {
            sample.stop(publishTimer);
        }
    }
}
