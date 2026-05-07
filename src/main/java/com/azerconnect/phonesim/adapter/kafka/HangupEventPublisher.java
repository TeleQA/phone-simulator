package com.azerconnect.phonesim.adapter.kafka;

import com.azerconnect.phonesim.config.KafkaProps;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class HangupEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(HangupEventPublisher.class);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(5);

    private final KafkaTemplate<String, Object> template;
    private final KafkaProps kafkaProps;
    private final Timer publishTimer;

    public HangupEventPublisher(KafkaTemplate<String, Object> callEventKafkaTemplate,
                                 KafkaProps kafkaProps,
                                 MeterRegistry meters) {
        this.template = callEventKafkaTemplate;
        this.kafkaProps = kafkaProps;
        this.publishTimer = Timer.builder("phonesim.kafka.hangup_event.publish")
                .description("Time spent publishing HangupEvent to the hangup-event Kafka topic")
                .register(meters);
    }

    public void publish(String testId, HangupEvent event) {
        Timer.Sample sample = Timer.start();
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(kafkaProps.hangupEventTopic(), testId, event);
        try {
            template.send(record).get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.debug("Published HangupEvent testId={} reason={}", testId, event.reason());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException("Interrupted publishing HangupEvent testId=" + testId, e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("Failed to publish HangupEvent testId={}: {}", testId, e.getMessage());
            throw new KafkaException("Failed to publish HangupEvent testId=" + testId, e);
        } finally {
            sample.stop(publishTimer);
        }
    }
}
