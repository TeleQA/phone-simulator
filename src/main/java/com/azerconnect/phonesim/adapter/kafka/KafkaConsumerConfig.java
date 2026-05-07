package com.azerconnect.phonesim.adapter.kafka;

import com.azerconnect.phonesim.config.KafkaProps;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    private static final String TRUSTED_PACKAGES = "com.azerconnect.phonesim.adapter.kafka";

    /* ---------------- FireEvent (timer fires from scheduler) ---------------- */

    @Bean
    public ConsumerFactory<String, FireEvent> fireEventConsumerFactory(KafkaProperties bootProps) {
        return jsonConsumerFactory(bootProps, FireEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, FireEvent> fireEventListenerFactory(
            ConsumerFactory<String, FireEvent> cf,
            KafkaProps kafkaProps) {
        return manualAckListenerFactory(cf, kafkaProps);
    }

    /* ---------------- AnswerEvent (from CAP simulator) ---------------- */

    @Bean
    public ConsumerFactory<String, AnswerEvent> answerEventConsumerFactory(KafkaProperties bootProps) {
        Map<String, Object> props = new HashMap<>(bootProps.buildConsumerProperties(null));
        // Use a distinct group id so it doesn't share offsets with the timer consumer.
        Object existingGroup = props.get(ConsumerConfig.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                (existingGroup != null ? existingGroup : "phonesim") + "-answer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, TRUSTED_PACKAGES);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, AnswerEvent.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AnswerEvent> answerEventListenerFactory(
            ConsumerFactory<String, AnswerEvent> cf,
            KafkaProps kafkaProps) {
        return manualAckListenerFactory(cf, kafkaProps);
    }

    /* ---------------- helpers ---------------- */

    private <T> ConsumerFactory<String, T> jsonConsumerFactory(KafkaProperties bootProps, Class<T> type) {
        Map<String, Object> props = new HashMap<>(bootProps.buildConsumerProperties(null));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, TRUSTED_PACKAGES);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, type.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> manualAckListenerFactory(
            ConsumerFactory<String, T> cf, KafkaProps kafkaProps) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cf);
        factory.setConcurrency(kafkaProps.listenerConcurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
