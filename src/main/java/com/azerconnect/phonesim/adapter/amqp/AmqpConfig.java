package com.azerconnect.phonesim.adapter.amqp;

import com.azerconnect.phonesim.config.AmqpProps;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AmqpConfig {

    @Bean
    public Queue callEventQueue(AmqpProps props) {
        // declare passive — CAP simulator owns the canonical definition; this lets us boot
        // even if the queue is pre-existing with different args
        return new Queue(props.queue(), true, false, false);
    }

    @Bean
    public Jackson2JsonMessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory factory,
                                          Jackson2JsonMessageConverter converter) {
        if (factory instanceof CachingConnectionFactory caching) {
            caching.setChannelCacheSize(25);
            caching.setRequestedHeartBeat(30);
            caching.setConnectionTimeout(5000);
        }
        RabbitTemplate template = new RabbitTemplate(factory);
        template.setMessageConverter(converter);
        template.setMandatory(true);
        return template;
    }
}
