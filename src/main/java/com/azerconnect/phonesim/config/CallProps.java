package com.azerconnect.phonesim.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Behaviour knobs for the voice/SMS call lifecycle.
 *
 * <ul>
 *   <li>{@code noAnswerTimeout} — how long phone-simulator waits for an {@code AnswerEvent}
 *       from the CAP simulator after publishing the {@code INITIAL} CallRecord. If the timer
 *       fires before answer, the call is moved to {@code FAILED} with reason
 *       {@code no_answer_timeout}.</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "phonesim.call")
public record CallProps(
        Duration noAnswerTimeout
) {
    public CallProps {
        if (noAnswerTimeout == null || noAnswerTimeout.isZero() || noAnswerTimeout.isNegative()) {
            noAnswerTimeout = Duration.ofSeconds(30);
        }
    }
}
