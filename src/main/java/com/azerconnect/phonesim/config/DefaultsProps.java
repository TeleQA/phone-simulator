package com.azerconnect.phonesim.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "phonesim.defaults")
public record DefaultsProps(
        int voiceMoServiceKey,
        int voiceMoRoamingServiceKey,
        int voiceMtServiceKey,
        int smsServiceKey,
        String smscAddress
) {
}
