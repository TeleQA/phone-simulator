package com.azerconnect.phonesim.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI phonesimOpenApi() {
        return new OpenAPI().info(new Info()
                .title("phone-simulator")
                .description("Phone simulator API for AI Call Simulator Agent platform")
                .version("v1"));
    }
}
