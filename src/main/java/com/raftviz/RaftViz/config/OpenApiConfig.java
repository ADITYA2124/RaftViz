package com.raftviz.RaftViz.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {
    @Bean
    public OpenAPI raftVizOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("RaftViz API")
                        .description("Raft consensus demo API for cluster state, dynamic discovery, client log writes, and per-node log inspection.")
                        .version("1.0.0")
                        .contact(new Contact().name("RaftViz"))
                        .license(new License().name("Project Internal Demo")));
    }
}
