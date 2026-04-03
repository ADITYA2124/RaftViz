package com.raftviz.RaftViz.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

@Configuration
public class ClusterConfig {
    @Value("${raft.node-id}") private String nodeId;
    @Value("${raft.advertise-addr}") private String advertiseAddr;

    @Bean
    public String nodeId() { return nodeId; }

    @Bean
    public URI advertisedUri() { return URI.create(advertiseAddr); }
}
