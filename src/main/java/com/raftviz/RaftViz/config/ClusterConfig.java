package com.raftviz.RaftViz.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
@Configuration
public class ClusterConfig {
    @Value("${raft.node-id}") private String nodeId;
    @Value("${raft.advertise-addr}") private String advertiseAddr;
    @Value("${raft.peers}") private String peersCsv;


    public record Peer(String id, URI uri) {}


    @Bean
    public String nodeId() { return nodeId; }


    @Bean
    public URI advertisedUri() { return URI.create(advertiseAddr); }

    @Bean
    public List<Peer> peers() {
        if (peersCsv == null || peersCsv.isBlank()) return List.of();
        String[] parts = peersCsv.split(",");
        List<Peer> out = new ArrayList<>();
        int i = 1;
        for (String p : parts) {
            String s = p.trim();
            if (s.isEmpty()) continue;
            if (s.contains("=")) {
                String[] kv = s.split("=", 2);
                out.add(new Peer(kv[0].trim(), URI.create(kv[1].trim())));
            } else {
                out.add(new Peer("p" + (i++), URI.create(s)));
            }
        }
        return out.stream()
                .filter(peer -> !peer.uri().toString().equalsIgnoreCase(advertiseAddr))
                .collect(Collectors.toList());
    }
}