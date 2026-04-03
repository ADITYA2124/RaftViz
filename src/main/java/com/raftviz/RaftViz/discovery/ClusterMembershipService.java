package com.raftviz.RaftViz.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.raftviz.RaftViz.model.ClusterNodeInfo;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class ClusterMembershipService {
    public record Peer(String id, URI uri) {
    }

    private static class MemberRecord {
        private final String nodeId;
        private volatile URI uri;
        private final boolean self;
        private volatile long lastSeenEpochMs;

        private MemberRecord(String nodeId, URI uri, boolean self, long lastSeenEpochMs) {
            this.nodeId = nodeId;
            this.uri = uri;
            this.self = self;
            this.lastSeenEpochMs = lastSeenEpochMs;
        }
    }

    private static class Announcement {
        public String nodeId;
        public String uri;
        public long timestamp;
    }

    private final String nodeId;
    private final URI selfUri;
    private final String multicastGroup;
    private final int multicastPort;
    private final long announceIntervalMs;
    private final long staleAfterMs;
    private final long evictAfterMs;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
    private final Map<String, MemberRecord> members = new ConcurrentHashMap<>();

    private volatile boolean running = true;
    private volatile MulticastSocket listenerSocket;

    public ClusterMembershipService(
            @Value("${raft.node-id}") String nodeId,
            @Value("${raft.advertise-addr}") URI selfUri,
            @Value("${raft.discovery.multicast-group:230.0.0.15}") String multicastGroup,
            @Value("${raft.discovery.multicast-port:4446}") int multicastPort,
            @Value("${raft.discovery.announce-interval-ms:1000}") long announceIntervalMs,
            @Value("${raft.discovery.stale-after-ms:4000}") long staleAfterMs,
            @Value("${raft.discovery.evict-after-ms:30000}") long evictAfterMs
    ) {
        this.nodeId = nodeId;
        this.selfUri = selfUri;
        this.multicastGroup = multicastGroup;
        this.multicastPort = multicastPort;
        this.announceIntervalMs = announceIntervalMs;
        this.staleAfterMs = staleAfterMs;
        this.evictAfterMs = evictAfterMs;
    }

    @PostConstruct
    public void start() {
        upsert(nodeId, selfUri, true, System.currentTimeMillis());
        scheduler.execute(this::listenForAnnouncements);
        scheduler.scheduleAtFixedRate(this::announceSelf, 0, announceIntervalMs, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::pruneExpiredNodes, evictAfterMs, Math.max(announceIntervalMs, 1000), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (listenerSocket != null) {
            listenerSocket.close();
        }
        scheduler.shutdownNow();
    }

    public List<Peer> activePeers() {
        long now = System.currentTimeMillis();
        return members.values().stream()
                .filter(member -> !member.self)
                .filter(member -> now - member.lastSeenEpochMs <= staleAfterMs)
                .sorted(Comparator.comparing(member -> member.nodeId))
                .map(member -> new Peer(member.nodeId, member.uri))
                .toList();
    }

    public List<ClusterNodeInfo> clusterNodes() {
        long now = System.currentTimeMillis();
        return members.values().stream()
                .sorted(Comparator.comparing(member -> member.nodeId))
                .map(member -> {
                    ClusterNodeInfo info = new ClusterNodeInfo();
                    info.nodeId = member.nodeId;
                    info.uri = member.uri.toString();
                    info.self = member.self;
                    info.lastSeenEpochMs = member.lastSeenEpochMs;
                    info.online = member.self || now - member.lastSeenEpochMs <= staleAfterMs;
                    return info;
                })
                .toList();
    }

    public Optional<URI> findUriByNodeId(String candidateNodeId) {
        MemberRecord member = members.get(candidateNodeId);
        return member == null ? Optional.empty() : Optional.of(member.uri);
    }

    private void announceSelf() {
        try (MulticastSocket socket = new MulticastSocket()) {
            Announcement announcement = new Announcement();
            announcement.nodeId = nodeId;
            announcement.uri = selfUri.toString();
            announcement.timestamp = System.currentTimeMillis();

            byte[] payload = objectMapper.writeValueAsBytes(announcement);
            DatagramPacket packet = new DatagramPacket(
                    payload,
                    payload.length,
                    InetAddress.getByName(multicastGroup),
                    multicastPort
            );
            socket.send(packet);
            upsert(nodeId, selfUri, true, announcement.timestamp);
        } catch (Exception ignored) {
        }
    }

    private void listenForAnnouncements() {
        try (MulticastSocket socket = new MulticastSocket(multicastPort)) {
            listenerSocket = socket;
            socket.setReuseAddress(true);
            socket.setSoTimeout(1000);
            InetAddress group = InetAddress.getByName(multicastGroup);
            socket.joinGroup(group);

            while (running) {
                try {
                    byte[] buffer = new byte[2048];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
                    String raw = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8);
                    Announcement announcement = objectMapper.readValue(raw, Announcement.class);
                    if (announcement.nodeId == null || announcement.nodeId.isBlank() || announcement.uri == null || announcement.uri.isBlank()) {
                        continue;
                    }
                    upsert(
                            announcement.nodeId,
                            URI.create(announcement.uri),
                            announcement.nodeId.equals(nodeId),
                            announcement.timestamp > 0 ? announcement.timestamp : System.currentTimeMillis()
                    );
                } catch (java.net.SocketTimeoutException ignored) {
                } catch (Exception ignored) {
                }
            }
        } catch (Exception ignored) {
        }
    }

    private void pruneExpiredNodes() {
        long now = System.currentTimeMillis();
        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, MemberRecord> entry : members.entrySet()) {
            MemberRecord member = entry.getValue();
            if (!member.self && now - member.lastSeenEpochMs > evictAfterMs) {
                toRemove.add(entry.getKey());
            }
        }
        toRemove.forEach(members::remove);
    }

    private void upsert(String discoveredNodeId, URI discoveredUri, boolean self, long lastSeenEpochMs) {
        members.compute(discoveredNodeId, (ignored, existing) -> {
            if (existing == null) {
                return new MemberRecord(discoveredNodeId, discoveredUri, self, lastSeenEpochMs);
            }
            existing.uri = discoveredUri;
            existing.lastSeenEpochMs = Math.max(existing.lastSeenEpochMs, lastSeenEpochMs);
            return existing;
        });
    }
}
