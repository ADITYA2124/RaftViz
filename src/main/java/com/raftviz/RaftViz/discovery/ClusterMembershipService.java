package com.raftviz.RaftViz.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.raftviz.RaftViz.model.ClusterNodeInfo;
import com.raftviz.RaftViz.model.NodeStatus;
import com.raftviz.RaftViz.util.HttpClient;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
    private final int discoveryPort;
    private final long announceIntervalMs;
    private final long staleAfterMs;
    private final long evictAfterMs;
    private final boolean localScanEnabled;
    private final int localScanStartPort;
    private final int localScanEndPort;
    private final HttpClient httpClient = new HttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final Map<String, MemberRecord> members = new ConcurrentHashMap<>();

    private volatile boolean running = true;
    private volatile DatagramSocket listenerSocket;

    public ClusterMembershipService(
            @Value("${raft.node-id}") String nodeId,
            @Value("${raft.advertise-addr}") URI selfUri,
            @Value("${raft.discovery.port:4446}") int discoveryPort,
            @Value("${raft.discovery.announce-interval-ms:1000}") long announceIntervalMs,
            @Value("${raft.discovery.stale-after-ms:4000}") long staleAfterMs,
            @Value("${raft.discovery.evict-after-ms:30000}") long evictAfterMs,
            @Value("${raft.discovery.local-scan-enabled:true}") boolean localScanEnabled,
            @Value("${raft.discovery.local-scan-start-port:8080}") int localScanStartPort,
            @Value("${raft.discovery.local-scan-end-port:8090}") int localScanEndPort
    ) {
        this.nodeId = nodeId;
        this.selfUri = selfUri;
        this.discoveryPort = discoveryPort;
        this.announceIntervalMs = announceIntervalMs;
        this.staleAfterMs = staleAfterMs;
        this.evictAfterMs = evictAfterMs;
        this.localScanEnabled = localScanEnabled;
        this.localScanStartPort = localScanStartPort;
        this.localScanEndPort = localScanEndPort;
    }

    @PostConstruct
    public void start() {
        upsert(nodeId, selfUri, true, System.currentTimeMillis());
        scheduler.execute(this::listenForAnnouncements);
        scheduler.scheduleAtFixedRate(this::announceSelf, 0, announceIntervalMs, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::pruneExpiredNodes, evictAfterMs, Math.max(announceIntervalMs, 1000), TimeUnit.MILLISECONDS);
        if (localScanEnabled) {
            scheduler.scheduleAtFixedRate(this::scanLocalPorts, 0, Math.max(announceIntervalMs * 2, 1500), TimeUnit.MILLISECONDS);
        }
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
        try {
            Announcement announcement = new Announcement();
            announcement.nodeId = nodeId;
            announcement.uri = selfUri.toString();
            announcement.timestamp = System.currentTimeMillis();
            byte[] payload = objectMapper.writeValueAsString(announcement).getBytes(StandardCharsets.UTF_8);

            Set<InetAddress> destinations = new LinkedHashSet<>();
            destinations.add(InetAddress.getByName("255.255.255.255"));
            destinations.addAll(interfaceBroadcastAddresses());

            try (DatagramSocket socket = new DatagramSocket()) {
                socket.setBroadcast(true);
                for (InetAddress destination : destinations) {
                    DatagramPacket packet = new DatagramPacket(payload, payload.length, destination, discoveryPort);
                    socket.send(packet);
                }
            }

            upsert(nodeId, selfUri, true, announcement.timestamp);
        } catch (Exception ignored) {
        }
    }

    private List<InetAddress> interfaceBroadcastAddresses() {
        List<InetAddress> broadcasts = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (!networkInterface.isUp() || networkInterface.isLoopback()) {
                    continue;
                }
                for (InterfaceAddress address : networkInterface.getInterfaceAddresses()) {
                    if (address.getBroadcast() != null) {
                        broadcasts.add(address.getBroadcast());
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return broadcasts;
    }

    private void listenForAnnouncements() {
        try (DatagramSocket socket = new DatagramSocket(null)) {
            socket.setReuseAddress(true);
            socket.setBroadcast(true);
            socket.bind(new InetSocketAddress(discoveryPort));
            socket.setSoTimeout(1000);
            listenerSocket = socket;

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

    private void scanLocalPorts() {
        for (int port = localScanStartPort; port <= localScanEndPort; port++) {
            if (port == selfUri.getPort()) {
                continue;
            }
            probeUri("http://127.0.0.1:" + port);
            probeUri("http://localhost:" + port);
        }
    }

    private void probeUri(String candidateUri) {
        try {
            NodeStatus status = httpClient.get(candidateUri + "/raft/state", NodeStatus.class);
            if (status == null || status.nodeId == null || status.nodeId.isBlank()) {
                return;
            }
            String uriFromStatus = status.nodeUri == null || status.nodeUri.isBlank() ? candidateUri : status.nodeUri;
            upsert(status.nodeId, URI.create(uriFromStatus), status.nodeId.equals(nodeId), System.currentTimeMillis());
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
