package com.raftviz.RaftViz.raft;

import com.raftviz.RaftViz.discovery.ClusterMembershipService;
import com.raftviz.RaftViz.model.LogEntry;
import com.raftviz.RaftViz.model.NodeRole;
import com.raftviz.RaftViz.model.NodeStatus;
import com.raftviz.RaftViz.rpc.AppendEntriesRequest;
import com.raftviz.RaftViz.rpc.AppendEntriesResponse;
import com.raftviz.RaftViz.rpc.RequestVoteRequest;
import com.raftviz.RaftViz.rpc.RequestVoteResponse;
import com.raftviz.RaftViz.util.HttpClient;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class RaftNode {
    private final String nodeId;
    private final URI self;
    private final ClusterMembershipService membershipService;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final HttpClient http = new HttpClient();

    private volatile NodeRole role = NodeRole.FOLLOWER;
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    private volatile String votedFor = null;
    private volatile String leaderId = null;

    private final List<LogEntry> log = new ArrayList<>();
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;

    private final java.util.Map<String, Long> nextIndex = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.Map<String, Long> matchIndex = new java.util.concurrent.ConcurrentHashMap<>();

    private final Random rnd = new Random();

    private final int electionTimeoutMin;
    private final int electionTimeoutMax;
    private final int heartbeatInterval;

    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTask;

    public RaftNode(
            @Value("${raft.node-id}") String nodeId,
            @Value("${raft.advertise-addr}") URI self,
            ClusterMembershipService membershipService,
            @Value("${raft.election-timeout-min}") int electionTimeoutMin,
            @Value("${raft.election-timeout-max}") int electionTimeoutMax,
            @Value("${raft.heartbeat-interval}") int heartbeatInterval
    ) {
        this.nodeId = nodeId;
        this.self = self;
        this.membershipService = membershipService;
        this.electionTimeoutMin = electionTimeoutMin;
        this.electionTimeoutMax = electionTimeoutMax;
        this.heartbeatInterval = heartbeatInterval;
    }

    @PostConstruct
    public void start() {
        resetElectionTimer();
    }

    private void resetElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
        }
        int delay = rnd.nextInt(electionTimeoutMax - electionTimeoutMin + 1) + electionTimeoutMin;
        electionTimer = scheduler.schedule(this::startElection, delay, TimeUnit.MILLISECONDS);
    }

    private void cancelElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
        }
    }

    private void startHeartbeat() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
        }
        heartbeatTask = scheduler.scheduleAtFixedRate(this::broadcastReplication, 0, heartbeatInterval, TimeUnit.MILLISECONDS);
    }

    private void stopHeartbeat() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
        }
    }

    private long lastLogIndex() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).index;
    }

    private int lastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).term;
    }

    private boolean logIsUpToDate(long idx, int term) {
        int myLastTerm = lastLogTerm();
        if (term != myLastTerm) {
            return term > myLastTerm;
        }
        return idx >= lastLogIndex();
    }

    private synchronized void startElection() {
        if (role == NodeRole.LEADER) {
            return;
        }

        stopHeartbeat();
        role = NodeRole.CANDIDATE;
        currentTerm.incrementAndGet();
        votedFor = nodeId;
        leaderId = null;
        int term = currentTerm.get();

        List<ClusterMembershipService.Peer> peers = membershipService.activePeers();
        int totalNodes = peers.size() + 1;
        AtomicInteger votes = new AtomicInteger(1);
        CountDownLatch latch = new CountDownLatch(peers.size());

        RequestVoteRequest req = new RequestVoteRequest();
        req.candidateId = nodeId;
        req.term = term;
        req.lastLogIndex = lastLogIndex();
        req.lastLogTerm = lastLogTerm();

        for (ClusterMembershipService.Peer peer : peers) {
            CompletableFuture.runAsync(() -> {
                try {
                    RequestVoteResponse resp = http.post(peer.uri() + "/raft/requestVote", req, RequestVoteResponse.class);
                    synchronized (this) {
                        if (resp == null) {
                            return;
                        }
                        if (resp.term > currentTerm.get()) {
                            becomeFollower(resp.term, null);
                        } else if (resp.voteGranted && term == currentTerm.get() && role == NodeRole.CANDIDATE) {
                            votes.incrementAndGet();
                        }
                    }
                } catch (Exception ignored) {
                } finally {
                    latch.countDown();
                }
            });
        }

        scheduler.execute(() -> {
            try {
                latch.await(800, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
            }
            synchronized (this) {
                if (role == NodeRole.CANDIDATE && term == currentTerm.get() && votes.get() > totalNodes / 2) {
                    becomeLeader(peers);
                } else if (role == NodeRole.CANDIDATE) {
                    becomeFollower(currentTerm.get(), null);
                }
            }
        });
    }

    private void becomeLeader(List<ClusterMembershipService.Peer> peers) {
        role = NodeRole.LEADER;
        leaderId = nodeId;
        cancelElectionTimer();
        long next = lastLogIndex() + 1;
        nextIndex.clear();
        matchIndex.clear();
        for (ClusterMembershipService.Peer peer : peers) {
            nextIndex.put(peer.id(), next);
            matchIndex.put(peer.id(), 0L);
        }
        startHeartbeat();
    }

    private void becomeFollower(int term, String newLeaderId) {
        currentTerm.set(term);
        role = NodeRole.FOLLOWER;
        votedFor = null;
        leaderId = newLeaderId;
        stopHeartbeat();
        resetElectionTimer();
    }

    private void broadcastReplication() {
        if (role != NodeRole.LEADER) {
            return;
        }
        List<ClusterMembershipService.Peer> peers = membershipService.activePeers();
        for (ClusterMembershipService.Peer peer : peers) {
            sendAppendEntries(peer);
        }
    }

    private void sendAppendEntries(ClusterMembershipService.Peer peer) {
        AppendEntriesRequest req;
        synchronized (this) {
            if (role != NodeRole.LEADER) {
                return;
            }
            long next = nextIndex.computeIfAbsent(peer.id(), ignored -> lastLogIndex() + 1);
            long prevIdx = next - 1;
            int prevTerm = prevIdx == 0 ? 0 : log.get((int) prevIdx - 1).term;

            req = new AppendEntriesRequest();
            req.leaderId = nodeId;
            req.term = currentTerm.get();
            req.prevLogIndex = prevIdx;
            req.prevLogTerm = prevTerm;
            req.entries = entriesFrom(next);
            req.leaderCommit = commitIndex;
        }

        CompletableFuture.runAsync(() -> {
            try {
                AppendEntriesResponse resp = http.post(peer.uri() + "/raft/appendEntries", req, AppendEntriesResponse.class);
                synchronized (this) {
                    if (resp == null || role != NodeRole.LEADER) {
                        return;
                    }
                    if (resp.term > currentTerm.get()) {
                        becomeFollower(resp.term, null);
                        return;
                    }
                    if (resp.success) {
                        matchIndex.put(peer.id(), resp.matchIndex);
                        nextIndex.put(peer.id(), resp.matchIndex + 1);
                        advanceCommitIndex();
                    } else {
                        long fallbackIndex = Math.max(1, resp.matchIndex + 1);
                        nextIndex.put(peer.id(), fallbackIndex);
                    }
                }
            } catch (Exception ignored) {
            }
        });
    }

    private List<LogEntry> entriesFrom(long nextIndexValue) {
        if (nextIndexValue > log.size()) {
            return List.of();
        }
        return new ArrayList<>(log.subList((int) nextIndexValue - 1, log.size()));
    }

    private void advanceCommitIndex() {
        if (log.isEmpty()) {
            return;
        }
        List<Long> matches = new ArrayList<>(matchIndex.values());
        matches.add(lastLogIndex());
        matches.sort(Comparator.naturalOrder());
        long majorityIndex = matches.get(matches.size() / 2);
        if (majorityIndex > commitIndex && majorityIndex <= log.size() && log.get((int) majorityIndex - 1).term == currentTerm.get()) {
            commitIndex = majorityIndex;
            lastApplied = Math.max(lastApplied, commitIndex);
        }
    }

    public synchronized RequestVoteResponse onRequestVote(RequestVoteRequest request) {
        RequestVoteResponse resp = new RequestVoteResponse();
        resp.term = currentTerm.get();

        if (request.term < currentTerm.get()) {
            resp.voteGranted = false;
            return resp;
        }

        if (request.term > currentTerm.get()) {
            becomeFollower(request.term, null);
        }

        boolean upToDate = logIsUpToDate(request.lastLogIndex, request.lastLogTerm);
        boolean canVote = votedFor == null || votedFor.equals(request.candidateId);

        resp.voteGranted = canVote && upToDate;
        if (resp.voteGranted) {
            votedFor = request.candidateId;
            leaderId = null;
            resetElectionTimer();
        }
        resp.term = currentTerm.get();
        return resp;
    }

    public synchronized AppendEntriesResponse onAppendEntries(AppendEntriesRequest request) {
        AppendEntriesResponse resp = new AppendEntriesResponse();
        resp.term = currentTerm.get();

        if (request.term < currentTerm.get()) {
            resp.success = false;
            resp.matchIndex = lastLogIndex();
            return resp;
        }

        if (request.term > currentTerm.get() || role != NodeRole.FOLLOWER) {
            becomeFollower(request.term, request.leaderId);
        } else {
            leaderId = request.leaderId;
            resetElectionTimer();
        }

        if (request.prevLogIndex > 0) {
            if (request.prevLogIndex > log.size()) {
                resp.success = false;
                resp.matchIndex = lastLogIndex();
                resp.term = currentTerm.get();
                return resp;
            }
            int prevTerm = log.get((int) request.prevLogIndex - 1).term;
            if (prevTerm != request.prevLogTerm) {
                while (log.size() >= request.prevLogIndex) {
                    log.remove(log.size() - 1);
                }
                resp.success = false;
                resp.matchIndex = lastLogIndex();
                resp.term = currentTerm.get();
                return resp;
            }
        }

        if (request.entries != null) {
            for (LogEntry entry : request.entries) {
                if (entry.index <= log.size()) {
                    LogEntry existing = log.get((int) entry.index - 1);
                    if (existing.term != entry.term || !java.util.Objects.equals(existing.command, entry.command)) {
                        while (log.size() >= entry.index) {
                            log.remove(log.size() - 1);
                        }
                        log.add(new LogEntry(entry.term, entry.index, entry.command));
                    }
                } else {
                    log.add(new LogEntry(entry.term, entry.index, entry.command));
                }
            }
        }

        if (request.leaderCommit > commitIndex) {
            commitIndex = Math.min(request.leaderCommit, lastLogIndex());
            lastApplied = Math.max(lastApplied, commitIndex);
        }

        resp.success = true;
        resp.matchIndex = lastLogIndex();
        resp.term = currentTerm.get();
        return resp;
    }

    public synchronized boolean isLeader() {
        return role == NodeRole.LEADER;
    }

    public void triggerElectionNow() {
        scheduler.execute(this::startElection);
    }

    public synchronized int bumpTerm() {
        int newTerm = currentTerm.incrementAndGet();
        role = NodeRole.FOLLOWER;
        votedFor = null;
        leaderId = null;
        stopHeartbeat();
        resetElectionTimer();
        return newTerm;
    }

    public synchronized int stepDown() {
        if (role == NodeRole.LEADER) {
            role = NodeRole.FOLLOWER;
            leaderId = null;
            votedFor = null;
            stopHeartbeat();
            resetElectionTimer();
        }
        return currentTerm.get();
    }

    public synchronized int bumpTermAndTriggerElection() {
        int newTerm = bumpTerm();
        scheduler.execute(this::startElection);
        return newTerm;
    }

    public synchronized String currentLeader() {
        return leaderId;
    }

    public synchronized String currentLeaderUri() {
        if (leaderId == null) {
            return null;
        }
        if (leaderId.equals(nodeId)) {
            return self.toString();
        }
        Optional<URI> leaderUri = membershipService.findUriByNodeId(leaderId);
        return leaderUri.map(URI::toString).orElse(null);
    }

    public synchronized long appendClientCommand(String command) {
        if (role != NodeRole.LEADER) {
            throw new IllegalStateException("not leader");
        }
        LogEntry entry = new LogEntry(currentTerm.get(), lastLogIndex() + 1, command);
        log.add(entry);
        for (ClusterMembershipService.Peer peer : membershipService.activePeers()) {
            sendAppendEntries(peer);
        }
        return entry.index;
    }

    public synchronized List<LogEntry> logs() {
        return log.stream()
                .map(entry -> new LogEntry(entry.term, entry.index, entry.command))
                .toList();
    }

    public synchronized NodeStatus status() {
        NodeStatus status = new NodeStatus();
        status.nodeId = nodeId;
        status.nodeUri = self.toString();
        status.role = role.name();
        status.currentTerm = currentTerm.get();
        status.leaderId = leaderId;
        status.leaderUri = currentLeaderUri();
        status.commitIndex = commitIndex;
        status.lastApplied = lastApplied;
        status.logSize = log.size();
        List<ClusterMembershipService.Peer> peers = membershipService.activePeers();
        status.clusterSize = peers.size() + 1;
        status.peers = peers.stream().map(peer -> peer.uri().toString()).toList();
        return status;
    }
}
