package com.raftviz.RaftViz.raft;
import com.raftviz.RaftViz.config.ClusterConfig.Peer;
import com.raftviz.RaftViz.model.LogEntry;
import com.raftviz.RaftViz.model.NodeRole;
import com.raftviz.RaftViz.model.NodeStatus;
import com.raftviz.RaftViz.rpc.*;
import com.raftviz.RaftViz.util.HttpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

    @Component
    public class RaftNode {
        private final String nodeId;
        private final URI self;
        private final List<Peer> peers;

        private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        private final HttpClient http = new HttpClient();

        private volatile NodeRole role = NodeRole.FOLLOWER;
        private final AtomicInteger currentTerm = new AtomicInteger(0);
        private volatile String votedFor = null;
        private volatile String leaderId = null;

        private final List<LogEntry> log = new CopyOnWriteArrayList<>();
        private volatile long commitIndex = 0;
        private volatile long lastApplied = 0;

        private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
        private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

        private final Random rnd = new Random();

        private final int electionTimeoutMin;
        private final int electionTimeoutMax;
        private final int heartbeatInterval;

        private ScheduledFuture<?> electionTimer;
        private ScheduledFuture<?> heartbeatTask;

        public RaftNode(
                @Value("${raft.node-id}") String nodeId,
                @Value("${raft.advertise-addr}") URI self,
                List<Peer> peers,
                @Value("${raft.election-timeout-min}") int electionTimeoutMin,
                @Value("${raft.election-timeout-max}") int electionTimeoutMax,
                @Value("${raft.heartbeat-interval}") int heartbeatInterval
        ) {
            this.nodeId = nodeId; this.self = self; this.peers = peers;
            this.electionTimeoutMin = electionTimeoutMin;
            this.electionTimeoutMax = electionTimeoutMax;
            this.heartbeatInterval = heartbeatInterval;
        }

        @PostConstruct
        public void start() {
            resetElectionTimer();
        }

        // --------------- Timers ---------------
        private void resetElectionTimer() {
            if (electionTimer != null) electionTimer.cancel(false);
            int delay = rnd.nextInt(electionTimeoutMax - electionTimeoutMin + 1) + electionTimeoutMin;
            electionTimer = scheduler.schedule(this::startElection, delay, TimeUnit.MILLISECONDS);
        }

        private void startHeartbeat() {
            if (heartbeatTask != null) heartbeatTask.cancel(false);
            heartbeatTask = scheduler.scheduleAtFixedRate(this::broadcastHeartbeat, 0, heartbeatInterval, TimeUnit.MILLISECONDS);
        }

        // --------------- State helpers ---------------
        private long lastLogIndex() { return log.isEmpty() ? 0 : log.get(log.size()-1).index; }
        private int lastLogTerm() { return log.isEmpty() ? 0 : log.get(log.size()-1).term; }

        private boolean logIsUpToDate(long idx, int term) {
            int myLastTerm = lastLogTerm();
            if (term != myLastTerm) return term > myLastTerm;
            return idx >= lastLogIndex();
        }

        // --------------- Elections ---------------
        private synchronized void startElection() {
            role = NodeRole.CANDIDATE;
            currentTerm.incrementAndGet();
            votedFor = nodeId;
            leaderId = null;
            int term = currentTerm.get();

            final AtomicInteger votes = new AtomicInteger(1); //self vote
            CountDownLatch latch = new CountDownLatch(peers.size());

            RequestVoteRequest req = new RequestVoteRequest();
            req.candidateId = nodeId;
            req.term = term;
            req.lastLogIndex = lastLogIndex();
            req.lastLogTerm = lastLogTerm();

            for (Peer p : peers) {
                CompletableFuture.runAsync(() -> {
                    try {
                        RequestVoteResponse resp = http.post(p.uri()+"/raft/requestVote", req, RequestVoteResponse.class);
                        synchronized (this) {
                            if (resp.term > currentTerm.get()) { // discovered higher term
                                currentTerm.set(resp.term);
                                role = NodeRole.FOLLOWER; votedFor = null; leaderId = null;
                            } else if (resp.voteGranted && term == currentTerm.get() && role == NodeRole.CANDIDATE) {
                                votes.incrementAndGet();
                            }
                        }
                    } catch (Exception ignored) {} finally { latch.countDown(); }
                });
            }

            scheduler.execute(() -> {
                try { latch.await(800, TimeUnit.MILLISECONDS); } catch (InterruptedException ignored) {}
                synchronized (this) {
                    if (role == NodeRole.CANDIDATE && votes.get() > peers.size()/2) {
                        becomeLeader();
                    } else {
                        role = NodeRole.FOLLOWER; // back to follower on split vote
                        resetElectionTimer();
                    }
                }
            });
        }

        private void becomeLeader() {
            role = NodeRole.LEADER;
            leaderId = nodeId;
            long next = lastLogIndex()+1;
            for (Peer p : peers) { nextIndex.put(p.id(), next); matchIndex.put(p.id(), 0L); }
            startHeartbeat();
        }

        // --------------- Heartbeats & Replication ---------------
        private void broadcastHeartbeat() {
            for (Peer p : peers) {
                sendAppendEntries(p, List.of());
            }
        }

        private void sendAppendEntries(Peer p, List<LogEntry> entries) {
            int term = currentTerm.get();
            long ni = nextIndex.getOrDefault(p.id(), lastLogIndex()+1);
            long prevIdx = ni - 1;
            int prevTerm = (prevIdx == 0) ? 0 : log.get((int)prevIdx - 1).term;

            AppendEntriesRequest req = new AppendEntriesRequest();
            req.leaderId = nodeId; req.term = term; req.prevLogIndex = prevIdx; req.prevLogTerm = prevTerm;
            req.entries = entries; req.leaderCommit = commitIndex;

            CompletableFuture.runAsync(() -> {
                try {
                    AppendEntriesResponse resp = http.post(p.uri()+"/raft/appendEntries", req, AppendEntriesResponse.class);
                    synchronized (this) {
                        if (resp.term > currentTerm.get()) {
                            currentTerm.set(resp.term);
                            role = NodeRole.FOLLOWER; votedFor = null; leaderId = null;
                            if (heartbeatTask != null) heartbeatTask.cancel(false);
                            resetElectionTimer();
                            return;
                        }
                        if (resp.success) {
                            long mi = (entries.isEmpty() ? prevIdx : entries.get(entries.size()-1).index);
                            matchIndex.put(p.id(), mi);
                            nextIndex.put(p.id(), mi + 1);
                            advanceCommitIndex();
                        } else {
                            // decrement nextIndex and retry at next heartbeat
                            long nextIdx = Math.max(1, nextIndex.getOrDefault(p.id(), 1L) - 1);
                            nextIndex.put(p.id(), nextIdx);
                        }
                    }
                } catch (Exception ignored) {}
            });
        }

        private void advanceCommitIndex() {
            // majority commit
            List<Long> matches = new ArrayList<>(matchIndex.values());
            matches.add(lastLogIndex()); // leader counts itself
            matches.sort(Long::compareTo);
            long majorityIndex = matches.get(matches.size()/2);
            if (majorityIndex > commitIndex && log.get((int)majorityIndex - 1).term == currentTerm.get()) {
                commitIndex = majorityIndex;
                lastApplied = Math.max(lastApplied, commitIndex);
            }
        }

        // --------------- RPC handlers ---------------
        public synchronized RequestVoteResponse onRequestVote(RequestVoteRequest r) {
            RequestVoteResponse resp = new RequestVoteResponse();
            resp.term = currentTerm.get();

            if (r.term < currentTerm.get()) { resp.voteGranted = false; return resp; }

            if (r.term > currentTerm.get()) {
                currentTerm.set(r.term); role = NodeRole.FOLLOWER; votedFor = null; leaderId = null;
            }

            boolean upToDate = logIsUpToDate(r.lastLogIndex, r.lastLogTerm);
            boolean canVote = (votedFor == null || votedFor.equals(r.candidateId));

            resp.voteGranted = canVote && upToDate;
            if (resp.voteGranted) { votedFor = r.candidateId; resetElectionTimer(); }
            resp.term = currentTerm.get();
            return resp;
        }

        public synchronized AppendEntriesResponse onAppendEntries(AppendEntriesRequest r) {
            AppendEntriesResponse resp = new AppendEntriesResponse();
            resp.term = currentTerm.get();

            if (r.term < currentTerm.get()) { resp.success = false; return resp; }

            if (r.term > currentTerm.get() || role != NodeRole.FOLLOWER) {
                currentTerm.set(r.term); role = NodeRole.FOLLOWER; votedFor = null;
            }
            leaderId = r.leaderId; resetElectionTimer();

            // If log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm, fail
            if (r.prevLogIndex > 0) {
                if (r.prevLogIndex > log.size()) { resp.success = false; return resp; }
                int prevTerm = log.get((int)r.prevLogIndex - 1).term;
                if (prevTerm != r.prevLogTerm) {
                    // delete entry and all that follow it
                    while (log.size() >= r.prevLogIndex) log.remove(log.size()-1);
                    resp.success = false; return resp;
                }
            }

            // append any new entries not already in the log
            if (r.entries != null) {
                for (LogEntry e : r.entries) {
                    if (e.index <= log.size()) {
                        if (log.get((int)e.index - 1).term != e.term) {
                            while (log.size() >= e.index) log.remove(log.size()-1);
                            log.add(e);
                        }
                    } else {
                        log.add(e);
                    }
                }
            }

            if (r.leaderCommit > commitIndex) {
                commitIndex = Math.min(r.leaderCommit, lastLogIndex());
                lastApplied = Math.max(lastApplied, commitIndex);
            }

            resp.success = true; resp.matchIndex = lastLogIndex(); resp.term = currentTerm.get();
            return resp;
        }

        // --------------- Client writes ---------------
        public synchronized boolean isLeader() { return role == NodeRole.LEADER; }
        public synchronized String currentLeader() { return leaderId; }

        public synchronized long appendClientCommand(String command) {
            if (role != NodeRole.LEADER) throw new IllegalStateException("not leader");
            LogEntry e = new LogEntry(currentTerm.get(), lastLogIndex()+1, command + " @" + Instant.now());
            log.add(e);
            // replicate to peers (optimistically piggyback on next heartbeat)
            for (Peer p : peers) sendAppendEntries(p, List.of(e));
            return e.index;
        }

        // --------------- Introspection ---------------
        public NodeStatus status() {
            NodeStatus s = new NodeStatus();
            s.nodeId = nodeId; s.role = role.name(); s.currentTerm = currentTerm.get();
            s.leaderId = leaderId; s.commitIndex = commitIndex; s.lastApplied = lastApplied;
            s.logSize = log.size();
            s.peers = peers.stream().map(p -> p.uri().toString()).toList();
            return s;
        }
}