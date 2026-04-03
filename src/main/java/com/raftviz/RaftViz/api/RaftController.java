package com.raftviz.RaftViz.api;

import com.raftviz.RaftViz.discovery.ClusterMembershipService;
import com.raftviz.RaftViz.model.ClusterNodeInfo;
import com.raftviz.RaftViz.model.ClusterNodeSnapshot;
import com.raftviz.RaftViz.model.LogEntry;
import com.raftviz.RaftViz.model.NodeStatus;
import com.raftviz.RaftViz.raft.RaftNode;
import com.raftviz.RaftViz.rpc.AppendEntriesRequest;
import com.raftviz.RaftViz.rpc.AppendEntriesResponse;
import com.raftviz.RaftViz.rpc.RequestVoteRequest;
import com.raftviz.RaftViz.rpc.RequestVoteResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.springframework.http.HttpStatus.NOT_FOUND;

@RestController
@RequestMapping("/raft")
@Tag(name = "Raft API", description = "Internal Raft RPCs plus cluster inspection endpoints")
public class RaftController {
    public static class SimulationResponse {
        public String action;
        public String message;
        public NodeStatus state;
    }

    private final RaftNode node;
    private final ClusterMembershipService membershipService;
    private final com.raftviz.RaftViz.util.HttpClient httpClient = new com.raftviz.RaftViz.util.HttpClient();

    public RaftController(RaftNode node, ClusterMembershipService membershipService) {
        this.node = node;
        this.membershipService = membershipService;
    }

    @PostMapping("/requestVote")
    @Operation(summary = "Request a vote", description = "Internal Raft RPC used during leader election.")
    public RequestVoteResponse requestVote(@RequestBody RequestVoteRequest request) {
        return node.onRequestVote(request);
    }

    @PostMapping("/appendEntries")
    @Operation(summary = "Append entries", description = "Internal Raft RPC used for replication and heartbeats.")
    public AppendEntriesResponse appendEntries(@RequestBody AppendEntriesRequest request) {
        return node.onAppendEntries(request);
    }

    @GetMapping("/state")
    @Operation(summary = "Get node state", description = "Returns the current node's role, term, leader, and replication progress.")
    public NodeStatus state() {
        return node.status();
    }

    @GetMapping("/logs")
    @Operation(summary = "Get local node logs", description = "Returns the log entries stored on the current node.")
    public List<LogEntry> logs() {
        return node.logs();
    }

    @GetMapping("/cluster")
    @Operation(summary = "Get discovered cluster members", description = "Returns dynamically discovered nodes and their connectivity status.")
    public List<ClusterNodeInfo> cluster() {
        return membershipService.clusterNodes();
    }

    @GetMapping("/cluster/state")
    @Operation(summary = "Get cluster state snapshot", description = "Returns discovered nodes together with server-side reachability and node state details.")
    public List<ClusterNodeSnapshot> clusterState() {
        List<ClusterNodeSnapshot> snapshots = new ArrayList<>();
        NodeStatus selfStatus = node.status();

        for (ClusterNodeInfo member : membershipService.clusterNodes()) {
            ClusterNodeSnapshot snapshot = new ClusterNodeSnapshot();
            snapshot.node = member;

            if (member.self || selfStatus.nodeId.equals(member.nodeId)) {
                snapshot.state = selfStatus;
                snapshot.reachable = true;
                snapshot.health = "ONLINE";
                snapshots.add(snapshot);
                continue;
            }

            try {
                snapshot.state = httpClient.get(member.uri + "/raft/state", NodeStatus.class);
                snapshot.reachable = snapshot.state != null;
                snapshot.health = snapshot.reachable ? "ONLINE" : (member.online ? "DEGRADED" : "OFFLINE");
            } catch (Exception ignored) {
                snapshot.reachable = false;
                snapshot.health = member.online ? "DEGRADED" : "OFFLINE";
                snapshot.state = offlineState(member);
            }

            snapshots.add(snapshot);
        }

        return snapshots;
    }

    @PostMapping("/simulate/trigger-election")
    @Operation(summary = "Trigger election on this node", description = "Forces this node to start a new election immediately.")
    public SimulationResponse triggerElection() {
        node.triggerElectionNow();
        SimulationResponse response = new SimulationResponse();
        response.action = "trigger-election";
        response.message = "Election triggered on " + node.status().nodeId;
        response.state = node.status();
        return response;
    }

    @PostMapping("/simulate/bump-term")
    @Operation(summary = "Bump current term on this node", description = "Increments this node's term and leaves it as a follower.")
    public SimulationResponse bumpTerm() {
        int term = node.bumpTerm();
        SimulationResponse response = new SimulationResponse();
        response.action = "bump-term";
        response.message = "Term bumped to " + term + " on " + node.status().nodeId;
        response.state = node.status();
        return response;
    }

    @PostMapping("/simulate/bump-term-and-elect")
    @Operation(summary = "Bump term and trigger election", description = "Increments the current term on this node and immediately starts an election.")
    public SimulationResponse bumpTermAndElect() {
        int term = node.bumpTermAndTriggerElection();
        SimulationResponse response = new SimulationResponse();
        response.action = "bump-term-and-elect";
        response.message = "Term bumped to " + term + " and election triggered on " + node.status().nodeId;
        response.state = node.status();
        return response;
    }

    @PostMapping("/simulate/step-down")
    @Operation(summary = "Force leader step-down", description = "If this node is the leader, it steps down to follower so a fresh election can happen.")
    public SimulationResponse stepDown() {
        int term = node.stepDown();
        SimulationResponse response = new SimulationResponse();
        response.action = "step-down";
        response.message = "Node stepped down at term " + term;
        response.state = node.status();
        return response;
    }

    @GetMapping("/nodes/{nodeId}/logs")
    @Operation(summary = "Get logs for a single node", description = "Fetches log entries from one specific node in the discovered cluster.")
    public Object logsForNode(@PathVariable("nodeId") String targetNodeId) {
        NodeStatus selfStatus = node.status();
        if (selfStatus.nodeId.equals(targetNodeId)) {
            return node.logs();
        }
        URI target = membershipService.findUriByNodeId(targetNodeId)
                .orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "Unknown node: " + targetNodeId));
        return new com.raftviz.RaftViz.util.HttpClient().get(target + "/raft/logs", List.class);
    }

    private NodeStatus offlineState(ClusterNodeInfo member) {
        NodeStatus status = new NodeStatus();
        status.nodeId = member.nodeId;
        status.nodeUri = member.uri;
        status.role = "OFFLINE";
        status.currentTerm = 0;
        status.leaderId = null;
        status.leaderUri = null;
        status.commitIndex = 0;
        status.lastApplied = 0;
        status.logSize = 0;
        status.clusterSize = 0;
        status.peers = List.of();
        return status;
    }
}
