package com.raftviz.RaftViz.api;

import com.raftviz.RaftViz.discovery.ClusterMembershipService;
import com.raftviz.RaftViz.model.ClusterNodeInfo;
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
import java.util.List;

import static org.springframework.http.HttpStatus.NOT_FOUND;

@RestController
@RequestMapping("/raft")
@Tag(name = "Raft API", description = "Internal Raft RPCs plus cluster inspection endpoints")
public class RaftController {
    private final RaftNode node;
    private final ClusterMembershipService membershipService;

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
}
