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
public class RaftController {
    private final RaftNode node;
    private final ClusterMembershipService membershipService;

    public RaftController(RaftNode node, ClusterMembershipService membershipService) {
        this.node = node;
        this.membershipService = membershipService;
    }

    @PostMapping("/requestVote")
    public RequestVoteResponse requestVote(@RequestBody RequestVoteRequest request) {
        return node.onRequestVote(request);
    }

    @PostMapping("/appendEntries")
    public AppendEntriesResponse appendEntries(@RequestBody AppendEntriesRequest request) {
        return node.onAppendEntries(request);
    }

    @GetMapping("/state")
    public NodeStatus state() {
        return node.status();
    }

    @GetMapping("/logs")
    public List<LogEntry> logs() {
        return node.logs();
    }

    @GetMapping("/cluster")
    public List<ClusterNodeInfo> cluster() {
        return membershipService.clusterNodes();
    }

    @GetMapping("/nodes/{nodeId}/logs")
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
