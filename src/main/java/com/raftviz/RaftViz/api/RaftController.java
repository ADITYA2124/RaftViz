package com.raftviz.RaftViz.api;

import com.raftviz.RaftViz.model.NodeStatus;
import com.raftviz.RaftViz.raft.RaftNode;
import com.raftviz.RaftViz.rpc.AppendEntriesRequest;
import com.raftviz.RaftViz.rpc.AppendEntriesResponse;
import com.raftviz.RaftViz.rpc.RequestVoteRequest;
import com.raftviz.RaftViz.rpc.RequestVoteResponse;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/raft")
public class RaftController {
    private final RaftNode node;
    public RaftController(RaftNode node) { this.node = node; }


    @PostMapping("/requestVote")
    public RequestVoteResponse requestVote(@RequestBody RequestVoteRequest r) {
        return node.onRequestVote(r);
    }


    @PostMapping("/appendEntries")
    public AppendEntriesResponse appendEntries(@RequestBody AppendEntriesRequest r) {
        return node.onAppendEntries(r);
    }


    @GetMapping("/state")
    public NodeStatus state() { return node.status(); }
}
