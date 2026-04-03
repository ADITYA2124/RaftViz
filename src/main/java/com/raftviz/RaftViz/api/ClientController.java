package com.raftviz.RaftViz.api;

import com.raftviz.RaftViz.raft.RaftNode;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Tag(name = "Client API", description = "Client-facing endpoints for appending commands to the Raft log")
public class ClientController {
    private final RaftNode node;

    public ClientController(RaftNode node) {
        this.node = node;
    }

    public static class LogReq {
        public String message;
    }

    public static class LogResp {
        public long index;
        public String leader;
    }

    @PostMapping("/log")
    @Operation(summary = "Append a client command", description = "Appends a command through the current leader. Followers redirect to the leader when known.")
    public ResponseEntity<?> append(@RequestBody LogReq req) {
        if (!node.isLeader()) {
            String leaderUri = node.currentLeaderUri();
            if (leaderUri != null) {
                return ResponseEntity.status(HttpStatus.TEMPORARY_REDIRECT)
                        .header(HttpHeaders.LOCATION, leaderUri + "/log")
                        .body("Redirecting to leader: " + leaderUri);
            }
            return ResponseEntity.status(503).body("No leader - try again");
        }

        long idx = node.appendClientCommand(req.message == null ? "(empty)" : req.message);
        LogResp resp = new LogResp();
        resp.index = idx;
        resp.leader = node.currentLeader();
        return ResponseEntity.ok(resp);
    }
}
