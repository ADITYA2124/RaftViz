package com.raftviz.RaftViz.rpc;

import com.raftviz.RaftViz.model.LogEntry;

import java.util.List;

public class AppendEntriesRequest {
    public String leaderId;
    public int term;
    public long prevLogIndex;
    public int prevLogTerm;
    public List<LogEntry> entries; // may be empty (heartbeat)
    public long leaderCommit;
}