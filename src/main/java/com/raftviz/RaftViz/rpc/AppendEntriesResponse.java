package com.raftviz.RaftViz.rpc;

public class AppendEntriesResponse {
    public int term;
    public boolean success;
    public long matchIndex; // highest index known to match on follower
}