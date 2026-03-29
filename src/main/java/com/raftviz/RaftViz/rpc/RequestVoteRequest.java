package com.raftviz.RaftViz.rpc;

public class RequestVoteRequest {
    public String candidateId;
    public int term;
    public long lastLogIndex;
    public int lastLogTerm;
}