package com.raftviz.RaftViz.model;

import java.util.List;

public class NodeStatus {
    public String nodeId;
    public String nodeUri;
    public String role;
    public int currentTerm;
    public String leaderId;
    public String leaderUri;
    public long commitIndex;
    public long lastApplied;
    public int logSize;
    public int clusterSize;
    public List<String> peers; // URIs
}
