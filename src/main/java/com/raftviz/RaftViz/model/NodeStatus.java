package com.raftviz.RaftViz.model;

import java.util.List;

public class NodeStatus {
    public String nodeId;
    public String role;
    public int currentTerm;
    public String leaderId;
    public long commitIndex;
    public long lastApplied;
    public int logSize;
    public List<String> peers; // URIs
}
