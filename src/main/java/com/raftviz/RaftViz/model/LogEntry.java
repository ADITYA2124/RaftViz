package com.raftviz.RaftViz.model;

public class LogEntry {
    public int term;
    public long index;
    public String command;

    public LogEntry() {
    }

    public LogEntry(int term, long index, String command) {
        this.term = term;
        this.index = index;
        this.command = command;
    }
}
