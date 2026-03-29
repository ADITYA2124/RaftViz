package com.raftviz.RaftViz.model;

public class LogEntry {
    public final int term;
    public final long index;
    public final String command;
    public LogEntry(int term, long index, String command) {
        this.term = term; this.index = index; this.command = command;
    }
}