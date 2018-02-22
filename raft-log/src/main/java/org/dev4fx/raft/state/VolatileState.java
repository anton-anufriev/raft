package org.dev4fx.raft.state;

public final class VolatileState {

    private long commitIndex = -1;
    private long lastApplied = -1;

    public long commitIndex() {
        return commitIndex;
    }

    public VolatileState commitIndex(final long commitIndex) {
        this.commitIndex = commitIndex;
        return this;
    }

    public long lastApplied() {
        return lastApplied;
    }

    public VolatileState lastApplied(final long lastApplied) {
        this.lastApplied = lastApplied;
        return this;
    }
}
