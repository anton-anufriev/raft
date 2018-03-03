package org.dev4fx.raft.state;

public interface VolatileState {
    long commitIndex();

    VolatileState commitIndex(long commitIndex);

    long lastApplied();

    VolatileState lastApplied(long lastApplied);
}
