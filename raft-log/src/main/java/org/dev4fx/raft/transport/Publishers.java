package org.dev4fx.raft.transport;

public interface Publishers {
    int ALL = -1;

    Publisher lookup(int serverId);
}
