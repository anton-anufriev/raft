package org.def4fx.raft.queue.api;

import org.agrona.DirectBuffer;

import java.io.Closeable;

public interface Poller extends Closeable {
    boolean poll(DirectBuffer buffer);
    void close();
}
