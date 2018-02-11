package org.def4fx.raft.queue.api;

import org.agrona.DirectBuffer;

import java.io.Closeable;

/**
 * Appends messages to a {@link Queue}.
 */
public interface Appender extends Closeable {
    boolean append(DirectBuffer buffer, int offset, int length);
    void close();
}
