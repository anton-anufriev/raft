package org.def4fx.raft.queue.api;

import java.io.Closeable;

/**
 * A pile of messages of varying byte length. Messages can only be appended or
 * sequentially read.
 */
public interface Queue extends Closeable {
    Appender appender();
    Enumerator enumerator();
    void close();
}
