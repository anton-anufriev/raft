package org.def4fx.raft.queue.api;

import org.agrona.DirectBuffer;

import java.io.Closeable;

/**
 * Enumerates messages stored in a {@link Queue}. The enumeration order is generally the appending order of the
 * messages.
 */
public interface Enumerator extends Closeable {
    boolean hasNextMessage();
    DirectBuffer readNextMessage();
    Enumerator skipNextMessage();
    void close();
}
