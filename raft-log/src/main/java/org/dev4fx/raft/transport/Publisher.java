package org.dev4fx.raft.transport;

import org.agrona.DirectBuffer;

public interface Publisher {
    boolean publish(DirectBuffer buffer, int offset, int length);
}
