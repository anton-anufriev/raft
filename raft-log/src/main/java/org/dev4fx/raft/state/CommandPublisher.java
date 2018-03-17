package org.dev4fx.raft.state;

import org.agrona.DirectBuffer;

public interface CommandPublisher {
    boolean publish(int sourceId, long sequence, DirectBuffer buffer, int offset, int length);
}
