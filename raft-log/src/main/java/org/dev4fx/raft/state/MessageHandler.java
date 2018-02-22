package org.dev4fx.raft.state;

import org.agrona.DirectBuffer;

public interface MessageHandler {
    void onMessage(DirectBuffer source, int offset, int length);
}
