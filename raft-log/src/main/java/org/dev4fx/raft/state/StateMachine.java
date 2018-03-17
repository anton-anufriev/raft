package org.dev4fx.raft.state;

import org.agrona.DirectBuffer;

public interface StateMachine {
    void onCommand(int sourceId, long sequence, DirectBuffer buffer, int offset, int length);
}