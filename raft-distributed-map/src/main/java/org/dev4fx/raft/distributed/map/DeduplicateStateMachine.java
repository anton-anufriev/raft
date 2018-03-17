package org.dev4fx.raft.distributed.map;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.dev4fx.raft.state.StateMachine;

import java.util.Objects;

public class DeduplicateStateMachine implements StateMachine {
    private final Long2LongHashMap lastReceivedSourceSequences;
    private final StateMachine delegateStateMachine;

    public DeduplicateStateMachine(final StateMachine delegateStateMachine,
                                   final Long2LongHashMap lastReceivedSourceSequences) {
        this.lastReceivedSourceSequences = Objects.requireNonNull(lastReceivedSourceSequences);
        this.delegateStateMachine = Objects.requireNonNull(delegateStateMachine);
    }

    @Override
    public void onCommand(final int sourceId, final long sequence, final DirectBuffer buffer, final int offset, final int length) {
        final long lastReceivedSourceSequence = lastReceivedSourceSequences.get(sourceId);
        if (sequence > lastReceivedSourceSequence) {
            delegateStateMachine.onCommand(sourceId, sequence, buffer, offset, length);
            lastReceivedSourceSequences.put(sourceId, sequence);
        }
    }
}
