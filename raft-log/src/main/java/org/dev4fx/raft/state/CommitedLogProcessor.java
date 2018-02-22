package org.dev4fx.raft.state;

import org.agrona.MutableDirectBuffer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.process.ProcessStep;

import java.util.Objects;

public class CommitedLogProcessor implements ProcessStep {
    private final PersistentState persistentState;
    private final VolatileState volatileState;
    private final MessageHandler stateMachine;
    private final MutableDirectBuffer buffer;
    private final int maxBatchSize;


    public CommitedLogProcessor(final PersistentState persistentState,
                                final VolatileState volatileState,
                                final MessageHandler stateMachine,
                                final MutableDirectBuffer buffer,
                                final int maxBatchSize) {
        this.persistentState = Objects.requireNonNull(persistentState);
        this.volatileState = Objects.requireNonNull(volatileState);
        this.stateMachine = Objects.requireNonNull(stateMachine);
        this.buffer = Objects.requireNonNull(buffer);
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public boolean execute() {
        long lastApplied = volatileState.lastApplied();
        int appliedCount = 0;
        while (lastApplied < volatileState.commitIndex() && appliedCount < maxBatchSize) {
            lastApplied++;
            appliedCount++;
            persistentState.wrap(lastApplied, buffer);
            stateMachine.onMessage(buffer, 0, buffer.capacity());
            volatileState.lastApplied(lastApplied);
        }
        return appliedCount > 0;
    }
}
