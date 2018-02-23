package org.dev4fx.raft.state;

import org.agrona.MutableDirectBuffer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.process.ProcessStep;

import java.util.Objects;

public class CommittedLogPromoter implements ProcessStep {
    private final PersistentState persistentState;
    private final VolatileState volatileState;
    private final MessageHandler stateMachine;
    private final MutableDirectBuffer commandDecoderBuffer;
    private final int maxBatchSize;


    public CommittedLogPromoter(final PersistentState persistentState,
                                final VolatileState volatileState,
                                final MessageHandler stateMachine,
                                final MutableDirectBuffer commandDecoderBuffer,
                                final int maxBatchSize) {
        this.persistentState = Objects.requireNonNull(persistentState);
        this.volatileState = Objects.requireNonNull(volatileState);
        this.stateMachine = Objects.requireNonNull(stateMachine);
        this.commandDecoderBuffer = Objects.requireNonNull(commandDecoderBuffer);
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public boolean execute() {
        long lastApplied = volatileState.lastApplied();
        int appliedCount = 0;
        while (lastApplied < volatileState.commitIndex() && appliedCount < maxBatchSize) {
            lastApplied++;
            appliedCount++;
            persistentState.wrap(lastApplied, commandDecoderBuffer);
            stateMachine.onMessage(commandDecoderBuffer, 0, commandDecoderBuffer.capacity());
            volatileState.lastApplied(lastApplied);
        }
        return appliedCount > 0;
    }
}
