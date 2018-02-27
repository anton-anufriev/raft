/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 hover-raft (tools4j), Anton Anufriev, Marco Terzer
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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
