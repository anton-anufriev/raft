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
package org.dev4fx.raft.distributed.map.in;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.dev4fx.raft.state.StateMachine;

import java.util.Objects;

public class DeduplicatingStateMachine implements StateMachine {
    private final Long2LongHashMap lastReceivedSourceSequences;
    private final StateMachine delegateStateMachine;

    public DeduplicatingStateMachine(final StateMachine delegateStateMachine,
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
