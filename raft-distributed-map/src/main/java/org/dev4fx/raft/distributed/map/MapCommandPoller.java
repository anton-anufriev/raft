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
package org.dev4fx.raft.distributed.map;

import org.dev4fx.raft.process.ProcessStep;

import java.io.Serializable;
import java.util.Objects;
import java.util.Queue;
import java.util.function.LongSupplier;
import java.util.function.ObjLongConsumer;

public class MapCommandPoller<K extends Serializable, V extends Serializable> implements ProcessStep {
    private final Queue<? extends MapCommand<K, V>> commandsQueue;
    private final ObjLongConsumer<? super MapCommand<K, V>> currentCommands;
    private final LongSupplier newSequenceGen;
    private final MapCommandHandler<K, V> encodingMapCommandHandler;

    public MapCommandPoller(final Queue<? extends MapCommand<K, V>> commandsQueue,
                            final ObjLongConsumer<? super MapCommand<K, V>> currentCommands,
                            final LongSupplier newSequenceGen,
                            final MapCommandHandler<K, V> encodingMapCommandHandler) {
        this.commandsQueue = Objects.requireNonNull(commandsQueue);
        this.currentCommands = Objects.requireNonNull(currentCommands);
        this.newSequenceGen = Objects.requireNonNull(newSequenceGen);
        this.encodingMapCommandHandler = Objects.requireNonNull(encodingMapCommandHandler);
    }

    @Override
    public boolean execute() {
        final MapCommand<K, V> command = commandsQueue.poll();
        if (command != null) {
            final long newSequence = newSequenceGen.getAsLong();
            command.accept(newSequence, encodingMapCommandHandler);
            currentCommands.accept(command, newSequence);
            return true;
        }
        return false;
    }
}