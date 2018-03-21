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
package org.dev4fx.raft.distributed.map.out;

import org.dev4fx.raft.distributed.map.command.Command;
import org.dev4fx.raft.distributed.map.command.CommandHandler;
import org.dev4fx.raft.process.ProcessStep;

import java.io.Serializable;
import java.util.Objects;
import java.util.Queue;
import java.util.function.LongSupplier;
import java.util.function.ObjLongConsumer;

public class CommandPoller<K extends Serializable, V extends Serializable> implements ProcessStep {
    private final Queue<? extends Command<K, V>> commandQueue;
    private final ObjLongConsumer<? super Command<K, V>> inflightCommands;
    private final LongSupplier newSequenceGen;
    private final CommandHandler<K, V> encodingCommandHandler;

    public CommandPoller(final Queue<? extends Command<K, V>> commandQueue,
                         final ObjLongConsumer<? super Command<K, V>> inflightCommands,
                         final LongSupplier newSequenceGen,
                         final CommandHandler<K, V> encodingCommandHandler) {
        this.commandQueue = Objects.requireNonNull(commandQueue);
        this.inflightCommands = Objects.requireNonNull(inflightCommands);
        this.newSequenceGen = Objects.requireNonNull(newSequenceGen);
        this.encodingCommandHandler = Objects.requireNonNull(encodingCommandHandler);
    }

    @Override
    public boolean execute() {
        final Command<K, V> command = commandQueue.poll();
        if (command != null) {
            final long newSequence = newSequenceGen.getAsLong();
            command.accept(newSequence, encodingCommandHandler);
            inflightCommands.accept(command, newSequence);
            return true;
        }
        return false;
    }
}