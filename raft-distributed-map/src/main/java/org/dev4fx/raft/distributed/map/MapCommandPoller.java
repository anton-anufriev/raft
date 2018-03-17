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