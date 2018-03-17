package org.dev4fx.raft.distributed.map;

import java.io.Serializable;

public class VoidResultMapCommandHandler<K extends Serializable, V extends Serializable> implements MapCommandHandler<K, V> {

    @Override
    public void onCommand(final long sequence, final PutCommand<K, V> putCommand) {
        throw new IllegalStateException("PutCommand results in non-Void value");
    }

    @Override
    public void onCommand(final long sequence, final RemoveCommand<K, V> removeCommand) {
        throw new IllegalStateException("RemoveCommand results in non-Void value");
    }

    @Override
    public void onCommand(final long sequence, final PutAllCommand<K, V> putAllCommand) {
        putAllCommand.setResult();
    }

    @Override
    public void onCommand(final long sequence, final ClearCommand clearCommand) {
        clearCommand.setResult();
    }
}