package org.dev4fx.raft.distributed.map;

import java.io.Serializable;

public class ValueResultMapCommandHandler<K extends Serializable, V extends Serializable> implements MapCommandHandler<K, V> {

    private V result;

    public void result(final V result) {
        this.result = result;
    }

    @Override
    public void onCommand(final long sequence, final PutCommand<K, V> putCommand) {
        putCommand.setResult(result);
    }

    @Override
    public void onCommand(final long sequence, final RemoveCommand<K, V> removeCommand) {
        removeCommand.setResult(result);
    }

    @Override
    public void onCommand(final long sequence, final PutAllCommand<K, V> putAllCommand) {
        throw new IllegalStateException("PutAllCommand results in Void value");
    }

    @Override
    public void onCommand(final long sequence, final ClearCommand clearCommand) {
        throw new IllegalStateException("ClearCommand results in Void value");
    }
}