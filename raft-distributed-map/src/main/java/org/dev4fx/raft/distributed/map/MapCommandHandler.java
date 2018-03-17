package org.dev4fx.raft.distributed.map;

import java.io.Serializable;

public interface MapCommandHandler<K extends Serializable, V extends Serializable> {
    default void onCommand(final long sequence, final PutCommand<K, V> putCommand) {}
    default void onCommand(final long sequence, final RemoveCommand<K, V> removeCommand) {};
    default void onCommand(final long sequence, final PutAllCommand<K, V> putAllCommand) {};
    default void onCommand(final long sequence, final ClearCommand clearCommand) {};
}