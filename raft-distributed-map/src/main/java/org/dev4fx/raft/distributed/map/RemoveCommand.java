package org.dev4fx.raft.distributed.map;

import java.io.Serializable;
import java.util.Objects;

public class RemoveCommand<K extends Serializable, V extends Serializable> implements MapCommand<K, V> {
    private final int mapId;
    private final K key;
    private final FutureResult<? super V> futureResult;

    public RemoveCommand(final int mapId,
                      final K key,
                      final FutureResult<? super V> futureResult) {
        this.mapId = mapId;
        this.key = Objects.requireNonNull(key);
        this.futureResult = Objects.requireNonNull(futureResult);
    }

    public int mapId() {
        return mapId;
    }

    public K key() {
        return key;
    }

    public void setResult(final V result) {
        futureResult.accept(result);
    }

    @Override
    public void accept(final long sequence, final MapCommandHandler<K, V> commandHandler) {
        commandHandler.onCommand(sequence, this);
    }
}