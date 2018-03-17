package org.dev4fx.raft.distributed.map;

import java.io.Serializable;
import java.util.Objects;

public class PutCommand<K extends Serializable, V extends Serializable> implements MapCommand<K, V> {
    private final int mapId;
    private final K key;
    private final V value;
    private final FutureResult<? super V> futureResult;

    public PutCommand(final int mapId,
                      final K key,
                      final V value,
                      final FutureResult<? super V> futureResult) {
        this.mapId = mapId;
        this.key = Objects.requireNonNull(key);
        this.value = Objects.requireNonNull(value);
        this.futureResult = Objects.requireNonNull(futureResult);
    }

    public int mapId() {
        return mapId;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }

    public void setResult(final V result) {
        futureResult.accept(result);
    }

    @Override
    public void accept(long sequence, final MapCommandHandler<K, V> commandHandler) {
        commandHandler.onCommand(sequence, this);
    }
}
