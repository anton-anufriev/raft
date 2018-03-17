package org.dev4fx.raft.distributed.map;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class PutAllCommand<K extends Serializable, V extends Serializable> implements MapCommand<K, V> {
    private final int mapId;
    private final Map<? extends K, ? extends V> values;
    private final FutureResult<Void> futureResult;

    public PutAllCommand(final int mapId,
                         final Map<? extends K, ? extends V> values,
                         final FutureResult<Void> futureResult) {
        this.mapId = mapId;
        this.values = Objects.requireNonNull(values);
        this.futureResult = Objects.requireNonNull(futureResult);
    }

    public int mapId() {
        return mapId;
    }

    public Map<? extends K, ? extends V> values() {
        return values;
    }

    public void setResult() {
        futureResult.accept(null);
    }

    @Override
    public void accept(final long sequence, final MapCommandHandler<K, V> commandHandler) {
        commandHandler.onCommand(sequence, this);
    }
}
