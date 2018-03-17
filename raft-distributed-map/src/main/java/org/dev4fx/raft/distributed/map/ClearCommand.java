package org.dev4fx.raft.distributed.map;

import java.io.Serializable;
import java.util.Objects;

public class ClearCommand<K extends Serializable, V extends Serializable> implements MapCommand<K, V> {
    private final int mapId;
    private final FutureResult<Void> futureResult;

    public ClearCommand(final int mapId,
                        final FutureResult<Void> futureResult) {
        this.mapId = mapId;
        this.futureResult = Objects.requireNonNull(futureResult);
    }

    public int mapId() {
        return mapId;
    }

    public void setResult() {
        futureResult.accept(null);
    }

    @Override
    public void accept(final long sequence, final MapCommandHandler<K, V> commandHandler) {
        commandHandler.onCommand(sequence, this);
    }
}
