package org.dev4fx.raft.distributed.map;

import java.io.Serializable;

public interface MapCommand<K extends Serializable, V extends Serializable> {
    void accept(long sequence, MapCommandHandler<K, V> commandHandler);
}
