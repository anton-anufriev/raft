package org.dev4fx.raft.distributed.map;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Future;

public interface DistributedMap<K extends Serializable, V extends Serializable> extends Map<K, V>{
    int mapId();

    Future<V> nonBlockingPut(K key, V value);

    Future<V> nonBlockingRemove(K key);

    Future<Void> nonBlockingPutAll(Map<? extends K, ? extends V> fromMap);

    Future<Void> nonBlockingClear();
}
