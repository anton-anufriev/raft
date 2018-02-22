package org.dev4fx.raft.transport;

import org.agrona.collections.Int2ObjectHashMap;

import java.util.Objects;
import java.util.function.IntFunction;

public class DefaultPublishers implements Publishers {
    private final IntFunction<Publisher> publisherFactory;
    private final Int2ObjectHashMap<Publisher> publisherMap;

    public DefaultPublishers(final IntFunction<Publisher> publisherFactory) {
        this.publisherFactory = Objects.requireNonNull(publisherFactory);
        this.publisherMap = new Int2ObjectHashMap<>();
    }

    @Override
    public Publisher lookup(final int serverId) {
        return publisherMap.computeIfAbsent(serverId, publisherFactory);
    }
}
