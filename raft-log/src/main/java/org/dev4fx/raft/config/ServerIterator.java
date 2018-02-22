package org.dev4fx.raft.config;

import org.agrona.collections.IntIntConsumer;

import java.util.function.Consumer;

public interface ServerIterator {
    Consumer<IntIntConsumer> forEach(int serverId, int serverCount);

    static ServerIterator create() {
        return (serverId, serverCount) -> intIntConsumer -> {
            for (int i = 0; i < serverCount; i++) {
                if (i != serverId) intIntConsumer.accept(serverId, i);
            }
        };
    }
}
