package org.dev4fx.raft.transport;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.collections.Int2ObjectHashMap;
import org.dev4fx.raft.state.MessageHandler;

import java.util.Objects;
import java.util.function.IntFunction;

public interface PollerFactory {
    Poller create(MessageHandler messageHandler, int messageLimit);

    static IntFunction<PollerFactory> aeronServerToPollerFactory(final Aeron aeron, final IntFunction<String> serverToChannel) {
        Objects.requireNonNull(aeron);
        Objects.requireNonNull(serverToChannel);
        final Int2ObjectHashMap<PollerFactory> pollerFactoryMap = new Int2ObjectHashMap<>();
        final IntFunction<PollerFactory> serverToPollerFactory = serverId -> aeronPollerFactory(aeron, serverToChannel.apply(serverId), serverId);
        return serverId -> pollerFactoryMap.computeIfAbsent(serverId, serverToPollerFactory);
    }

    static PollerFactory aeronPollerFactory(final Aeron aeron, final String channel, final int streamId) {
        Objects.requireNonNull(aeron);
        Objects.requireNonNull(channel);
        return (messageHandler, messageLimit) -> {
            Objects.requireNonNull(messageHandler);
            final Subscription subscription = aeron.addSubscription(channel, streamId);
            final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> messageHandler.onMessage(buffer, offset, length);
            return () -> subscription.poll(fragmentHandler, messageLimit) > 0;
        };
    }
}
