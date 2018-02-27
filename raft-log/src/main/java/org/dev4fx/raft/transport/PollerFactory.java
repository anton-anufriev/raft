/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 hover-raft (tools4j), Anton Anufriev, Marco Terzer
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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
