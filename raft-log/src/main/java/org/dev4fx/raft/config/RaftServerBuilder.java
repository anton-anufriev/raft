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
package org.dev4fx.raft.config;

import io.aeron.Aeron;
import org.agrona.concurrent.IdleStrategy;
import org.dev4fx.raft.mmap.impl.RegionRingFactory;
import org.dev4fx.raft.process.ProcessStep;
import org.dev4fx.raft.process.Service;
import org.dev4fx.raft.state.StateMachine;
import org.dev4fx.raft.timer.Clock;
import org.dev4fx.raft.transport.PollerFactory;
import org.dev4fx.raft.transport.Publisher;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;

public interface RaftServerBuilder {
    RaftServerBuilder stateMachineFactory(final IntFunction<? extends StateMachine> stateMachineHandler);
    RaftServerBuilder onLeaderTransitionHandler(final IntConsumer onLeaderTransitionHandler);
    RaftServerBuilder onFollowerTransitionHandler(final IntConsumer onFollowerTransitionHandler);
    RaftServerBuilder minElectionTimeoutMillis(final int minElectionTimeoutMillis);
    RaftServerBuilder maxElectionTimeoutMillis(final int maxElectionTimeoutMillis);
    RaftServerBuilder heartbeatTimeoutMillis(final int heartbeatTimeoutMillis);
    RaftServerBuilder maxMessagesPollable(final int maxMessagesPollable);
    RaftServerBuilder maxCommandsPollable(final int maxCommandsPollable);
    RaftServerBuilder maxPromotionBatchSize(final int maxPromotionBatchSize);
    RaftServerBuilder maxAppendBatchSize(final int maxAppendBatchSize);
    RaftServerBuilder regionRingFactory(final RegionRingFactory regionRingFactory);
    RaftServerBuilder regionRingSize(final int regionRingSize);
    RaftServerBuilder indexRegionsToMapAhead(final int indexRegionsToMapAhead);
    RaftServerBuilder payloadRegionsToMapAhead(final int payloadRegionsToMapAhead);
    RaftServerBuilder indexRegionSizeGranularityMultiplier(final int indexRegionSizeGranularityMultiplier);
    RaftServerBuilder payloadRegionSizeGranularityMultiplier(final int payloadRegionSizeGranularityMultiplier);
    RaftServerBuilder encoderBufferSize(final int encoderBufferSize);
    RaftServerBuilder clock(final Clock clock);
    RaftServerBuilder idleStrategyFactory(final IntFunction<? extends IdleStrategy> idleStrategyFactory);
    RaftServerBuilder exceptionHandler(final BiConsumer<? super String, ? super Exception> exceptionHandler);
    RaftServerBuilder gracefulShutdownTimeout(final long gracefulShutdownTimeout, final TimeUnit gracefulShutdownTimeunit);
    RaftServerBuilder applicationProcessStepFactory(IntFunction<? extends ProcessStep> processStepFactory);

    Service.Start build(String logDirectory, int serverId, int clusterSize) throws IOException;

    static RaftServerBuilder forAeronTransport(final Aeron aeron, final String commandChannel, final int commandStreamId,
                                               final IntFunction<String> serverToChannel) {
        return new DefaultRaftServerBuilder(aeron, commandChannel, commandStreamId, serverToChannel);
    }

    static RaftServerBuilder forCustomTransport(final PollerFactory commandPollerFactory,
                                                final IntFunction<? extends PollerFactory> serverToPollerFactory,
                                                final IntFunction<? extends Publisher> serverToPublisherFactory) {
        return new DefaultRaftServerBuilder(commandPollerFactory, serverToPollerFactory, serverToPublisherFactory);
    }
}
