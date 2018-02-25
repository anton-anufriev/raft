package org.dev4fx.raft.config;

import io.aeron.Aeron;
import org.agrona.concurrent.IdleStrategy;
import org.dev4fx.raft.mmap.impl.RegionRingFactory;
import org.dev4fx.raft.process.Service;
import org.dev4fx.raft.state.MessageHandler;
import org.dev4fx.raft.timer.Clock;
import org.dev4fx.raft.transport.PollerFactory;
import org.dev4fx.raft.transport.Publisher;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;

public interface RaftServerBuilder {
    RaftServerBuilder stateMachineFactory(final IntFunction<? extends MessageHandler> stateMachineHandler);
    RaftServerBuilder onLeaderTransitionHandler(final IntConsumer onLeaderTransitionHandler);
    RaftServerBuilder onFollowerTransitionHandler(final IntConsumer onFollowerTransitionHandler);
    RaftServerBuilder minElectionTimeoutMillis(final int minElectionTimeoutMillis);
    RaftServerBuilder maxElectionTimeoutMillis(final int maxElectionTimeoutMillis);
    RaftServerBuilder heartbeatTimeoutMillis(final int heartbeatTimeoutMillis);
    RaftServerBuilder maxMessagesPollable(final int maxMessagesPollable);
    RaftServerBuilder maxCommandsPollable(final int maxCommandsPollable);
    RaftServerBuilder maxPromotionBatchSize(final int maxPromotionBatchSize);
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
