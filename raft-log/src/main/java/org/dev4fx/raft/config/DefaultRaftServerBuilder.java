package org.dev4fx.raft.config;

import io.aeron.Aeron;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.def4fx.raft.mmap.api.FileSizeEnsurer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.log.impl.DefaultPersistentState;
import org.dev4fx.raft.mmap.impl.MappedFile;
import org.dev4fx.raft.mmap.impl.RegionFactory;
import org.dev4fx.raft.mmap.impl.RegionRingAccessor;
import org.dev4fx.raft.mmap.impl.RegionRingFactory;
import org.dev4fx.raft.process.Process;
import org.dev4fx.raft.process.ProcessStep;
import org.dev4fx.raft.process.Service;
import org.dev4fx.raft.sbe.*;
import org.dev4fx.raft.state.*;
import org.dev4fx.raft.timer.Clock;
import org.dev4fx.raft.timer.Timer;
import org.dev4fx.raft.transport.LoggingPublisher;
import org.dev4fx.raft.transport.Poller;
import org.dev4fx.raft.transport.PollerFactory;
import org.dev4fx.raft.transport.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.*;
import java.util.stream.IntStream;

public class DefaultRaftServerBuilder implements RaftServerBuilder {
    private static final BiConsumer<? super String, ? super Exception> DEFAULT_EXCEPTION_HANDLER = (s, e) -> {
        e.printStackTrace();
        throw new RuntimeException(e);
    };
    private static final IntFunction<? extends IdleStrategy>  DEFAULT_IDLE_STRATEGY_FACTORY = serverId -> new BackoffIdleStrategy(100L, 100L, 10L, 100L);
    private static final RegionRingFactory DEFAULT_REGION_RING_FACTORY = RegionRingFactory.forSync(RegionFactory.SYNC);
    private static final IntFunction<? extends MessageHandler> DEFAULT_STATE_MACHINE_FACTORY = LoggingStateMachine::new;
    private static final long MAX_FILE_SIZE = 64 * 16 * 1024 * 1024;

    private final PollerFactory commandPollerFactory;
    private final IntFunction<? extends PollerFactory> serverToPollerFactory;
    private final IntFunction<? extends Publisher> serverToPublisherFactory;

    private IntFunction<? extends MessageHandler> stateMachineFactory;
    private IntConsumer onLeaderTransitionHandler = serverId -> {};
    private IntConsumer onFollowerTransitionHandler = serverId -> {};
    private int minElectionTimeoutMillis = 1100;
    private int maxElectionTimeoutMillis = 1500;
    private int heartbeatTimeoutMillis = 1000;
    private int maxMessagesPollable = 1;
    private int maxCommandsPollable = 1;
    private int maxPromotionBatchSize = 1;
    private int maxAppendBatchSize = 1;
    private RegionRingFactory regionRingFactory;
    private int regionRingSize = 4;
    private int indexRegionsToMapAhead = 1;
    private int payloadRegionsToMapAhead = 1;
    private int indexRegionSizeGranularityMultiplier = 512;
    private int payloadRegionSizeGranularityMultiplier = 1024;
    private int encoderBufferSize = 8024;
    private Clock clock = Clock.DEFAULT;
    private IntFunction<? extends IdleStrategy> idleStrategyFactory;
    private BiConsumer<? super String, ? super Exception> exceptionHandler;
    private long gracefulShutdownTimeout = 10;
    private TimeUnit gracefulShutdownTimeunit = TimeUnit.SECONDS;

    public DefaultRaftServerBuilder(final Aeron aeron,
                                    final String commandChannel,
                                    final int commandStreamId,
                                    final IntFunction<String> serverToChannel) {
        this.exceptionHandler = DEFAULT_EXCEPTION_HANDLER;
        this.idleStrategyFactory = DEFAULT_IDLE_STRATEGY_FACTORY;
        this.regionRingFactory = DEFAULT_REGION_RING_FACTORY;
        this.stateMachineFactory = DEFAULT_STATE_MACHINE_FACTORY;

        this.commandPollerFactory = PollerFactory.aeronPollerFactory(aeron, commandChannel, commandStreamId);
        this.serverToPollerFactory = PollerFactory.aeronServerToPollerFactory(aeron, serverToChannel);
        this.serverToPublisherFactory = serverId -> Publisher.aeronPublisher(aeron, serverToChannel.apply(serverId), serverId);
    }

    public DefaultRaftServerBuilder(final PollerFactory commandPollerFactory,
                                    final IntFunction<? extends PollerFactory> serverToPollerFactory,
                                    final IntFunction<? extends Publisher> serverToPublisherFactory) {

        this.exceptionHandler = DEFAULT_EXCEPTION_HANDLER;
        this.idleStrategyFactory = DEFAULT_IDLE_STRATEGY_FACTORY;
        this.regionRingFactory = DEFAULT_REGION_RING_FACTORY;
        this.stateMachineFactory = DEFAULT_STATE_MACHINE_FACTORY;

        this.commandPollerFactory = commandPollerFactory;
        this.serverToPollerFactory = serverToPollerFactory;
        this.serverToPublisherFactory = serverToPublisherFactory;
    }

    @Override
    public RaftServerBuilder stateMachineFactory(final IntFunction<? extends MessageHandler> stateMachineFactory) {
        this.stateMachineFactory = Objects.requireNonNull(stateMachineFactory);
        return this;
    }

    @Override
    public RaftServerBuilder onLeaderTransitionHandler(final IntConsumer onLeaderTransitionHandler) {
        this.onLeaderTransitionHandler = Objects.requireNonNull(onLeaderTransitionHandler);
        return this;
    }

    @Override
    public RaftServerBuilder onFollowerTransitionHandler(final IntConsumer onFollowerTransitionHandler) {
        this.onFollowerTransitionHandler = Objects.requireNonNull(onFollowerTransitionHandler);
        return this;
    }

    @Override
    public RaftServerBuilder minElectionTimeoutMillis(final int minElectionTimeoutMillis) {
        this.minElectionTimeoutMillis = minElectionTimeoutMillis;
        return this;
    }

    @Override
    public RaftServerBuilder maxElectionTimeoutMillis(final int maxElectionTimeoutMillis) {
        this.maxElectionTimeoutMillis = maxElectionTimeoutMillis;
        return this;
    }

    @Override
    public RaftServerBuilder heartbeatTimeoutMillis(final int heartbeatTimeoutMillis) {
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
        return this;
    }

    @Override
    public RaftServerBuilder maxMessagesPollable(final int maxMessagesPollable) {
        this.maxMessagesPollable = maxMessagesPollable;
        return this;
    }

    @Override
    public RaftServerBuilder maxCommandsPollable(final int maxCommandsPollable) {
        this.maxCommandsPollable = maxCommandsPollable;
        return this;
    }

    @Override
    public RaftServerBuilder maxPromotionBatchSize(final int maxPromotionBatchSize) {
        this.maxPromotionBatchSize = maxPromotionBatchSize;
        return this;
    }

    @Override
    public RaftServerBuilder maxAppendBatchSize(final int maxAppendBatchSize) {
        this.maxAppendBatchSize = maxAppendBatchSize;
        return this;
    }

    @Override
    public RaftServerBuilder regionRingFactory(final RegionRingFactory regionRingFactory) {
        this.regionRingFactory = Objects.requireNonNull(regionRingFactory);
        return this;
    }

    @Override
    public RaftServerBuilder regionRingSize(final int regionRingSize) {
        this.regionRingSize = regionRingSize;
        return this;
    }

    @Override
    public RaftServerBuilder indexRegionsToMapAhead(final int indexRegionsToMapAhead) {
        this.indexRegionsToMapAhead = indexRegionsToMapAhead;
        return this;
    }

    @Override
    public RaftServerBuilder payloadRegionsToMapAhead(final int payloadRegionsToMapAhead) {
        this.payloadRegionsToMapAhead = payloadRegionsToMapAhead;
        return this;
    }

    @Override
    public RaftServerBuilder indexRegionSizeGranularityMultiplier(final int indexRegionSizeGranularityMultiplier) {
        this.indexRegionSizeGranularityMultiplier = indexRegionSizeGranularityMultiplier;
        return this;
    }

    @Override
    public RaftServerBuilder payloadRegionSizeGranularityMultiplier(final int payloadRegionSizeGranularityMultiplier) {
        this.payloadRegionSizeGranularityMultiplier = payloadRegionSizeGranularityMultiplier;
        return this;
    }

    @Override
    public RaftServerBuilder encoderBufferSize(final int encoderBufferSize) {
        this.encoderBufferSize = encoderBufferSize;
        return this;
    }

    @Override
    public RaftServerBuilder clock(final Clock clock) {
        this.clock = Objects.requireNonNull(clock);
        return this;
    }

    @Override
    public RaftServerBuilder idleStrategyFactory(final IntFunction<? extends IdleStrategy> idleStrategyFactory) {
        this.idleStrategyFactory = Objects.requireNonNull(idleStrategyFactory);
        return this;
    }

    @Override
    public RaftServerBuilder exceptionHandler(final BiConsumer<? super String, ? super Exception> exceptionHandler) {
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
        return this;
    }

    @Override
    public RaftServerBuilder gracefulShutdownTimeout(final long gracefulShutdownTimeout, final TimeUnit gracefulShutdownTimeunit) {
        this.gracefulShutdownTimeout = gracefulShutdownTimeout;
        this.gracefulShutdownTimeunit = Objects.requireNonNull(gracefulShutdownTimeunit);
        return this;
    }

    @Override
    public Service.Start build(final String logDirectory, final int serverId, final int clusterSize) throws IOException {
        Objects.requireNonNull(logDirectory);

        if (serverId < 0 || serverId >= clusterSize) {
            throw new IllegalArgumentException("Invalid serverId. Must be value [0..clusterSize)");
        }
        final Logger outLogger = LoggerFactory.getLogger("OUT");
        final Logger inLogger = LoggerFactory.getLogger("IN");
        final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        final AppendRequestEncoder appendRequestEncoder = new AppendRequestEncoder();
        final AppendResponseEncoder appendResponseEncoder = new AppendResponseEncoder();
        final VoteResponseEncoder voteResponseEncoder = new VoteResponseEncoder();
        final VoteRequestEncoder voteRequestEncoder = new VoteRequestEncoder();

        final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        final VoteRequestDecoder voteRequestDecoder = new VoteRequestDecoder();
        final VoteResponseDecoder voteResponseDecoder = new VoteResponseDecoder();
        final AppendRequestDecoder appendRequestDecoder = new AppendRequestDecoder();
        final AppendResponseDecoder appendResponseDecoder = new AppendResponseDecoder();
        final CommandRequestDecoder commandRequestDecoder = new CommandRequestDecoder();
        final StringBuilder stringBuilder = new StringBuilder();

        final UnsafeBuffer commandDecoderBuffer = new UnsafeBuffer();
        final ByteBuffer encoderByteBuffer = ByteBuffer.allocateDirect(encoderBufferSize);
        final UnsafeBuffer encoderBuffer = new UnsafeBuffer(encoderByteBuffer);

        final Publisher publisher = new LoggingPublisher(
                serverToPublisherFactory.apply(serverId),
                outLogger,
                messageHeaderDecoder,
                voteRequestDecoder,
                voteResponseDecoder,
                appendRequestDecoder,
                appendResponseDecoder,
                commandRequestDecoder,
                stringBuilder);

        final int regionSizeGranularity = (int) MappedFile.REGION_SIZE_GRANULARITY;

        final File directoryFile = new File(logDirectory);
        IoUtil.ensureDirectoryExists(directoryFile, "raft log directory");
        final File headerFile = new File(logDirectory, "logHeader" + serverId);
        final File indexFile = new File(logDirectory, "logIndex" + serverId);
        final File payloadFile = new File(logDirectory, "logPayload" + serverId);

        final int headerRegionSize = regionSizeGranularity;
        final int indexRegionSize = regionSizeGranularity * indexRegionSizeGranularityMultiplier;
        final int payloadRegionSize = regionSizeGranularity * payloadRegionSizeGranularityMultiplier;


        final MappedFile headerMappedFile = new MappedFile(headerFile, MappedFile.Mode.READ_WRITE,
                headerRegionSize, (file, mode) -> {});

        final MappedFile indexMappedFile = new MappedFile(indexFile, MappedFile.Mode.READ_WRITE,
                headerRegionSize, (file, mode) -> {});

        final MappedFile payloadMappedFile = new MappedFile(payloadFile, MappedFile.Mode.READ_WRITE,
                payloadRegionSize, (file, mode) -> {});


        final RegionRingAccessor headerRegionRingAccessor = new RegionRingAccessor(
                regionRingFactory.create(
                        regionRingSize,
                        headerRegionSize,
                        headerMappedFile::getFileChannel,
                        FileSizeEnsurer.forWritableFile(headerMappedFile::getFileLength, headerMappedFile::setFileLength, MAX_FILE_SIZE),
                        headerMappedFile.getMode().getMapMode()),
                headerRegionSize,
                0,
                headerMappedFile::close);

        final RegionRingAccessor indexRegionRingAccessor = new RegionRingAccessor(
                regionRingFactory.create(
                        regionRingSize,
                        indexRegionSize,
                        indexMappedFile::getFileChannel,
                        FileSizeEnsurer.forWritableFile(indexMappedFile::getFileLength, indexMappedFile::setFileLength, MAX_FILE_SIZE),
                        indexMappedFile.getMode().getMapMode()),
                indexRegionSize,
                indexRegionsToMapAhead,
                indexMappedFile::close);

        final RegionRingAccessor payloadRegionRingAccessor = new RegionRingAccessor(
                regionRingFactory.create(
                        regionRingSize,
                        payloadRegionSize,
                        payloadMappedFile::getFileChannel,
                        FileSizeEnsurer.forWritableFile(payloadMappedFile::getFileLength, payloadMappedFile::setFileLength, MAX_FILE_SIZE),
                        indexMappedFile.getMode().getMapMode()),
                payloadRegionSize,
                payloadRegionsToMapAhead,
                payloadMappedFile::close);

        final Supplier<Timer> heartbeatTimerFactory = () -> new Timer(clock, heartbeatTimeoutMillis, heartbeatTimeoutMillis);

        final PersistentState persistentState = new DefaultPersistentState(indexRegionRingAccessor, payloadRegionRingAccessor, headerRegionRingAccessor);
        final VolatileState volatileState = new VolatileState();
        final FollowersState followersState = new FollowersState(serverId, clusterSize, heartbeatTimerFactory);

        final Timer electionTimer = new Timer(clock, minElectionTimeoutMillis, maxElectionTimeoutMillis);

        final AppendRequestHandler appendRequestHandler = new AppendRequestHandler(persistentState,
                volatileState,
                electionTimer, messageHeaderEncoder,
                appendResponseEncoder,
                encoderBuffer,
                publisher,
                serverId);

        final VoteRequestHandler voteRequestHandler = new VoteRequestHandler(persistentState,
                electionTimer, messageHeaderEncoder,
                voteResponseEncoder,
                encoderBuffer,
                publisher,
                serverId);

        final Predicate<HeaderDecoder> destinationFilter = DestinationFilter.forServer(serverId);

        final ServerState followerServerState = new HeaderFilteringServerState(destinationFilter,
                new LoggingServerState(
                        new HighTermHandlingServerState(
                                new FollowerServerState(
                                        serverId,
                                        appendRequestHandler,
                                        voteRequestHandler,
                                        electionTimer,
                                        onFollowerTransitionHandler),
                                persistentState, inLogger),
                        stringBuilder,
                        inLogger
                ));

        final ServerState candidateServerState = new HeaderFilteringServerState(destinationFilter,
                new LoggingServerState(
                        new HighTermHandlingServerState(
                                new CandidateServerState(persistentState,
                                        followersState,
                                        appendRequestHandler,
                                        electionTimer,
                                        serverId,
                                        messageHeaderEncoder,
                                        voteRequestEncoder,
                                        encoderBuffer,
                                        publisher),
                                persistentState, inLogger),
                        stringBuilder,
                        inLogger
                ));

        final ServerState leaderServerState = new HeaderFilteringServerState(destinationFilter,
                new LoggingServerState(
                        new HighTermHandlingServerState(
                                new LeaderServerState(persistentState,
                                        volatileState,
                                        followersState,
                                        serverId,
                                        appendRequestEncoder,
                                        messageHeaderEncoder,
                                        encoderBuffer,
                                        commandDecoderBuffer,
                                        publisher,
                                        onLeaderTransitionHandler,
                                        maxAppendBatchSize),
                                persistentState, inLogger),
                        stringBuilder,
                        inLogger
                ));

        final ServerMessageHandler serverMessageHandler = new ServerMessageHandler(messageHeaderDecoder,
                voteRequestDecoder,
                voteResponseDecoder,
                appendRequestDecoder,
                appendResponseDecoder,
                candidateServerState,
                leaderServerState,
                followerServerState);

        final List<ProcessStep> processSteps = new ArrayList<>(clusterSize - 1 + 3);

        IntStream.range(0, clusterSize)
                .filter(destinationId -> destinationId != serverId)
                .forEach(destinationId -> {
                    final Poller destinationPoller = serverToPollerFactory.apply(destinationId)
                            .create(serverMessageHandler, maxMessagesPollable);
                    processSteps.add(destinationPoller::poll);
                });

        final Poller commandPoller = commandPollerFactory.create(serverMessageHandler, maxCommandsPollable);
        processSteps.add(commandPoller::poll);
        processSteps.add(serverMessageHandler);
        processSteps.add(new CommittedLogPromoter(persistentState, volatileState, stateMachineFactory.apply(serverId), commandDecoderBuffer, maxPromotionBatchSize));

        final Runnable onProcessStart = serverMessageHandler::init;
        final Runnable onProcessStop = () -> {
            headerRegionRingAccessor.close();
            indexRegionRingAccessor.close();
            payloadRegionRingAccessor.close();
            headerMappedFile.close();
            indexMappedFile.close();
            payloadMappedFile.close();
        };

        return new Process("Server" + serverId,
                onProcessStart,
                onProcessStop,
                idleStrategyFactory.apply(serverId),
                exceptionHandler,
                gracefulShutdownTimeout,
                gracefulShutdownTimeunit,
                processSteps.toArray(new ProcessStep[processSteps.size()])
        );
    }
}