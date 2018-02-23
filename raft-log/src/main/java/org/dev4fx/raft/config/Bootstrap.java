package org.dev4fx.raft.config;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.def4fx.raft.mmap.api.FileSizeEnsurer;
import org.dev4fx.raft.io.FileUtil;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.log.impl.DefaultPersistentState;
import org.dev4fx.raft.mmap.impl.*;
import org.dev4fx.raft.process.Process;
import org.dev4fx.raft.process.ProcessStep;
import org.dev4fx.raft.process.Service;
import org.dev4fx.raft.sbe.*;
import org.dev4fx.raft.state.*;
import org.dev4fx.raft.timer.Timer;
import org.dev4fx.raft.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class Bootstrap {
    private static final long MAX_FILE_SIZE = 64 * 16 * 1024 * 1024;

    public static void main(final String[] args) throws IOException {

        final MediaDriver driver = MediaDriver.launchEmbedded();
        final Aeron.Context ctx = new Aeron.Context()
                .driverTimeoutMs(1000000)
                .availableImageHandler(Bootstrap::printAvailableImage)
                .unavailableImageHandler(Bootstrap::printUnavailableImage);

        ctx.aeronDirectoryName(driver.aeronDirectoryName());

        final Aeron aeron = Aeron.connect(ctx);

        final BiConsumer<? super String, ? super Exception> exceptionHandler = (s, e) -> {
            e.printStackTrace();
            throw new RuntimeException(e);
        };

        final String ipcChannel = "aeron:ipc";
        final IntFunction<String> serverTochannelMap = value -> ipcChannel;

        final MessageHandler stateMachine0 = new LoggingStateMachine(0);
        final MessageHandler stateMachine1 = new LoggingStateMachine(1);
        final MessageHandler stateMachine2 = new LoggingStateMachine(2);

        final String commandChannel = ipcChannel;
        final int commandStreamId = 100;
        final int heartbeatTimeout = 1000;
        final int minElectionTimeout = 1100;
        final int maxElectionTimeout = 1500;


        final Process process0 = process(aeron,
                commandChannel,
                commandStreamId,
                0,
                3,
                minElectionTimeout,
                maxElectionTimeout,
                heartbeatTimeout,
                1,
                serverTochannelMap,
                stateMachine0,
                1,
                new BackoffIdleStrategy(100L, 100L, 10L, 100L),
                exceptionHandler,
                10, TimeUnit.SECONDS);

        final Process process1 = process(aeron,
                commandChannel,
                commandStreamId,
                1,
                3,
                minElectionTimeout,
                maxElectionTimeout,
                heartbeatTimeout,
                1,
                serverTochannelMap,
                stateMachine1,
                1,
                new BackoffIdleStrategy(100L, 100L, 10L, 100L),
                exceptionHandler,
                10, TimeUnit.SECONDS);

        final Process process2 = process(aeron,
                commandChannel,
                commandStreamId,
                2,
                3,
                minElectionTimeout,
                maxElectionTimeout,
                heartbeatTimeout,
                1,
                serverTochannelMap,
                stateMachine2,
                1,
                new BackoffIdleStrategy(100L, 100L, 10L, 100L),
                exceptionHandler,
                10, TimeUnit.SECONDS);

        final Service.Stop process0Shutdown = process0.start();
        final Service.Stop process1Shutdown = process1.start();
        final Service.Stop process2Shutdown = process2.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            process0Shutdown.stop();
            process1Shutdown.stop();
            process2Shutdown.stop();
        }));

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        final String testMessage = "#------------------------------------------------#\n";

        final ByteBuffer byteBuffer = ByteBuffer.allocate(testMessage.getBytes().length);
        final UnsafeBuffer commandPayloadEncoderBuffer = new UnsafeBuffer(byteBuffer);
        commandPayloadEncoderBuffer.putBytes(0, testMessage.getBytes());
        final int commandPayloadLength = testMessage.getBytes().length;


        final ByteBuffer commandEncoderByteBuffer = ByteBuffer.allocateDirect(8024);
        final UnsafeBuffer commandEncoderBuffer = new UnsafeBuffer(commandEncoderByteBuffer);


        final Publisher commandPublisher = new AeronPublisher(aeron.addPublication(commandChannel, commandStreamId));
        final CommandSender commandSender = new CommandSender(commandPublisher,
                new MessageHeaderEncoder(),
                new CommandRequestEncoder(),
                commandEncoderBuffer);

        final IdGenerator idGen = new IdGenerator(() -> System.currentTimeMillis() * 10);
        final int commandSource = 222;

        commandSender.publish(commandSource, idGen.getAsLong(), commandPayloadEncoderBuffer, 0, commandPayloadLength);

        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        process0Shutdown.stop();

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        commandSender.publish(commandSource, idGen.getAsLong(), commandPayloadEncoderBuffer, 0, commandPayloadLength);

        process0Shutdown.awaitShutdown();
        process1Shutdown.awaitShutdown();
        process2Shutdown.awaitShutdown();
    }


    public static Process process(final Aeron aeron,
                           final String commandChannel,
                           final int commandStreamId,
                           final int serverId,
                           final int serverCount,
                           final int minElectionTimeoutMillis,
                           final int maxElectionTimeoutMillis,
                           final int heartbeatTimeoutMillis,
                           final int maxMessagesPolledForSubscription,
                           final IntFunction<String> serverTochannelMap,
                           final MessageHandler stateMachineHandler,
                           final int maxCommitBatchSize,
                           final IdleStrategy idleStrategy,
                           final BiConsumer<? super String, ? super Exception> exceptionHandler,
                           final long gracefulShutdownTimeout,
                           final TimeUnit gracefulShutdownTimeunit) throws IOException {
        if (serverId < 0 || serverId >= serverCount) {
            throw new IllegalArgumentException("Invalid serverId. Must be value [0..serverCount)");
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
        final ByteBuffer encoderByteBuffer = ByteBuffer.allocateDirect(8024);
        final UnsafeBuffer encoderBuffer = new UnsafeBuffer(encoderByteBuffer);

        final Publisher publisher = new LoggingPublisher(
                new AeronPublisher(aeron.addPublication(serverTochannelMap.apply(serverId), serverId)),
                outLogger,
                messageHeaderDecoder,
                voteRequestDecoder,
                voteResponseDecoder,
                appendRequestDecoder,
                appendResponseDecoder,
                commandRequestDecoder,
                stringBuilder);

        final Publishers publishers = serverId1 -> publisher;

        final String headerFileName = FileUtil.sharedMemDir("logHeader" + serverId).getAbsolutePath();
        final String indexFileName = FileUtil.sharedMemDir("logIndex" + serverId).getAbsolutePath();
        final String payloadFileName = FileUtil.sharedMemDir("logPayload" + serverId).getAbsolutePath();
        final int headerRegionSize = (int) Math.max(MappedFile.REGION_SIZE_GRANULARITY, 64);
        final int indexRegionSize = (int) Math.max(MappedFile.REGION_SIZE_GRANULARITY, 1L << 16);
        final int payloadRegionSize = (int) Math.max(MappedFile.REGION_SIZE_GRANULARITY, 1L << 16) * 64;

        //final RegionRingFactory asyncFactory = RegionRingFactory.forAsync(RegionFactory.ASYNC_VOLATILE_STATE_MACHINE, processors::add);
        final RegionRingFactory syncFactory = RegionRingFactory.forSync(RegionFactory.SYNC);


        final MappedFile headerFile = new MappedFile(headerFileName, MappedFile.Mode.READ_WRITE,
                headerRegionSize, FileInitialiser::initFile);

        final MappedFile indexFile = new MappedFile(indexFileName, MappedFile.Mode.READ_WRITE,
                headerRegionSize, FileInitialiser::initFile);

        final MappedFile payloadFile = new MappedFile(payloadFileName, MappedFile.Mode.READ_WRITE,
                payloadRegionSize, FileInitialiser::initFile);


        final RegionRingAccessor headerRegionRingAccessor = new RegionRingAccessor(
                syncFactory.create(
                        4,
                        headerRegionSize,
                        headerFile::getFileChannel,
                        FileSizeEnsurer.forWritableFile(headerFile::getFileLength, headerFile::setFileLength, MAX_FILE_SIZE),
                        headerFile.getMode().getMapMode()),
                headerRegionSize,
                0,
                headerFile::close);

        final RegionRingAccessor indexRegionRingAccessor = new RegionRingAccessor(
                syncFactory.create(
                        4,
                        indexRegionSize,
                        indexFile::getFileChannel,
                        FileSizeEnsurer.forWritableFile(indexFile::getFileLength, indexFile::setFileLength, MAX_FILE_SIZE),
                        indexFile.getMode().getMapMode()),
                indexRegionSize,
                1,
                indexFile::close);

        final RegionRingAccessor payloadRegionRingAccessor = new RegionRingAccessor(
                syncFactory.create(
                        4,
                        payloadRegionSize,
                        payloadFile::getFileChannel,
                        FileSizeEnsurer.forWritableFile(payloadFile::getFileLength, payloadFile::setFileLength, MAX_FILE_SIZE),
                        indexFile.getMode().getMapMode()),
                payloadRegionSize,
                1,
                payloadFile::close);

        final Supplier<Timer> heartbeatTimerFactory = () -> new Timer(heartbeatTimeoutMillis, heartbeatTimeoutMillis);

        final PersistentState persistentState = new DefaultPersistentState(indexRegionRingAccessor, payloadRegionRingAccessor, headerRegionRingAccessor);
        final VolatileState volatileState = new VolatileState();
        final FollowersState followersState = new FollowersState(serverId, serverCount, heartbeatTimerFactory);

        final Timer electionTimer = new Timer(minElectionTimeoutMillis, maxElectionTimeoutMillis);

        final AppendRequestHandler appendRequestHandler = new AppendRequestHandler(persistentState,
                volatileState,
                electionTimer, messageHeaderEncoder,
                appendResponseEncoder,
                encoderBuffer,
                publishers,
                serverId);

        final VoteRequestHandler voteRequestHandler = new VoteRequestHandler(persistentState,
                electionTimer, messageHeaderEncoder,
                voteResponseEncoder,
                encoderBuffer,
                publishers,
                serverId);

        final Predicate<HeaderDecoder> destinationFilter = DestinationFilter.forServer(serverId);

        final ServerState followerServerState = new HeaderFilteringServerState(destinationFilter,
            new LoggingServerState(
                new HighTermHandlingServerState(
                        new FollowerServerState(appendRequestHandler,
                                voteRequestHandler,
                                electionTimer),
                        persistentState),
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
                                publishers),
                        persistentState),
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
                                publishers),
                        persistentState),
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

        final List<ProcessStep> processSteps = new ArrayList<>(serverCount + 2);

        IntStream.range(0, serverCount)
                .filter(destinationId -> destinationId != serverId)
                .forEach(destinationId -> {
                    outLogger.info("Creating subscriptions: From {}, to {}", serverId, destinationId);
                    processSteps.add(new AeronSubscriptionPoller(aeron.addSubscription(serverTochannelMap.apply(destinationId), destinationId), serverMessageHandler, maxMessagesPolledForSubscription));
                });
        processSteps.add(new AeronSubscriptionPoller(aeron.addSubscription(commandChannel, commandStreamId), serverMessageHandler, 1));
        processSteps.add(serverMessageHandler);
        processSteps.add(new CommittedLogPromoter(persistentState, volatileState, stateMachineHandler, commandDecoderBuffer, maxCommitBatchSize));

        final Runnable onProcessStart = serverMessageHandler::init;
        final Runnable onProcessStop = () -> {
            headerRegionRingAccessor.close();
            indexRegionRingAccessor.close();
            payloadRegionRingAccessor.close();
            headerFile.close();
            indexFile.close();
            payloadFile.close();
        };

        return new Process("Server" + serverId,
                onProcessStart,
                onProcessStop,
                idleStrategy,
                exceptionHandler,
                gracefulShutdownTimeout,
                gracefulShutdownTimeunit,
                processSteps.toArray(new ProcessStep[processSteps.size()])
        );
    }


    public static void printAvailableImage(final Image image)
    {
        final Subscription subscription = image.subscription();
        System.out.println(String.format(
                "Available image on %s streamId=%d sessionId=%d from %s",
                subscription.channel(), subscription.streamId(), image.sessionId(), image.sourceIdentity()));
    }

    /**
     * Print the information for an unavailable image to stdout.
     *
     * @param image that has gone inactive
     */
    public static void printUnavailableImage(final Image image)
    {
        final Subscription subscription = image.subscription();
        System.out.println(String.format(
                "Unavailable image on %s streamId=%d sessionId=%d",
                subscription.channel(), subscription.streamId(), image.sessionId()));
    }

}
