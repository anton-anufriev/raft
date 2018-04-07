package org.dev4fx.raft.perf;

import io.aeron.Aeron;
import io.aeron.shadow.org.HdrHistogram.Histogram;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.dev4fx.raft.config.RaftServerBuilder;
import org.dev4fx.raft.mmap.impl.RegionFactory;
import org.dev4fx.raft.mmap.impl.RegionRingFactory;
import org.dev4fx.raft.process.Process;
import org.dev4fx.raft.process.Service;
import org.dev4fx.raft.sbe.CommandRequestEncoder;
import org.dev4fx.raft.sbe.MessageHeaderEncoder;
import org.dev4fx.raft.state.CommandPublisher;
import org.dev4fx.raft.state.DefaultCommandPublisher;
import org.dev4fx.raft.state.StateMachine;
import org.dev4fx.raft.transport.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;

public class RaftPerf {
    private final static Logger LOGGER = LoggerFactory.getLogger(RaftPerf.class);

    public static void main(final String[] args) throws Exception {

        final Aeron.Context ctx = new Aeron.Context()
                .driverTimeoutMs(1000000);

        final Aeron aeron = Aeron.connect(ctx);

        final String commandChannel = "aeron:ipc";
        final int commandStreamId = 100;

        final int server = Integer.parseInt(args[0]);
        final int clusterSize = Integer.parseInt(args[1]);
        final int messages = Integer.parseInt(args[2]);
        final int warmUpMessages = Integer.parseInt(args[3]);
        final RegionMappingConfig regionMappingConfig = RegionMappingConfig.valueOf(args[4]);
        final String serverChannel = args[5]; // "aeron:ipc";
        final String raftDirectory = args[6]; //"/Users/anton/IdeaProjects/raftPerf";


        final Histogram latencyHistogram = new Histogram(1, TimeUnit.MINUTES.toNanos(1),3);

        final Publisher aeronPublisher = Publisher.aeronPublisher(aeron, commandChannel, commandStreamId);
        final CommandPublisher commandPublisher = new DefaultCommandPublisher(aeronPublisher,
                new MessageHeaderEncoder(), new CommandRequestEncoder(), new UnsafeBuffer(ByteBuffer.allocateDirect(1024)));

        final AtomicLong receivedSequence = new AtomicLong();

        final StateMachine stateMachine = (sourceId, sequence, buffer, offset, length) -> {

            if (sequence > warmUpMessages) {
                final long timeNanos = buffer.getLong(offset);
                final long latency = System.nanoTime() - timeNanos;
                latencyHistogram.recordValue(latency);
            }

            receivedSequence.set(sequence);

            if (sequence == messages) {
                latencyHistogram.outputPercentileDistribution(System.out, 1.0);
            }
        };

        final IntConsumer commandInjectionKickOff = serverId -> {
            final Thread commandThread = new Thread(() -> {
                sleep(30000);
                final MutableDirectBuffer payloadBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(1024));

                long messageSequence = 1;
                while (messageSequence <= messages) {
                    payloadBuffer.putLong(0, System.nanoTime());
                    commandPublisher.publish(serverId, messageSequence, payloadBuffer, 0, 8);
                    while(messageSequence != receivedSequence.getAndSet(0)) {}
                    messageSequence++;
                }
            });
            commandThread.start();
        };

        final RegionRingFactory regionRingFactory;
        final Runnable onRegionRingsCreatedHandler;

        if (regionMappingConfig == RegionMappingConfig.ASYNC) {
            final RegionMappingProcessStepBuilder regionMappingProcessStepBuilder = new RegionMappingProcessStepBuilder();
            regionRingFactory = RegionRingFactory.forAsync(RegionFactory.ASYNC_VOLATILE_STATE_MACHINE, regionMappingProcessStepBuilder);
            onRegionRingsCreatedHandler = () -> {
                final Process regionMapper = new Process("RegionMapper",
                        () -> {
                        },
                        () -> {
                        },
                        new BusySpinIdleStrategy()::idle,
                        (s, e) -> LOGGER.error("{} {}", s, e, e),
                        10,
                        TimeUnit.SECONDS,
                        regionMappingProcessStepBuilder.build()
                );
                regionMapper.start();
            };
        } else {
            regionRingFactory = RegionRingFactory.forSync(RegionFactory.SYNC);
            onRegionRingsCreatedHandler = () -> {};
        }

        final RaftServerBuilder builder = RaftServerBuilder
                .forAeronTransport(aeron, commandChannel, commandStreamId, serverId -> serverChannel)
                .maxAppendBatchSize(4)
                .regionRingFactory(regionRingFactory)
                .onRegionRingsCreatedHandler(onRegionRingsCreatedHandler)
                .idleStrategyFactory(serverId -> new BusySpinIdleStrategy()::idle)
                .stateMachineFactory(serverId -> stateMachine)
                .onLeaderTransitionHandler(commandInjectionKickOff);

        final Service.Start process = builder.build(raftDirectory, server, clusterSize);

        process.start();
    }

    private static void sleep(final int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private enum RegionMappingConfig {
        SYNC,
        ASYNC
    }
}