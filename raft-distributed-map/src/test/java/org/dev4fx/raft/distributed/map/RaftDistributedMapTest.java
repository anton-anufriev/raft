package org.dev4fx.raft.distributed.map;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.dev4fx.raft.config.Bootstrap;
import org.dev4fx.raft.config.RaftServerBuilder;
import org.dev4fx.raft.process.MutableProcessStepChain;
import org.dev4fx.raft.process.ProcessStep;
import org.dev4fx.raft.process.Service;
import org.dev4fx.raft.sbe.CommandRequestEncoder;
import org.dev4fx.raft.sbe.MessageHeaderEncoder;
import org.dev4fx.raft.state.CommandPublisher;
import org.dev4fx.raft.state.DefaultCommandPublisher;
import org.dev4fx.raft.state.LoggingStateMachine;
import org.dev4fx.raft.state.StateMachine;
import org.dev4fx.raft.transport.Publisher;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

import static org.junit.Assert.*;

public class RaftDistributedMapTest {

    public static void main(final String[] args) throws Exception {

        final MediaDriver driver = MediaDriver.launchEmbedded();
        final Aeron.Context ctx = new Aeron.Context()
                .driverTimeoutMs(1000000)
                .availableImageHandler(Bootstrap::printAvailableImage)
                .unavailableImageHandler(Bootstrap::printUnavailableImage);

        ctx.aeronDirectoryName(driver.aeronDirectoryName());

        final Aeron aeron = Aeron.connect(ctx);

        final String commandChannel = "aeron:ipc";
        final int commandStreamId = 100;

        DistributedMap<String, String> distributedMap0_1 =  createServer(aeron, commandChannel, commandStreamId, 0, 3);
        DistributedMap<String, String> distributedMap1_1 =  createServer(aeron, commandChannel, commandStreamId, 1, 3);
        DistributedMap<String, String> distributedMap2_1 =  createServer(aeron, commandChannel, commandStreamId, 2, 3);

        Thread.sleep(10000);

        distributedMap0_1.put("TestKey", "TestResult");

//        Thread.sleep(10000);

        distributedMap1_1.remove("TestKey");
        distributedMap1_1.put("AnotherTestKey2", "AnotherTestValue2");
        distributedMap1_1.put("AnotherTestKey3", "AnotherTestValue3");

        final Map<String, String> moreEntries = new HashMap<>();
        moreEntries.put("MoreKey1", "MoreValue1");
        moreEntries.put("MoreKey2", "MoreValue2");
        moreEntries.put("MoreKey3", "MoreValue3");
        moreEntries.put("MoreKey4", "MoreValue4");
        moreEntries.put("MoreKey5", "MoreValue5");


        distributedMap2_1.putAll(moreEntries);

//        Thread.sleep(5000);


        System.out.println(distributedMap0_1.toString());
        System.out.println(distributedMap1_1.toString());
        System.out.println(distributedMap2_1.toString());

        final Set<String> keySet = distributedMap1_1.keySet();

        final List<String> removeList = Arrays.asList("MoreKey2", "MoreKey3");
        final List<String> retailList = Arrays.asList("AnotherTestKey2", "MoreKey1");

        keySet.retainAll(removeList);
        keySet.removeAll(retailList);

        Thread.sleep(1000);

        System.out.println(distributedMap0_1.toString());
        System.out.println(distributedMap1_1.toString());
        System.out.println(distributedMap2_1.toString());

    }

    public static DistributedMap<String, String> createServer(final Aeron aeron, final String commandChannel, final int commandStreamId, final int serverId, final int clusterSize) throws IOException {

        final UnsafeBuffer commandEncoderBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(8*1024));
        final Publisher aeronCommandPublisher = Publisher.aeronPublisher(aeron, commandChannel, commandStreamId);
        final CommandPublisher commandPublisher = new DefaultCommandPublisher(aeronCommandPublisher,
                new MessageHeaderEncoder(),
                new CommandRequestEncoder(),
                commandEncoderBuffer);

        final MutableLong serverSequence = new MutableLong(System.currentTimeMillis() * 100);
        final LongSupplier serverSequenceGenerator = () -> { serverSequence.set(serverSequence.value + 1); return serverSequence.get();};

        final Int2ObjectHashMap<StateMachine> mapStateMachines = new Int2ObjectHashMap<>();

        final MutableProcessStepChain pollerChain = MutableProcessStepChain.create();

        final DistributedMap<String, String> distributedMap = createMap(1, serverId,
                serverSequenceGenerator, pollerChain::thenStep,
                (stateMachine, mapId) -> mapStateMachines.put(mapId, stateMachine),
                commandPublisher);


        final Long2LongHashMap lastReceivedSequences = new Long2LongHashMap(-1);

        final StateMachine stateMachine = new DeduplicateStateMachine(
                new RoutingStateMachine(mapStateMachines::get),
                lastReceivedSequences
        );

        final String serverChannel = "aeron:ipc";
        final IntFunction<String> serverToChannel = serverId1 -> serverChannel;

        final RaftServerBuilder builder = RaftServerBuilder
                .forAeronTransport(aeron, commandChannel, commandStreamId, serverToChannel)
                .maxAppendBatchSize(4)
                .stateMachineFactory(serverId1 -> stateMachine)
                .applicationProcessStep(pollerChain.getOrNull());

        final String raftDirectory = "/Users/anton/IdeaProjects/distMap";

        final Service.Start process = builder.build(raftDirectory, serverId, clusterSize);
        process.start();

        return distributedMap;

    }

    public static DistributedMap<String, String> createMap(final int mapId, final int serverId,
                                             final LongSupplier sequenceGenerator,
                                             final Consumer<? super ProcessStep> pollerHandler,
                                             final ObjIntConsumer<StateMachine> stateMachineHandler,
                                             final CommandPublisher commandPublisher) {
        final ConcurrentMap<String, String> concurrentMap = new ConcurrentHashMap<>();
        final Queue<MapCommand<String, String>> mapCommandQueue = new ManyToOneConcurrentArrayQueue<>(100);

        final RaftDistributedMap<String, String> map = new RaftDistributedMap<>(
                mapId,
                concurrentMap,
                mapCommandQueue);

        final UnsafeBuffer encodingBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));
        final UnsafeBuffer entryEncodingBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));

        final Serialiser<String> stringSerialiser = (value, buffer, offset) -> buffer.putStringWithoutLengthAscii(offset, value);
        final Deserialiser<String> stringDeserialiser = DirectBuffer::getStringWithoutLengthAscii;
        MapCommand<String, String> noOpCommand = (sequence1, commandHandler) -> {};

        final MapCommandHandler<String, String> encodingCommandHandler = new EncodingMapCommandHandler<>(
                serverId,
                encodingBuffer,
                entryEncodingBuffer,
                stringSerialiser,
                stringSerialiser,
                commandPublisher
        );

        final Long2ObjectHashMap<MapCommand<String, String>> inflightCommands = new Long2ObjectHashMap<>();

        final ProcessStep poller = new MapCommandPoller<>(
                mapCommandQueue,
                (mapCommand, sequence) -> inflightCommands.put(sequence, mapCommand),
                sequenceGenerator,
                encodingCommandHandler);

        final LongFunction<MapCommand<String, String>> commandRemoving = sequence -> {
            final MapCommand<String, String> command = inflightCommands.remove(sequence);
            return command != null ? command : noOpCommand;
        };

        final StateMachine mapStateMachine = new MapStateMachine<>(
                mapId, concurrentMap, stringDeserialiser, stringDeserialiser,
                commandRemoving);

        pollerHandler.accept(poller);
        stateMachineHandler.accept(mapStateMachine, mapId);

        return map;
    }
}