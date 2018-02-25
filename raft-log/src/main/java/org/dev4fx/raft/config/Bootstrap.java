package org.dev4fx.raft.config;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.UnsafeBuffer;
import org.dev4fx.raft.process.Service;
import org.dev4fx.raft.sbe.CommandRequestEncoder;
import org.dev4fx.raft.sbe.MessageHeaderEncoder;
import org.dev4fx.raft.state.CommandSender;
import org.dev4fx.raft.state.LoggingStateMachine;
import org.dev4fx.raft.transport.Publisher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;

public class Bootstrap {

    public static void main(final String[] args) throws IOException {

        final MediaDriver driver = MediaDriver.launchEmbedded();
        final Aeron.Context ctx = new Aeron.Context()
                .driverTimeoutMs(1000000)
                .availableImageHandler(Bootstrap::printAvailableImage)
                .unavailableImageHandler(Bootstrap::printUnavailableImage);

        ctx.aeronDirectoryName(driver.aeronDirectoryName());

        final Aeron aeron = Aeron.connect(ctx);

        final String testMessage = "#------------------------------------------------#\n";

        final ByteBuffer byteBuffer = ByteBuffer.allocate(testMessage.getBytes().length);
        final UnsafeBuffer commandPayloadEncoderBuffer = new UnsafeBuffer(byteBuffer);
        commandPayloadEncoderBuffer.putBytes(0, testMessage.getBytes());
        final int commandPayloadLength = testMessage.getBytes().length;


        final ByteBuffer commandEncoderByteBuffer = ByteBuffer.allocateDirect(8*1024);
        final UnsafeBuffer commandEncoderBuffer = new UnsafeBuffer(commandEncoderByteBuffer);


        final String commandChannel = "aeron:ipc";
        final int commandStreamId = 100;

        final Publisher commandPublisher = Publisher.aeronPublisher(aeron, commandChannel, commandStreamId);
        final CommandSender commandSender = new CommandSender(commandPublisher,
                new MessageHeaderEncoder(),
                new CommandRequestEncoder(),
                commandEncoderBuffer);

        final IdGenerator idGen = new IdGenerator(() -> System.currentTimeMillis() * 10);
        final int commandSource = 222;

        final String raftDirectory = "/Users/anton/IdeaProjects/raft";

        final AtomicBoolean firstElection = new AtomicBoolean(true);
        final IntConsumer onLeaderTransitionHandler = serverId -> {
            if (firstElection.getAndSet(false)) {
                commandSender.publish(commandSource, idGen.getAsLong(), commandPayloadEncoderBuffer, 0, commandPayloadLength);
            }
        };

        final String serverChannel = "aeron:ipc";
        final IntFunction<String> serverToChannel = value -> serverChannel;

        final RaftServerBuilder builder = RaftServerBuilder
                .forAeronTransport(aeron, commandChannel, commandStreamId, serverToChannel)
                .stateMachineFactory(LoggingStateMachine::new)
                .onLeaderTransitionHandler(onLeaderTransitionHandler);

        final Service.Start process0 = builder.build(raftDirectory, 0, 5);
        final Service.Start process1 = builder.build(raftDirectory, 1, 5);
        final Service.Start process2 = builder.build(raftDirectory, 2, 5);
//        final Service.Start process3 = builder.build(raftDirectory, 3, 5);
//        final Service.Start process4 = builder.build(raftDirectory, 4, 5);

        final Service.Stop process0Shutdown = process0.start();
        final Service.Stop process1Shutdown = process1.start();
        final Service.Stop process2Shutdown = process2.start();
//        final Service.Stop process3Shutdown = process3.start();
//        final Service.Stop process4Shutdown = process4.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            process0Shutdown.stop();
            process1Shutdown.stop();
            process2Shutdown.stop();
//            process3Shutdown.stop();
//            process4Shutdown.stop();
        }));

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

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
