package org.def4fx.raft.queue.impl;

import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.def4fx.raft.mmap.api.Processor;
import org.def4fx.raft.queue.api.Appender;
import org.def4fx.raft.queue.api.Enumerator;
import org.def4fx.raft.queue.util.FileUtil;
import org.def4fx.raft.queue.util.HistogramPrinter;
import org.dev4fx.raft.mmap.impl.MappedFile;
import org.dev4fx.raft.mmap.impl.RegionFactory;
import org.dev4fx.raft.mmap.impl.RegionRingFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MappedQueueTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MappedQueueTest.class);

    public static void main(String... args) throws Exception {
        final String fileName = FileUtil.sharedMemDir("regiontest").getAbsolutePath();
        LOGGER.info("File: {}", fileName);
        final int regionSize = (int) Math.max(MappedFile.REGION_SIZE_GRANULARITY, 1L << 16) * 64;//64 KB
        LOGGER.info("regionSize: {}", regionSize);


        final List<Processor> processors = new ArrayList<>(2);
        final RegionRingFactory asyncFactory = RegionRingFactory.forAsync(RegionFactory.ASYNC_VOLATILE_STATE_MACHINE, processors::add);
        final RegionRingFactory syncFactory = RegionRingFactory.forSync(RegionFactory.SYNC);

        final MappedQueue mappedQueue = new MappedQueue(fileName, regionSize, asyncFactory, 4, 2,64 * 16 * 1024 * 1024);
        final Appender appender = mappedQueue.appender();

        final Thread thread = new Thread(() -> {
            final Processor[] processors1 = processors.toArray(new Processor[processors.size()]);
            while (true) {
                for(final Processor processor : processors1) {
                    processor.process();
                }
            }
        });
        thread.setName("async-processor");
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler((t, e) -> LOGGER.error("{} {}", e, t));
        thread.start();


        final String testMessage = "#------------------------------------------------#\n";

        final ByteBuffer byteBuffer = ByteBuffer.allocate(8 + testMessage.getBytes().length);
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
        unsafeBuffer.putBytes(8, testMessage.getBytes());

        final int size = 8 + testMessage.getBytes().length;

//        final Histogram histogram = new Histogram(1, TimeUnit.MINUTES.toNanos(10), 3);

        final long messagesPerSecond = 90000;
        final long maxNanosPerMessage = 1000000000 / messagesPerSecond;
        final int messages = 1000000;
        final int warmup = 100000;

        final Enumerator enumerator = mappedQueue.enumerator();

        final Thread pollerThread = new Thread(() -> {
            final Histogram histogram = new Histogram(1, TimeUnit.SECONDS.toNanos(1), 3);
            long lastTimeNanos = 0;
            //final IdleStrategy idleStrategy = new BackoffIdleStrategy(400, 100, 1, TimeUnit.MICROSECONDS.toNanos(100));
            int received = 0;
            while (true) {
                if (enumerator.hasNextMessage()) {
                    received++;
                    final DirectBuffer messageBuffer = enumerator.readNextMessage();
                    long end = System.nanoTime();
                    if (received > warmup) {
                        final long startNanos = messageBuffer.getLong(0);
                        final long timeNanos = end - startNanos;
                        histogram.recordValue(timeNanos);
                        lastTimeNanos = timeNanos;
                    }
                    if (received == messages) break;
                }
            }
            HistogramPrinter.printHistogram(histogram);
            System.out.println("lastTimeNanos " + lastTimeNanos);
        });
        pollerThread.setName("async-processor");
        pollerThread.setDaemon(true);
        pollerThread.setUncaughtExceptionHandler((t, e) -> LOGGER.error("{}", e));
        pollerThread.start();


        for (int i = 0; i < messages; i++) {
            final long start = System.nanoTime();
            unsafeBuffer.putLong(0, start);
            appender.append(unsafeBuffer, 0, size);
            long end = System.nanoTime();
            final long waitUntil = start + maxNanosPerMessage;
            while (end < waitUntil) {
                end = System.nanoTime();
            }
        }

        pollerThread.join();


//        while (enumerator.hasNextMessage()) {
//            final DirectBuffer messageBuffer = enumerator.readNextMessage();
//            final long timeNanos = messageBuffer.getLong(0);
//        }


//        HistogramPrinter.printHistogram(histogram);
    }
}