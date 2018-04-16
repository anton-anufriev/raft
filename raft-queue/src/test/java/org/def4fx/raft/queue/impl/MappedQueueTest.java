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
package org.def4fx.raft.queue.impl;

import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.def4fx.raft.queue.api.Appender;
import org.def4fx.raft.queue.api.Poller;
import org.def4fx.raft.queue.util.FileUtil;
import org.def4fx.raft.queue.util.HistogramPrinter;
import org.dev4fx.raft.mmap.impl.MappedFile;
import org.dev4fx.raft.mmap.api.RegionFactory;
import org.dev4fx.raft.mmap.api.RegionRingFactory;
import org.dev4fx.raft.process.Process;
import org.dev4fx.raft.process.MutableProcessStepChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class MappedQueueTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MappedQueueTest.class);

    private static final Supplier<RegionRingFactory> ASYNC = () -> {
        final MutableProcessStepChain processStepChain = new MutableProcessStepChain();
        return RegionRingFactory.forAsync(RegionFactory.ASYNC_VOLATILE_STATE_MACHINE,
                processor -> processStepChain.thenStep(processor::process),
                () -> {
                    final Process regionMapper = new Process("RegionMapper",
                            () -> {}, () -> {},
                            new BusySpinIdleStrategy()::idle,
                            (s, e) -> LOGGER.error("{} {}", s, e, e),
                            10, TimeUnit.SECONDS,
                            processStepChain.getOrNoop()
                    );
                    regionMapper.start();
                });
    };
    private static final Supplier<RegionRingFactory> SYNC = () -> RegionRingFactory.forSync(RegionFactory.SYNC);

    private enum RegionMappingConfig implements Supplier<RegionRingFactory> {
        SYNC {
            @Override
            public RegionRingFactory get() {
                return MappedQueueTest.SYNC.get();
            }
        },
        ASYNC {
            @Override
            public RegionRingFactory get() {
                return MappedQueueTest.ASYNC.get();
            }
        }
    }


    public static void main(String... args) throws Exception {
        final String fileName = FileUtil.sharedMemDir("regiontest").getAbsolutePath();
        LOGGER.info("File: {}", fileName);
        final int regionSize = (int) Math.max(MappedFile.REGION_SIZE_GRANULARITY, 1L << 16) * 1024 * 4;//64 KB
        LOGGER.info("regionSize: {}", regionSize);

        final RegionMappingConfig regionMappingConfig = RegionMappingConfig.valueOf(args[0]);
        final RegionRingFactory regionRingFactory = regionMappingConfig.get();

        final MappedQueue mappedQueue = new MappedQueue(fileName, regionSize, regionRingFactory, 4, 1,64L * 16 * 1024 * 1024 * 4);
        final Appender appender = mappedQueue.appender();

        regionRingFactory.onComplete();

        final String testMessage = "#------------------------------------------------#\n";

        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(8 + testMessage.getBytes().length);
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
        unsafeBuffer.putBytes(8, testMessage.getBytes());

        final int size = 8 + testMessage.getBytes().length;

//        final Histogram histogram = new Histogram(1, TimeUnit.MINUTES.toNanos(10), 3);

        final long messagesPerSecond = 90000;
        final long maxNanosPerMessage = 1000000000 / messagesPerSecond;
        final int messages = 2000000;
        final int warmup = 100000;

        final Poller poller = mappedQueue.poller();

        final Thread pollerThread = new Thread(() -> {
            final Histogram histogram = new Histogram(1, TimeUnit.SECONDS.toNanos(1), 3);
            long lastTimeNanos = 0;
            final DirectBuffer messageBuffer = new UnsafeBuffer();
            int received = 0;
            while (true) {
                if (poller.poll(messageBuffer)) {
                    received++;
                    long end = System.nanoTime();
                    if (received > warmup) {
                        final long startNanos = messageBuffer.getLong(0);
                        final long timeNanos = end - startNanos;
                        histogram.recordValue(timeNanos);
                        lastTimeNanos = Long.max(lastTimeNanos, timeNanos);
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
    }
}