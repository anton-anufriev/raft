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
package org.dev4fx.raft.log.impl;


import org.agrona.concurrent.UnsafeBuffer;
import org.dev4fx.raft.mmap.api.FileSizeEnsurer;
import org.dev4fx.raft.io.FileUtil;
import org.dev4fx.raft.mmap.impl.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class DefaultPersistentStateTest {
    private static final long MAX_FILE_SIZE = 64 * 16 * 1024 * 1024;

    private DefaultPersistentState log;

    @Before
    public void setUp() throws Exception {


        final String headerFileName = FileUtil.sharedMemDir("logHeader").getAbsolutePath();
        final String indexFileName = FileUtil.sharedMemDir("logIndex").getAbsolutePath();
        final String payloadFileName = FileUtil.sharedMemDir("logPayload").getAbsolutePath();
        final int headerRegionSize = (int) Math.max(MappedFile.REGION_SIZE_GRANULARITY, 64);
        final int indexRegionSize = (int) Math.max(MappedFile.REGION_SIZE_GRANULARITY, 1L << 16);
        final int payloadRegionSize = (int) Math.max(MappedFile.REGION_SIZE_GRANULARITY, 1L << 16) * 64;

        //final RegionRingFactory asyncFactory = RegionRingFactory.forAsync(RegionFactory.ASYNC_VOLATILE_STATE_MACHINE, processors::add);
        final RegionRingFactory syncFactory = RegionRingFactory.forSync(RegionFactory.SYNC);


        final MappedFile headerFile = new MappedFile(headerFileName, MappedFile.Mode.READ_WRITE,
                headerRegionSize, DefaultPersistentStateTest::initFile);

        final MappedFile indexFile = new MappedFile(indexFileName, MappedFile.Mode.READ_WRITE,
                headerRegionSize, DefaultPersistentStateTest::initFile);

        final MappedFile payloadFile = new MappedFile(payloadFileName, MappedFile.Mode.READ_WRITE,
                payloadRegionSize, DefaultPersistentStateTest::initFile);


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


        log = new DefaultPersistentState(indexRegionRingAccessor, payloadRegionRingAccessor, headerRegionRingAccessor);
    }

    @Test
    public void name() throws Exception {

        final String testMessage = "#------------------------------------------------#\n";
                                    //    ---------------------------------------------#

        final ByteBuffer byteBuffer = ByteBuffer.allocate(testMessage.getBytes().length);
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
        unsafeBuffer.putBytes(0, testMessage.getBytes());

        final UnsafeBuffer payloadBuffer = new UnsafeBuffer();


        final long size = log.size();
        final long lastIndex = log.lastIndex();
        final int lastTerm = log.lastTerm();
        //final long lastIndex = log.

        if (lastIndex > 1) {
            log.wrap(lastIndex - 1, payloadBuffer);
            System.out.println("prev log: " + payloadBuffer.getStringWithoutLengthAscii(0, payloadBuffer.capacity()));
        }


        System.out.println("size = " + size);
        System.out.println("lastIndex = " + lastIndex);
        System.out.println("lastTerm = " + lastTerm);

        log.append(lastTerm + 1, unsafeBuffer, 0, testMessage.getBytes().length);


        final long sizeAfter = log.size();
        final long lastIndexAfter = log.lastIndex();
        final int lastTermAfter = log.lastTerm();
        //final long lastIndex = log.


        System.out.println("sizeAfter = " + sizeAfter);
        System.out.println("lastIndexAfter = " + lastIndexAfter);
        System.out.println("lastTermAfter = " + lastTermAfter);

        log.close();

    }

    public static void initFile(final FileChannel fileChannel, final MappedFile.Mode mode) throws IOException {
        switch (mode) {
            case READ_ONLY:
                if (fileChannel.size() < 8) {
                    throw new IllegalArgumentException("Invalid io format");
                }
                break;
            case READ_WRITE:
                if (fileChannel.size() >= 8) break;
            case READ_WRITE_CLEAR:
                final FileLock lock = fileChannel.lock();
                try {
                    fileChannel.truncate(0);
                    fileChannel.transferFrom(InitialBytes.ZERO, 0, 8);
                    fileChannel.force(true);
                } finally {
                    lock.release();
                }
                break;
            default:
                throw new IllegalArgumentException("Invalid mode: " + mode);
        }
    }

}