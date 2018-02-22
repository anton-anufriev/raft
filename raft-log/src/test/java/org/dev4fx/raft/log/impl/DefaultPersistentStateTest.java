package org.dev4fx.raft.log.impl;


import org.agrona.concurrent.UnsafeBuffer;
import org.def4fx.raft.mmap.api.FileSizeEnsurer;
import org.dev4fx.raft.io.FileUtil;
import org.dev4fx.raft.mmap.impl.*;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

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
}