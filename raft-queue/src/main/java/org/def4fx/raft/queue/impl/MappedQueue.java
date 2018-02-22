package org.def4fx.raft.queue.impl;

import org.def4fx.raft.mmap.api.FileSizeEnsurer;
import org.def4fx.raft.mmap.api.RegionAccessor;
import org.def4fx.raft.queue.api.Appender;
import org.def4fx.raft.queue.api.Poller;
import org.def4fx.raft.queue.api.Queue;
import org.dev4fx.raft.mmap.impl.FileInitialiser;
import org.dev4fx.raft.mmap.impl.MappedFile;
import org.dev4fx.raft.mmap.impl.RegionRingAccessor;
import org.dev4fx.raft.mmap.impl.RegionRingFactory;

import java.io.IOException;

public class MappedQueue implements Queue {
    private final RegionAccessor appenderRegionRingAccessor;
    private final RegionAccessor enumeratorRegionRingAccessor;

    public MappedQueue(final String fileName,
                       final int regionSize,
                       final RegionRingFactory factory,
                       final int ringSize,
                       final int regionsToMapAhead,
                       final long maxFileSize) throws IOException {
        final MappedFile appenderFile = new MappedFile(fileName, MappedFile.Mode.READ_WRITE_CLEAR,
                regionSize, FileInitialiser::initFile);
        final MappedFile enumeratorFile = new MappedFile(fileName, MappedFile.Mode.READ_ONLY,
                regionSize, FileInitialiser::initFile);

        appenderRegionRingAccessor = new RegionRingAccessor(
                factory.create(
                        ringSize,
                        regionSize,
                        appenderFile::getFileChannel,
                        FileSizeEnsurer.forWritableFile(appenderFile::getFileLength, appenderFile::setFileLength, maxFileSize),
                        appenderFile.getMode().getMapMode()),
                regionSize,
                regionsToMapAhead,
                appenderFile::close);

        enumeratorRegionRingAccessor = new RegionRingAccessor(
                factory.create(
                        ringSize,
                        regionSize,
                        enumeratorFile::getFileChannel,
                        FileSizeEnsurer.NO_OP,
                        enumeratorFile.getMode().getMapMode()),
                regionSize,
                regionsToMapAhead,
                enumeratorFile::close);
    }

    @Override
    public Appender appender() {
        return new MappedAppender(appenderRegionRingAccessor, 64);
    }

    @Override
    public Poller poller() {
        return new MappedPoller(enumeratorRegionRingAccessor);
    }

    @Override
    public void close() {
        appenderRegionRingAccessor.close();
        enumeratorRegionRingAccessor.close();
    }
}
