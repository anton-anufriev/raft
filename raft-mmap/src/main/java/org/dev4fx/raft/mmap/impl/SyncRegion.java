package org.dev4fx.raft.mmap.impl;

import org.agrona.DirectBuffer;
import org.def4fx.raft.mmap.api.FileSizeEnsurer;
import org.def4fx.raft.mmap.api.Region;

import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.function.Supplier;

public class SyncRegion implements Region {
    private static final long NULL = -1;

    private final Supplier<FileChannel> fileChannelSupplier;
    private final Region.IoMapper ioMapper;
    private final Region.IoUnMapper ioUnMapper;
    private final FileSizeEnsurer fileSizeEnsurer;
    private final FileChannel.MapMode mapMode;
    private final int length;

    private long currentPosition = NULL;
    private long currentAddress = NULL;

    public SyncRegion(final Supplier<FileChannel> fileChannelSupplier,
                      final IoMapper ioMapper,
                      final IoUnMapper ioUnMapper,
                      final FileSizeEnsurer fileSizeEnsurer,
                      final FileChannel.MapMode mapMode,
                      final int length) {
        this.fileChannelSupplier = Objects.requireNonNull(fileChannelSupplier);
        this.ioMapper = Objects.requireNonNull(ioMapper);
        this.ioUnMapper = Objects.requireNonNull(ioUnMapper);
        this.fileSizeEnsurer = Objects.requireNonNull(fileSizeEnsurer);
        this.mapMode = Objects.requireNonNull(mapMode);
        this.length = length;
    }

    @Override
    public boolean wrap(final long position, final DirectBuffer source) {
        final int regionOffset = (int) (position & (this.length - 1));
        final long regionStartPosition = position - regionOffset;
        if (map(regionStartPosition)) {
            source.wrap(currentAddress + regionOffset, this.length - regionOffset);
            return true;
        }
        return false;
    }

    @Override
    public boolean map(final long regionStartPosition) {
        if (regionStartPosition < 0) throw new IllegalArgumentException("Invalid regionStartPosition " + regionStartPosition);

        if (currentPosition == regionStartPosition) return true;

        if (currentAddress != NULL) {
            ioUnMapper.unmap(fileChannelSupplier.get(), currentAddress, length);
            currentAddress = NULL;
        }
        if (fileSizeEnsurer.ensureSize(regionStartPosition + length)) {
            currentAddress = ioMapper.map(fileChannelSupplier.get(), mapMode, regionStartPosition, length);
            currentPosition = regionStartPosition;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean unmap() {
        if (currentAddress != NULL) {
            ioUnMapper.unmap(fileChannelSupplier.get(), currentAddress, length);
            currentAddress = NULL;
            currentPosition = NULL;
        }
        return true;
    }

    @Override
    public void close() {
        unmap();
    }

    @Override
    public int size() {
        return length;
    }

}
