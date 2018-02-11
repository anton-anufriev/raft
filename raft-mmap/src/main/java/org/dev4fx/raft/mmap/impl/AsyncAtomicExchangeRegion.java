package org.dev4fx.raft.mmap.impl;

import org.agrona.DirectBuffer;
import org.def4fx.raft.mmap.api.AsyncRegion;
import org.def4fx.raft.mmap.api.FileSizeEnsurer;
import org.def4fx.raft.mmap.api.Region;

import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class AsyncAtomicExchangeRegion implements AsyncRegion {
    private static final long NULL = -1;

    private final Supplier<FileChannel> fileChannelSupplier;
    private final Region.IoMapper ioMapper;
    private final Region.IoUnMapper ioUnMapper;
    private final FileSizeEnsurer fileSizeEnsurer;
    private final FileChannel.MapMode mapMode;
    private final int length;
    private final long timeoutNanos;


    private final AtomicLong requestPosition = new AtomicLong(NULL);
    private final AtomicLong responseAddress = new AtomicLong(NULL);

    private long readerPosition = NULL;
    private long readerAddress = NULL;

    private long writerPosition = NULL;
    private long writerAddress = NULL;

    public AsyncAtomicExchangeRegion(final Supplier<FileChannel> fileChannelSupplier,
                                     final IoMapper ioMapper,
                                     final IoUnMapper ioUnMapper,
                                     final FileSizeEnsurer fileSizeEnsurer,
                                     final FileChannel.MapMode mapMode,
                                     final int length,
                                     final long timeout,
                                     final TimeUnit timeUnits) {
        this.fileChannelSupplier = Objects.requireNonNull(fileChannelSupplier);
        this.ioMapper = Objects.requireNonNull(ioMapper);
        this.ioUnMapper = Objects.requireNonNull(ioUnMapper);
        this.fileSizeEnsurer = Objects.requireNonNull(fileSizeEnsurer);
        this.mapMode = Objects.requireNonNull(mapMode);
        this.length = length;
        this.timeoutNanos = timeUnits.toNanos(timeout);
    }

    @Override
    public boolean wrap(final long position, final DirectBuffer source) {
        final int regionOffset = (int) (position % this.length);
        final long regionStartPosition = position - regionOffset;
        if (awaitMapped(regionStartPosition)) {
            source.wrap(readerAddress + regionOffset, this.length - regionOffset);
            return true;
        }
        return false;
    }

    @Override
    public boolean map(final long regionStartPosition) {
        if (readerPosition == regionStartPosition) return true;

        readerPosition = NULL;
        requestPosition.set(regionStartPosition); //can be lazy

        return false;
    }

    @Override
    public boolean unmap() {
        final boolean hadBeenUnmapped = readerPosition == NULL;
        readerPosition = NULL;
        requestPosition.set(NULL); //can be lazy

        return hadBeenUnmapped;
    }

    private boolean awaitMapped(final long regionStartPosition) {
        if (!map(regionStartPosition)) {
            final long timeOutTimeNanos = System.nanoTime() + timeoutNanos;
            long respAddress;
            do {
                if (timeOutTimeNanos <= System.nanoTime()) return false; // timeout
                respAddress = responseAddress.get();
            } while (readerAddress == respAddress);

            readerAddress = respAddress;
            readerPosition = regionStartPosition;
        }
        return true;
    }

    @Override
    public boolean process() {
        final long reqPosition = requestPosition.get();
        if (writerPosition != reqPosition) {
            if (writerAddress != NULL) {
                ioUnMapper.unmap(fileChannelSupplier.get(), writerAddress, length);
                writerAddress = NULL;
            }
            if (reqPosition != NULL) {
                if (fileSizeEnsurer.ensureSize(reqPosition + length)) {
                    writerAddress = ioMapper.map(fileChannelSupplier.get(), mapMode, reqPosition, length);
                }
            }
            writerPosition = reqPosition;
            responseAddress.set(writerAddress); //can be lazy
            return true;
        }
        return false;
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
