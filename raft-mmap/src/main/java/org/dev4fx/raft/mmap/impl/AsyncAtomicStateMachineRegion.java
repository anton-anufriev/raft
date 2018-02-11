package org.dev4fx.raft.mmap.impl;

import org.agrona.DirectBuffer;
import org.def4fx.raft.mmap.api.AsyncRegion;
import org.def4fx.raft.mmap.api.AsyncRegionState;
import org.def4fx.raft.mmap.api.FileSizeEnsurer;
import org.def4fx.raft.mmap.api.Region;

import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class AsyncAtomicStateMachineRegion implements AsyncRegion {
    private static final long NULL = -1;

    private final Supplier<FileChannel> fileChannelSupplier;
    private final Region.IoMapper ioMapper;
    private final Region.IoUnMapper ioUnMapper;
    private final FileSizeEnsurer fileSizeEnsurer;
    private final FileChannel.MapMode mapMode;
    private final int length;
    private final long timeoutNanos;

    private final UnmappedRegionState unmapped;
    private final MapRequestedRegionState mapRequested;
    private final MappedRegionState mapped;
    private final UnMapRequestedRegionState unmapRequested;

    private final AtomicReference<AsyncRegionState> currentState;

    private long position = NULL;
    private long address = NULL;
    private long requestedPosition;

    public AsyncAtomicStateMachineRegion(final Supplier<FileChannel> fileChannelSupplier,
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

        this.unmapped = new UnmappedRegionState();
        this.mapRequested = new MapRequestedRegionState();
        this.mapped = new MappedRegionState();
        this.unmapRequested = new UnMapRequestedRegionState();
        this.currentState = new AtomicReference<>(unmapped);
    }

    @Override
    public boolean wrap(final long position, final DirectBuffer source) {
        final int regionOffset = (int) (position % this.length);
        final long regionStartPosition = position - regionOffset;
        if (awaitMapped(regionStartPosition)) {
            source.wrap(address + regionOffset, this.length - regionOffset);
            return true;
        }
        return false;
    }

    private boolean awaitMapped(final long position) {
        if (this.position != position) {
            final long timeOutTimeNanos = System.nanoTime() + timeoutNanos;
            while (!map(position)) {
                if (timeOutTimeNanos <= System.nanoTime()) return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        if (position > 0) unmap();
    }

    @Override
    public int size() {
        return length;
    }

    @Override
    public boolean process() {
        final AsyncRegionState readState = this.currentState.get();
        final AsyncRegionState nextState = readState.processRequest();
        if (readState != nextState) {
            this.currentState.set(nextState);
            return true;
        }
        return false;
    }

    public boolean map(final long position) {
        final AsyncRegionState readState = this.currentState.get();
        final AsyncRegionState nextState = readState.requestMap(position);
        if (readState != nextState) this.currentState.set(nextState);
        return nextState == mapped;
    }

    public boolean unmap() {
        final AsyncRegionState readState = this.currentState.get();
        final AsyncRegionState nextState = readState.requestUnmap();
        if (readState != nextState) this.currentState.set(nextState);
        return nextState == unmapped;
    }

    private final class UnmappedRegionState implements AsyncRegionState {
        @Override
        public AsyncRegionState requestMap(final long position) {
            requestedPosition = position;
            return mapRequested;
        }

        @Override
        public AsyncRegionState requestUnmap() {
            return this;
        }

        @Override
        public AsyncRegionState processRequest() {
            return this;
        }
    }

    private final class MapRequestedRegionState implements AsyncRegionState {

        @Override
        public AsyncRegionState requestMap(final long position) {
            return this;
        }

        @Override
        public AsyncRegionState requestUnmap() {
            return this;
        }

        @Override
        public AsyncRegionState processRequest() {
            if (address != NULL) {
                ioUnMapper.unmap(fileChannelSupplier.get(), address, length);
                address = NULL;
            }

            if (fileSizeEnsurer.ensureSize(requestedPosition + length)) {
                address = ioMapper.map(fileChannelSupplier.get(), mapMode, requestedPosition, length);
                position = requestedPosition;
                requestedPosition = NULL;

                return mapped;
            } else {
                return this;
            }
        }
    }

    private final class MappedRegionState implements AsyncRegionState {
        @Override
        public AsyncRegionState requestMap(final long position) {
            if (AsyncAtomicStateMachineRegion.this.position != position) {
                requestedPosition = position;
                AsyncAtomicStateMachineRegion.this.position = NULL;
                return mapRequested;
            }
            return this;
        }

        @Override
        public AsyncRegionState requestUnmap() {
            position = NULL;
            return unmapRequested;
        }

        @Override
        public AsyncRegionState processRequest() {
            return this;
        }
    }

    private final class UnMapRequestedRegionState implements AsyncRegionState {
        @Override
        public AsyncRegionState requestMap(final long position) {
            return this;
        }

        @Override
        public AsyncRegionState requestUnmap() {
            return this;
        }

        @Override
        public AsyncRegionState processRequest() {
            ioUnMapper.unmap(fileChannelSupplier.get(), address, length);
            address = NULL;
            return unmapped;
        }
    }
}
