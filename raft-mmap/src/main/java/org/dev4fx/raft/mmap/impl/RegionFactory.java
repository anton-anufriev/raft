package org.dev4fx.raft.mmap.impl;

import org.def4fx.raft.mmap.api.AsyncRegion;
import org.def4fx.raft.mmap.api.FileSizeEnsurer;
import org.def4fx.raft.mmap.api.Region;

import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public interface RegionFactory<T extends Region> {
    RegionFactory<AsyncRegion> ASYNC_ATOMIC_STATE_MACHINE = (size, fileChannelSupplier, fileSizeEnsurer, mapMode) -> new AsyncAtomicStateMachineRegion(fileChannelSupplier, Region.IoMapper.DEFAULT, Region.IoUnMapper.DEFAULT, fileSizeEnsurer, mapMode, size, 2, TimeUnit.SECONDS);
    RegionFactory<AsyncRegion> ASYNC_VOLATILE_STATE_MACHINE = (size, fileChannelSupplier, fileSizeEnsurer, mapMode) -> new AsyncVolatileStateMachineRegion(fileChannelSupplier, Region.IoMapper.DEFAULT, Region.IoUnMapper.DEFAULT, fileSizeEnsurer, mapMode, size, 2, TimeUnit.SECONDS);
    RegionFactory<AsyncRegion> ASYNC_ATOMIC_EXCHANGE = (size, fileChannelSupplier, fileSizeEnsurer, mapMode) -> new AsyncAtomicExchangeRegion(fileChannelSupplier, Region.IoMapper.DEFAULT, Region.IoUnMapper.DEFAULT, fileSizeEnsurer, mapMode, size, 2, TimeUnit.SECONDS);
    RegionFactory<Region> SYNC = (size, fileChannelSupplier, fileSizeEnsurer, mapMode) -> new SyncRegion(fileChannelSupplier, Region.IoMapper.DEFAULT, Region.IoUnMapper.DEFAULT, fileSizeEnsurer, mapMode, size);

    T create(int size,
             Supplier<FileChannel> fileChannelSupplier,
             FileSizeEnsurer fileSizeEnsurer,
             FileChannel.MapMode mapMode);

}
