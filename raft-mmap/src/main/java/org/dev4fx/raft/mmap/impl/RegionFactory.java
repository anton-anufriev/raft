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
package org.dev4fx.raft.mmap.impl;

import org.dev4fx.raft.mmap.api.AsyncRegion;
import org.dev4fx.raft.mmap.api.FileSizeEnsurer;
import org.dev4fx.raft.mmap.api.Region;

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
