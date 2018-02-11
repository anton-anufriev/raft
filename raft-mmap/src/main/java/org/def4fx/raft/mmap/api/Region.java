package org.def4fx.raft.mmap.api;


import org.agrona.IoUtil;

import java.nio.channels.FileChannel;

public interface Region extends RegionAccessor, RegionMapper {
    interface IoUnMapper {
        IoUnMapper DEFAULT = IoUtil::unmap;

        void unmap(FileChannel fileChannel, long address, long length);
    }

    interface IoMapper {
        IoMapper DEFAULT = IoUtil::map;

        long map(FileChannel fileChannel, FileChannel.MapMode mapMode, long offset, long length);
    }
}
