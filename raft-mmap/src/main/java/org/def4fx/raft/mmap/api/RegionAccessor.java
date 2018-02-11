package org.def4fx.raft.mmap.api;

import org.agrona.DirectBuffer;

import java.io.Closeable;

public interface RegionAccessor extends Closeable {
    /**
     * Wraps the buffer starting from given position to the end of the mapped region.
     * Once mapped, buffer.capacity will indicate the length of the mapped memory.
     * @param position - position in the file.
     * @param buffer
     * @return true if mapped successfully, otherwise false.
     */
    boolean wrap(long position, DirectBuffer buffer);
    int size();

    @Override
    void close();
}
