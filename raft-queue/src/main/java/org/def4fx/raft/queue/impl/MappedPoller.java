package org.def4fx.raft.queue.impl;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.def4fx.raft.mmap.api.RegionAccessor;
import org.def4fx.raft.queue.api.Poller;

import java.util.Objects;

public class MappedPoller implements Poller {
    private final static int LENGTH_SIZE = 4;
    private final RegionAccessor regionAccessor;
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer();

    private long position = 0;

    public MappedPoller(final RegionAccessor regionAccessor) {
        this.regionAccessor = Objects.requireNonNull(regionAccessor);
    }

    @Override
    public boolean poll(final DirectBuffer buffer) {
        if (regionAccessor.wrap(position, unsafeBuffer)) {
            final int length = unsafeBuffer.getIntVolatile(0);
            if (length > 0) {
                buffer.wrap(unsafeBuffer, LENGTH_SIZE, length);
                position += LENGTH_SIZE + length;
                return true;
            } else if (length < 0) {
                position += LENGTH_SIZE + -length;
                return poll(buffer);
            }
        }
        return false;
    }


    @Override
    public void close() {
        regionAccessor.close();
    }
}
