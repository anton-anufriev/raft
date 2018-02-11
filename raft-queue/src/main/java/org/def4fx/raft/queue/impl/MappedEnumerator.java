package org.def4fx.raft.queue.impl;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.def4fx.raft.mmap.api.RegionAccessor;
import org.def4fx.raft.queue.api.Enumerator;

import java.util.Objects;

public class MappedEnumerator implements Enumerator {
    private final static int LENGTH_SIZE = 4;
    private final RegionAccessor regionAccessor;
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer();
    private final UnsafeBuffer messageBuffer = new UnsafeBuffer();

    private long position = 0;
    private int nextLength = 0;

    public MappedEnumerator(final RegionAccessor regionAccessor) {
        this.regionAccessor = Objects.requireNonNull(regionAccessor);
    }

    @Override
    public boolean hasNextMessage() {
        if (regionAccessor.wrap(position, unsafeBuffer)) {
            final int length = unsafeBuffer.getIntVolatile(0);
            if (length > 0) {
                nextLength = length;
                return true;
            } else if (length < 0) {
                position += LENGTH_SIZE + -length;
                return hasNextMessage();
            }
        }
        return false;
    }

    @Override
    public DirectBuffer readNextMessage() {
        if (nextLength > 0) {
            messageBuffer.wrap(unsafeBuffer, LENGTH_SIZE, nextLength);
            position += LENGTH_SIZE + nextLength;
            nextLength = 0;
            return messageBuffer;
        } else {
            throw new IllegalStateException("Not ready to read message");
        }
    }

    @Override
    public Enumerator skipNextMessage() {
        if (nextLength > 0) {
            position += LENGTH_SIZE + nextLength;
            nextLength = 0;
        } else {
            throw new IllegalStateException("Not ready to read message");
        }
        return this;
    }

    @Override
    public void close() {
        regionAccessor.close();
    }
}
