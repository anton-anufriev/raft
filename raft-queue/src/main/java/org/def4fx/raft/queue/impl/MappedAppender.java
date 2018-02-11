package org.def4fx.raft.queue.impl;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.def4fx.raft.mmap.api.RegionAccessor;
import org.def4fx.raft.queue.api.Appender;

import java.util.Objects;

public class MappedAppender implements Appender {
    private final static int LENGTH_SIZE = 4;
    private final RegionAccessor regionAccessor;
    private final int alignment;
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer();

    private long position = 0;

    public MappedAppender(final RegionAccessor regionAccessor, final int alignment) {
        this.regionAccessor = Objects.requireNonNull(regionAccessor);
        this.alignment = alignment;
        if (Integer.bitCount(alignment) > 1) throw new IllegalArgumentException("alignment " + alignment + " must be power of two");
    }

    @Override
    public boolean append(final DirectBuffer buffer, final int offset, final int length) {
        final int messageLength = length + LENGTH_SIZE;

        final int paddedMessageLength = BitUtil.align(messageLength, alignment);
//        final int cacheLineRemainder = messageLength & alignmentMask;
//        final int paddedMessageLength = cacheLineRemainder == 0 ? messageLength :
//                messageLength + alignment - cacheLineRemainder;

        if (paddedMessageLength > regionAccessor.size()) {
            throw new IllegalStateException("Length is too big");
        }

        if (regionAccessor.wrap(position, unsafeBuffer)) {
            final int capacity = unsafeBuffer.capacity();

            if (capacity < paddedMessageLength) {
                unsafeBuffer.setMemory(LENGTH_SIZE, length, (byte) 0);
                unsafeBuffer.putIntOrdered(0, -(capacity - LENGTH_SIZE));
                position += capacity;
                append(buffer, offset, length);
            }

            buffer.getBytes(offset, unsafeBuffer, LENGTH_SIZE, length);

            unsafeBuffer.putIntOrdered(0, paddedMessageLength - LENGTH_SIZE);
            position += paddedMessageLength;
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        regionAccessor.close();
    }
}
