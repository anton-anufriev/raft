package org.dev4fx.raft.log.impl;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.def4fx.raft.mmap.api.RegionAccessor;
import org.dev4fx.raft.log.api.PersistentState;

import java.util.Objects;

public class DefaultPersistentState implements PersistentState {
    private static final int TERM_OFFSET = 0;
    private static final int TERM_SIZE = 4;

    private static final int SIZE_OFFSET = 0;
    private static final int SIZE_SIZE = 8;
    private static final int CURRENT_TERM_OFFSET = SIZE_OFFSET + SIZE_SIZE;
    private static final int CURRENT_TERM_SIZE = TERM_SIZE;
    private static final int VOTE_OFFSET = CURRENT_TERM_OFFSET + CURRENT_TERM_SIZE;
    private static final int VOTE_SIZE = 4;

    private static final int PAYLOAD_POSITION_OFFSET = TERM_OFFSET + TERM_SIZE;
    private static final int PAYLOAD_POSITION_SIZE = 8;
    private static final int PAYLOAD_LENGTH_OFFSET = PAYLOAD_POSITION_OFFSET + PAYLOAD_POSITION_SIZE;
    private static final int PAYLOAD_LENGTH_SIZE = 4;
    private static final int INDEX_ROW_SIZE = TERM_SIZE + PAYLOAD_POSITION_SIZE + PAYLOAD_LENGTH_SIZE;

    private final RegionAccessor indexAccessor;
    private final RegionAccessor payLoadAccessor;
    private final RegionAccessor headerAccessor;

    private final UnsafeBuffer headerBuffer;
    private final UnsafeBuffer indexBuffer;
    private final UnsafeBuffer payloadBuffer;

    public DefaultPersistentState(final RegionAccessor indexAccessor,
                                  final RegionAccessor payLoadAccessor,
                                  final RegionAccessor headerAccessor) {
        this.indexAccessor = Objects.requireNonNull(indexAccessor);
        this.payLoadAccessor = Objects.requireNonNull(payLoadAccessor);
        this.headerAccessor = Objects.requireNonNull(headerAccessor);
        headerBuffer = new UnsafeBuffer();
        indexBuffer = new UnsafeBuffer();
        payloadBuffer = new UnsafeBuffer();
        headerAccessor.wrap(0, headerBuffer);
    }

    @Override
    public void append(final int term, final DirectBuffer buffer, final int offset, final int length) {
        final long lastIndex = lastIndex();
        long newPayloadPosition = 0;
        if (lastIndex > NULL_INDEX) {
            wrapIndex(lastIndex);
            final long lastPayloadPosition = indexPayloadPosition();
            final int lastPayloadLength = indexPayloadLength();
            newPayloadPosition = lastPayloadPosition + lastPayloadLength;
        }

        if (payLoadAccessor.wrap(newPayloadPosition, payloadBuffer)) {
            if (payloadBuffer.capacity() < length) {
                newPayloadPosition = newPayloadPosition + payloadBuffer.capacity();
                if (!payLoadAccessor.wrap(newPayloadPosition, payloadBuffer)) {
                    throw new IllegalStateException("Failed to wrap payload buffer for position " + newPayloadPosition);
                }
            }
            buffer.getBytes(offset, payloadBuffer, 0, length);
            wrapIndex(lastIndex + 1);
            indexTerm(term);
            indexPayloadPosition(newPayloadPosition);
            indexPayloadLength(length);
            size(size() + 1);
        } else {
            throw new IllegalStateException("Failed to wrap payload buffer for position " + newPayloadPosition);
        }
    }

    @Override
    public long size() {
        final long size = headerBuffer.getLong(SIZE_OFFSET);
        return size < 0 ? 0 : size;
    }

    private void size(final long size) {
        headerBuffer.putLong(SIZE_OFFSET, size);
    }

    @Override
    public int currentTerm() {
        return headerBuffer.getInt(CURRENT_TERM_OFFSET);
    }

    @Override
    public void currentTerm(final int term) {
        headerBuffer.putInt(CURRENT_TERM_OFFSET, term);
    }

    @Override
    public int vote() {
        return headerBuffer.getInt(VOTE_OFFSET);
    }

    @Override
    public void vote(final int serverId) {
        headerBuffer.putInt(VOTE_OFFSET, serverId);
    }


    private void wrapIndex(long index) {
        if (!indexAccessor.wrap(index * INDEX_ROW_SIZE, indexBuffer)) {
            throw new IllegalStateException("Failed to wrap index buffer for index " + index);
        }
    }

    private int indexTerm() {
        return indexBuffer.getInt(TERM_OFFSET);
    }

    private void indexTerm(final int term) {
        indexBuffer.putInt(TERM_OFFSET, term);
    }

    private long indexPayloadPosition() {
        return indexBuffer.getLong(PAYLOAD_POSITION_OFFSET);
    }

    private void indexPayloadPosition(final long payloadPosition) {
        indexBuffer.putLong(PAYLOAD_POSITION_OFFSET, payloadPosition);
    }

    private int indexPayloadLength() {
        return indexBuffer.getInt(PAYLOAD_LENGTH_OFFSET);
    }

    private void indexPayloadLength(final int payloadLength) {
        indexBuffer.putInt(PAYLOAD_LENGTH_OFFSET, payloadLength);
    }

    @Override
    public int term(final long index) {
        if (index > NULL_INDEX && index <= lastIndex()) {
            wrapIndex(index);
            return indexTerm();
        } else {
            return currentTerm();
        }
    }

    @Override
    public void wrap(final long index, final DirectBuffer buffer) {
        final long lastIndex = lastIndex();
//        final long size = size();
        if (index > NULL_INDEX && index <= lastIndex) {
            wrapIndex(index);
            final long payloadPosition = indexPayloadPosition();
            final int payloadLength = indexPayloadLength();
            if (payLoadAccessor.wrap(payloadPosition, payloadBuffer)) {
                buffer.wrap(payloadBuffer, 0, payloadLength);
            } else {
                throw new IllegalStateException("Failed to wrap payload buffer for position " + payloadPosition);
            }
        } else {
            throw new IllegalArgumentException("Index [" + index + "] must be positive and <= " + lastIndex);
        }
    }

    @Override
    public void truncate(final long size) {
        final long currentSize = size();
        if (size >= 0 && size <= currentSize) {
            size(size);
        } else {
            throw new IllegalArgumentException("Size [" + size + "] must be positive and <= current size " + currentSize);
        }
    }

    @Override
    public void close() {
        payLoadAccessor.close();
        indexAccessor.close();
        headerAccessor.close();
    }
}
