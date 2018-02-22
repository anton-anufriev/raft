package org.dev4fx.raft.transport;

import io.aeron.Publication;
import org.agrona.DirectBuffer;

import java.util.Objects;

public class AeronPublisher implements Publisher {
    private final Publication publication;

    public AeronPublisher(final Publication publication) {
        this.publication = Objects.requireNonNull(publication);
    }

    @Override
    public boolean publish(final DirectBuffer buffer, final int offset, final int length) {
        return publication.offer(buffer, offset, length) >= 0;
    }
}
