package org.dev4fx.raft.transport;

import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.DirectBuffer;

import java.util.Objects;

public interface Publisher {
    boolean publish(DirectBuffer buffer, int offset, int length);

    static Publisher aeronPublisher(final Aeron aeron, final String channel, final int streamId) {
        Objects.requireNonNull(aeron);
        Objects.requireNonNull(channel);
        final Publication publication = aeron.addPublication(channel, streamId);
        return (buffer, offset, length) -> publication.offer(buffer, offset, length) >= 0;
    }
}
