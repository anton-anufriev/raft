package org.dev4fx.raft.state;

import org.dev4fx.raft.sbe.HeaderDecoder;

import java.util.function.Predicate;

public class DestinationFilter implements Predicate<HeaderDecoder> {
    private final int serverId;

    private DestinationFilter(final int serverId) {
        this.serverId = serverId;
    }

    public static Predicate<HeaderDecoder> forServer(final int serverId) {
        return new DestinationFilter(serverId);
    }

    @Override
    public boolean test(final HeaderDecoder headerDecoder) {
        final int destinationId = headerDecoder.destinationId();
        return destinationId == serverId || destinationId == Server.ALL;
    }
}
