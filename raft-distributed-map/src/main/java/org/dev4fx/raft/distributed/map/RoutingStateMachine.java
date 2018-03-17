package org.dev4fx.raft.distributed.map;

import org.agrona.DirectBuffer;
import org.dev4fx.raft.dmap.sbe.MessageHeaderDecoder;
import org.dev4fx.raft.state.StateMachine;

import java.util.Objects;
import java.util.function.IntFunction;

public class RoutingStateMachine implements StateMachine {
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final IntFunction<? extends StateMachine> mapIdToMapStateMachine;

    public RoutingStateMachine(final IntFunction<? extends StateMachine> mapIdToMapStateMachine) {
        this.mapIdToMapStateMachine = Objects.requireNonNull(mapIdToMapStateMachine);
    }

    @Override
    public void onCommand(final int sourceId, final long sequence, final DirectBuffer buffer, final int offset, final int length) {
        messageHeaderDecoder.wrap(buffer, offset);
        mapIdToMapStateMachine.apply(messageHeaderDecoder.mapId())
                .onCommand(sourceId, sequence, buffer, offset, length);
    }
}