package org.dev4fx.raft.state;

import org.agrona.DirectBuffer;
import org.dev4fx.raft.sbe.*;

import java.util.Objects;
import java.util.function.Predicate;

public class HeaderFilteringServerState implements ServerState {
    private final Predicate<HeaderDecoder> filter;
    private final ServerState delegateServerState;

    public HeaderFilteringServerState(final Predicate<HeaderDecoder> filter,
                                      final ServerState delegateServerState) {
        this.filter = Objects.requireNonNull(filter);
        this.delegateServerState = Objects.requireNonNull(delegateServerState);
    }

    @Override
    public Role role() {
        return delegateServerState.role();
    }

    @Override
    public void onTransition() {
        delegateServerState.onTransition();
    }

    @Override
    public Transition processTick() {
        return delegateServerState.processTick();
    }

    @Override
    public Transition onVoteRequest(final VoteRequestDecoder voteRequestDecoder) {
        if (!filter.test(voteRequestDecoder.header())) return Transition.STEADY;
        return delegateServerState.onVoteRequest(voteRequestDecoder);
    }

    @Override
    public Transition onVoteResponse(final VoteResponseDecoder voteResponseDecoder) {
        if (!filter.test(voteResponseDecoder.header())) return Transition.STEADY;
        return delegateServerState.onVoteResponse(voteResponseDecoder);
    }

    @Override
    public Transition onAppendRequest(final AppendRequestDecoder appendRequestDecoder) {
        if (!filter.test(appendRequestDecoder.header())) return Transition.STEADY;
        return delegateServerState.onAppendRequest(appendRequestDecoder);
    }

    @Override
    public Transition onAppendResponse(final AppendResponseDecoder appendResponseDecoder) {
        if (!filter.test(appendResponseDecoder.header())) return Transition.STEADY;
        return delegateServerState.onAppendResponse(appendResponseDecoder);
    }

    @Override
    public Transition onCommandRequest(final DirectBuffer buffer, final int offset, final int length) {
        return delegateServerState.onCommandRequest(buffer, offset, length);
    }

    @Override
    public Transition onTimeoutNow() {
        return delegateServerState.onTimeoutNow();
    }
}
