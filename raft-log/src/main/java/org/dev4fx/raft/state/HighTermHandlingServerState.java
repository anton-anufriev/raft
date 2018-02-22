package org.dev4fx.raft.state;

import org.agrona.DirectBuffer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.sbe.*;

import java.util.Objects;

public class HighTermHandlingServerState implements ServerState {
    private final ServerState delegateServerState;
    private final PersistentState persistentState;

    public HighTermHandlingServerState(final ServerState delegateServerState,
                                       final PersistentState persistentState) {
        this.delegateServerState = Objects.requireNonNull(delegateServerState);
        this.persistentState = Objects.requireNonNull(persistentState);
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
        if (updateHighTerm(voteRequestDecoder.header())) {
            return Transition.TO_FOLLOWER;
        }
        return delegateServerState.onVoteRequest(voteRequestDecoder);
    }

    @Override
    public Transition onVoteResponse(final VoteResponseDecoder voteResponseDecoder) {
        if (updateHighTerm(voteResponseDecoder.header())) {
            return Transition.TO_FOLLOWER;
        }
        return delegateServerState.onVoteResponse(voteResponseDecoder);
    }

    @Override
    public Transition onAppendRequest(final AppendRequestDecoder appendRequestDecoder) {
        if (updateHighTerm(appendRequestDecoder.header())) {
            return Transition.TO_FOLLOWER;
        }
        return delegateServerState.onAppendRequest(appendRequestDecoder);
    }

    @Override
    public Transition onAppendResponse(final AppendResponseDecoder appendResponseDecoder) {
        if (updateHighTerm(appendResponseDecoder.header())) {
            return Transition.TO_FOLLOWER;
        }
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

    private boolean updateHighTerm(final HeaderDecoder headerDecoder) {
        final int messageTerm = headerDecoder.term();
        if (messageTerm > persistentState.currentTerm()) {
            persistentState.clearVoteAndSetCurrentTerm(messageTerm);
            return true;
        }
        return false;
    }

}
