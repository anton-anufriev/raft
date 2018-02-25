package org.dev4fx.raft.state;

import org.agrona.DirectBuffer;
import org.dev4fx.raft.sbe.*;
import org.slf4j.Logger;

import java.util.Objects;

public class LoggingServerState implements ServerState {
    private final ServerState delegateServerState;
    private final StringBuilder stringBuilder;
    private final Logger logger;

    public LoggingServerState(final ServerState delegateServerState,
                              final StringBuilder stringBuilder,
                              final Logger logger) {
        this.delegateServerState = Objects.requireNonNull(delegateServerState);
        this.stringBuilder = Objects.requireNonNull(stringBuilder);
        this.logger = Objects.requireNonNull(logger);
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
        stringBuilder.setLength(0);
        voteRequestDecoder.appendTo(stringBuilder);
        logger.info("onVoteRequest: {}", stringBuilder);
        return delegateServerState.onVoteRequest(voteRequestDecoder);
    }

    @Override
    public Transition onVoteResponse(final VoteResponseDecoder voteResponseDecoder) {
        stringBuilder.setLength(0);
        voteResponseDecoder.appendTo(stringBuilder);
        logger.info("onVoteResponse: {}", stringBuilder);
        return delegateServerState.onVoteResponse(voteResponseDecoder);
    }

    @Override
    public Transition onAppendRequest(final AppendRequestDecoder appendRequestDecoder) {
        stringBuilder.setLength(0);
        appendRequestDecoder.appendTo(stringBuilder);
        logger.info("onAppendRequest: {}", stringBuilder);
        return delegateServerState.onAppendRequest(appendRequestDecoder);
    }

    @Override
    public Transition onAppendResponse(final AppendResponseDecoder appendResponseDecoder) {
        stringBuilder.setLength(0);
        appendResponseDecoder.appendTo(stringBuilder);
        logger.info("onAppendResponse: {}", stringBuilder);
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
