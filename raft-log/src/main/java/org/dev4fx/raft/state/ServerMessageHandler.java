package org.dev4fx.raft.state;

import org.agrona.DirectBuffer;
import org.dev4fx.raft.process.ProcessStep;
import org.dev4fx.raft.sbe.*;

import java.util.Objects;
import java.util.function.Function;

import static org.dev4fx.raft.state.Transition.STEADY;

public final class ServerMessageHandler implements MessageHandler, ProcessStep {
    private final MessageHeaderDecoder messageHeaderDecoder;
    private final VoteRequestDecoder voteRequestDecoder;
    private final VoteResponseDecoder voteResponseDecoder;
    private final AppendRequestDecoder appendRequestDecoder;
    private final AppendResponseDecoder appendResponseDecoder;

    private ServerState serverState;
    private final Function<Role, ServerState> roleToState;

    public ServerMessageHandler(final MessageHeaderDecoder messageHeaderDecoder,
                                final VoteRequestDecoder voteRequestDecoder,
                                final VoteResponseDecoder voteResponseDecoder,
                                final AppendRequestDecoder appendRequestDecoder,
                                final AppendResponseDecoder appendResponseDecoder,
                                final ServerState candidateState,
                                final ServerState leaderState,
                                final ServerState followerState) {
        this.messageHeaderDecoder = Objects.requireNonNull(messageHeaderDecoder);
        this.voteRequestDecoder = Objects.requireNonNull(voteRequestDecoder);
        this.voteResponseDecoder = Objects.requireNonNull(voteResponseDecoder);
        this.appendRequestDecoder = Objects.requireNonNull(appendRequestDecoder);
        this.appendResponseDecoder = Objects.requireNonNull(appendResponseDecoder);
        Objects.requireNonNull(candidateState);
        Objects.requireNonNull(leaderState);
        Objects.requireNonNull(followerState);
        this.serverState = followerState;
        this.roleToState = role -> {
            if (role == leaderState.role()) return leaderState;
            if (role == followerState.role()) return followerState;
            if (role == candidateState.role()) return candidateState;
            throw new IllegalArgumentException("No state for role " + role);
        };
    }

    public void init() {
        this.serverState.onTransition();
    }

    @Override
    public boolean execute() {
        final Transition transition = serverState.processTick();

        if (transition != STEADY) {
            if (serverState.role() != transition.targetRole()) {
                serverState = roleToState.apply(transition.targetRole());
            }
            return true;
        }
        return false;
    }

    @Override
    public void onMessage(final DirectBuffer source, final int offset, final int length) {
        messageHeaderDecoder.wrap(source, offset);
        final int templateId = messageHeaderDecoder.templateId();
        final int headerLength = messageHeaderDecoder.encodedLength();
        final Transition transition;
        switch (templateId) {
            case VoteRequestDecoder.TEMPLATE_ID :
                voteRequestDecoder.wrap(source,headerLength + offset,
                        VoteRequestDecoder.BLOCK_LENGTH,
                        VoteRequestDecoder.SCHEMA_VERSION);
                transition = serverState.onVoteRequest(voteRequestDecoder);
                break;
            case VoteResponseDecoder.TEMPLATE_ID :
                voteResponseDecoder.wrap(source,headerLength + offset,
                        VoteResponseDecoder.BLOCK_LENGTH,
                        VoteResponseDecoder.SCHEMA_VERSION);
                transition = serverState.onVoteResponse(voteResponseDecoder);
                break;
            case AppendRequestDecoder.TEMPLATE_ID :
                appendRequestDecoder.wrap(source,headerLength + offset,
                        AppendRequestDecoder.BLOCK_LENGTH,
                        AppendRequestDecoder.SCHEMA_VERSION);
                transition = serverState.onAppendRequest(appendRequestDecoder);
                break;
            case AppendResponseDecoder.TEMPLATE_ID :
                appendResponseDecoder.wrap(source,headerLength + offset,
                        AppendResponseDecoder.BLOCK_LENGTH,
                        AppendResponseDecoder.SCHEMA_VERSION);
                transition = serverState.onAppendResponse(appendResponseDecoder);
                break;
            case CommandRequestDecoder.TEMPLATE_ID :
                transition = serverState.onCommandRequest(source, offset, length);
                break;
            default:
                transition = STEADY;
        }

        if (transition != STEADY) {
            if (serverState.role() != transition.targetRole()) {
                serverState = roleToState.apply(transition.targetRole());
                serverState.onTransition();
                if (transition.replayEvent()) {
                    this.onMessage(source, offset, length);
                }
            }
        }
    }
}
