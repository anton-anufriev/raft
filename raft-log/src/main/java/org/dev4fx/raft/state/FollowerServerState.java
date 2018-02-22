package org.dev4fx.raft.state;

import org.dev4fx.raft.sbe.AppendRequestDecoder;
import org.dev4fx.raft.sbe.VoteRequestDecoder;
import org.dev4fx.raft.timer.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Function;

public class FollowerServerState implements ServerState {
    private static final Logger LOGGER = LoggerFactory.getLogger(FollowerServerState.class);

    private final Function<? super AppendRequestDecoder, ? extends Transition> appendRequestHandler;
    private final Function<? super VoteRequestDecoder, ? extends Transition> voteRequestHandler;
    private final Timer electionTimer;
    private final int serverId;

    public FollowerServerState(final Function<? super AppendRequestDecoder, ? extends Transition> appendRequestHandler,
                               final Function<? super VoteRequestDecoder, ? extends Transition> voteRequestHandler,
                               final int serverId,
                               final Timer electionTimer) {
        this.appendRequestHandler = Objects.requireNonNull(appendRequestHandler);
        this.voteRequestHandler = Objects.requireNonNull(voteRequestHandler);
        this.electionTimer = Objects.requireNonNull(electionTimer);
        this.serverId = serverId;
    }

    @Override
    public Role role() {
        return Role.FOLLOWER;
    }

    @Override
    public void onTransition() {
        LOGGER.info("FOLLOWER: transitioned, server: {}", serverId);
        electionTimer.restart();
    }

    @Override
    public Transition processTick() {
        if (electionTimer.hasTimeoutElapsed()) {
            LOGGER.info("FOLLOWER: electionTimer, server: {}", serverId);

            return Transition.TO_CANDIDATE;
        }
        return Transition.STEADY;
    }

    @Override
    public Transition onAppendRequest(final AppendRequestDecoder appendRequestDecoder) {
        return appendRequestHandler.apply(appendRequestDecoder);
    }

    @Override
    public Transition onVoteRequest(final VoteRequestDecoder voteRequestDecoder) {
        return voteRequestHandler.apply(voteRequestDecoder);
    }
}
