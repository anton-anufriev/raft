package org.dev4fx.raft.state;

import org.dev4fx.raft.sbe.AppendRequestDecoder;
import org.dev4fx.raft.sbe.VoteRequestDecoder;
import org.dev4fx.raft.timer.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.IntConsumer;

public class FollowerServerState implements ServerState {
    private static final Logger LOGGER = LoggerFactory.getLogger(Role.FOLLOWER.name());

    private final int serverId;
    private final BiFunction<? super AppendRequestDecoder, ? super Logger, ? extends Transition> appendRequestHandler;
    private final BiFunction<? super VoteRequestDecoder, ? super Logger, ? extends Transition> voteRequestHandler;
    private final Timer electionTimer;
    private final IntConsumer onFollowerTransitionHandler;

    public FollowerServerState(final int serverId,
                               final BiFunction<? super AppendRequestDecoder, ? super Logger, ? extends Transition> appendRequestHandler,
                               final BiFunction<? super VoteRequestDecoder, ? super Logger, ? extends Transition> voteRequestHandler,
                               final Timer electionTimer, final IntConsumer onFollowerTransitionHandler) {
        this.serverId = serverId;
        this.appendRequestHandler = Objects.requireNonNull(appendRequestHandler);
        this.voteRequestHandler = Objects.requireNonNull(voteRequestHandler);
        this.electionTimer = Objects.requireNonNull(electionTimer);
        this.onFollowerTransitionHandler = Objects.requireNonNull(onFollowerTransitionHandler);
    }

    @Override
    public Role role() {
        return Role.FOLLOWER;
    }

    @Override
    public void onTransition() {
        LOGGER.info("Transitioned");
        electionTimer.restart();
        onFollowerTransitionHandler.accept(serverId);
    }

    @Override
    public Transition processTick() {
        if (electionTimer.hasTimeoutElapsed()) {
            LOGGER.info("Election timer elapsed");

            return Transition.TO_CANDIDATE_NO_REPLAY;
        }
        return Transition.STEADY;
    }

    @Override
    public Transition onAppendRequest(final AppendRequestDecoder appendRequestDecoder) {
        return appendRequestHandler.apply(appendRequestDecoder, LOGGER);
    }

    @Override
    public Transition onVoteRequest(final VoteRequestDecoder voteRequestDecoder) {
        return voteRequestHandler.apply(voteRequestDecoder, LOGGER);
    }
}
