package org.dev4fx.raft.state;

import org.agrona.MutableDirectBuffer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.sbe.*;
import org.dev4fx.raft.timer.Timer;
import org.dev4fx.raft.transport.Publishers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Function;

public class CandidateServerState implements ServerState {
    private static final Logger LOGGER = LoggerFactory.getLogger(CandidateServerState.class);

    private final PersistentState persistentState;
    private final FollowersState followersState;
    private final Function<? super AppendRequestDecoder, ? extends Transition> appendRequestHandler;
    private final Timer electionTimer;
    private final int serverId;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final VoteRequestEncoder voteRequestEncoder;
    private final MutableDirectBuffer encoderBuffer;
    private final Publishers publishers;

    private int votesCount;

    public CandidateServerState(final PersistentState persistentState,
                                final FollowersState followersState,
                                final Function<? super AppendRequestDecoder, ? extends Transition> appendRequestHandler,
                                final Timer electionTimer,
                                final int serverId,
                                final MessageHeaderEncoder messageHeaderEncoder,
                                final VoteRequestEncoder voteRequestEncoder,
                                final MutableDirectBuffer encoderBuffer,
                                final Publishers publishers) {
        this.persistentState = Objects.requireNonNull(persistentState);
        this.followersState = Objects.requireNonNull(followersState);
        this.appendRequestHandler = Objects.requireNonNull(appendRequestHandler);
        this.electionTimer = Objects.requireNonNull(electionTimer);
        this.serverId = serverId;
        this.messageHeaderEncoder = Objects.requireNonNull(messageHeaderEncoder);
        this.voteRequestEncoder = Objects.requireNonNull(voteRequestEncoder);
        this.encoderBuffer = Objects.requireNonNull(encoderBuffer);
        this.publishers = Objects.requireNonNull(publishers);
    }

    @Override
    public Role role() {
        return Role.CANDIDATE;
    }

    @Override
    public void onTransition() {
        LOGGER.info("CANDIDATE: transitioned, server: {}", serverId);
        startNewElection();
    }

    @Override
    public Transition processTick() {
        if (electionTimer.hasTimeoutElapsed()) {
            LOGGER.info("CANDIDATE: electionTimer, new election, server: {}", serverId);
            startNewElection();
        }
        return Transition.STEADY;
    }


    private void startNewElection() {
        LOGGER.info("CANDIDATE: Start new election, server: {}", serverId);

        persistentState.clearVoteAndIncCurrentTerm();
        electionTimer.restart();
        voteForMyself();
        requestVoteFromAllServers();
    }

    @Override
    public Transition onAppendRequest(final AppendRequestDecoder appendRequestDecoder) {
        final HeaderDecoder header = appendRequestDecoder.header();

        final int appendRequestTerm = header.term();
        final int currentTerm = persistentState.currentTerm();

        if (appendRequestTerm >= currentTerm) {
            return Transition.TO_FOLLOWER;
        } else {
            final Transition transition = appendRequestHandler.apply(appendRequestDecoder);
            return transition;
        }
    }

    @Override
    public Transition onVoteResponse(final VoteResponseDecoder voteResponseDecoder) {
        final int currentTerm = persistentState.currentTerm();
        final int term = voteResponseDecoder.header().term();
        final BooleanType voteGranted = voteResponseDecoder.voteGranted();

        if (term == currentTerm && voteGranted == BooleanType.T) {
            LOGGER.info("CANDIDATE: Vote granted to {}", serverId);
            return incVoteCount();
        }
        LOGGER.info("CANDIDATE: Vote NOT granted to {}", serverId);
        return Transition.STEADY;
    }

    private void voteForMyself() {
        persistentState.vote(serverId);
        votesCount = 1;
    }

    private void requestVoteFromAllServers() {
        final int currentTerm = persistentState.currentTerm();
        final long lastIndex = persistentState.lastIndex();
        final int lastTerm = persistentState.lastTerm();

        final int headerLength = messageHeaderEncoder.wrap(encoderBuffer, 0)
                .schemaId(VoteRequestEncoder.SCHEMA_ID)
                .version(VoteRequestEncoder.SCHEMA_VERSION)
                .blockLength(VoteRequestEncoder.BLOCK_LENGTH)
                .templateId(VoteRequestEncoder.TEMPLATE_ID)
                .encodedLength();

        voteRequestEncoder.wrap(encoderBuffer, headerLength)
                .header()
                .destinationId(Publishers.ALL)
                .sourceId(serverId)
                .term(currentTerm);

        voteRequestEncoder.lastLogKey()
                .index(lastIndex)
                .term(lastTerm);

        publishers.lookup(Publishers.ALL).publish(encoderBuffer, 0, headerLength + voteRequestEncoder.encodedLength());
    }

    private Transition incVoteCount() {
        votesCount++;
        final int majority = followersState.majority();
        if (votesCount >= majority) {
            return Transition.TO_LEADER;
        }
        return Transition.STEADY;
    }
}
