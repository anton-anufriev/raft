package org.dev4fx.raft.state;

import org.agrona.MutableDirectBuffer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.sbe.*;
import org.dev4fx.raft.timer.Timer;
import org.dev4fx.raft.transport.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.BiFunction;

public class CandidateServerState implements ServerState {
    private static final Logger LOGGER = LoggerFactory.getLogger(Role.CANDIDATE.name());

    private final PersistentState persistentState;
    private final FollowersState followersState;
    private final BiFunction<? super AppendRequestDecoder, ? super Logger, ? extends Transition> appendRequestHandler;
    private final Timer electionTimer;
    private final int serverId;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final VoteRequestEncoder voteRequestEncoder;
    private final MutableDirectBuffer encoderBuffer;
    private final Publisher publisher;

    private int votesCount;

    public CandidateServerState(final PersistentState persistentState,
                                final FollowersState followersState,
                                final BiFunction<? super AppendRequestDecoder, ? super Logger, ? extends Transition> appendRequestHandler,
                                final Timer electionTimer,
                                final int serverId,
                                final MessageHeaderEncoder messageHeaderEncoder,
                                final VoteRequestEncoder voteRequestEncoder,
                                final MutableDirectBuffer encoderBuffer,
                                final Publisher publisher) {
        this.persistentState = Objects.requireNonNull(persistentState);
        this.followersState = Objects.requireNonNull(followersState);
        this.appendRequestHandler = Objects.requireNonNull(appendRequestHandler);
        this.electionTimer = Objects.requireNonNull(electionTimer);
        this.serverId = serverId;
        this.messageHeaderEncoder = Objects.requireNonNull(messageHeaderEncoder);
        this.voteRequestEncoder = Objects.requireNonNull(voteRequestEncoder);
        this.encoderBuffer = Objects.requireNonNull(encoderBuffer);
        this.publisher = Objects.requireNonNull(publisher);
    }

    @Override
    public Role role() {
        return Role.CANDIDATE;
    }

    @Override
    public void onTransition() {
        LOGGER.info("Transitioned");
        startNewElection();
    }

    @Override
    public Transition processTick() {
        if (electionTimer.hasTimeoutElapsed()) {
            LOGGER.info("Election timer elapsed");
            startNewElection();
        }
        return Transition.STEADY;
    }

    private void startNewElection() {
        LOGGER.info("Starting new election");

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
            return Transition.TO_FOLLOWER_REPLAY;
        } else {
            return appendRequestHandler.apply(appendRequestDecoder, LOGGER);
        }
    }

    @Override
    public Transition onVoteResponse(final VoteResponseDecoder voteResponseDecoder) {
        final HeaderDecoder header = voteResponseDecoder.header();
        final int currentTerm = persistentState.currentTerm();
        final int term = header.term();
        final int sourceId = header.sourceId();
        final BooleanType voteGranted = voteResponseDecoder.voteGranted();

        if (term == currentTerm && voteGranted == BooleanType.T) {
            LOGGER.info("Vote granted by server {}", sourceId);
            return incVoteCount();
        }
        LOGGER.info("Vote declined by server {}", sourceId);
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
                .destinationId(Server.ALL)
                .sourceId(serverId)
                .term(currentTerm);

        voteRequestEncoder.lastLogKey()
                .index(lastIndex)
                .term(lastTerm);

        publisher.publish(encoderBuffer, 0, headerLength + voteRequestEncoder.encodedLength());
    }

    private Transition incVoteCount() {
        votesCount++;
        final int majority = followersState.majority();
        if (votesCount >= majority) {
            LOGGER.info("Received votes {}, majority {}", votesCount, majority);
            return Transition.TO_LEADER_NO_REPLAY;
        }
        return Transition.STEADY;
    }
}
