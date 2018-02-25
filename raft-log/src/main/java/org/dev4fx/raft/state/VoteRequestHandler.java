package org.dev4fx.raft.state;

import org.agrona.MutableDirectBuffer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.sbe.*;
import org.dev4fx.raft.timer.Timer;
import org.dev4fx.raft.transport.Publisher;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.function.BiFunction;

public class VoteRequestHandler implements BiFunction<VoteRequestDecoder, Logger, Transition> {
    private final PersistentState persistentState;
    private final Timer electionTimer;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final VoteResponseEncoder voteResponseEncoder;
    private final MutableDirectBuffer encoderBuffer;
    private final Publisher publisher;
    private final int serverId;


    public VoteRequestHandler(final PersistentState persistentState,
                              final Timer electionTimer,
                              final MessageHeaderEncoder messageHeaderEncoder,
                              final VoteResponseEncoder voteResponseEncoder,
                              final MutableDirectBuffer encoderBuffer,
                              final Publisher publisher,
                              final int serverId) {
        this.persistentState = Objects.requireNonNull(persistentState);
        this.electionTimer = Objects.requireNonNull(electionTimer);
        this.messageHeaderEncoder = Objects.requireNonNull(messageHeaderEncoder);
        this.voteResponseEncoder = Objects.requireNonNull(voteResponseEncoder);
        this.encoderBuffer = Objects.requireNonNull(encoderBuffer);
        this.publisher = Objects.requireNonNull(publisher);
        this.serverId = serverId;
    }

    @Override
    public Transition apply(final VoteRequestDecoder voteRequestDecoder, final Logger logger) {
        final HeaderDecoder header = voteRequestDecoder.header();
        final int requestTerm = header.term();
        final int candidateId = header.sourceId();
        final LogKeyDecoder lastLogKey = voteRequestDecoder.lastLogKey();
        final long requestLastLogIndex = lastLogKey.index();
        final int requestLastLogTerm = lastLogKey.term();

        final Transition transition;
        final boolean granted;
        if (persistentState.currentTerm() <= requestTerm && persistentState.lastKeyCompareTo(requestLastLogIndex, requestLastLogTerm) <= 0) {
            logger.info("Current term <= requestTerm and persisted log lesser than log from source");
            if (persistentState.vote() == PersistentState.NULL_VOTE) {
                logger.info("Have not voted yet");
                persistentState.vote(candidateId);
                transition = Transition.TO_FOLLOWER_NO_REPLAY;
                granted = true;
            } else {
                final int vote = persistentState.vote();
                logger.info("Already vote, reject vote if not the same, {} = {}", vote, candidateId);
                granted = vote == candidateId;
                transition = Transition.STEADY;
            }
        } else {
            logger.info("Rejecting vote as current term > requestTerm or persisted log bigger than log from source");
            granted = false;
            transition = Transition.STEADY;
        }
        if (granted) {
            electionTimer.restart();
        }

        final int headerLength = messageHeaderEncoder.wrap(encoderBuffer, 0)
                .schemaId(VoteResponseEncoder.SCHEMA_ID)
                .version(VoteResponseEncoder.SCHEMA_VERSION)
                .blockLength(VoteResponseEncoder.BLOCK_LENGTH)
                .templateId(VoteResponseEncoder.TEMPLATE_ID)
                .encodedLength();

        voteResponseEncoder.wrap(encoderBuffer, headerLength)
                .header()
                .destinationId(candidateId)
                .sourceId(serverId)
                .term(persistentState.currentTerm());

        voteResponseEncoder
                .voteGranted(granted ? BooleanType.T : BooleanType.F);

        publisher.publish(encoderBuffer, 0, headerLength + voteResponseEncoder.encodedLength());
        return transition;
    }
}
