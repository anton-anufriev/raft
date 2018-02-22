package org.dev4fx.raft.state;

import org.agrona.MutableDirectBuffer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.sbe.*;
import org.dev4fx.raft.timer.Timer;
import org.dev4fx.raft.transport.Publishers;

import java.util.Objects;
import java.util.function.Function;

public class VoteRequestHandler implements Function<VoteRequestDecoder, Transition> {
    private final PersistentState persistentState;
    private final Timer electionTimer;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final VoteResponseEncoder voteResponseEncoder;
    private final MutableDirectBuffer encoderBuffer;
    private final Publishers publishers;
    private final int serverId;


    public VoteRequestHandler(final PersistentState persistentState,
                              final Timer electionTimer,
                              final MessageHeaderEncoder messageHeaderEncoder,
                              final VoteResponseEncoder voteResponseEncoder,
                              final MutableDirectBuffer encoderBuffer,
                              final Publishers publishers,
                              final int serverId) {
        this.persistentState = Objects.requireNonNull(persistentState);
        this.electionTimer = Objects.requireNonNull(electionTimer);
        this.messageHeaderEncoder = Objects.requireNonNull(messageHeaderEncoder);
        this.voteResponseEncoder = Objects.requireNonNull(voteResponseEncoder);
        this.encoderBuffer = Objects.requireNonNull(encoderBuffer);
        this.publishers = Objects.requireNonNull(publishers);
        this.serverId = serverId;
    }

    @Override
    public Transition apply(final VoteRequestDecoder voteRequestDecoder) {
        final HeaderDecoder header = voteRequestDecoder.header();
        final int requestTerm = header.term();
        final int candidateId = header.sourceId();
        final LogKeyDecoder lastLogKey = voteRequestDecoder.lastLogKey();
        final long requestLastLogIndex = lastLogKey.index();
        final int requestLastLogTerm = lastLogKey.term();

        final Transition transition;
        final boolean granted;
        if (persistentState.currentTerm() <= requestTerm && persistentState.lastKeyCompareTo(requestLastLogIndex, requestLastLogTerm) <= 0) {
            if (persistentState.vote() == PersistentState.NULL_VOTE) {
                persistentState.vote(candidateId);
                //Why transition is TO_FOLLOWER?
                //in this case we should not replay the event!?
                transition = Transition.TO_FOLLOWER;
                granted = true;
            } else {
                granted = persistentState.vote() == candidateId;
                transition = Transition.STEADY;
            }
        } else {
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

        publishers.lookup(candidateId).publish(encoderBuffer, 0, headerLength + voteResponseEncoder.encodedLength());
        return transition;
    }
}
