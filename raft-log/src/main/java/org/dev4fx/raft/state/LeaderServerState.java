package org.dev4fx.raft.state;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.sbe.*;
import org.dev4fx.raft.transport.Publishers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class LeaderServerState implements ServerState {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderServerState.class);

    private final PersistentState persistentState;
    private final VolatileState volatileState;
    private final FollowersState followersState;
    private final int serverId;
    private final AppendRequestEncoder appendRequestEncoder;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final MutableDirectBuffer encoderBuffer;
    private final MutableDirectBuffer commandBuffer;

    private final Publishers publishers;


    public LeaderServerState(final PersistentState persistentState,
                             final VolatileState volatileState,
                             final FollowersState followersState,
                             final int serverId,
                             final AppendRequestEncoder appendRequestEncoder,
                             final MessageHeaderEncoder messageHeaderEncoder,
                             final MutableDirectBuffer encoderBuffer,
                             final MutableDirectBuffer commandBuffer,
                             final Publishers publishers) {
        this.persistentState = Objects.requireNonNull(persistentState);
        this.volatileState = Objects.requireNonNull(volatileState);
        this.followersState = Objects.requireNonNull(followersState);
        this.serverId = serverId;
        this.appendRequestEncoder = Objects.requireNonNull(appendRequestEncoder);
        this.messageHeaderEncoder = Objects.requireNonNull(messageHeaderEncoder);
        this.encoderBuffer = Objects.requireNonNull(encoderBuffer);
        this.commandBuffer = Objects.requireNonNull(commandBuffer);
        this.publishers = Objects.requireNonNull(publishers);
    }


    @Override
    public Role role() {
        return Role.LEADER;
    }

    @Override
    public void onTransition() {
        LOGGER.info("LEADER: transitioned, server: {}", serverId);
        followersState.resetFollowers(persistentState.size());
        sendAndResetHeartbeatAppendRequestToAll(true);
    }

    @Override
    public Transition processTick() {
        followersState.forEach(follower -> {
            if (follower.heartbeatTimer().hasTimeoutElapsed()) {
                LOGGER.info("LEADER: heartbeatTimer, send heartbeat from server: {} to {}", serverId, follower.serverId());
                sendAppendRequest(follower, true);
                follower.heartbeatTimer().reset();
            }
        });
        return Transition.STEADY;
    }


    @Override
    public Transition onAppendResponse(final AppendResponseDecoder appendResponseDecoder) {
        final HeaderDecoder headerDecoder = appendResponseDecoder.header();
        final int destinationId = headerDecoder.destinationId();
        final int sourceId = headerDecoder.sourceId();
        final long requestPrevLogIndex = appendResponseDecoder.prevLogIndex();

        if (destinationId == serverId) {
            final Follower follower = followersState.follower(sourceId);
            final BooleanType successful = appendResponseDecoder.successful();
            if (successful == BooleanType.F) {
                LOGGER.info("LEADER: appendResponse unsuccessful, server: {}", serverId);
                if (!follower.comparePreviousAndDecrementNextIndex(requestPrevLogIndex)) {
                    LOGGER.info("LEADER: unsuccessful appendResponse is out of order, server: {}", serverId);
                }
                sendAppendRequest(follower, true);
            } else {
                LOGGER.info("LEADER: appendResponse successful, server: {}", serverId);
                final long matchLogIndex = appendResponseDecoder.matchLogIndex();
                if (!follower.comparePreviousAndUpdateMatchAndNextIndex(requestPrevLogIndex, matchLogIndex)) {
                    LOGGER.info("LEADER: successful appendResponse is out of order, server: {}", serverId);
                }
                //if follower.matchLogIndex < follower.nextIndex - 1
                // sendAppendRequestTo(follower, false)
            }
            follower.heartbeatTimer().reset();
        }
        updateCommitIndex();
        return Transition.STEADY;
    }

    @Override
    public Transition onCommandRequest(DirectBuffer buffer, int offset, int length) {
        LOGGER.info("LEADER: command received on server: {}, {}", serverId);
        persistentState.append(persistentState.currentTerm(), buffer, offset, length);
        //!!!! adds latency
        sendAndResetHeartbeatAppendRequestToAll(false);

        return Transition.STEADY;
    }

    private void updateCommitIndex() {
        long currentCommitIndex = volatileState.commitIndex();
        int currentTerm = persistentState.currentTerm();

        long nextCommitIndex = followersState.majorityCommitIndex(currentCommitIndex, currentTerm, persistentState::term);

        if (nextCommitIndex > currentCommitIndex) {
            LOGGER.info("LEADER: update commit index {}", nextCommitIndex);
            volatileState.commitIndex(nextCommitIndex);
        }
    }

    private void sendAndResetHeartbeatAppendRequestToAll(final boolean empty) {
        followersState.forEach(empty, (empty1 , follower) -> {
            sendAppendRequest(follower, empty1);
            follower.heartbeatTimer().reset();
        });
    }

    private boolean sendAppendRequest(final Follower follower, final boolean empty) {

        final int currentTerm = persistentState.currentTerm();

        final long prevLogIndex = follower.previousIndex();
        final int termAtPrevLogIndex = persistentState.term(prevLogIndex);

        final int headerLength = messageHeaderEncoder.wrap(encoderBuffer, 0)
                .schemaId(AppendRequestEncoder.SCHEMA_ID)
                .version(AppendRequestEncoder.SCHEMA_VERSION)
                .blockLength(AppendRequestEncoder.BLOCK_LENGTH)
                .templateId(AppendRequestEncoder.TEMPLATE_ID)
                .encodedLength();

        appendRequestEncoder.wrap(encoderBuffer, headerLength)
                .header()
                .destinationId(follower.serverId())
                .sourceId(serverId)
                .term(currentTerm);

        appendRequestEncoder
                .commitLogIndex(volatileState.commitIndex())
                .prevLogKey()
                    .index(prevLogIndex)
                    .term(termAtPrevLogIndex);

        final int messageLength;
        if (empty) {
            appendRequestEncoder.logEntriesCount(0);
            messageLength = headerLength + appendRequestEncoder.encodedLength();
        } else {
            final long nextLogIndex = follower.nextIndex();
            final int termAtNextLogIndex = persistentState.term(nextLogIndex);
            persistentState.wrap(nextLogIndex, commandBuffer);
            final int commandLength = commandBuffer.capacity();

            final VarDataEncodingEncoder commandEncoder = appendRequestEncoder
                    .logEntriesCount(1)
                    .next()
                    .term(termAtNextLogIndex)
                    .command().length(commandLength);

            final int commandOffset = commandEncoder.offset() + commandEncoder.encodedLength();

            commandBuffer.getBytes(0, encoderBuffer, commandOffset, commandLength);
            messageLength = commandOffset + commandLength;
        }

        return publishers.lookup(follower.serverId()).publish(encoderBuffer, 0, messageLength);
    }
}
