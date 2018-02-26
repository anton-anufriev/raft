package org.dev4fx.raft.state;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.sbe.*;
import org.dev4fx.raft.transport.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.IntConsumer;

public class LeaderServerState implements ServerState {
    private static final Logger LOGGER = LoggerFactory.getLogger(Role.LEADER.name());

    private final PersistentState persistentState;
    private final VolatileState volatileState;
    private final FollowersState followersState;
    private final int serverId;
    private final AppendRequestEncoder appendRequestEncoder;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final MutableDirectBuffer encoderBuffer;
    private final MutableDirectBuffer commandDecoderBuffer;

    private final Publisher publisher;
    private final IntConsumer onLeaderTransitionHandler;
    private final int maxBatchSize;


    public LeaderServerState(final PersistentState persistentState,
                             final VolatileState volatileState,
                             final FollowersState followersState,
                             final int serverId,
                             final AppendRequestEncoder appendRequestEncoder,
                             final MessageHeaderEncoder messageHeaderEncoder,
                             final MutableDirectBuffer encoderBuffer,
                             final MutableDirectBuffer commandDecoderBuffer,
                             final Publisher publisher,
                             final IntConsumer onLeaderTransitionHandler,
                             final int maxBatchSize) {
        this.persistentState = Objects.requireNonNull(persistentState);
        this.volatileState = Objects.requireNonNull(volatileState);
        this.followersState = Objects.requireNonNull(followersState);
        this.serverId = serverId;
        this.appendRequestEncoder = Objects.requireNonNull(appendRequestEncoder);
        this.messageHeaderEncoder = Objects.requireNonNull(messageHeaderEncoder);
        this.encoderBuffer = Objects.requireNonNull(encoderBuffer);
        this.commandDecoderBuffer = Objects.requireNonNull(commandDecoderBuffer);
        this.publisher = Objects.requireNonNull(publisher);
        this.onLeaderTransitionHandler = Objects.requireNonNull(onLeaderTransitionHandler);
        this.maxBatchSize = maxBatchSize;
    }


    @Override
    public Role role() {
        return Role.LEADER;
    }

    @Override
    public void onTransition() {
        LOGGER.info("Transitioned");
        followersState.resetFollowers(persistentState.size());
        sendAppendRequestToAllAndResetHeartbeatTimer();
        onLeaderTransitionHandler.accept(serverId);
    }

    @Override
    public Transition processTick() {
        followersState.forEach(follower -> {
            if (follower.heartbeatTimer().hasTimeoutElapsed()) {
                LOGGER.info("Heartbeat timer elapsed, send heartbeat to {}", follower.serverId());
                sendAppendRequest(follower, false);
                follower.heartbeatTimer().reset();
            }
        });
        return Transition.STEADY;
    }


    @Override
    public Transition onAppendResponse(final AppendResponseDecoder appendResponseDecoder) {
        final HeaderDecoder headerDecoder = appendResponseDecoder.header();
        final int sourceId = headerDecoder.sourceId();
        final long requestPrevLogIndex = appendResponseDecoder.prevLogIndex();

        final Follower follower = followersState.follower(sourceId);
        final BooleanType successful = appendResponseDecoder.successful();
        if (successful == BooleanType.F) {
            LOGGER.info("Unsuccessful appendResponse from server {}", sourceId);
            if (!follower.comparePreviousAndDecrementNextIndex(requestPrevLogIndex)) {
                LOGGER.info("Unsuccessful appendResponse prevLogIndex {} does not match {}", requestPrevLogIndex, follower.previousIndex());
            }
            sendAppendRequest(follower, true);
        } else {
            LOGGER.info("Successful appendResponse from server {}", sourceId);
            final long matchLogIndex = appendResponseDecoder.matchLogIndex();
            if (!follower.comparePreviousAndUpdateMatchAndNextIndex(requestPrevLogIndex, matchLogIndex)) {
                LOGGER.info("Successful appendResponse prevLogIndex {} does not match {}", requestPrevLogIndex, follower.previousIndex());
            }
            if (follower.matchIndex() < follower.previousIndex()) {
                sendAppendRequest(follower, false);
            }
        }
        follower.heartbeatTimer().reset();
        updateCommitIndex();
        return Transition.STEADY;
    }

    @Override
    public Transition onCommandRequest(DirectBuffer buffer, int offset, int length) {
        LOGGER.info("Command received, length={}", length);
        persistentState.append(persistentState.currentTerm(), buffer, offset, length);
        sendAppendRequestToAllAndResetHeartbeatTimer();

        return Transition.STEADY;
    }

    private void updateCommitIndex() {
        long currentCommitIndex = volatileState.commitIndex();
        int currentTerm = persistentState.currentTerm();

        //FIXME check term at NULL_INDEX
        long nextCommitIndex = followersState.majorityCommitIndex(currentCommitIndex, currentTerm, persistentState::term);

        if (nextCommitIndex > currentCommitIndex) {
            LOGGER.info("Update commit index {}", nextCommitIndex);
            volatileState.commitIndex(nextCommitIndex);
        }
    }

    private void sendAppendRequestToAllAndResetHeartbeatTimer() {
        followersState.forEach(follower -> {
            sendAppendRequest(follower, false);
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

        int messageLength = 0;
        if (empty) {
            appendRequestEncoder.logEntriesCount(0);
            messageLength = headerLength + appendRequestEncoder.encodedLength();
        } else {
            final long matchIndex = follower.matchIndex();
            long nextLogIndex = follower.nextIndex();
            final long lastIndex = persistentState.lastIndex();


            if (matchIndex == prevLogIndex && nextLogIndex <= lastIndex) {

                final long endOfBatchIndex = Long.min(prevLogIndex + maxBatchSize, lastIndex);

                final AppendRequestEncoder.LogEntriesEncoder logEntriesEncoder = appendRequestEncoder
                        .logEntriesCount((int) (endOfBatchIndex - prevLogIndex));

                while(nextLogIndex <= endOfBatchIndex) {

                    final int termAtNextLogIndex = persistentState.term(nextLogIndex);
                    persistentState.wrap(nextLogIndex, commandDecoderBuffer);
                    final int commandLength = commandDecoderBuffer.capacity();

                    final VarDataEncodingEncoder commandEncoder = logEntriesEncoder.next()
                            .term(termAtNextLogIndex)
                            .command().length(commandLength);

                    final int commandOffset = commandEncoder.offset() + commandEncoder.encodedLength();

                    commandDecoderBuffer.getBytes(0, encoderBuffer, commandOffset, commandLength);
                    messageLength = commandOffset + commandLength;
                    nextLogIndex++;
                }
            } else {
                appendRequestEncoder.logEntriesCount(0);
                messageLength = headerLength + appendRequestEncoder.encodedLength();
            }
        }

        return messageLength > 0 && publisher.publish(encoderBuffer, 0, messageLength);
    }
}
