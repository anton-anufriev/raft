/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 hover-raft (tools4j), Anton Anufriev, Marco Terzer
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.dev4fx.raft.state;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.sbe.*;
import org.dev4fx.raft.transport.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.LongToIntFunction;

public class LeaderServerState implements ServerState {
    private static final Logger LOGGER = LoggerFactory.getLogger(Role.LEADER.name());

    private final PersistentState persistentState;
    private final VolatileState volatileState;
    private final Peers peers;
    private final int serverId;
    private final AppendRequestEncoder appendRequestEncoder;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final MutableDirectBuffer encoderBuffer;
    private final MutableDirectBuffer commandDecoderBuffer;

    private final Publisher publisher;
    private final IntConsumer onLeaderTransitionHandler;
    private final int maxBatchSize;

    private final LongToIntFunction indexToTermLookup;
    private final Consumer<Peer> sendAppendRequestAndResetHeartbeatTimerForAll;
    private final Consumer<Peer> resetHeartbeatTimerForAll;



    public LeaderServerState(final PersistentState persistentState,
                             final VolatileState volatileState,
                             final Peers peers,
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
        this.peers = Objects.requireNonNull(peers);
        this.serverId = serverId;
        this.appendRequestEncoder = Objects.requireNonNull(appendRequestEncoder);
        this.messageHeaderEncoder = Objects.requireNonNull(messageHeaderEncoder);
        this.encoderBuffer = Objects.requireNonNull(encoderBuffer);
        this.commandDecoderBuffer = Objects.requireNonNull(commandDecoderBuffer);
        this.publisher = Objects.requireNonNull(publisher);
        this.onLeaderTransitionHandler = Objects.requireNonNull(onLeaderTransitionHandler);
        this.maxBatchSize = maxBatchSize;
        this.indexToTermLookup = this.persistentState::term;

        this.sendAppendRequestAndResetHeartbeatTimerForAll = peer -> {
            sendAppendRequest(peer.serverId(), peer.nextIndex(), peer.matchIndex(), false);
            peer.heartbeatTimer().reset();
        };

        this.resetHeartbeatTimerForAll = peer -> peer.heartbeatTimer().reset();

    }


    @Override
    public Role role() {
        return Role.LEADER;
    }

    @Override
    public void onTransition() {
        LOGGER.info("Transitioned");
        peers.resetAsFollowers(persistentState.size());
        sendAppendRequestToAllAndResetHeartbeatTimer();
        onLeaderTransitionHandler.accept(serverId);
    }

    @Override
    public Transition processTick() {
        peers.forEach(peer -> {
            if (peer.heartbeatTimer().hasTimeoutElapsed()) {
                LOGGER.info("Heartbeat timer elapsed, send heartbeat to {}", peer.serverId());
                sendAppendRequest(peer.serverId(), peer.nextIndex(), peer.matchIndex(), false);
                peer.heartbeatTimer().reset();
            }
        });
        return Transition.STEADY;
    }


    @Override
    public Transition onAppendResponse(final AppendResponseDecoder appendResponseDecoder) {
        final HeaderDecoder headerDecoder = appendResponseDecoder.header();
        final int sourceId = headerDecoder.sourceId();
        final long requestPrevLogIndex = appendResponseDecoder.prevLogIndex();

        final Peer peer = peers.peer(sourceId);
        final BooleanType successful = appendResponseDecoder.successful();
        if (successful == BooleanType.F) {
            //LOGGER.info("Unsuccessful appendResponse from server {}", sourceId);
            if (!peer.comparePreviousAndDecrementNextIndex(requestPrevLogIndex)) {
                //LOGGER.info("Unsuccessful appendResponse prevLogIndex {} does not match {} from server {}, awaiting newer response", requestPrevLogIndex, peer.previousIndex(), sourceId);
            } else {
                sendAppendRequest(peer.serverId(), peer.nextIndex(), peer.matchIndex(), true);
            }
        } else {
            //LOGGER.info("Successful appendResponse from server {}", sourceId);
            final long matchLogIndex = appendResponseDecoder.matchLogIndex();
            if (!peer.comparePreviousAndUpdateMatchAndNextIndex(requestPrevLogIndex, matchLogIndex)) {
                //LOGGER.info("Successful appendResponse prevLogIndex {} does not match {} from server {}, awaiting newer response", requestPrevLogIndex, peer.previousIndex(), sourceId);
            } else {
                if (peer.matchIndex() < persistentState.lastIndex()) {
                    sendAppendRequest(peer.serverId(), peer.nextIndex(), peer.matchIndex(), false);
                }
            }
        }
        peer.heartbeatTimer().reset();
        updateCommitIndex();
        return Transition.STEADY;
    }

    @Override
    public Transition onCommandRequest(DirectBuffer buffer, int offset, int length) {
        //LOGGER.info("Command received, length={}", length);
        persistentState.append(persistentState.currentTerm(), buffer, offset, length);
        sendAppendRequestToAllAndResetHeartbeatTimer();

        return Transition.STEADY;
    }

    private void updateCommitIndex() {
        long currentCommitIndex = volatileState.commitIndex();
        int currentTerm = persistentState.currentTerm();

        //FIXME check term at NULL_INDEX
        long nextCommitIndex = peers.majorityCommitIndex(currentCommitIndex, currentTerm, indexToTermLookup);

        if (nextCommitIndex > currentCommitIndex) {
            //LOGGER.info("Update commit index {}", nextCommitIndex);
            volatileState.commitIndex(nextCommitIndex);
        }
    }

    private void sendAppendRequestToAllAndResetHeartbeatTimer() {
        final long matchIndexPrecedingNextIndex = peers.matchIndexPrecedingNextIndexAndEqualAtAllPeers();
        if (matchIndexPrecedingNextIndex != Peer.NULL_INDEX) {
            sendAppendRequest(Peers.ALL, matchIndexPrecedingNextIndex + 1, matchIndexPrecedingNextIndex, false);
            peers.forEach(resetHeartbeatTimerForAll);
        } else {
            peers.forEach(sendAppendRequestAndResetHeartbeatTimerForAll);
        }
    }

    private boolean sendAppendRequest(final int destinationId,
                                      final long nextIndex,
                                      final long matchIndex,
                                      final boolean empty) {

        final int currentTerm = persistentState.currentTerm();

        final long prevLogIndex = nextIndex - 1;
        final int termAtPrevLogIndex = persistentState.term(prevLogIndex);

        final int headerLength = messageHeaderEncoder.wrap(encoderBuffer, 0)
                .schemaId(AppendRequestEncoder.SCHEMA_ID)
                .version(AppendRequestEncoder.SCHEMA_VERSION)
                .blockLength(AppendRequestEncoder.BLOCK_LENGTH)
                .templateId(AppendRequestEncoder.TEMPLATE_ID)
                .encodedLength();

        appendRequestEncoder.wrap(encoderBuffer, headerLength)
                .header()
                .destinationId(destinationId)
                .sourceId(serverId)
                .term(currentTerm);

        appendRequestEncoder
                .commitLogIndex(volatileState.commitIndex())
                .prevLogKey()
                    .index(prevLogIndex)
                    .term(termAtPrevLogIndex);

        if (empty) {
            appendRequestEncoder.logEntriesCount(0);
        } else {
            long nextLogIndex = nextIndex;
            final long lastIndex = persistentState.lastIndex();

            if (matchIndex == prevLogIndex) {

                final long endOfBatchIndex = Long.min(prevLogIndex + maxBatchSize, lastIndex);

                final AppendRequestEncoder.LogEntriesEncoder logEntriesEncoder = appendRequestEncoder
                        .logEntriesCount((int) (endOfBatchIndex - prevLogIndex));

                while(nextLogIndex <= endOfBatchIndex) {

                    final int termAtNextLogIndex = persistentState.term(nextLogIndex);
                    persistentState.wrap(nextLogIndex, commandDecoderBuffer);
                    final int commandLength = commandDecoderBuffer.capacity();

                    logEntriesEncoder.next()
                            .term(termAtNextLogIndex)
                            .putCommand(commandDecoderBuffer, 0, commandLength);

                    nextLogIndex++;
                }
            } else {
                appendRequestEncoder.logEntriesCount(0);
            }
        }

        return publisher.publish(encoderBuffer, 0, headerLength + appendRequestEncoder.encodedLength());
    }
}
