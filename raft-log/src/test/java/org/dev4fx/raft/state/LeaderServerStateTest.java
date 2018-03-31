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

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.sbe.*;
import org.dev4fx.raft.timer.Timer;
import org.dev4fx.raft.transport.Publisher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LeaderServerStateTest {

    @Mock
    private PersistentState persistentState;
    @Mock
    private VolatileState volatileState;
    @Mock
    private Peers peers;
    @Mock
    private Publisher publisher;
    @Mock
    private IntConsumer onLeaderTransitionHandler;
    @Mock
    private Peer peer;
    @Mock
    private Timer timer;
    @Mock
    private AppendResponseDecoder appendResponseDecoder;
    @Mock
    private HeaderDecoder headerDecoder;


    @Captor
    private ArgumentCaptor<Consumer<? super Peer>> peerConsumerCaptor;

    private int serverId = 1;
    private int maxBatchSize = 1;
    private AppendRequestEncoder appendRequestEncoder = new AppendRequestEncoder();
    private MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private MutableDirectBuffer encoderBuffer = new UnsafeBuffer(ByteBuffer.allocate(512));
    private MutableDirectBuffer commandDecoderBuffer = new UnsafeBuffer(ByteBuffer.allocate(512));


    private LeaderServerState leaderServerState;

    @Before
    public void setUp() throws Exception {
        leaderServerState = new LeaderServerState(persistentState,
                volatileState, peers, serverId, appendRequestEncoder,
                messageHeaderEncoder, encoderBuffer, commandDecoderBuffer,
                publisher, onLeaderTransitionHandler, maxBatchSize);
    }

    @Test
    public void role() throws Exception {
        assertThat(leaderServerState.role()).isEqualTo(Role.LEADER);
    }

    @Test
    public void onTransition_resets_peers_signals_on_leader_handler_and_sends_empty_request() throws Exception {
        //given
        final long logSize = 20;
        final int peerServerId = 2;
        final long peerPrevIndex = 19;
        final int currentTerm = 5;
        final int peerPrevTerm = 4;
        final long commitIndex = 15;

        when(persistentState.size()).thenReturn(logSize);
        when(peer.nextIndex()).thenReturn(peerPrevIndex+1);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(persistentState.term(peerPrevIndex)).thenReturn(peerPrevTerm);
        when(peer.serverId()).thenReturn(peerServerId);
        when(volatileState.commitIndex()).thenReturn(commitIndex);
        when(peer.heartbeatTimer()).thenReturn(timer);
        when(peers.matchIndexPrecedingNextIndexAndEqualAtAllPeers()).thenReturn(Peer.NULL_INDEX);

        //when
        leaderServerState.onTransition();

        verify(peers).resetAsFollowers(logSize);
        verify(onLeaderTransitionHandler).accept(serverId);
        verify(peers).forEach(peerConsumerCaptor.capture());


        peerConsumerCaptor.getValue().accept(peer);

        verify(publisher).publish(encoderBuffer, 0, 44);

        assertEmptyAppendRequest(appendRequestEncoder, serverId, peerServerId,
                currentTerm, peerPrevTerm, peerPrevIndex, commitIndex);

        verify(timer).reset();
    }

    private void assertEmptyAppendRequest(final AppendRequestEncoder appendRequestEncoder,
                                          final int sourceId,
                                          final int destination,
                                          final int term,
                                          final int prevLogTerm,
                                          final long prevLogIndex,
                                          final long commitIndex) {
        final StringBuilder voteRequest = new StringBuilder();
        appendRequestEncoder.appendTo(voteRequest);

        System.out.println(voteRequest);

        assertThat(voteRequest)
                .contains("sourceId=" + sourceId)
                .contains("destinationId=" + destination)
                .contains("term=" + term)
                .contains("prevLogKey=(term=" + prevLogTerm)
                .contains("index="+prevLogIndex)
                .contains("commitLogIndex="+commitIndex)
                .contains("logEntries=[]");
    }

    private void assertLogAppendRequest(final AppendRequestEncoder appendRequestEncoder,
                                          final int sourceId,
                                          final int destination,
                                          final int term,
                                          final int prevLogTerm,
                                          final long prevLogIndex,
                                          final long commitIndex,
                                          final int nextLogTerm,
                                          final int commandLength) {
        final StringBuilder voteRequest = new StringBuilder();
        appendRequestEncoder.appendTo(voteRequest);

        System.out.println(voteRequest);

        assertThat(voteRequest)
                .contains("sourceId=" + sourceId)
                .contains("destinationId=" + destination)
                .contains("term=" + term)
                .contains("prevLogKey=(term=" + prevLogTerm)
                .contains("index="+prevLogIndex)
                .contains("commitLogIndex="+commitIndex)
                .contains("ogEntries=[(term=" + nextLogTerm)
                .contains("command="+ commandLength);
    }

    @Test
    public void processTick_sends_log_append_request_when_prevIndex_equals_match_index() throws Exception {
        //given
        final long logSize = 21;
        final int peerServerId = 2;
        final long peerPrevIndex = 17;
        final long peerMatchIndex = 17;
        final long peerNexIndex = peerPrevIndex + 1;
        final int currentTerm = 5;
        final int peerPrevTerm = 4;
        final int peerNexTerm=5;
        final long commitIndex = 15;

        when(persistentState.lastIndex()).thenReturn(logSize - 1);

        when(peer.nextIndex()).thenReturn(peerNexIndex);

        when(peer.matchIndex()).thenReturn(peerMatchIndex);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(persistentState.term(peerPrevIndex)).thenReturn(peerPrevTerm);
        when(persistentState.term(peerNexIndex)).thenReturn(peerNexTerm);

        when(peer.serverId()).thenReturn(peerServerId);
        when(volatileState.commitIndex()).thenReturn(commitIndex);
        when(peer.heartbeatTimer()).thenReturn(timer);
        when(timer.hasTimeoutElapsed()).thenReturn(true);

        final String commandText = "This is the command";
        final byte[] commandArray = commandText.getBytes();

        doAnswer(invocation -> {
            invocation.<MutableDirectBuffer>getArgument(1).wrap(commandArray, 0, commandArray.length);
            return invocation;
        }).when(persistentState).wrap(peerNexIndex, commandDecoderBuffer);

        //when
        final Transition transition = leaderServerState.processTick();

        //then
        assertThat(transition).isEqualTo(Transition.STEADY);

        verify(peers).forEach(peerConsumerCaptor.capture());
        peerConsumerCaptor.getValue().accept(peer);

        verify(publisher).publish(encoderBuffer, 0, 71);

        assertLogAppendRequest(appendRequestEncoder, serverId, peerServerId,
                currentTerm, peerPrevTerm, peerPrevIndex, commitIndex, peerNexTerm, commandArray.length);

        verify(timer).reset();
    }

    @Test
    public void onAppendResponse_updates_peer_match_index_and_advances_next_index_and_sends_log_append_request_when_successful() throws Exception {
        //given
        final int responseTerm = 1;
        final int currentTerm = 1;
        final int peerServerId = 2;

        final long prevLogIndex = -1;

        final long matchIndex = 0;
        final int matchTerm = 1;

        final long logSize = 2;
        final long commitIndex = -1;

        when(appendResponseDecoder.header()).thenReturn(headerDecoder);
        when(appendResponseDecoder.prevLogIndex()).thenReturn(prevLogIndex);
        when(appendResponseDecoder.matchLogIndex()).thenReturn(matchIndex);
        when(appendResponseDecoder.successful()).thenReturn(BooleanType.T);
        when(headerDecoder.sourceId()).thenReturn(peerServerId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(peers.peer(peerServerId)).thenReturn(peer);

        when(peer.comparePreviousAndUpdateMatchAndNextIndex(prevLogIndex, matchIndex)).thenReturn(true);

        when(persistentState.lastIndex()).thenReturn(logSize - 1);

        when(peer.nextIndex()).thenReturn(matchIndex + 1);

        when(peer.matchIndex()).thenReturn(matchIndex);
        when(persistentState.term(matchIndex)).thenReturn(matchTerm);
        when(persistentState.term(matchIndex + 1)).thenReturn(currentTerm);

        when(peer.serverId()).thenReturn(peerServerId);
        when(volatileState.commitIndex()).thenReturn(commitIndex);
        when(peer.heartbeatTimer()).thenReturn(timer);

        final String commandText = "This is the command";
        final byte[] commandArray = commandText.getBytes();
        commandDecoderBuffer.wrap(commandArray, 0 , commandArray.length);


        //when
        leaderServerState.onAppendResponse(appendResponseDecoder);

        //then

        verify(publisher).publish(encoderBuffer, 0, 71);

        assertLogAppendRequest(appendRequestEncoder, serverId, peerServerId,
                currentTerm, matchTerm, matchIndex, commitIndex, currentTerm, commandArray.length);

        verify(timer).reset();

    }

    @Test
    public void onAppendResponse_decrements_next_index_and_sends_empty_append_request_when_unsuccessful() throws Exception {
        //given
        final int responseTerm = 5;
        final int currentTerm = 5;
        final int peerServerId = 2;

        final long prevLogIndex = 10;

        final long matchIndex = -1;
        final int prevPrevTerm = 4;

        final long logSize = 11;
        final long commitIndex = -1;

        when(appendResponseDecoder.header()).thenReturn(headerDecoder);
        when(appendResponseDecoder.prevLogIndex()).thenReturn(prevLogIndex);
        when(appendResponseDecoder.successful()).thenReturn(BooleanType.F);
        when(headerDecoder.sourceId()).thenReturn(peerServerId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(peers.peer(peerServerId)).thenReturn(peer);

        when(peer.comparePreviousAndDecrementNextIndex(prevLogIndex)).thenReturn(true);

        when(peer.nextIndex()).thenReturn(prevLogIndex);

        when(persistentState.term(prevLogIndex - 1)).thenReturn(prevPrevTerm);

        when(peer.serverId()).thenReturn(peerServerId);
        when(volatileState.commitIndex()).thenReturn(commitIndex);
        when(peer.heartbeatTimer()).thenReturn(timer);

        //when
        leaderServerState.onAppendResponse(appendResponseDecoder);

        //then

        verify(publisher).publish(encoderBuffer, 0, 44);

        assertEmptyAppendRequest(appendRequestEncoder, serverId, peerServerId,
                currentTerm, prevPrevTerm, prevLogIndex - 1, commitIndex);

        verify(timer).reset();
    }

    @Test
    public void onCommandRequest_appends_command_and_sends_log_append_request_when_prevIndex_equals_match_index() throws Exception {
        //given
        final long logSize = 21;
        final int peerServerId = 2;
        final long peerPrevIndex = 17;
        final long peerMatchIndex = 17;
        final long peerNexIndex = peerPrevIndex + 1;
        final int currentTerm = 5;
        final int peerPrevTerm = 4;
        final int peerNexTerm=5;
        final long commitIndex = 15;

        when(persistentState.lastIndex()).thenReturn(logSize - 1);

        when(peer.nextIndex()).thenReturn(peerNexIndex);

        when(peer.matchIndex()).thenReturn(peerMatchIndex);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(persistentState.term(peerPrevIndex)).thenReturn(peerPrevTerm);
        when(persistentState.term(peerNexIndex)).thenReturn(peerNexTerm);

        when(peer.serverId()).thenReturn(peerServerId);
        when(volatileState.commitIndex()).thenReturn(commitIndex);
        when(peer.heartbeatTimer()).thenReturn(timer);
        when(peers.matchIndexPrecedingNextIndexAndEqualAtAllPeers()).thenReturn(Peer.NULL_INDEX);



        final String commandText = "This is the command";
        final byte[] commandArray = commandText.getBytes();
        commandDecoderBuffer.wrap(commandArray, 0 , commandArray.length);

        //when
        leaderServerState.onCommandRequest(commandDecoderBuffer, 0, commandArray.length);

        //then
        verify(persistentState).append(currentTerm, commandDecoderBuffer, 0, commandArray.length);

        verify(peers).forEach(peerConsumerCaptor.capture());
        peerConsumerCaptor.getValue().accept(peer);

        verify(publisher).publish(encoderBuffer, 0, 71);

        assertLogAppendRequest(appendRequestEncoder, serverId, peerServerId,
                currentTerm, peerPrevTerm, peerPrevIndex, commitIndex, peerNexTerm, commandArray.length);

        verify(timer).reset();
    }


}