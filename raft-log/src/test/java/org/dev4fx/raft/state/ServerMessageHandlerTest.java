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
import org.dev4fx.raft.sbe.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ServerMessageHandlerTest {
    @Mock
    private MessageHeaderDecoder messageHeaderDecoder;
    @Mock
    private VoteRequestDecoder voteRequestDecoder;
    @Mock
    private VoteResponseDecoder voteResponseDecoder;
    @Mock
    private AppendRequestDecoder appendRequestDecoder;
    @Mock
    private AppendResponseDecoder appendResponseDecoder;

    @Mock
    private ServerState candidateState;
    @Mock
    private ServerState leaderState;
    @Mock
    private ServerState followerState;
    @Mock
    private DirectBuffer directBuffer;

    private InOrder inOrder;
            
    private ServerMessageHandler serverMessageHandler;

    @Before
    public void setUp() throws Exception {
        when(followerState.role()).thenReturn(Role.FOLLOWER);
        when(candidateState.role()).thenReturn(Role.CANDIDATE);
        when(leaderState.role()).thenReturn(Role.LEADER);

        inOrder = inOrder(candidateState, leaderState, followerState);
    }

    ServerMessageHandler createServerMessageHandler(final ServerState initialState) {
        return new ServerMessageHandler(messageHeaderDecoder, voteRequestDecoder,
                voteResponseDecoder, appendRequestDecoder, appendResponseDecoder,
                candidateState, leaderState, followerState, initialState);
    }

    @Test
    public void execute_transitions_to_another_state_when_resulted_with_transition_other_than_steady() throws Exception {
        //given
        serverMessageHandler = createServerMessageHandler(followerState);

        when(followerState.processTick()).thenReturn(Transition.TO_CANDIDATE_NO_REPLAY);
        when(candidateState.processTick()).thenReturn(Transition.TO_LEADER_NO_REPLAY);
        when(leaderState.processTick()).thenReturn(Transition.TO_FOLLOWER_NO_REPLAY);

        serverMessageHandler.execute();
        serverMessageHandler.execute();
        serverMessageHandler.execute();

        inOrder.verify(followerState).processTick();
        inOrder.verify(candidateState).processTick();
        inOrder.verify(leaderState).processTick();
    }

    @Test
    public void execute_does_not_transition_when_resulted_with_steady_transition() throws Exception {
        //given
        serverMessageHandler = createServerMessageHandler(followerState);

        when(followerState.processTick()).thenReturn(Transition.STEADY);

        serverMessageHandler.execute();
        serverMessageHandler.execute();
        serverMessageHandler.execute();

        inOrder.verify(followerState, times(3)).processTick();
    }

    @Test
    public void follower_handles_voteRequest_and_remains_in_same_state() throws Exception {
        //given
        serverMessageHandler = createServerMessageHandler(followerState);

        final int offset = 0;
        final int length = 20;
        final int headerLength = 8;
        when(messageHeaderDecoder.encodedLength()).thenReturn(headerLength);
        when(messageHeaderDecoder.templateId()).thenReturn(VoteRequestDecoder.TEMPLATE_ID);

        when(followerState.onVoteRequest(voteRequestDecoder)).thenReturn(Transition.STEADY);

        //when
        serverMessageHandler.onMessage(directBuffer, offset, length);

        //then
        verify(messageHeaderDecoder).wrap(directBuffer, offset);
        verify(voteRequestDecoder).wrap(directBuffer,headerLength + offset,
                VoteRequestDecoder.BLOCK_LENGTH,
                VoteRequestDecoder.SCHEMA_VERSION);
        verify(followerState, times(1)).onVoteRequest(voteRequestDecoder);
    }

    @Test
    public void candidate_handles_appendRequest_and_transitions_to_follower_replaying_appendRequest() throws Exception {
        //given
        serverMessageHandler = createServerMessageHandler(candidateState);

        final int offset = 0;
        final int length = 20;
        final int headerLength = 8;
        when(messageHeaderDecoder.encodedLength()).thenReturn(headerLength);
        when(messageHeaderDecoder.templateId()).thenReturn(AppendRequestDecoder.TEMPLATE_ID);

        when(candidateState.onAppendRequest(appendRequestDecoder)).thenReturn(Transition.TO_FOLLOWER_REPLAY);
        when(followerState.onAppendRequest(appendRequestDecoder)).thenReturn(Transition.STEADY);

        //when
        serverMessageHandler.onMessage(directBuffer, offset, length);

        //then
        verify(messageHeaderDecoder, times(2)).wrap(directBuffer, offset);
        verify(appendRequestDecoder, times(2)).wrap(directBuffer,headerLength + offset,
                AppendRequestDecoder.BLOCK_LENGTH,
                AppendRequestDecoder.SCHEMA_VERSION);
        verify(candidateState).onAppendRequest(appendRequestDecoder);
        verify(followerState).onTransition();
        verify(followerState).onAppendRequest(appendRequestDecoder);
    }

    @Test
    public void candidate_handles_voteResponse_and_transitions_to_leader() throws Exception {
        //given
        serverMessageHandler = createServerMessageHandler(candidateState);

        final int offset = 0;
        final int length = 20;
        final int headerLength = 8;
        when(messageHeaderDecoder.encodedLength()).thenReturn(headerLength);
        when(messageHeaderDecoder.templateId()).thenReturn(VoteResponseDecoder.TEMPLATE_ID);

        when(candidateState.onVoteResponse(voteResponseDecoder)).thenReturn(Transition.TO_LEADER_NO_REPLAY);

        //when
        serverMessageHandler.onMessage(directBuffer, offset, length);

        //then
        verify(messageHeaderDecoder).wrap(directBuffer, offset);
        verify(voteResponseDecoder).wrap(directBuffer,headerLength + offset,
                VoteResponseDecoder.BLOCK_LENGTH,
                VoteResponseDecoder.SCHEMA_VERSION);
        verify(candidateState).onVoteResponse(voteResponseDecoder);
        verify(leaderState).onTransition();
    }

    @Test
    public void leader_handles_appendResponse_and_remains_in_same_state() throws Exception {
        //given
        serverMessageHandler = createServerMessageHandler(leaderState);

        final int offset = 0;
        final int length = 20;
        final int headerLength = 8;
        when(messageHeaderDecoder.encodedLength()).thenReturn(headerLength);
        when(messageHeaderDecoder.templateId()).thenReturn(AppendResponseDecoder.TEMPLATE_ID);

        when(leaderState.onAppendResponse(appendResponseDecoder)).thenReturn(Transition.STEADY);

        //when
        serverMessageHandler.onMessage(directBuffer, offset, length);

        //then
        verify(messageHeaderDecoder).wrap(directBuffer, offset);
        verify(appendResponseDecoder).wrap(directBuffer,headerLength + offset,
                AppendResponseDecoder.BLOCK_LENGTH,
                AppendResponseDecoder.SCHEMA_VERSION);
        verify(leaderState).onAppendResponse(appendResponseDecoder);
    }

    @Test
    public void leader_handles_commandRequest_and_remains_in_same_state() throws Exception {
        //given
        serverMessageHandler = createServerMessageHandler(leaderState);

        final int offset = 0;
        final int length = 20;
        when(messageHeaderDecoder.templateId()).thenReturn(CommandRequestDecoder.TEMPLATE_ID);
        when(leaderState.onCommandRequest(directBuffer, offset, length)).thenReturn(Transition.STEADY);

        //when
        serverMessageHandler.onMessage(directBuffer, offset, length);

        //then
        verify(leaderState).onCommandRequest(directBuffer, offset, length);
    }

    @Test
    public void leader_handles_unknown_command_and_remains_in_same_state() throws Exception {
        //given
        serverMessageHandler = createServerMessageHandler(leaderState);

        final int offset = 0;
        final int length = 20;
        when(messageHeaderDecoder.templateId()).thenReturn(1234);

        //when
        serverMessageHandler.onMessage(directBuffer, offset, length);

        //then
        verifyNoMoreInteractions(leaderState);
    }

    @Test
    public void init_should_perform_on_transition() throws Exception {
        //given
        serverMessageHandler = createServerMessageHandler(followerState);

        //when
        serverMessageHandler.init();

        //then
        verify(followerState).onTransition();
    }

}