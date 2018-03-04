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
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.sbe.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HighTermHandlingServerStateTest {

    @Mock
    private ServerState delegateServerState;
    @Mock
    private PersistentState persistentState;
    @Mock
    private Logger logger;

    @Mock
    private AppendRequestDecoder appendRequestDecoder;
    @Mock
    private AppendResponseDecoder appendResponseDecoder;
    @Mock
    private VoteRequestDecoder voteRequestDecoder;
    @Mock
    private VoteResponseDecoder voteResponseDecoder;
    @Mock
    private HeaderDecoder headerDecoder;
    @Mock
    private DirectBuffer commandBuffer;

    private HighTermHandlingServerState highTermHandlingServerState;


    @Before
    public void setUp() throws Exception {
        when(appendRequestDecoder.header()).thenReturn(headerDecoder);
        when(voteRequestDecoder.header()).thenReturn(headerDecoder);
        when(voteResponseDecoder.header()).thenReturn(headerDecoder);
        when(appendResponseDecoder.header()).thenReturn(headerDecoder);

        highTermHandlingServerState = new HighTermHandlingServerState(delegateServerState, persistentState, logger);
    }

    @Test
    public void role() throws Exception {
        //given
        when(delegateServerState.role()).thenReturn(Role.FOLLOWER);

        //when
        assertThat(highTermHandlingServerState.role()).isEqualTo(Role.FOLLOWER);
    }

    @Test
    public void onTransition() throws Exception {
        //when
        highTermHandlingServerState.onTransition();
        //then
        verify(delegateServerState).onTransition();
    }

    @Test
    public void processTick() throws Exception {
        //when
        highTermHandlingServerState.processTick();
        //then
        verify(delegateServerState).processTick();
    }

    @Test
    public void onVoteRequest_clears_vote_and_sets_term_from_request_when_requestTerm_greater_than_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 6;
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        final Transition transition = highTermHandlingServerState.onVoteRequest(voteRequestDecoder);

        //then
        assertThat(transition).isEqualTo(Transition.TO_FOLLOWER_REPLAY);
        verify(persistentState).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(0)).onVoteRequest(voteRequestDecoder);
    }

    @Test
    public void onVoteRequest_delegates_when_requestTerm_same_as_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 5;
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        highTermHandlingServerState.onVoteRequest(voteRequestDecoder);

        //then
        verify(persistentState, times(0)).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(1)).onVoteRequest(voteRequestDecoder);
    }

    @Test
    public void onVoteResponse_clears_vote_and_sets_term_from_request_when_requestTerm_greater_than_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 6;
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        final Transition transition = highTermHandlingServerState.onVoteResponse(voteResponseDecoder);

        //then
        assertThat(transition).isEqualTo(Transition.TO_FOLLOWER_NO_REPLAY);
        verify(persistentState).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(0)).onVoteResponse(voteResponseDecoder);
    }

    @Test
    public void onVoteResponse_delegates_when_requestTerm_same_as_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 5;
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        highTermHandlingServerState.onVoteResponse(voteResponseDecoder);

        //then
        verify(persistentState, times(0)).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(1)).onVoteResponse(voteResponseDecoder);
    }

    @Test
    public void onAppendRequest_clears_vote_and_sets_term_from_request_when_requestTerm_greater_than_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 6;
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        final Transition transition = highTermHandlingServerState.onAppendRequest(appendRequestDecoder);

        //then
        assertThat(transition).isEqualTo(Transition.TO_FOLLOWER_REPLAY);
        verify(persistentState).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(0)).onAppendRequest(appendRequestDecoder);
    }

    @Test
    public void onAppendRequest_delegates_when_requestTerm_same_as_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 5;
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        highTermHandlingServerState.onAppendRequest(appendRequestDecoder);

        //then
        verify(persistentState, times(0)).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(1)).onAppendRequest(appendRequestDecoder);
    }

    @Test
    public void onAppendResponse_clears_vote_and_sets_term_from_request_when_requestTerm_greater_than_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 6;
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        final Transition transition = highTermHandlingServerState.onAppendResponse(appendResponseDecoder);

        //then
        assertThat(transition).isEqualTo(Transition.TO_FOLLOWER_NO_REPLAY);
        verify(persistentState).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(0)).onAppendResponse(appendResponseDecoder);
    }

    @Test
    public void onAppendResponse_delegates_when_requestTerm_same_as_current_term() throws Exception {
        //given
        final int currentTerm = 5;
        final int requestTerm = 5;
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(headerDecoder.term()).thenReturn(requestTerm);

        //when
        highTermHandlingServerState.onAppendResponse(appendResponseDecoder);

        //then
        verify(persistentState, times(0)).clearVoteForAndSetCurrentTerm(requestTerm);
        verify(delegateServerState, times(1)).onAppendResponse(appendResponseDecoder);
    }

    @Test
    public void onCommandRequest() throws Exception {
        //when
        highTermHandlingServerState.onCommandRequest(commandBuffer, 0 ,100);
        //then
        verify(delegateServerState).onCommandRequest(commandBuffer, 0, 100);
    }

    @Test
    public void onTimeoutNow() throws Exception {
        //when
        highTermHandlingServerState.onTimeoutNow();
        //then
        verify(delegateServerState).onTimeoutNow();
    }

}