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
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HeaderFilteringServerStateTest {
    @Mock
    private Predicate<HeaderDecoder> filter;
    @Mock
    private ServerState delegateServerState;

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


    private HeaderFilteringServerState headerFilteringServerState;

    @Before
    public void setUp() throws Exception {
        when(appendRequestDecoder.header()).thenReturn(headerDecoder);
        when(voteRequestDecoder.header()).thenReturn(headerDecoder);
        when(voteResponseDecoder.header()).thenReturn(headerDecoder);
        when(appendResponseDecoder.header()).thenReturn(headerDecoder);

        headerFilteringServerState = new HeaderFilteringServerState(filter, delegateServerState);
    }

    @Test
    public void role() throws Exception {
        //given
        when(delegateServerState.role()).thenReturn(Role.FOLLOWER);

        //when
        assertThat(headerFilteringServerState.role()).isEqualTo(Role.FOLLOWER);
    }

    @Test
    public void onTransition() throws Exception {
        //when
        headerFilteringServerState.onTransition();
        //then
        verify(delegateServerState).onTransition();
    }

    @Test
    public void processTick() throws Exception {
        //when
        headerFilteringServerState.processTick();
        //then
        verify(delegateServerState).processTick();
    }

    @Test
    public void onVoteRequest_delegates_when_filter_test_is_true() throws Exception {
        //given
        when(filter.test(headerDecoder)).thenReturn(true);

        //when
        final Transition transition = headerFilteringServerState.onVoteRequest(voteRequestDecoder);

        //then
        verify(delegateServerState).onVoteRequest(voteRequestDecoder);
    }

    @Test
    public void onVoteRequest_returns_steady_transition_when_filter_test_is_false() throws Exception {
        //given
        when(filter.test(headerDecoder)).thenReturn(false);

        //when
        final Transition transition = headerFilteringServerState.onVoteRequest(voteRequestDecoder);

        //then
        assertThat(transition).isEqualTo(Transition.STEADY);
        verify(delegateServerState, times(0)).onVoteRequest(voteRequestDecoder);
    }

    @Test
    public void onVoteResponse_delegates_when_filter_test_is_true() throws Exception {
        //given
        when(filter.test(headerDecoder)).thenReturn(true);

        //when
        final Transition transition = headerFilteringServerState.onVoteResponse(voteResponseDecoder);

        //then
        verify(delegateServerState).onVoteResponse(voteResponseDecoder);
    }

    @Test
    public void onVoteResponse_returns_steady_transition_when_filter_test_is_false() throws Exception {
        //given
        when(filter.test(headerDecoder)).thenReturn(false);

        //when
        final Transition transition = headerFilteringServerState.onVoteResponse(voteResponseDecoder);

        //then
        assertThat(transition).isEqualTo(Transition.STEADY);
        verify(delegateServerState, times(0)).onVoteResponse(voteResponseDecoder);
    }

    @Test
    public void onAppendRequest_delegates_when_filter_test_is_true() throws Exception {
        //given
        when(filter.test(headerDecoder)).thenReturn(true);

        //when
        final Transition transition = headerFilteringServerState.onAppendRequest(appendRequestDecoder);

        //then
        verify(delegateServerState).onAppendRequest(appendRequestDecoder);
    }

    @Test
    public void onAppendRequest_returns_steady_transition_when_filter_test_is_false() throws Exception {
        //given
        when(filter.test(headerDecoder)).thenReturn(false);

        //when
        final Transition transition = headerFilteringServerState.onAppendRequest(appendRequestDecoder);

        //then
        assertThat(transition).isEqualTo(Transition.STEADY);
        verify(delegateServerState, times(0)).onAppendRequest(appendRequestDecoder);
    }

    @Test
    public void onAppendResponse_delegates_when_filter_test_is_true() throws Exception {
        //given
        when(filter.test(headerDecoder)).thenReturn(true);

        //when
        final Transition transition = headerFilteringServerState.onAppendResponse(appendResponseDecoder);

        //then
        verify(delegateServerState).onAppendResponse(appendResponseDecoder);
    }

    @Test
    public void onAppendResponse_returns_steady_transition_when_filter_test_is_false() throws Exception {
        //given
        when(filter.test(headerDecoder)).thenReturn(false);

        //when
        final Transition transition = headerFilteringServerState.onAppendResponse(appendResponseDecoder);

        //then
        assertThat(transition).isEqualTo(Transition.STEADY);
        verify(delegateServerState, times(0)).onAppendResponse(appendResponseDecoder);
    }

    @Test
    public void onCommandRequest() throws Exception {
        //when
        headerFilteringServerState.onCommandRequest(commandBuffer, 0 ,100);
        //then
        verify(delegateServerState).onCommandRequest(commandBuffer, 0, 100);

    }

    @Test
    public void onTimeoutNow() throws Exception {
        //when
        headerFilteringServerState.onTimeoutNow();
        //then
        verify(delegateServerState).onTimeoutNow();
    }

}