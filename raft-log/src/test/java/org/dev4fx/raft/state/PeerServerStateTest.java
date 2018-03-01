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

import org.dev4fx.raft.sbe.AppendRequestDecoder;
import org.dev4fx.raft.sbe.VoteRequestDecoder;
import org.dev4fx.raft.timer.Timer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.BiFunction;
import java.util.function.IntConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PeerServerStateTest {
    private FollowerServerState followerServerState;

    private int serverId = 0;

    @Mock
    private BiFunction<AppendRequestDecoder, Logger, Transition> appendRequestHandler;
    @Mock
    private BiFunction<VoteRequestDecoder, Logger, Transition> voteRequestHandler;

    @Mock
    private Timer electionTimer;

    @Mock
    private IntConsumer onFollowerTransitionHandler;

    @Mock
    private AppendRequestDecoder appendRequestDecoder;
    @Mock
    private VoteRequestDecoder voteRequestDecoder;

    @Before
    public void setUp() throws Exception {
        followerServerState = new FollowerServerState(serverId,
                appendRequestHandler, voteRequestHandler, electionTimer,
                onFollowerTransitionHandler);
    }

    @Test
    public void onTransition_should_restart_election_timer_and_notify_transition_listener() throws Exception {
        //when
        followerServerState.onTransition();

        //then
        verify(electionTimer).restart();
        verify(onFollowerTransitionHandler).accept(serverId);
    }

    @Test
    public void processTick_should_switch_to_candidate_when_election_timer_has_elapsed() throws Exception {
        //given
        when(electionTimer.hasTimeoutElapsed()).thenReturn(true);

        //when + then
        assertThat(followerServerState.processTick()).isEqualTo(Transition.TO_CANDIDATE_NO_REPLAY);
    }

    @Test
    public void processTick_should_be_steady_when_election_timer_has_not_elapsed() throws Exception {
        //given
        when(electionTimer.hasTimeoutElapsed()).thenReturn(false);

        //when + then
        assertThat(followerServerState.processTick()).isEqualTo(Transition.STEADY);
    }

    @Test
    public void onAppendRequest_should_delegate_to_appendRequestHandler() throws Exception {
        //when
        followerServerState.onAppendRequest(appendRequestDecoder);

        //then
        verify(appendRequestHandler).apply(same(appendRequestDecoder), any(Logger.class));
    }

    @Test
    public void onVoteRequest() throws Exception {
        //when
        followerServerState.onVoteRequest(voteRequestDecoder);

        //then
        verify(voteRequestHandler).apply(same(voteRequestDecoder), any(Logger.class));
    }

    @Test
    public void role_should_be_FOLLOWER() throws Exception {
        assertThat(followerServerState.role()).isEqualTo(Role.FOLLOWER);
    }
}