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

import org.dev4fx.raft.timer.Timer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class DefaultPeerTest {
    private int serverId = 1;
    @Mock
    private Timer heartbeatTimer;
    private DefaultPeer peer;

    @Before
    public void setUp() throws Exception {
        peer = new DefaultPeer(serverId, heartbeatTimer);
    }

    @Test
    public void serverId() throws Exception {
        assertThat(peer.serverId()).isEqualTo(serverId);
    }

    @Test
    public void heartbeatTimer() throws Exception {
        assertThat(peer.heartbeatTimer()).isEqualTo(heartbeatTimer);
    }

    @Test
    public void nextIndex() throws Exception {
        peer.nextIndex(12);
        assertThat(peer.nextIndex()).isEqualTo(12);
    }

    @Test
    public void matchIndex() throws Exception {
        peer.nextIndex(10)
                .comparePreviousAndUpdateMatchAndNextIndex(9, 12);
        assertThat(peer.matchIndex()).isEqualTo(12);
    }


    @Test
    public void grantedVote() throws Exception {
        peer.setGrantedVote(true);
        assertThat(peer.grantedVote()).isTrue();

        peer.setGrantedVote(false);
        assertThat(peer.grantedVote()).isFalse();
    }

    @Test
    public void comparePreviousAndDecrementNextIndex() throws Exception {
        peer.nextIndex(10);

        assertThat(peer.comparePreviousAndDecrementNextIndex(9)).isTrue();
        assertThat(peer.comparePreviousAndDecrementNextIndex(9)).isFalse();
        assertThat(peer.comparePreviousAndDecrementNextIndex(8)).isTrue();
        assertThat(peer.comparePreviousAndDecrementNextIndex(7)).isTrue();
    }

    @Test
    public void comparePreviousAndUpdateMatchIndex_returns_false_when_previous_index_does_not_match() throws Exception {
        assertThat(peer.nextIndex(10)
                .comparePreviousAndUpdateMatchAndNextIndex(10, 12)).isFalse();
    }

    @Test
    public void comparePreviousAndUpdateMatchIndex_returns_true_when_previous_index_matchs() throws Exception {
        assertThat(peer.nextIndex(10)
                .comparePreviousAndUpdateMatchAndNextIndex(9, 12)).isTrue();
    }

    @Test
    public void reset() throws Exception {
        peer.reset();

        assertThat(peer.grantedVote()).isFalse();
        assertThat(peer.matchIndex()).isEqualTo(-1);
        assertThat(peer.nextIndex()).isEqualTo(-1);
        assertThat(peer.previousIndex()).isEqualTo(-2);
    }

}