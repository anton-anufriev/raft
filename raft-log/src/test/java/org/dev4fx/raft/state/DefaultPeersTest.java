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
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class DefaultPeersTest {

    @Mock
    private Timer timer;
    private Supplier<Timer> timerFactory;

    @Before
    public void setUp() throws Exception {
        timerFactory = () -> timer;
    }

    @Test
    public void majority() throws Exception {

        final Peers peers = new DefaultPeers(0, 3, timerFactory);
        assertThat(peers.majority()).isEqualTo(2);
    }

    @Test
    public void peersMajority() throws Exception {
    }

    @Test
    public void peer() throws Exception {
    }

    @Test
    public void resetAsFollowers() throws Exception {
    }

    @Test
    public void reset() throws Exception {
    }

    @Test
    public void forEach() throws Exception {
    }

    @Test
    public void forEach1() throws Exception {
    }

    @Test
    public void majorityCommitIndex() throws Exception {
    }

    @Test
    public void majorityOfVotes() throws Exception {
    }

}