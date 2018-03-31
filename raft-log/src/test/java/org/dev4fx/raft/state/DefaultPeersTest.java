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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.tools4j.spockito.Spockito;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Spockito.class)
public class DefaultPeersTest {

    @Test
    @Spockito.Unroll({
            "| serverCount  | majority |",
            "|--------------|----------|",
            "| 3            | 2        |",
            "| 5            | 3        |",
            "| 7            | 4        |",
            "| 9            | 5        |",
            "| 11           | 6        |",
            "| 13           | 7        |",
    })
    @Spockito.Name("[{row}]: {serverCount}, {majority}")
    public void majority(final int serverCount, final int majority) throws Exception {

        final Peers peers = new DefaultPeers(0, serverCount, peerId -> mock(Peer.class));
        assertThat(peers.majority()).isEqualTo(majority);
    }

    @Test
    @Spockito.Unroll({
            "| serverCount  | peersMajority |",
            "|--------------|---------------|",
            "| 3            | 1             |",
            "| 5            | 2             |",
            "| 7            | 3             |",
            "| 9            | 4             |",
            "| 11           | 5             |",
            "| 13           | 6             |",
    })
    @Spockito.Name("[{row}]: {serverCount}, {peersMajority}")
    public void peersMajority(final int serverCount, final int peersMajority) throws Exception {
        final Peers peers = new DefaultPeers(0, serverCount, peerId -> mock(Peer.class));
        assertThat(peers.peersMajority()).isEqualTo(peersMajority);
    }

    @Test
    public void peer_when_within_boundary() throws Exception {
        final Peers peers = new DefaultPeers(0, 3, peerId -> mock(Peer.class));
        assertThat(peers.peer(1)).isNotNull();
        assertThat(peers.peer(2)).isNotNull();
    }

    @Test(expected = IllegalArgumentException.class)
    public void peer_when_higher_than_upper_boundary() throws Exception {
        final Peers peers = new DefaultPeers(0, 3, peerId -> mock(Peer.class));
        peers.peer(3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void peer_when_lower_than_lower_boundary() throws Exception {
        final Peers peers = new DefaultPeers(0, 3, peerId -> mock(Peer.class));
        peers.peer(-1);
    }

    @Test
    public void resetAsFollowers() throws Exception {
        final int clusterSize = 3;
        final int leaderId = 0;

        final Map<Integer, Peer> peersMap = new HashMap<>();
        Peers.forEachPeer(clusterSize, leaderId, peerId -> peersMap.put(peerId, mock(Peer.class, Answers.RETURNS_SELF)));

        final Map<Integer, Timer> timerMap = new HashMap<>();
        Peers.forEachPeer(clusterSize, leaderId, peerId -> timerMap.put(peerId, mock(Timer.class)));

        peersMap.forEach((peerId, peer) -> {
            when(peer.heartbeatTimer()).thenReturn(timerMap.get(peerId));
            when(peer.serverId()).thenReturn(peerId);
        });

        final Peers peers = new DefaultPeers(leaderId, clusterSize, peersMap::get);
        peers.resetAsFollowers(10);

        peersMap.forEach((peerId, peer) -> {
            verify(peer).reset();
            verify(peer).nextIndex(10);
            verify(peer.heartbeatTimer()).reset();
        });
    }

    @Test
    public void reset() throws Exception {
        final int clusterSize = 3;
        final int leaderId = 0;

        final Map<Integer, Peer> peersMap = new HashMap<>();
        Peers.forEachPeer(clusterSize, leaderId, peerId -> peersMap.put(peerId, mock(Peer.class, Answers.RETURNS_SELF)));

        peersMap.forEach((peerId, peer) -> {
            when(peer.serverId()).thenReturn(peerId);
        });

        final Peers peers = new DefaultPeers(leaderId, clusterSize, peersMap::get);
        peers.reset();

        peersMap.forEach((peerId, peer) -> {
            verify(peer).reset();
        });
    }

    @Test
    @Spockito.Unroll({
            "| leaderCommitIndex  | leaderTerm  | peer1MatchIndex  | peer1MatchTerm  | peer2MatchIndex | peer2MatchTerm  | peer3MatchIndex  | peer3MatchTerm  | peer4MatchIndex | peer4MatchTerm  | peersMajorityCommitIndex  |",
            "|--------------------|-------------|------------------|-----------------|-----------------|-----------------|------------------|-----------------|-----------------|-----------------|---------------------------|",
            "| 10                 | 5           | 11               | 5               | 9               | 4               | 8                | 3               | 11              | 5               | 11                        |",
            "| 10                 | 5           | 9                | 4               | 9               | 4               | 8                | 3               | 11              | 5               | 10                        |",
            "| 10                 | 5           | 9                | 4               | 9               | 4               | 9                | 4               | 11              | 5               | 10                        |",
            "| 10                 | 5           | 12               | 5               | 12              | 5               | 12               | 5               | 6               | 3               | 12                        |",
    })
    public void majorityCommitIndex(final long leaderCommitIndex,
                                    final int leaderTerm,
                                    final long peer1MatchIndex,
                                    final int  peer1MatchTerm,
                                    final long peer2MatchIndex,
                                    final int  peer2MatchTerm,
                                    final long peer3MatchIndex,
                                    final int  peer3MatchTerm,
                                    final long peer4MatchIndex,
                                    final int  peer4MatchTerm,
                                    final long peersMajorityCommitIndex) throws Exception {
        final int clusterSize = 5;
        final int leaderId = 0;

        final Peers peers = new DefaultPeers(leaderId, clusterSize, peerId -> {
            final Peer peer = mock(Peer.class);
            when(peer.serverId()).thenReturn(peerId);
            when(peer.matchIndex()).thenReturn(peerId == 1 ? peer1MatchIndex :
                                                peerId == 2 ? peer2MatchIndex :
                                                    peerId == 3 ? peer3MatchIndex : peer4MatchIndex);
            return peer;
        });

        final Map<Long, Integer> indexToTerm = new HashMap<>();
        indexToTerm.put(peer1MatchIndex, peer1MatchTerm);
        indexToTerm.put(peer2MatchIndex, peer2MatchTerm);
        indexToTerm.put(peer3MatchIndex, peer3MatchTerm);
        indexToTerm.put(peer4MatchIndex, peer4MatchTerm);

        assertThat(peers.majorityCommitIndex(leaderCommitIndex, leaderTerm, indexToTerm::get)).isEqualTo(peersMajorityCommitIndex);
    }

    @Test
    @Spockito.Unroll({
            "| peer1VoteGranted  |  peer2VoteGranted | peer3VoteGranted  | peer4VoteGranted  | majorityOfVotes  |",
            "|-------------------|-------------------|-------------------|-------------------|------------------|",
            "| false             |  true             | false             | true              | true             |",
            "| false             |  false            | false             | true              | false            |",
    })
    public void majorityOfVotes(final boolean peer1VoteGranted,
                                final boolean peer2VoteGranted,
                                final boolean peer3VoteGranted,
                                final boolean peer4VoteGranted,
                                final boolean majorityOfVotes) throws Exception {
        final int clusterSize = 5;
        final int leaderId = 0;

        final Peers peers = new DefaultPeers(leaderId, clusterSize, peerId -> {
            final Peer peer = mock(Peer.class);
            when(peer.serverId()).thenReturn(peerId);
            when(peer.grantedVote()).thenReturn(
                    peerId == 1 ? peer1VoteGranted :
                        peerId == 2 ? peer2VoteGranted :
                            peerId == 3 ? peer3VoteGranted : peer4VoteGranted);
            return peer;
        });

        assertThat(peers.majorityOfVotes()).isEqualTo(majorityOfVotes);
    }



    @Test
    @Spockito.Unroll({
            "| peer1MatchIndex  | peer1NextIndex  | peer2MatchIndex | peer2NextIndex  | peer3MatchIndex  | peer3NextIndex  | peer4MatchIndex | peer4NextIndex  | matchIndexPrecedingNextIndexAndEqualAtAllPeers  |",
            "|------------------|-----------------|-----------------|-----------------|------------------|-----------------|-----------------|-----------------|-------------------------------------------------|",
            "| 10               | 11              | 10              | 11              | 10               | 11              | 10              | 11              | 10                                              |",
            "| -1               | -1              | 10              | 11              | 10               | 11              | 10              | 11              | -1                                              |",
            "| 10               | 11              | 10              | 11              | 10               | 11              | -1              | -1              | -1                                              |",
            "| 10               | 11              | 10              | 11              | 10               | 11              | 9               | 10              | -1                                              |",
    })
    public void matchIndexPrecedingNextIndexAndEqualAtAllPeers(final long peer1MatchIndex,
                                                               final long peer1NextIndex,
                                                               final long peer2MatchIndex,
                                                               final long peer2NextIndex,
                                                               final long peer3MatchIndex,
                                                               final long peer3NextIndex,
                                                               final long peer4MatchIndex,
                                                               final long peer4NextIndex,
                                                               final long lastEqualMatchIndexAtAllPeers) throws Exception {
        final int clusterSize = 5;
        final int leaderId = 0;

        final Peers peers = new DefaultPeers(leaderId, clusterSize, peerId -> {
            final Peer peer = mock(Peer.class);
            when(peer.serverId()).thenReturn(peerId);
            when(peer.matchIndex()).thenReturn(peerId == 1 ? peer1MatchIndex :
                                               peerId == 2 ? peer2MatchIndex :
                                               peerId == 3 ? peer3MatchIndex : peer4MatchIndex);
            when(peer.nextIndex()).thenReturn(peerId == 1 ? peer1NextIndex :
                                              peerId == 2 ? peer2NextIndex :
                                              peerId == 3 ? peer3NextIndex : peer4NextIndex);

            return peer;
        });

        assertThat(peers.matchIndexPrecedingNextIndexAndEqualAtAllPeers()).isEqualTo(lastEqualMatchIndexAtAllPeers);
    }
}