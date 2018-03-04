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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.LongToIntFunction;

public final class DefaultPeers implements Peers {

    private final int serverId;
    private final Peer[] peers;
    private final int majority;
    private final BiConsumer<? super Consumer<? super Peer>, ? super Peer> forEachBiConsumer;

    public DefaultPeers(final int serverId,
                        final int serverCount,
                        final IntFunction<? extends Peer> peerFactory) {
        this.serverId = serverId;
        this.peers = init(serverId, serverCount, peerFactory);
        this.majority = -Math.floorDiv(serverCount, -2);
        this.forEachBiConsumer = Consumer::accept;
    }

    @Override
    public int majority() {
        return majority;
    }

    @Override
    public int peersMajority() {
        return majority - 1;
    }

    @Override
    public Peer peer(int serverId) {
        if (serverId < 0 || serverId >= peers.length) {
            throw new IllegalArgumentException("ServerId " + serverId + " is out of boundary, >= 0 && < " + peers.length);
        }
        return peers[serverId];
    }

    @Override
    public void resetAsFollowers(final long nextIndex) {
        forEach(nextIndex, (nextIndex1, peer) -> peer
                .reset()
                .nextIndex(nextIndex1)
                .heartbeatTimer().reset());
    }

    @Override
    public void reset() {
        forEach(Peer::reset);
    }

    private static Peer[] init(final int serverId, final int serverCount, final IntFunction<? extends Peer> peerFactory) {
        final Peer[] peers = new Peer[serverCount];
        for (int id = 0; id < serverCount; id++) {
            if (id != serverId) {
                peers[id] = peerFactory.apply(id);
            }
        }
        return peers;
    }

    @Override
    public void forEach(final Consumer<? super Peer> consumer) {
        forEach(consumer, forEachBiConsumer);
    }

    @Override
    public <T> void forEach(T value, final BiConsumer<T, ? super Peer> consumer) {
        for (final Peer peer : peers) {
            if (peer != null && peer.serverId() != serverId) {
                consumer.accept(value, peer);
            }
        }
    }

    @Override
    public long majorityCommitIndex(final long leaderCommitIndex,
                                    final int currentTerm,
                                    final LongToIntFunction termAtIndex) {

        long minIndexHigherThanCommitIndex = Long.MAX_VALUE;
        int higherThanCommitCount = 0;

        for (final Peer peer : peers) {
            if (peer != null && peer.serverId() != serverId) {
                final long matchIndex = peer.matchIndex();
                if (matchIndex > leaderCommitIndex) {
                    final int matchTerm = termAtIndex.applyAsInt(matchIndex);
                    if (matchTerm == currentTerm) {
                        higherThanCommitCount++;
                        minIndexHigherThanCommitIndex = Long.min(matchIndex, minIndexHigherThanCommitIndex);
                    }
                }

            }
        }
        return higherThanCommitCount >= peersMajority() ? minIndexHigherThanCommitIndex : leaderCommitIndex;
    }

    @Override
    public boolean majorityOfVotes() {
        final int neededVotes = peersMajority();
        int receivedVotes = 0;
        for (final Peer peer : peers) {
            if (peer != null && peer.serverId() != serverId) {
                if (peer.grantedVote()) receivedVotes++;
                if (receivedVotes == neededVotes) {
                    return true;
                }
            }
        }
        return false;
    }
}
