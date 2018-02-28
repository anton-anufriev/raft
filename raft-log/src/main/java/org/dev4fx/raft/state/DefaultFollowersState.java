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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongToIntFunction;
import java.util.function.Supplier;

public final class DefaultFollowersState implements FollowersState {

    private final int serverId;
    private final Follower[] followers;
    private final int majority;
    private final BiConsumer<? super Consumer<? super Follower>, ? super Follower> forEachBiConsumer;

    public DefaultFollowersState(final int serverId,
                                 final int serverCount,
                                 final Supplier<Timer> timerFactory) {
        this.serverId = serverId;
        this.followers = initFollowers(serverId, serverCount, timerFactory);
        this.majority = -Math.floorDiv(serverCount, -2);
        this.forEachBiConsumer = Consumer::accept;
    }

    @Override
    public int majority() {
        return majority;
    }

    @Override
    public int followersMajority() {
        return majority - 1;
    }

    @Override
    public Follower follower(int followerId) {
        return followers[followerId];
    }

    @Override
    public void resetFollowers(final long nextIndex) {
        forEach(nextIndex, (nextIndex1, follower) -> follower.nextIndex(nextIndex1).resetMatchIndex().heartbeatTimer().reset());
    }

    private static Follower[] initFollowers(final int serverId, final int serverCount, Supplier<Timer> timerFactory) {
        final Follower[] followers = new Follower[serverCount];
        for (int id = 0; id < serverCount; id++) {
            if (id != serverId) {
                followers[id] = new Follower(id, timerFactory.get());
            }
        }
        return followers;
    }

    @Override
    public void forEach(final Consumer<? super Follower> consumer) {
        forEach(consumer, forEachBiConsumer);
    }

    @Override
    public <T> void forEach(T value, final BiConsumer<T, ? super Follower> consumer) {
        for (final Follower follower : followers) {
            if (follower != null && follower.serverId() != serverId) {
                consumer.accept(value, follower);
            }
        }
    }

    @Override
    public long majorityCommitIndex(final long leaderCommitIndex,
                                    final int currentTerm,
                                    final LongToIntFunction termAtIndex) {

        long minIndexHigherThanCommitIndex = Long.MAX_VALUE;
        int higherThanCommitCount = 0;

        for (final Follower follower : followers) {
            if (follower != null && follower.serverId() != serverId) {
                final long matchIndex = follower.matchIndex();
                if (matchIndex > leaderCommitIndex) {
                    final int matchTerm = termAtIndex.applyAsInt(matchIndex);
                    if (matchTerm == currentTerm) {
                        higherThanCommitCount++;
                        minIndexHigherThanCommitIndex = Long.min(matchIndex, minIndexHigherThanCommitIndex);
                    }
                }

            }
        }
        return higherThanCommitCount >= followersMajority() ? minIndexHigherThanCommitIndex : leaderCommitIndex;
    }

}
