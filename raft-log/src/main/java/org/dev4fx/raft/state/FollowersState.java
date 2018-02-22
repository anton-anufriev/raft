package org.dev4fx.raft.state;

import org.dev4fx.raft.timer.Timer;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongToIntFunction;
import java.util.function.Supplier;

public final class FollowersState {

    private final int serverId;
    private final int serverCount;
    private final Follower[] followers;
    private final int majority;
    private final BiConsumer<? super Consumer<? super Follower>, ? super Follower> forEachBiConsumer;

    public FollowersState(final int serverId,
                          final int serverCount,
                          final Supplier<Timer> timerFactory) {
        this.serverId = serverId;
        this.serverCount = serverCount;
        this.followers = initFollowers(serverId, serverCount, timerFactory);
        this.majority = -Math.floorDiv(serverCount, -2);
        this.forEachBiConsumer = Consumer::accept;
    }

    public int majority() {
        return majority;
    }

    public int followersMajority() {
        return majority - 1;
    }

    public Follower follower(int followerId) {
        return followers[followerId];
    }

    public void resetFollowers(final long nextIndex) {
        forEach(nextIndex, (nextIndex1, follower) -> follower.nextIndex(nextIndex1).resetMatchIndex().heartbeatTimer().reset());
    }

    public int serverCount() {
        return serverCount;
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

    public void forEach(final Consumer<? super Follower> consumer) {
        forEach(consumer, forEachBiConsumer);
    }

    public <T> void forEach(T value, final BiConsumer<T, ? super Follower> consumer) {
        for (final Follower follower : followers) {
            if (follower != null && follower.serverId() != serverId) {
                consumer.accept(value, follower);
            }
        }
    }

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
