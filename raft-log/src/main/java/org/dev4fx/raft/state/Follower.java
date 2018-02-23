package org.dev4fx.raft.state;

import org.dev4fx.raft.timer.Timer;

import java.util.Objects;

public final class Follower {

    private final int serverId;
    private final Timer heartbeatTimer;

    private long nextIndex;
    private long matchIndex;

    public Follower(final int serverId,
                    final Timer heartbeatTimer) {
        this.serverId = serverId;
        this.heartbeatTimer = Objects.requireNonNull(heartbeatTimer);
        nextIndex = -1;
        matchIndex = -1;
    }

    public int serverId() {
        return serverId;
    }

    public Timer heartbeatTimer() {
        return heartbeatTimer;
    }

    public long nextIndex() {
        return nextIndex;
    }

    public long previousIndex() {
        return nextIndex() - 1;
    }

    public long matchIndex() {
        return matchIndex;
    }

    public Follower nextIndex(final long index) {
        this.nextIndex = index;
        return this;
    }

    public boolean comparePreviousAndDecrementNextIndex(final long previousIndex) {
        if (previousIndex() == previousIndex) {
            this.nextIndex--;
            return true;
        }
        return false;
    }

    public boolean comparePreviousAndUpdateMatchAndNextIndex(final long previousIndex, final long matchIndex) {
        if (previousIndex() == previousIndex) {
            this.matchIndex = matchIndex;
            this.nextIndex = matchIndex + 1;
            return true;
        }
        return false;
    }

    public Follower resetMatchIndex() {
        this.matchIndex = -1;
        return this;
    }
}
