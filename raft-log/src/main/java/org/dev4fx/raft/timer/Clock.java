package org.dev4fx.raft.timer;

public interface Clock {
    long currentTimeMillis();

    Clock DEFAULT = () -> System.currentTimeMillis();

    static Clock fixed(final long time) {
        return () -> time;
    }
}
