package org.dev4fx.raft.timer;

import java.util.Objects;
import java.util.Random;

public final class Timer {

    private final Random rnd = new Random();
    private final Clock clock;
    private final long minTimeoutMillis;
    private final long maxTimeoutMillis;

    private long timerStartMillis;
    private long timeoutMillis;

    public Timer(final long minTimeoutMillis, final long maxTimeoutMillis) {
        this(Clock.DEFAULT, minTimeoutMillis, maxTimeoutMillis);
    }

    public Timer(final Clock clock, final long minTimeoutMillis, final long maxTimeoutMillis) {
        this.clock = Objects.requireNonNull(clock);
        this.minTimeoutMillis = minTimeoutMillis;
        this.maxTimeoutMillis = maxTimeoutMillis;
        restart();
    }

    /**
     * Starts a new timeout. The timeout is random between minTimeoutMillis and maxTimeoutMillis.
     */
    public void restart() {
        timeoutMillis = newTimeoutMillis(minTimeoutMillis, maxTimeoutMillis);
        reset();
    }

    /**
     * Resets the current timeout to the start without calculating a new
     * random timout.
     */
    public void reset() {
        timerStartMillis = clock.currentTimeMillis();
    }

    /**
     * Forced timeout after receiving a DirectTimeoutNow.
     */
    public void timeoutNow() {
        timeoutMillis = 0;
    }

    /**
     * Returns true if the timeout has elapsed if compared with the current time.
     * @return true if timeout has elapsed
     */
    public boolean hasTimeoutElapsed() {
        return clock.currentTimeMillis() - timerStartMillis >= timeoutMillis;
    }

    private long newTimeoutMillis(final long minTimeoutMillis, final long maxTimeoutMillis) {
        final int diff = (int)(maxTimeoutMillis - minTimeoutMillis);
        long timeout = minTimeoutMillis;
        if (diff > 0) {
            timeout += rnd.nextInt(diff);
        }
        return timeout;
    }
}
