package org.dev4fx.raft.config;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class IdGenerator implements LongSupplier {
    private final AtomicLong current;

    public IdGenerator(final LongSupplier seedSupplier) {
        current = new AtomicLong(seedSupplier.getAsLong());
    }

    @Override
    public long getAsLong() {
        return current.incrementAndGet();
    }
}
