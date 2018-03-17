package org.dev4fx.raft.distributed.map;

import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class FutureResult<V> implements Consumer<V>, Supplier<Future<V>> {

    private final Future<V> future;
    private final CountDownLatch done;
    private V result;

    public FutureResult() {
        this.done = new CountDownLatch(1);

        this.future = new Future<V>() {
            @Override
            public boolean cancel(final boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return done.getCount() == 0;
            }

            @Override
            public V get() throws InterruptedException, ExecutionException {
                done.await();
                return result;
            }

            @Override
            public V get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                if (done.await(timeout, unit)) {
                    return result;
                } else {
                    throw new TimeoutException();
                }
            }
        };
    }

    @Override
    public void accept(final V value) {
        this.result = value;
        done.countDown();
    }

    @Override
    public Future<V> get() {
        return future;
    }
}
