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
package org.dev4fx.raft.distributed.map.command;

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
