package org.dev4fx.raft.process;

import org.agrona.concurrent.IdleStrategy;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public class Process implements Service.Start {

    private final ProcessLoop processLoop;
    private final String name;
    private final Thread thread;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final long gracefulShutdownTimeout;
    private final TimeUnit gracefulShutdownTimeunit;
    private final AtomicLong gracefulShutdownMaxTime = new AtomicLong();


    public Process(final String name,
                   final Runnable onStartHandler,
                   final Runnable onStopHandler,
                   final IdleStrategy idleStrategy,
                   final BiConsumer<? super String, ? super Exception> exceptionHandler,
                   final long gracefulShutdownTimeout,
                   final TimeUnit gracefulShutdownTimeunit,
                   final ProcessStep... steps) {
        this.gracefulShutdownTimeunit = Objects.requireNonNull(gracefulShutdownTimeunit);
        this.gracefulShutdownTimeout = gracefulShutdownTimeout;

        this.processLoop = new ProcessLoop(name,
                onStartHandler,
                onStopHandler,
                stopping::get,
                () -> System.currentTimeMillis() > gracefulShutdownMaxTime.get(),
                idleStrategy,
                exceptionHandler,
                steps);
        this.thread = new Thread(processLoop, name);
        this.name = name;
    }

    @Override
    public Service.Stop start() {
        thread.start();

        return new Service.Stop() {
            @Override
            public void stop() {
                final long gracefulShutdownTimeoutMillis = gracefulShutdownTimeunit.toMillis(gracefulShutdownTimeout);
                gracefulShutdownMaxTime.set(System.currentTimeMillis() + gracefulShutdownTimeoutMillis);
                stopping.set(true);
                try {
                    thread.join(gracefulShutdownTimeoutMillis);
                    thread.interrupt();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for service thread " + name + " to stop", e);
                }
            }

            @Override
            public void awaitShutdown() {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for service thread " + name + " to shutdown", e);
                }
            }
        };
    }
}
