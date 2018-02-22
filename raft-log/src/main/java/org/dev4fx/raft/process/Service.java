package org.dev4fx.raft.process;

public interface Service {
    interface Start {
        Stop start();
    }
    interface Stop {
        void stop();
        void awaitShutdown();
    }
}
