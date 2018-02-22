package org.dev4fx.raft.transport;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.dev4fx.raft.process.ProcessStep;
import org.dev4fx.raft.state.MessageHandler;

import java.util.Objects;

public class AeronSubscriptionPoller implements ProcessStep {
    private Subscription subscription;
    private FragmentHandler fragmentHandler;
    private final int messageLimit;

    public AeronSubscriptionPoller(final Subscription subscription,
                                   final MessageHandler messageHandler,
                                   final int messageLimit) {
        this.subscription = Objects.requireNonNull(subscription);
        this.messageLimit = messageLimit;
        Objects.requireNonNull(messageHandler);
        this.fragmentHandler = (buffer, offset, length, header) -> messageHandler.onMessage(buffer, offset, length);
    }

    @Override
    public boolean execute() {
        return subscription.poll(fragmentHandler, messageLimit) > 0;
    }
}
