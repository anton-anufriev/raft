package org.dev4fx.raft.log.api;

import org.agrona.DirectBuffer;
import org.dev4fx.raft.state.LogContainment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public interface PersistentState extends Closeable {
    Logger LOGGER = LoggerFactory.getLogger(PersistentState.class);
    long NULL_INDEX = -1;
    int NULL_TERM = 0;
    int NULL_VOTE = -1;


    void append(int term, DirectBuffer buffer, int offset, int length);

    long size();

    default long lastIndex() {
        return size() - 1;
    }

    default int lastTerm() {
        return term(lastIndex());
    }

    int term(long index);

    void wrap(long index, DirectBuffer buffer);

    void truncate(long index);

    int vote();

    void vote(int serverId);

    int currentTerm();

    void currentTerm(int term);

    default int clearVoteAndSetCurrentTerm(final int term) {
        vote(NULL_VOTE);
        currentTerm(term);
        return term;
    }

    default int clearVoteAndIncCurrentTerm() {
        vote(NULL_VOTE);
        final int incTerm = currentTerm() + 1;
        currentTerm(incTerm);
        return incTerm;
    }

    default LogContainment contains(final long index, int termAtIndex) {
        return LogContainment.containmentFor(index, termAtIndex, this);
    }

    default int lastKeyCompareTo(final long index, final int term) {
        final int termCompare = Integer.compare(lastTerm(), term);
        return termCompare == 0 ? Long.compare(lastIndex(), index) : termCompare;
    }


    @Override
    void close();
}
