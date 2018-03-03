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
package org.dev4fx.raft.log.api;

import org.agrona.DirectBuffer;
import org.dev4fx.raft.state.LogContainment;

import java.io.Closeable;

public interface PersistentState extends Closeable {
    long NULL_INDEX = -1;
    int NULL_TERM = 0;
    int NOT_VOTED_YET = -1;


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

    int votedFor();

    void votedFor(int serverId);

    default boolean hasNotVotedYet() {
        return votedFor() == NOT_VOTED_YET;
    }

    int currentTerm();

    void currentTerm(int term);

    default int clearVoteForAndSetCurrentTerm(final int term) {
        votedFor(NOT_VOTED_YET);
        currentTerm(term);
        return term;
    }

    default int clearVoteForAndIncCurrentTerm() {
        votedFor(NOT_VOTED_YET);
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
