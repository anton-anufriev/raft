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
package org.dev4fx.raft.state;

import org.dev4fx.raft.timer.Timer;

import java.util.Objects;

public final class DefaultPeer implements Peer {

    private final int serverId;
    private final Timer heartbeatTimer;

    private long nextIndex;
    private long matchIndex;
    private boolean grantedVote;

    public DefaultPeer(final int serverId,
                       final Timer heartbeatTimer) {
        this.serverId = serverId;
        this.heartbeatTimer = Objects.requireNonNull(heartbeatTimer);
        reset();
    }

    @Override
    public int serverId() {
        return serverId;
    }

    @Override
    public Timer heartbeatTimer() {
        return heartbeatTimer;
    }

    @Override
    public long nextIndex() {
        return nextIndex;
    }

    @Override
    public long matchIndex() {
        return matchIndex;
    }

    @Override
    public Peer nextIndex(final long index) {
        this.nextIndex = index;
        return this;
    }

    @Override
    public boolean grantedVote() {
        return grantedVote;
    }

    @Override
    public Peer setGrantedVote(final boolean grantedVote) {
        this.grantedVote = grantedVote;
        return this;
    }

    @Override
    public boolean comparePreviousAndDecrementNextIndex(final long previousIndex) {
        if (previousIndex() == previousIndex) {
            this.nextIndex--;
            return true;
        }
        return false;
    }

    @Override
    public boolean comparePreviousAndUpdateMatchAndNextIndex(final long previousIndex, final long matchIndex) {
        if (previousIndex() == previousIndex) {
            this.matchIndex = matchIndex;
            this.nextIndex = matchIndex + 1;
            return true;
        }
        return false;
    }

    @Override
    public Peer reset() {
        this.grantedVote = false;
        this.matchIndex = -1;
        this.nextIndex = -1;
        return this;
    }
}
