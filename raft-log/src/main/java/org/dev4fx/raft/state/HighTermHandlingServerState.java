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

import org.agrona.DirectBuffer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.sbe.*;
import org.slf4j.Logger;

import java.util.Objects;

public class HighTermHandlingServerState implements ServerState {
    private final ServerState delegateServerState;
    private final PersistentState persistentState;
    private final Logger logger;

    public HighTermHandlingServerState(final ServerState delegateServerState,
                                       final PersistentState persistentState,
                                       final Logger logger) {
        this.delegateServerState = Objects.requireNonNull(delegateServerState);
        this.persistentState = Objects.requireNonNull(persistentState);
        this.logger = Objects.requireNonNull(logger);
    }

    @Override
    public Role role() {
        return delegateServerState.role();
    }

    @Override
    public void onTransition() {
        delegateServerState.onTransition();
    }

    @Override
    public Transition processTick() {
        return delegateServerState.processTick();
    }

    @Override
    public Transition onVoteRequest(final VoteRequestDecoder voteRequestDecoder) {
        if (updateHighTerm(voteRequestDecoder.header())) {
            return Transition.TO_FOLLOWER_REPLAY;
        }
        return delegateServerState.onVoteRequest(voteRequestDecoder);
    }

    @Override
    public Transition onVoteResponse(final VoteResponseDecoder voteResponseDecoder) {
        if (updateHighTerm(voteResponseDecoder.header())) {
            return Transition.TO_FOLLOWER_NO_REPLAY;
        }
        return delegateServerState.onVoteResponse(voteResponseDecoder);
    }

    @Override
    public Transition onAppendRequest(final AppendRequestDecoder appendRequestDecoder) {
        if (updateHighTerm(appendRequestDecoder.header())) {
            return Transition.TO_FOLLOWER_REPLAY;
        }
        return delegateServerState.onAppendRequest(appendRequestDecoder);
    }

    @Override
    public Transition onAppendResponse(final AppendResponseDecoder appendResponseDecoder) {
        if (updateHighTerm(appendResponseDecoder.header())) {
            return Transition.TO_FOLLOWER_NO_REPLAY;
        }
        return delegateServerState.onAppendResponse(appendResponseDecoder);
    }

    @Override
    public Transition onCommandRequest(final DirectBuffer buffer, final int offset, final int length) {
        return delegateServerState.onCommandRequest(buffer, offset, length);
    }

    @Override
    public Transition onTimeoutNow() {
        return delegateServerState.onTimeoutNow();
    }

    private boolean updateHighTerm(final HeaderDecoder headerDecoder) {
        final int messageTerm = headerDecoder.term();
        final int currentTerm = persistentState.currentTerm();
        if (messageTerm > currentTerm) {
            //logger.info("Updating to higher term {} from current {}", messageTerm, currentTerm);
            persistentState.clearVoteForAndSetCurrentTerm(messageTerm);
            return true;
        }
        return false;
    }

}
