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

import org.agrona.MutableDirectBuffer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.sbe.*;
import org.dev4fx.raft.timer.Timer;
import org.dev4fx.raft.transport.Publisher;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.function.BiFunction;

public class AppendRequestHandler implements BiFunction<AppendRequestDecoder, Logger, Transition> {
    private final PersistentState persistentState;
    private final VolatileState volatileState;
    private final Timer electionTimeout;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final AppendResponseEncoder appendResponseEncoder;
    private final MutableDirectBuffer encoderBuffer;
    private final Publisher publisher;
    private final int serverId;

    public AppendRequestHandler(final PersistentState persistentState,
                                final VolatileState volatileState,
                                final Timer electionTimeout,
                                final MessageHeaderEncoder messageHeaderEncoder,
                                final AppendResponseEncoder appendResponseEncoder,
                                final MutableDirectBuffer encoderBuffer,
                                final Publisher publisher,
                                final int serverId) {
        this.persistentState = Objects.requireNonNull(persistentState);
        this.volatileState = Objects.requireNonNull(volatileState);
        this.electionTimeout = Objects.requireNonNull(electionTimeout);
        this.messageHeaderEncoder = Objects.requireNonNull(messageHeaderEncoder);
        this.appendResponseEncoder = Objects.requireNonNull(appendResponseEncoder);
        this.encoderBuffer = Objects.requireNonNull(encoderBuffer);
        this.publisher = Objects.requireNonNull(publisher);
        this.serverId = serverId;
    }

    @Override
    public Transition apply(final AppendRequestDecoder appendRequestDecoder, final Logger logger) {
        final HeaderDecoder header = appendRequestDecoder.header();
        final int appendRequestTerm = header.term();
        final int leaderId = header.sourceId();
        final int currentTerm = persistentState.currentTerm();

        final LogKeyDecoder prevLogKeyDecoder = appendRequestDecoder.prevLogKey();
        final long requestPrevIndex = prevLogKeyDecoder.index();
        final int requestPrevTermAtIndex = prevLogKeyDecoder.term();

        final boolean successful;
        long matchLogIndex = -1;

        if (appendRequestTerm < currentTerm) {
            successful = false;
        } else {
            final long leaderCommitIndex = appendRequestDecoder.commitLogIndex();

            final LogContainment containment = persistentState.contains(requestPrevIndex, requestPrevTermAtIndex);

            switch (containment) {
                case IN:
                    matchLogIndex = appendToLog(requestPrevIndex, appendRequestDecoder, logger);

                    //From paper:  If leaderCommit > commitIndex, set commitIndex =
                    // min(leaderCommit, index of last new entry).
                    // I think, "index of last new entry" implies not empty persistentState entries.
                    if (leaderCommitIndex > volatileState.commitIndex()) {
                        volatileState.commitIndex(Long.min(leaderCommitIndex, matchLogIndex));
                    }

                    successful = true;
                    break;
                case OUT:
                    successful = false;
                    break;
                case CONFLICT:
                    persistentState.truncate(requestPrevIndex);
                    successful = false;
                    break;
                default:
                    throw new IllegalStateException("Unknown LogContainment " + containment);
            }
            electionTimeout.restart();
        }

        final int headerLength = messageHeaderEncoder.wrap(encoderBuffer, 0)
                .schemaId(AppendResponseEncoder.SCHEMA_ID)
                .version(AppendResponseEncoder.SCHEMA_VERSION)
                .blockLength(AppendResponseEncoder.BLOCK_LENGTH)
                .templateId(AppendResponseEncoder.TEMPLATE_ID)
                .encodedLength();

        appendResponseEncoder.wrap(encoderBuffer, headerLength)
                .header()
                .destinationId(leaderId)
                .sourceId(serverId)
                .term(currentTerm);

        appendResponseEncoder
                .matchLogIndex(matchLogIndex)
                .prevLogIndex(requestPrevIndex)
                .successful(successful ? BooleanType.T : BooleanType.F);

        publisher.publish(encoderBuffer, 0, headerLength + appendResponseEncoder.encodedLength());
        return Transition.STEADY;
    }

    private long appendToLog(final long prevLogIndex, final AppendRequestDecoder appendRequestDecoder, final Logger logger) {
        long nextIndex = prevLogIndex;

        for (final AppendRequestDecoder.LogEntriesDecoder logEntryDecoder : appendRequestDecoder.logEntries()) {
            nextIndex++;
            final int nextTermAtIndex = logEntryDecoder.term();
            final int commandHeaderLength = AppendRequestDecoder.LogEntriesDecoder.commandHeaderLength();
            final int offset = appendRequestDecoder.limit() + commandHeaderLength;
            final int length = logEntryDecoder.commandLength();

            final LogContainment containment = persistentState.contains(nextIndex, nextTermAtIndex);
            switch (containment) {
                case OUT:
                    persistentState.append(nextTermAtIndex,
                            appendRequestDecoder.buffer(),
                            offset,
                            length);
                    //logger.info("Appended index {}, term {}, offset {}, length {}", nextIndex, nextTermAtIndex, offset, length);
                    break;
                case IN:
                    //logger.info("Skipped index {}, term {}", nextIndex, nextTermAtIndex);
                    break;
                default:
                    throw new IllegalStateException("Should not be in conflict");
            }
            appendRequestDecoder.limit(offset + length);
        }
        return nextIndex;
    }

}
