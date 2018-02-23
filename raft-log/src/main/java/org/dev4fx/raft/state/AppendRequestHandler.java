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
        final int appendRequestTerm = appendRequestDecoder.header().term();
        final int currentTerm = persistentState.currentTerm();

        final int leaderId = appendRequestDecoder.header().sourceId();

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
                    final AppendRequestDecoder.LogEntriesDecoder logEntries = appendRequestDecoder.logEntries();

                    matchLogIndex = appendToLog(requestPrevIndex, logEntries);

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

    private long appendToLog(final long prevLogIndex, final AppendRequestDecoder.LogEntriesDecoder logEntries) {
        long nextIndex = prevLogIndex;

        for (final AppendRequestDecoder.LogEntriesDecoder logEntryDecoder : logEntries) {
            nextIndex++;
            final int nextTermAtIndex = logEntryDecoder.term();
            final VarDataEncodingDecoder commandDecoder = logEntryDecoder.command();

            final LogContainment containment = persistentState.contains(nextIndex, nextTermAtIndex);
            switch (containment) {
                case OUT:
                    persistentState.append(nextTermAtIndex,
                            commandDecoder.buffer(),
                            commandDecoder.offset() + VarDataEncodingDecoder.varDataEncodingOffset(),
                            (int) commandDecoder.length());
                    break;
                case IN:
                    break;
                default:
                    throw new RuntimeException("Should not be in conflict");
            }
        }
        return nextIndex;
    }

}
