package org.dev4fx.raft.transport;

import org.agrona.DirectBuffer;
import org.dev4fx.raft.sbe.*;
import org.slf4j.Logger;

import java.util.Objects;

public class LoggingPublisher implements Publisher {
    private final Publisher delegatePublisher;
    private final Logger logger;
    private final MessageHeaderDecoder messageHeaderDecoder;
    private final VoteRequestDecoder voteRequestDecoder;
    private final VoteResponseDecoder voteResponseDecoder;
    private final AppendRequestDecoder appendRequestDecoder;
    private final AppendResponseDecoder appendResponseDecoder;
    private final CommandRequestDecoder commandRequestDecoder;
    private final StringBuilder stringBuilder;


    public LoggingPublisher(final Publisher delegatePublisher,
                            final Logger logger,
                            final MessageHeaderDecoder messageHeaderDecoder,
                            final VoteRequestDecoder voteRequestDecoder,
                            final VoteResponseDecoder voteResponseDecoder,
                            final AppendRequestDecoder appendRequestDecoder,
                            final AppendResponseDecoder appendResponseDecoder,
                            final CommandRequestDecoder commandRequestDecoder,
                            final StringBuilder stringBuilder) {
        this.delegatePublisher = Objects.requireNonNull(delegatePublisher);
        this.logger = Objects.requireNonNull(logger);
        this.messageHeaderDecoder = Objects.requireNonNull(messageHeaderDecoder);
        this.voteRequestDecoder = Objects.requireNonNull(voteRequestDecoder);
        this.voteResponseDecoder = Objects.requireNonNull(voteResponseDecoder);
        this.appendRequestDecoder = Objects.requireNonNull(appendRequestDecoder);
        this.appendResponseDecoder = Objects.requireNonNull(appendResponseDecoder);
        this.commandRequestDecoder = Objects.requireNonNull(commandRequestDecoder);
        this.stringBuilder = Objects.requireNonNull(stringBuilder);
    }

    @Override
    public boolean publish(final DirectBuffer buffer, final int offset, final int length) {
        stringBuilder.setLength(0);
        messageHeaderDecoder.wrap(buffer, offset);
        final int templateId = messageHeaderDecoder.templateId();
        final int headerLenght = messageHeaderDecoder.encodedLength();
        switch (templateId) {
            case VoteRequestDecoder.TEMPLATE_ID :
                voteRequestDecoder.wrap(buffer,headerLenght + offset,
                        VoteRequestDecoder.BLOCK_LENGTH,
                        VoteRequestDecoder.SCHEMA_VERSION);
                voteRequestDecoder.appendTo(stringBuilder);
                break;
            case VoteResponseDecoder.TEMPLATE_ID :
                voteResponseDecoder.wrap(buffer,headerLenght + offset,
                        VoteResponseDecoder.BLOCK_LENGTH,
                        VoteResponseDecoder.SCHEMA_VERSION);
                voteResponseDecoder.appendTo(stringBuilder);
                break;
            case AppendRequestDecoder.TEMPLATE_ID :
                appendRequestDecoder.wrap(buffer,headerLenght + offset,
                        AppendRequestDecoder.BLOCK_LENGTH,
                        AppendRequestDecoder.SCHEMA_VERSION);
                appendRequestDecoder.appendTo(stringBuilder);
                break;
            case AppendResponseDecoder.TEMPLATE_ID :
                appendResponseDecoder.wrap(buffer,headerLenght + offset,
                        AppendResponseDecoder.BLOCK_LENGTH,
                        AppendResponseDecoder.SCHEMA_VERSION);
                appendResponseDecoder.appendTo(stringBuilder);
                break;
            case CommandRequestDecoder.TEMPLATE_ID :
                commandRequestDecoder.wrap(buffer,headerLenght + offset,
                        CommandRequestDecoder.BLOCK_LENGTH,
                        CommandRequestDecoder.SCHEMA_VERSION);
                commandRequestDecoder.appendTo(stringBuilder);
                break;
        }
        logger.info("{}", stringBuilder);

        return delegatePublisher.publish(buffer, offset, length);
    }
}
