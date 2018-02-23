package org.dev4fx.raft.state;

import org.agrona.DirectBuffer;
import org.dev4fx.raft.sbe.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingStateMachine implements MessageHandler {
    private final static Logger LOGGER = LoggerFactory.getLogger("SM");

    private final int serverId;
    private final CommandRequestDecoder commandRequestDecoder = new CommandRequestDecoder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final StringBuilder stringBuilder = new StringBuilder();

    public LoggingStateMachine(final int serverId) {
        this.serverId = serverId;
    }

    @Override
    public void onMessage(final DirectBuffer source, final int offset, final int length) {
        stringBuilder.setLength(0);
        messageHeaderDecoder.wrap(source, offset);
        final int templateId = messageHeaderDecoder.templateId();
        final int headerLenght = messageHeaderDecoder.encodedLength();
        switch (templateId) {
            case CommandRequestDecoder.TEMPLATE_ID :
                commandRequestDecoder.wrap(source,headerLenght + offset,
                        CommandRequestDecoder.BLOCK_LENGTH,
                        CommandRequestDecoder.SCHEMA_VERSION);
                stringBuilder
                        .append("Command: sourceId=")
                        .append(commandRequestDecoder.sourceId())
                        .append(", sequence=")
                        .append(commandRequestDecoder.sequence())
                        .append(", payload=");

                final VarDataEncodingDecoder payloadDecoder = commandRequestDecoder.payload();
                stringBuilder.append(payloadDecoder.buffer().getStringAscii(payloadDecoder.offset()));
                LOGGER.info("{}", stringBuilder);

                break;
        }
    }
}
