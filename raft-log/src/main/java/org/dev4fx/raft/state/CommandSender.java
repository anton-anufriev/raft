package org.dev4fx.raft.state;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.dev4fx.raft.sbe.*;
import org.dev4fx.raft.transport.Publisher;

import java.util.Objects;

public class CommandSender {
    private final Publisher publisher;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final CommandRequestEncoder commandRequestEncoder;
    private final MutableDirectBuffer encoderBuffer;


    public CommandSender(final Publisher publisher,
                         final MessageHeaderEncoder messageHeaderEncoder,
                         final CommandRequestEncoder commandRequestEncoder,
                         final MutableDirectBuffer encoderBuffer) {
        this.publisher = Objects.requireNonNull(publisher);
        this.messageHeaderEncoder = Objects.requireNonNull(messageHeaderEncoder);
        this.commandRequestEncoder = Objects.requireNonNull(commandRequestEncoder);
        this.encoderBuffer = Objects.requireNonNull(encoderBuffer);
    }

    public boolean publish(final int sourceId, final long sequence, final DirectBuffer buffer, final int offset, final int length) {
        final int headerLength = messageHeaderEncoder.wrap(encoderBuffer, 0)
                .schemaId(CommandRequestEncoder.SCHEMA_ID)
                .version(CommandRequestEncoder.SCHEMA_VERSION)
                .blockLength(CommandRequestEncoder.BLOCK_LENGTH)
                .templateId(CommandRequestEncoder.TEMPLATE_ID)
                .encodedLength();

        int messageLength = commandRequestEncoder.wrap(encoderBuffer, headerLength)
                .sourceId(sourceId)
                .sequence(sequence)
                .putPayload(buffer, offset, length)
                .encodedLength();

        return publisher.publish(encoderBuffer, 0, headerLength + messageLength);
    }
}
