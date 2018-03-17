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
import org.agrona.MutableDirectBuffer;
import org.dev4fx.raft.sbe.*;
import org.dev4fx.raft.transport.Publisher;

import java.util.Objects;

public class DefaultCommandPublisher implements CommandPublisher {
    private final Publisher publisher;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final CommandRequestEncoder commandRequestEncoder;
    private final MutableDirectBuffer encoderBuffer;


    public DefaultCommandPublisher(final Publisher publisher,
                                   final MessageHeaderEncoder messageHeaderEncoder,
                                   final CommandRequestEncoder commandRequestEncoder,
                                   final MutableDirectBuffer encoderBuffer) {
        this.publisher = Objects.requireNonNull(publisher);
        this.messageHeaderEncoder = Objects.requireNonNull(messageHeaderEncoder);
        this.commandRequestEncoder = Objects.requireNonNull(commandRequestEncoder);
        this.encoderBuffer = Objects.requireNonNull(encoderBuffer);
    }

    @Override
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
