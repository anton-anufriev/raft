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

import org.agrona.concurrent.UnsafeBuffer;
import org.dev4fx.raft.sbe.CommandRequestDecoder;
import org.dev4fx.raft.sbe.CommandRequestEncoder;
import org.dev4fx.raft.sbe.MessageHeaderDecoder;
import org.dev4fx.raft.sbe.MessageHeaderEncoder;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class LoggingStateMachineTest {

    private UnsafeBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocate(512));
    private StringBuilder stringBuilder = new StringBuilder();
    private MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private CommandRequestEncoder encoder = new CommandRequestEncoder();
    private StateMachine stateMachine;
    private CommandMessageHandler commandMessageHandler;

    @Before
    public void setUp() throws Exception {
        stateMachine = new LoggingStateMachine(2, stringBuilder);
        commandMessageHandler = new CommandMessageHandler(stateMachine);

    }

    @Test
    public void onMessage() throws Exception {
        //given
        final byte[] commandBytes = "Command text".getBytes();
        final int headerLength = messageHeaderEncoder.wrap(directBuffer, 0)
                .blockLength(CommandRequestEncoder.BLOCK_LENGTH)
                .templateId(CommandRequestEncoder.TEMPLATE_ID)
                .version(CommandRequestEncoder.SCHEMA_VERSION)
                .schemaId(CommandRequestEncoder.SCHEMA_ID)
                .encodedLength();

        encoder.wrap(directBuffer, headerLength);
        final int messageLength = encoder.sourceId(4)
                .sequence(15L)
                .putPayload(commandBytes, 0, commandBytes.length)
                .encodedLength();
        //when
        commandMessageHandler.onMessage(directBuffer, 0, headerLength + messageLength);

        //then
        assertThat(stringBuilder).contains("sourceId=" + 4)
                .contains("sequence=" + 15)
                .contains("payload=Command text");
    }

}