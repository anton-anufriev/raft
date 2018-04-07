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
import org.agrona.concurrent.UnsafeBuffer;
import org.dev4fx.raft.sbe.CommandRequestEncoder;
import org.dev4fx.raft.sbe.MessageHeaderEncoder;
import org.dev4fx.raft.transport.Publisher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DefaultCommandPublisherTest {
    @Mock
    private Publisher publisher;
    private MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private CommandRequestEncoder commandRequestEncoder = new CommandRequestEncoder();
    private MutableDirectBuffer encoderBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(512));

    private CommandPublisher commandPublisher;

    @Before
    public void setUp() throws Exception {
        commandPublisher = new DefaultCommandPublisher(publisher, messageHeaderEncoder,
                commandRequestEncoder, encoderBuffer);
    }

    @Test
    public void publish() throws Exception {
        final UnsafeBuffer command = new UnsafeBuffer("test command".getBytes());
        commandPublisher.publish(12, 234, command, 0, command.capacity());

        verify(publisher).publish(encoderBuffer, 0, 34);

        final StringBuilder commandMessage = new StringBuilder();
        commandRequestEncoder.appendTo(commandMessage);

        System.out.println(commandMessage);

        assertThat(commandMessage)
                .contains("sourceId=" + 12)
                .contains("sequence=" + 234)
                .contains("payload=" + command.capacity() + " bytes of raw data");
    }
}