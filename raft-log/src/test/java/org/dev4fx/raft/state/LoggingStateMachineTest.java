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
    private LoggingStateMachine stateMachine;

    @Before
    public void setUp() throws Exception {
        stateMachine = new LoggingStateMachine(2,
                new CommandRequestDecoder(), new MessageHeaderDecoder(), stringBuilder);
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
        stateMachine.onMessage(directBuffer, 0, headerLength + messageLength);

        //then
        assertThat(stringBuilder).contains("sourceId=" + 4)
                .contains("sequence=" + 15)
                .contains("payload=Command text");
    }

}