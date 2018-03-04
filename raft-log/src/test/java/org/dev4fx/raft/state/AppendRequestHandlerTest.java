package org.dev4fx.raft.state;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.dev4fx.raft.log.api.PersistentState;
import org.dev4fx.raft.sbe.*;
import org.dev4fx.raft.timer.Timer;
import org.dev4fx.raft.transport.Publisher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AppendRequestHandlerTest {
    @Mock
    private PersistentState persistentState;
    @Mock
    private VolatileState volatileState;
    @Mock
    private Timer electionTimeout;
    private AppendResponseEncoder appendResponseEncoder = new AppendResponseEncoder();
    private MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private MutableDirectBuffer encoderBuffer = new UnsafeBuffer(ByteBuffer.allocate(512));


    @Mock
    private Publisher publisher;
    private int serverId = 1;
    @Mock
    private AppendRequestDecoder appendRequestDecoder;
    @Mock
    private HeaderDecoder headerDecoder;
    @Mock
    private LogKeyDecoder prevLogKeyDecoder;
    @Mock
    private AppendRequestDecoder.LogEntriesDecoder logEntriesDecoder;
    @Mock
    private Logger logger;
    
    private AppendRequestHandler appendRequestHandler;

    @Before
    public void setUp() throws Exception {
        when(appendRequestDecoder.header()).thenReturn(headerDecoder);
        when(appendRequestDecoder.prevLogKey()).thenReturn(prevLogKeyDecoder);

        appendRequestHandler = new AppendRequestHandler(persistentState, volatileState,
                electionTimeout, messageHeaderEncoder, appendResponseEncoder, encoderBuffer,
                publisher, serverId);
    }

    @Test
    public void apply_appends_logEntry_when_terms_are_equal_and_prevLogEntry_is_contained_and_nextLogEntry_is_not() throws Exception {
        //given
        final int requestTerm = 3;
        final int currentTerm = 3;
        final int leaderId = 2;
        final long prevLogIndex = 10;
        final int prevLogTerm = 2;
        final long leaderCommitIndex = 12;


        final long nextLogIndex = prevLogIndex + 1;
        final int nextLogTerm = 2;
        final int nextCommandLength = 20;
        final int nextCommandLimit = 16;
        final int nextCommandOffset = nextCommandLimit + AppendRequestDecoder.LogEntriesDecoder.commandHeaderLength();
        final UnsafeBuffer nextCommandBuffer = new UnsafeBuffer();

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(leaderId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(prevLogKeyDecoder.index()).thenReturn(prevLogIndex);
        when(prevLogKeyDecoder.term()).thenReturn(prevLogTerm);
        when(appendRequestDecoder.commitLogIndex()).thenReturn(leaderCommitIndex);
        when(volatileState.commitIndex()).thenReturn(-1L);

        when(persistentState.contains(prevLogIndex, prevLogTerm)).thenReturn(LogContainment.IN);
        when(persistentState.contains(nextLogIndex, nextLogTerm)).thenReturn(LogContainment.OUT);

        when(appendRequestDecoder.logEntries()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.iterator()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.hasNext()).thenReturn(true, false);
        when(logEntriesDecoder.next()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.term()).thenReturn(nextLogTerm);
        when(logEntriesDecoder.commandLength()).thenReturn(nextCommandLength);
        when(appendRequestDecoder.limit()).thenReturn(nextCommandLimit);
        when(appendRequestDecoder.buffer()).thenReturn(nextCommandBuffer);

        //when
        final Transition transition = appendRequestHandler.apply(appendRequestDecoder, logger);
        assertThat(transition).isEqualTo(transition);

        final StringBuilder appendResponse = new StringBuilder();
        appendResponseEncoder.appendTo(appendResponse);

        assertThat(appendResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + leaderId)
                .contains("term=" + currentTerm)
                .contains("matchLogIndex=" + nextLogIndex)
                .contains("prevLogIndex="+prevLogIndex)
                .contains("successful=T");


        verify(persistentState).append(nextLogTerm, nextCommandBuffer, nextCommandOffset, nextCommandLength);
        verify(volatileState).commitIndex(nextLogIndex);
        verify(electionTimeout).restart();
    }


    @Test
    public void apply_skips_logEntry_when_terms_are_equal_and_prevLogEntry_and_next_logEntry_are_contained_in_server_log() throws Exception {
        //given
        final int requestTerm = 3;
        final int currentTerm = 3;
        final int leaderId = 2;
        final long prevLogIndex = 10;
        final int prevLogTerm = 2;
        final long leaderCommitIndex = 12;


        final long nextLogIndex = prevLogIndex + 1;
        final int nextLogTerm = 2;
        final int nextCommandLength = 20;
        final int nextCommandLimit = 16;
        final int nextCommandOffset = nextCommandLimit + AppendRequestDecoder.LogEntriesDecoder.commandHeaderLength();
        final UnsafeBuffer nextCommandBuffer = new UnsafeBuffer();

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(leaderId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(prevLogKeyDecoder.index()).thenReturn(prevLogIndex);
        when(prevLogKeyDecoder.term()).thenReturn(prevLogTerm);
        when(appendRequestDecoder.commitLogIndex()).thenReturn(leaderCommitIndex);
        when(volatileState.commitIndex()).thenReturn(-1L);

        when(persistentState.contains(prevLogIndex, prevLogTerm)).thenReturn(LogContainment.IN);
        when(persistentState.contains(nextLogIndex, nextLogTerm)).thenReturn(LogContainment.IN);

        when(appendRequestDecoder.logEntries()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.iterator()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.hasNext()).thenReturn(true, false);
        when(logEntriesDecoder.next()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.term()).thenReturn(nextLogTerm);

        //when
        final Transition transition = appendRequestHandler.apply(appendRequestDecoder, logger);
        assertThat(transition).isEqualTo(transition);

        final StringBuilder appendResponse = new StringBuilder();
        appendResponseEncoder.appendTo(appendResponse);

        assertThat(appendResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + leaderId)
                .contains("term=" + currentTerm)
                .contains("matchLogIndex=" + nextLogIndex)
                .contains("prevLogIndex="+prevLogIndex)
                .contains("successful=T");


        verify(persistentState, times(0)).append(nextLogTerm, nextCommandBuffer, nextCommandOffset, nextCommandLength);
        verify(volatileState).commitIndex(nextLogIndex);
        verify(electionTimeout).restart();
    }

    @Test(expected = IllegalStateException.class)
    public void apply_throws_exception_logEntry_when_terms_are_equal_and_prevLogEntry_is_contained_and_next_log_index_conflicts() throws Exception {
        //given
        final int requestTerm = 3;
        final int currentTerm = 3;
        final int leaderId = 2;
        final long prevLogIndex = 10;
        final int prevLogTerm = 2;
        final long leaderCommitIndex = 12;


        final long nextLogIndex = prevLogIndex + 1;
        final int nextLogTerm = 2;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(leaderId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(prevLogKeyDecoder.index()).thenReturn(prevLogIndex);
        when(prevLogKeyDecoder.term()).thenReturn(prevLogTerm);
        when(appendRequestDecoder.commitLogIndex()).thenReturn(leaderCommitIndex);
        when(volatileState.commitIndex()).thenReturn(-1L);

        when(persistentState.contains(prevLogIndex, prevLogTerm)).thenReturn(LogContainment.IN);
        when(persistentState.contains(nextLogIndex, nextLogTerm)).thenReturn(LogContainment.CONFLICT);

        when(appendRequestDecoder.logEntries()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.iterator()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.hasNext()).thenReturn(true, false);
        when(logEntriesDecoder.next()).thenReturn(logEntriesDecoder);
        when(logEntriesDecoder.term()).thenReturn(nextLogTerm);

        //when
        final Transition transition = appendRequestHandler.apply(appendRequestDecoder, logger);
        assertThat(transition).isEqualTo(transition);
    }

    @Test
    public void apply_unsuccessful_when_terms_are_equal_and_prevLogEntry_is_not_contained() throws Exception {
        //given
        final int requestTerm = 3;
        final int currentTerm = 3;
        final int leaderId = 2;
        final long prevLogIndex = 10;
        final int prevLogTerm = 2;
        final long leaderCommitIndex = 12;


        final long nextLogIndex = prevLogIndex + 1;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(leaderId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(prevLogKeyDecoder.index()).thenReturn(prevLogIndex);
        when(prevLogKeyDecoder.term()).thenReturn(prevLogTerm);
        when(appendRequestDecoder.commitLogIndex()).thenReturn(leaderCommitIndex);
        when(volatileState.commitIndex()).thenReturn(-1L);

        when(persistentState.contains(prevLogIndex, prevLogTerm)).thenReturn(LogContainment.OUT);

        //when
        final Transition transition = appendRequestHandler.apply(appendRequestDecoder, logger);
        assertThat(transition).isEqualTo(transition);

        final StringBuilder appendResponse = new StringBuilder();
        appendResponseEncoder.appendTo(appendResponse);

        assertThat(appendResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + leaderId)
                .contains("term=" + currentTerm)
                .contains("matchLogIndex=" + -1)
                .contains("prevLogIndex="+prevLogIndex)
                .contains("successful=F");


        verify(volatileState, times(0)).commitIndex(nextLogIndex);
        verify(electionTimeout).restart();
    }

    @Test
    public void apply_unsuccessful_truncates_log_to_prevLogIndex_when_terms_are_equal_and_prevLogEntry_is_conflicts() throws Exception {
        //given
        final int requestTerm = 3;
        final int currentTerm = 3;
        final int leaderId = 2;
        final long prevLogIndex = 10;
        final int prevLogTerm = 2;
        final long leaderCommitIndex = 12;


        final long nextLogIndex = prevLogIndex + 1;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(leaderId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(prevLogKeyDecoder.index()).thenReturn(prevLogIndex);
        when(prevLogKeyDecoder.term()).thenReturn(prevLogTerm);
        when(appendRequestDecoder.commitLogIndex()).thenReturn(leaderCommitIndex);
        when(volatileState.commitIndex()).thenReturn(-1L);

        when(persistentState.contains(prevLogIndex, prevLogTerm)).thenReturn(LogContainment.CONFLICT);

        //when
        final Transition transition = appendRequestHandler.apply(appendRequestDecoder, logger);
        assertThat(transition).isEqualTo(transition);

        final StringBuilder appendResponse = new StringBuilder();
        appendResponseEncoder.appendTo(appendResponse);

        assertThat(appendResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + leaderId)
                .contains("term=" + currentTerm)
                .contains("matchLogIndex=" + -1)
                .contains("prevLogIndex="+prevLogIndex)
                .contains("successful=F");


        verify(volatileState, times(0)).commitIndex(nextLogIndex);
        verify(persistentState).truncate(prevLogIndex);
        verify(electionTimeout).restart();
    }

    @Test
    public void apply_unsuccessful_when_request_term_less_than_current_term() throws Exception {
        //given
        final int requestTerm = 2;
        final int currentTerm = 3;
        final int leaderId = 2;
        final long prevLogIndex = 10;
        final int prevLogTerm = 2;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(leaderId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(prevLogKeyDecoder.index()).thenReturn(prevLogIndex);
        when(prevLogKeyDecoder.term()).thenReturn(prevLogTerm);

        //when
        final Transition transition = appendRequestHandler.apply(appendRequestDecoder, logger);
        assertThat(transition).isEqualTo(transition);

        final StringBuilder appendResponse = new StringBuilder();
        appendResponseEncoder.appendTo(appendResponse);

        assertThat(appendResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + leaderId)
                .contains("term=" + currentTerm)
                .contains("matchLogIndex=" + -1)
                .contains("prevLogIndex="+prevLogIndex)
                .contains("successful=F");

        verify(electionTimeout, times(0)).restart();
    }
}