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
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class VoteRequestHandlerTest {
    @Mock
    private PersistentState persistentState;
    @Mock
    private Timer electionTimer;

    private MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private VoteResponseEncoder voteResponseEncoder = new VoteResponseEncoder();
    private MutableDirectBuffer encoderBuffer = new UnsafeBuffer(ByteBuffer.allocate(512));

    @Mock
    private Publisher publisher;
    private int serverId = 1;

    @Mock
    private VoteRequestDecoder voteRequestDecoder;
    @Mock
    private HeaderDecoder headerDecoder;
    @Mock
    private LogKeyDecoder lastLogKeyDecoder;
    @Mock
    private Logger logger;


    private VoteRequestHandler voteRequestHandler;

    @Before
    public void setUp() throws Exception {
        when(voteRequestDecoder.header()).thenReturn(headerDecoder);
        when(voteRequestDecoder.lastLogKey()).thenReturn(lastLogKeyDecoder);
        voteRequestHandler = new VoteRequestHandler(persistentState, electionTimer, messageHeaderEncoder, voteResponseEncoder, encoderBuffer, publisher, serverId);
    }

    @Test
    public void apply_grants_vote_and_transitions_to_follower_not_replaying_the_request_when_terms_are_equal_and_log_is_less_that_request_log() throws Exception {
        //given
        final int requestTerm = 5;
        final int currentTerm = 5;
        final int candidateId = 2;
        final long lastLogIndex = 10;
        final int lastLogTerm = 4;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(candidateId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(lastLogKeyDecoder.term()).thenReturn(lastLogTerm);
        when(lastLogKeyDecoder.index()).thenReturn(lastLogIndex);

        when(persistentState.lastKeyCompareTo(lastLogIndex, lastLogTerm)).thenReturn(-1);
        when(persistentState.hasNotVotedYet()).thenReturn(true);

        //when
        final Transition transition = voteRequestHandler.apply(voteRequestDecoder, logger);

        assertThat(transition).isEqualTo(Transition.TO_FOLLOWER_NO_REPLAY);

        final StringBuilder voteResponse = new StringBuilder();
        voteResponseEncoder.appendTo(voteResponse);

        assertThat(voteResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + candidateId)
                .contains("term=" + currentTerm)
                .contains("voteGranted=" + BooleanType.T);
    }

    @Test
    public void apply_grants_vote_and_transitions_to_follower_not_replaying_the_request_when_terms_are_equal_and_log_is_equal_to_request_log() throws Exception {
        //given
        final int requestTerm = 5;
        final int currentTerm = 5;
        final int candidateId = 2;
        final long lastLogIndex = 10;
        final int lastLogTerm = 4;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(candidateId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(lastLogKeyDecoder.term()).thenReturn(lastLogTerm);
        when(lastLogKeyDecoder.index()).thenReturn(lastLogIndex);

        when(persistentState.lastKeyCompareTo(lastLogIndex, lastLogTerm)).thenReturn(0);
        when(persistentState.hasNotVotedYet()).thenReturn(true);

        //when
        final Transition transition = voteRequestHandler.apply(voteRequestDecoder, logger);

        assertThat(transition).isEqualTo(Transition.TO_FOLLOWER_NO_REPLAY);

        final StringBuilder voteResponse = new StringBuilder();
        voteResponseEncoder.appendTo(voteResponse);

        assertThat(voteResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + candidateId)
                .contains("term=" + currentTerm)
                .contains("voteGranted=" + BooleanType.T);
    }

    @Test
    public void apply_rejects_vote_when_terms_are_equal_and_log_is_less_that_request_log_and_server_has_already_voted_another_leader() throws Exception {
        //given
        final int requestTerm = 5;
        final int currentTerm = 5;
        final int candidateId = 2;
        final int anotherServer = 0;
        final long lastLogIndex = 10;
        final int lastLogTerm = 4;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(candidateId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(lastLogKeyDecoder.term()).thenReturn(lastLogTerm);
        when(lastLogKeyDecoder.index()).thenReturn(lastLogIndex);

        when(persistentState.lastKeyCompareTo(lastLogIndex, lastLogTerm)).thenReturn(-1);
        when(persistentState.hasNotVotedYet()).thenReturn(false);
        when(persistentState.votedFor()).thenReturn(anotherServer);

        //when
        final Transition transition = voteRequestHandler.apply(voteRequestDecoder, logger);

        assertThat(transition).isEqualTo(Transition.STEADY);

        final StringBuilder voteResponse = new StringBuilder();
        voteResponseEncoder.appendTo(voteResponse);

        assertThat(voteResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + candidateId)
                .contains("term=" + currentTerm)
                .contains("voteGranted=" + BooleanType.F);
    }

    @Test
    public void apply_grants_vote_when_terms_are_equal_and_log_is_less_that_request_log_and_server_has_already_voted_for_the_candidate() throws Exception {
        //given
        final int requestTerm = 5;
        final int currentTerm = 5;
        final int candidateId = 2;
        final long lastLogIndex = 10;
        final int lastLogTerm = 4;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(candidateId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(lastLogKeyDecoder.term()).thenReturn(lastLogTerm);
        when(lastLogKeyDecoder.index()).thenReturn(lastLogIndex);

        when(persistentState.lastKeyCompareTo(lastLogIndex, lastLogTerm)).thenReturn(-1);
        when(persistentState.hasNotVotedYet()).thenReturn(false);
        when(persistentState.votedFor()).thenReturn(candidateId);

        //when
        final Transition transition = voteRequestHandler.apply(voteRequestDecoder, logger);

        assertThat(transition).isEqualTo(Transition.STEADY);

        final StringBuilder voteResponse = new StringBuilder();
        voteResponseEncoder.appendTo(voteResponse);

        assertThat(voteResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + candidateId)
                .contains("term=" + currentTerm)
                .contains("voteGranted=" + BooleanType.T);
    }

    @Test
    public void apply_rejects_vote_when_terms_are_equal_and_log_is_greater_than_request_log() throws Exception {
        //given
        final int requestTerm = 5;
        final int currentTerm = 5;
        final int candidateId = 2;
        final long lastLogIndex = 10;
        final int lastLogTerm = 4;

        when(headerDecoder.term()).thenReturn(requestTerm);
        when(headerDecoder.sourceId()).thenReturn(candidateId);
        when(persistentState.currentTerm()).thenReturn(currentTerm);
        when(lastLogKeyDecoder.term()).thenReturn(lastLogTerm);
        when(lastLogKeyDecoder.index()).thenReturn(lastLogIndex);

        when(persistentState.lastKeyCompareTo(lastLogIndex, lastLogTerm)).thenReturn(1);

        //when
        final Transition transition = voteRequestHandler.apply(voteRequestDecoder, logger);

        assertThat(transition).isEqualTo(Transition.STEADY);

        final StringBuilder voteResponse = new StringBuilder();
        voteResponseEncoder.appendTo(voteResponse);

        assertThat(voteResponse)
                .contains("sourceId=" + serverId)
                .contains("destinationId=" + candidateId)
                .contains("term=" + currentTerm)
                .contains("voteGranted=" + BooleanType.F);
    }

}