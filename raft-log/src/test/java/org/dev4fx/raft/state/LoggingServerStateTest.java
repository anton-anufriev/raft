package org.dev4fx.raft.state;

import org.agrona.DirectBuffer;
import org.dev4fx.raft.sbe.AppendRequestDecoder;
import org.dev4fx.raft.sbe.AppendResponseDecoder;
import org.dev4fx.raft.sbe.VoteRequestDecoder;
import org.dev4fx.raft.sbe.VoteResponseDecoder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class LoggingServerStateTest {
    @Mock
    private ServerState delegateServerState;
    private StringBuilder stringBuilder = new StringBuilder();
    @Mock
    private Logger logger;
    @Mock
    private AppendRequestDecoder appendRequestDecoder;
    @Mock
    private AppendResponseDecoder appendResponseDecoder;
    @Mock
    private VoteRequestDecoder voteRequestDecoder;
    @Mock
    private VoteResponseDecoder voteResponseDecoder;
    @Mock
    private DirectBuffer commandBuffer;


    private LoggingServerState serverState;

    @Before
    public void setUp() throws Exception {
        serverState = new LoggingServerState(delegateServerState, stringBuilder, logger);
    }

    @Test
    public void role() throws Exception {
        serverState.role();
        verify(delegateServerState).role();
    }

    @Test
    public void onTransition() throws Exception {
        serverState.onTransition();
        verify(delegateServerState).onTransition();
    }

    @Test
    public void processTick() throws Exception {
        serverState.processTick();
        verify(delegateServerState).processTick();
    }

    @Test
    public void onVoteRequest() throws Exception {
        serverState.onVoteRequest(voteRequestDecoder);
        verify(delegateServerState).onVoteRequest(voteRequestDecoder);
        verify(voteRequestDecoder).appendTo(stringBuilder);
    }

    @Test
    public void onVoteResponse() throws Exception {
        serverState.onVoteResponse(voteResponseDecoder);
        verify(delegateServerState).onVoteResponse(voteResponseDecoder);
        verify(voteResponseDecoder).appendTo(stringBuilder);
    }

    @Test
    public void onAppendRequest() throws Exception {
        serverState.onAppendRequest(appendRequestDecoder);
        verify(delegateServerState).onAppendRequest(appendRequestDecoder);
        verify(appendRequestDecoder).appendTo(stringBuilder);
    }

    @Test
    public void onAppendResponse() throws Exception {
        serverState.onAppendResponse(appendResponseDecoder);
        verify(delegateServerState).onAppendResponse(appendResponseDecoder);
        verify(appendResponseDecoder).appendTo(stringBuilder);
    }

    @Test
    public void onCommandRequest() throws Exception {
        serverState.onCommandRequest(commandBuffer, 0, 10);
        verify(delegateServerState).onCommandRequest(commandBuffer, 0, 10);
    }

    @Test
    public void onTimeoutNow() throws Exception {
        serverState.onTimeoutNow();
        verify(delegateServerState).onTimeoutNow();
    }

}