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
import org.dev4fx.raft.log.api.PersistentState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CommittedLogPromoterTest {
    @Mock
    private PersistentState persistentState;
    @Mock
    private VolatileState volatileState;
    @Mock
    private MessageHandler stateMachine;

    @Mock
    private MutableDirectBuffer commandDecoderBuffer;
    private int maxBatchSize = 2;
            
    private CommittedLogPromoter committedLogPromoter;

    @Before
    public void setUp() throws Exception {
        committedLogPromoter = new CommittedLogPromoter(persistentState, volatileState,
                stateMachine, commandDecoderBuffer, maxBatchSize);
    }

    @Test
    public void execute() throws Exception {
        when(volatileState.lastApplied()).thenReturn(10L);
        when(volatileState.commitIndex()).thenReturn(15L);
        when(commandDecoderBuffer.capacity()).thenReturn(20, 25);

        committedLogPromoter.execute();

        verify(persistentState).wrap(11, commandDecoderBuffer);
        verify(stateMachine).onMessage(commandDecoderBuffer, 0, 20);
        verify(volatileState).lastApplied(11);

        verify(persistentState).wrap(12, commandDecoderBuffer);
        verify(stateMachine).onMessage(commandDecoderBuffer, 0, 25);
        verify(volatileState).lastApplied(12);
    }

}