package org.dev4fx.raft.distributed.map;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;

public class FutureResultTest {
    private FutureResult<Void> voidFutureResult = new FutureResult<>();
    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void accept() throws Exception {

        final Future<Void> future = voidFutureResult.get();
        final Thread thread = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            voidFutureResult.accept(null);
        });

        thread.start();

        future.get();
    }

    @Test
    public void get() throws Exception {
    }

}