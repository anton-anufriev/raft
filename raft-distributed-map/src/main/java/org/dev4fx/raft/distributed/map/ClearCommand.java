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
package org.dev4fx.raft.distributed.map;

import java.io.Serializable;
import java.util.Objects;

public class ClearCommand<K extends Serializable, V extends Serializable> implements MapCommand<K, V> {
    private final int mapId;
    private final FutureResult<Void> futureResult;

    public ClearCommand(final int mapId,
                        final FutureResult<Void> futureResult) {
        this.mapId = mapId;
        this.futureResult = Objects.requireNonNull(futureResult);
    }

    public int mapId() {
        return mapId;
    }

    public void setResult() {
        futureResult.accept(null);
    }

    @Override
    public void accept(final long sequence, final MapCommandHandler<K, V> commandHandler) {
        commandHandler.onCommand(sequence, this);
    }
}
