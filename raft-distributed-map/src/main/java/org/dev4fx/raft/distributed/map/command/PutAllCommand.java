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
package org.dev4fx.raft.distributed.map.command;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class PutAllCommand<K extends Serializable, V extends Serializable> implements Command<K, V> {
    private final int mapId;
    private final Map<? extends K, ? extends V> values;
    private final FutureResult<Void> futureResult;

    public PutAllCommand(final int mapId,
                         final Map<? extends K, ? extends V> values,
                         final FutureResult<Void> futureResult) {
        this.mapId = mapId;
        this.values = Objects.requireNonNull(values);
        this.futureResult = Objects.requireNonNull(futureResult);
    }

    public int mapId() {
        return mapId;
    }

    public Map<? extends K, ? extends V> values() {
        return values;
    }

    public void complete() {
        futureResult.accept(null);
    }

    @Override
    public void accept(final long sequence, final CommandHandler<K, V> commandHandler) {
        commandHandler.onCommand(sequence, this);
    }
}
