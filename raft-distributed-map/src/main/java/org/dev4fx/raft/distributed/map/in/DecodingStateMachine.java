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
package org.dev4fx.raft.distributed.map.in;

import org.agrona.DirectBuffer;
import org.dev4fx.raft.distributed.map.codec.Deserialiser;
import org.dev4fx.raft.distributed.map.command.Command;
import org.dev4fx.raft.distributed.map.command.ValueCompletionCommandHandler;
import org.dev4fx.raft.distributed.map.command.VoidCompletionCommandHandler;
import org.dev4fx.raft.dmap.sbe.*;
import org.dev4fx.raft.state.StateMachine;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongFunction;

/**
 * This class is not thread-safe.
 * @param <K>
 * @param <V>
 */
public class DecodingStateMachine<K extends Serializable, V extends Serializable> implements StateMachine {
    private final int mapId;
    private final ConcurrentMap<K, V> map;
    private final Deserialiser<K> keyDeserialiser;
    private final Deserialiser<V> valueDeserialiser;
    private final LongFunction<? extends Command<K, V>> sequenceToCommand;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final PutCommandDecoder putCommandDecoder = new PutCommandDecoder();
    private final RemoveCommandDecoder removeCommandDecoder = new RemoveCommandDecoder();
    private final PutAllCommandDecoder putAllCommandDecoder = new PutAllCommandDecoder();
    private final ClearCommandDecoder clearCommandDecoder = new ClearCommandDecoder();

    private final ValueCompletionCommandHandler<K,V> valueResultProvider = new ValueCompletionCommandHandler<>();
    private final VoidCompletionCommandHandler<K,V> voidResultProvider = new VoidCompletionCommandHandler<>();

    public DecodingStateMachine(final int mapId,
                                final ConcurrentMap<K, V> map,
                                final Deserialiser<K> keyDeserialiser,
                                final Deserialiser<V> valueDeserialiser,
                                final LongFunction<? extends Command<K, V>> sequenceToCommand) {
        this.mapId = mapId;
        this.map = Objects.requireNonNull(map);
        this.keyDeserialiser = Objects.requireNonNull(keyDeserialiser);
        this.valueDeserialiser = Objects.requireNonNull(valueDeserialiser);
        this.sequenceToCommand = Objects.requireNonNull(sequenceToCommand);
    }

    @Override
    public void onCommand(final int sourceId, final long sequence, final DirectBuffer buffer, final int offset, final int length) {
        messageHeaderDecoder.wrap(buffer, offset);
        final int templateId = messageHeaderDecoder.templateId();
        final int headerLength = messageHeaderDecoder.encodedLength();
        assert mapId == messageHeaderDecoder.mapId();
        K key;
        V value;
        switch (templateId) {
            case PutCommandDecoder.TEMPLATE_ID:
                putCommandDecoder.wrap(buffer,headerLength + offset,
                        PutCommandDecoder.BLOCK_LENGTH,
                        PutCommandDecoder.SCHEMA_VERSION);

                key = keyDeserialiser.deserialise(
                                putCommandDecoder.buffer(),
                                putCommandDecoder.limit() + PutCommandDecoder.keyHeaderLength(),
                                putCommandDecoder.keyLength());
                putCommandDecoder.limit( putCommandDecoder.limit() + PutCommandDecoder.keyHeaderLength() + putCommandDecoder.keyLength());

                value = valueDeserialiser.deserialise(
                        putCommandDecoder.buffer(),
                        putCommandDecoder.limit() + PutCommandDecoder.valueHeaderLength(),
                                putCommandDecoder.valueLength());
                putCommandDecoder.limit( putCommandDecoder.limit() + PutCommandDecoder.valueHeaderLength() + putCommandDecoder.valueLength());

                valueResultProvider.result(map.put(key, value));

                sequenceToCommand.apply(sequence).accept(sequence, valueResultProvider);
                break;
            case PutAllCommandDecoder.TEMPLATE_ID:
                putAllCommandDecoder.wrap(buffer,headerLength + offset,
                        PutAllCommandDecoder.BLOCK_LENGTH,
                        PutAllCommandDecoder.SCHEMA_VERSION);

                for (final PutAllCommandDecoder.EntriesDecoder entriesDecoder : putAllCommandDecoder.entries()) {
                    key = keyDeserialiser.deserialise(
                            putAllCommandDecoder.buffer(),
                            putAllCommandDecoder.limit() + PutAllCommandDecoder.EntriesDecoder.keyHeaderLength(),
                            entriesDecoder.keyLength());
                    putAllCommandDecoder.limit( putAllCommandDecoder.limit() + PutAllCommandDecoder.EntriesDecoder.keyHeaderLength() + entriesDecoder.keyLength());

                    value = valueDeserialiser.deserialise(
                            putAllCommandDecoder.buffer(),
                            putAllCommandDecoder.limit() + PutAllCommandDecoder.EntriesDecoder.valueHeaderLength(),
                            entriesDecoder.valueLength());
                    putAllCommandDecoder.limit( putAllCommandDecoder.limit() + PutAllCommandDecoder.EntriesDecoder.valueHeaderLength() + entriesDecoder.valueLength());
                    map.put(key, value);
                }
                sequenceToCommand.apply(sequence).accept(sequence, voidResultProvider);
                break;
            case RemoveCommandDecoder.TEMPLATE_ID:
                removeCommandDecoder.wrap(buffer,headerLength + offset,
                        RemoveCommandDecoder.BLOCK_LENGTH,
                        RemoveCommandDecoder.SCHEMA_VERSION);

                key = keyDeserialiser.deserialise(
                        removeCommandDecoder.buffer(),
                        removeCommandDecoder.limit() + RemoveCommandDecoder.keyHeaderLength(),
                        removeCommandDecoder.keyLength());

                valueResultProvider.result(map.remove(key));
                sequenceToCommand.apply(sequence).accept(sequence, valueResultProvider);
                break;
            case ClearCommandDecoder.TEMPLATE_ID:
                clearCommandDecoder.wrap(buffer,headerLength + offset,
                        ClearCommandDecoder.BLOCK_LENGTH,
                        ClearCommandDecoder.SCHEMA_VERSION);

                map.clear();
                sequenceToCommand.apply(sequence).accept(sequence, voidResultProvider);
                break;
            default:
                throw new IllegalStateException("Unknown command with templateId " + templateId);
        }
    }
}