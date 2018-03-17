package org.dev4fx.raft.distributed.map;

import org.agrona.DirectBuffer;
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
public class MapStateMachine<K extends Serializable, V extends Serializable> implements StateMachine {
    private final int mapId;
    private final ConcurrentMap<K, V> map;
    private final Deserialiser<K> keyDeserialiser;
    private final Deserialiser<V> valueDeserialiser;
    private final LongFunction<? extends MapCommand<K, V>> sequenceToCommand;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final PutCommandDecoder putCommandDecoder = new PutCommandDecoder();
    private final RemoveCommandDecoder removeCommandDecoder = new RemoveCommandDecoder();
    private final PutAllCommandDecoder putAllCommandDecoder = new PutAllCommandDecoder();
    private final ClearCommandDecoder clearCommandDecoder = new ClearCommandDecoder();

    private final ValueResultMapCommandHandler<K,V> valueResultProvider = new ValueResultMapCommandHandler<>();
    private final VoidResultMapCommandHandler<K,V> voidResultProvider = new VoidResultMapCommandHandler<>();

    public MapStateMachine(final int mapId,
                           final ConcurrentMap<K, V> map,
                           final Deserialiser<K> keyDeserialiser,
                           final Deserialiser<V> valueDeserialiser,
                           final LongFunction<? extends MapCommand<K, V>> sequenceToCommand) {
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