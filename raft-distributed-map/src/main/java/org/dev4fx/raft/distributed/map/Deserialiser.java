package org.dev4fx.raft.distributed.map;


import org.agrona.DirectBuffer;

import java.io.Serializable;

public interface Deserialiser<T extends Serializable> {
    T deserialise(DirectBuffer buffer, int offset, int length);
}
