package org.dev4fx.raft.distributed.map;


import org.agrona.MutableDirectBuffer;

import java.io.Serializable;

public interface Serialiser<T extends Serializable> {
    int serialise(T value, MutableDirectBuffer buffer, int offset);
}