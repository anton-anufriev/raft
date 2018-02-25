package org.dev4fx.raft.state;

import org.dev4fx.raft.log.api.PersistentState;

public enum LogContainment {
    IN, OUT, CONFLICT;

    public static LogContainment containmentFor(final long index, final long termAtIndex, final PersistentState persistentState) {
        if (index >= persistentState.size()) {
            return OUT;
        } else {
            if (index < 0) return IN;
            //FIXME check term at NULL_INDEX
            final int logTermAtIndex = persistentState.term(index);
            return termAtIndex == logTermAtIndex ? IN : CONFLICT;
        }
    }
}
