package org.def4fx.raft.mmap.api;

public interface AsyncRegionState {
    AsyncRegionState requestMap(final long regionStartPosition);
    AsyncRegionState requestUnmap();
    AsyncRegionState processRequest();
}
