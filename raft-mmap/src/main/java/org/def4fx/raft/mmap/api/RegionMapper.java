package org.def4fx.raft.mmap.api;

public interface RegionMapper {
    /**
     * Attempts to map a region.
     * In synchronous implementations, it is expected to be mapped immediately,
     *    if not mapped yet.
     * In asynchronous implementations, if the region is not mapped yet, mapping
     *    will be performed asynchronously and false will be returned.
     *
     * @param regionStartPosition - start position of a region, must be power of two and aligned with
     *                            the length of the region.
     * @return true if the region is mapped after the call, otherwise - false.
     */
    boolean map(final long regionStartPosition);
    /**
     * Attempts to unmap a region.
     * In synchronous implementations, it is expected to be unmapped immediately,
     *    if not unmapped yet.
     * In asynchronous implementations, if the region is not unmapped yet, mapping
     *    will be performed asynchronously and false will be returned.
     *
     * @return true if the region is unmapped after the call, otherwise - false.
     */
    boolean unmap();

}
