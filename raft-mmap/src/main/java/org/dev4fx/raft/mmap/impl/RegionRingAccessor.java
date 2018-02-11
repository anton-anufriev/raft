package org.dev4fx.raft.mmap.impl;

import org.agrona.DirectBuffer;
import org.def4fx.raft.mmap.api.Region;
import org.def4fx.raft.mmap.api.RegionAccessor;

import java.util.Objects;
import java.util.function.LongFunction;

public class RegionRingAccessor implements RegionAccessor {
    private final Region[] regions;
    private final int regionsLength;
    private final int regionsToMapAhead;
    private final int regionSize;
    private final Runnable onClose;
    private final int regionsLengthMask;
    private final int regionSizeMask;

    private long currentAbsoluteIndex = -1;

    public RegionRingAccessor(final Region[] regions, final int regionSize, final int regionsToMapAhead, final Runnable onClose) {
        this.regions = Objects.requireNonNull(regions);
        this.onClose = Objects.requireNonNull(onClose);
        if (regionsToMapAhead >= regions.length) throw new IllegalArgumentException("regionsToMapAhead " + regionsToMapAhead + " must be less that regions.length " + regions.length);
        if (regionsToMapAhead < 0) throw new IllegalArgumentException("regionsToMapAhead " + regionsToMapAhead + " must positive");
        this.regionSize = regionSize;
        this.regionsToMapAhead = regionsToMapAhead;
        this.regionsLength = regions.length;
        assertPowerOfTwo(regionsLength, v -> "regionsLength must be a power of two, but is " + v);
        assertPowerOfTwo(regionSize, v -> "regionSize must be a power of two, but is " + v);
        regionsLengthMask = regionsLength - 1;
        regionSizeMask = regionSize - 1;
    }

    private void assertPowerOfTwo(final int value, final LongFunction<String> comment) {
        if(Integer.bitCount(value) != 1) throw new IllegalArgumentException(comment.apply(value));
    }

    @Override
    public boolean wrap(final long position, final DirectBuffer buffer) {
        final long absoluteIndex = position / regionSize;

        final boolean wrapped = regions[(int) (absoluteIndex & regionsLengthMask)].wrap(position, buffer);
        if (wrapped) {
            if (currentAbsoluteIndex < absoluteIndex) { // moving forward
                for (int i = 1; i <= regionsToMapAhead; i++) {
                    final long mapIndex = absoluteIndex + i;
                    regions[(int) (mapIndex & regionsLengthMask)].map(mapIndex * regionSize);
                }
//                for (long mapIndex = absoluteIndex + 1; mapIndex <= absoluteIndex + regionsToMapAhead; mapIndex++) {
//                    regions[(int) (mapIndex % regionsLength)].map(mapIndex * regionSize);
//                }
                if (currentAbsoluteIndex >= 0)
                    regions[(int) (currentAbsoluteIndex & regionsLengthMask)].unmap();
            } else if (currentAbsoluteIndex > absoluteIndex) { // moving backward
                for (int i = 1; i <= regionsToMapAhead; i++) {
                    final long mapIndex = absoluteIndex - i;
                    regions[(int) (mapIndex & regionsLengthMask)].map(mapIndex * regionSize);
                }
//                for (long mapIndex = absoluteIndex - 1; mapIndex >= 0 && mapIndex >= absoluteIndex - regionsToMapAhead; mapIndex--) {
//                    regions[(int) (mapIndex % regionsLength)].map(mapIndex * regionSize);
//                }
                if (currentAbsoluteIndex >= 0)
                    regions[(int) (currentAbsoluteIndex & regionsLengthMask)].unmap();

            }
        }
        currentAbsoluteIndex = absoluteIndex;

        return wrapped;
    }

    @Override
    public void close() {
        for (final Region region : regions) {
            region.close();
        }
        onClose.run();
    }

    @Override
    public int size() {
        return regionSize;
    }
}
