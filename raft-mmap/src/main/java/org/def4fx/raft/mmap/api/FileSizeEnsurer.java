package org.def4fx.raft.mmap.api;

import org.agrona.collections.MutableLong;

import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public interface FileSizeEnsurer {
    FileSizeEnsurer NO_OP = minSize -> true;

    boolean ensureSize(long minSize);


    static FileSizeEnsurer forWritableFile(final LongSupplier fileSizeGetter, final LongConsumer fileSizeSetter, final long maxSize) {
        Objects.requireNonNull(fileSizeGetter);
        Objects.requireNonNull(fileSizeSetter);
        final MutableLong fileSize = new MutableLong(0);

        return minSize -> {
            if (fileSize.get() < minSize) {
                final long len = fileSizeGetter.getAsLong();
                if (len < minSize) {
                    if (minSize > maxSize) {
                        throw new IllegalStateException("Exceeded max file size " + maxSize + ", requested size " + minSize);
                    }
                    fileSizeSetter.accept(minSize);
                    fileSize.set(minSize);
                } else {
                    fileSize.set(len);
                }
            }
            return true;
        };
    }
}
