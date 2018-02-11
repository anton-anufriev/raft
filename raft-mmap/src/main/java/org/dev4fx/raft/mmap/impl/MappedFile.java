package org.dev4fx.raft.mmap.impl;

import sun.nio.ch.FileChannelImpl;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public final class MappedFile implements Closeable {
    public static long REGION_SIZE_GRANULARITY = regionSizeGranularity();

    public enum Mode {
        READ_ONLY("r", FileChannel.MapMode.READ_ONLY),
        READ_WRITE("rw", FileChannel.MapMode.READ_WRITE),
        /** Delete io contents on open*/
        READ_WRITE_CLEAR("rw", FileChannel.MapMode.READ_WRITE);

        private final String rasMode;
        private final FileChannel.MapMode mapMode;
        Mode(final String rasMode, final FileChannel.MapMode mapMode) {
            this.rasMode = Objects.requireNonNull(rasMode);
            this.mapMode = Objects.requireNonNull(mapMode);
        }

        public String getRandomAccessMode() {
            return rasMode;
        }
        public FileChannel.MapMode getMapMode() {
            return mapMode;
        }
    }

    public interface FileInitialiser {
        void init(FileChannel file, Mode mode) throws IOException;
    }

    private final RandomAccessFile file;
    private final FileChannel fileChannel;
    private final Mode mode;
    private final long regionSize;

    public MappedFile(final String fileName, final Mode mode, final long regionSize) throws IOException {
        this(new File(fileName), mode, regionSize);
    }

    public MappedFile(final String fileName, final Mode mode, final long regionSize, final FileInitialiser fileInitialiser) throws IOException {
        this(new File(fileName), mode, regionSize, fileInitialiser);
    }

    public MappedFile(final File file, final Mode mode, final long regionSize) throws IOException {
        this(file, mode, regionSize, (c,m) -> {});
    }

    public MappedFile(final File file, final Mode mode, final long regionSize, final FileInitialiser fileInitialiser) throws IOException {
        if (regionSize <= 0 || (regionSize % REGION_SIZE_GRANULARITY) != 0) {
            throw new IllegalArgumentException("Region size must be positive and a multiple of " + REGION_SIZE_GRANULARITY + " but was " + regionSize);
        }
        if (mode == Mode.READ_WRITE_CLEAR && file.exists()) {
            file.delete();
        }
        if (!file.exists()) {
            if (mode == Mode.READ_ONLY) {
                throw new FileNotFoundException(file.getAbsolutePath());
            }
            file.createNewFile();
        }
        final RandomAccessFile raf = new RandomAccessFile(file, mode.getRandomAccessMode());
        this.file = Objects.requireNonNull(raf);
        this.mode = Objects.requireNonNull(mode);
        this.regionSize = regionSize;
        this.fileChannel = raf.getChannel();
        fileInitialiser.init(fileChannel, mode);
    }

    public Mode getMode() {
        return mode;
    }

    public long getRegionSize() {
        return regionSize;
    }

    public long getFileLength() {
        try {
            return file.length();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setFileLength(final long length) {
        try {
            file.setLength(length);
        } catch (final IOException e) {
            throw new RuntimeException("could not set io length to " + length, e);
        }
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public void close() {
        try {
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static long regionSizeGranularity() {
        try {
            final Method method = FileChannelImpl.class.getDeclaredMethod("initIDs");
            method.setAccessible(true);
            final long result = (long)method.invoke(null);
            method.setAccessible(false);
            return result;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
