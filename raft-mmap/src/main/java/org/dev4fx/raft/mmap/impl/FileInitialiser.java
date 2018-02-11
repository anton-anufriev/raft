package org.dev4fx.raft.mmap.impl;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class FileInitialiser {

    public static void initFile(final FileChannel fileChannel, final MappedFile.Mode mode) throws IOException {
        switch (mode) {
            case READ_ONLY:
                if (fileChannel.size() < 8) {
                    throw new IllegalArgumentException("Invalid io format");
                }
                break;
            case READ_WRITE:
                if (fileChannel.size() >= 8) break;
            case READ_WRITE_CLEAR:
                final FileLock lock = fileChannel.lock();
                try {
                    fileChannel.truncate(0);
                    fileChannel.transferFrom(InitialBytes.MINUS_ONE, 0, 8);
                    fileChannel.force(true);
                } finally {
                    lock.release();
                }
                break;
            default:
                throw new IllegalArgumentException("Invalid mode: " + mode);
        }
    }
}
