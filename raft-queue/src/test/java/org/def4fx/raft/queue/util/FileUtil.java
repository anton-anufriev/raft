package org.def4fx.raft.queue.util;

import java.io.File;
import java.io.IOException;

public class FileUtil {

    public static final File IO_TMPDIR = new File(System.getProperty("java.io.tmpdir"));
    public static final File SHARED_MEM_DIR = new File("/dev/shm");

    public static File tmpDirFile(final String name) {
        return new File(IO_TMPDIR, name);
    }

    public static File sharedMemDir(final String name) {
        return SHARED_MEM_DIR.isDirectory() ? new File(SHARED_MEM_DIR, name) : tmpDirFile(name);
    }

    public static void deleteTmpDirFilesMatching(final String namePrefix) throws IOException {
        deleteFilesMatching(IO_TMPDIR, namePrefix);
    }
    public static void deleteFilesMatching(final File dir, final String namePrefix) throws IOException {
        for (final File file : dir.listFiles((d, n) -> n.startsWith(namePrefix))) {
            deleteRecursively(file);
        }
    }

    public static void deleteRecursively(final File file) throws IOException {
        if (file.isDirectory()) {
            for (File sub : file.listFiles()) {
                deleteRecursively(sub);
            }
        }
        if (!file.delete()) {
            throw new IOException("could not delete: " + file.getAbsolutePath());
        }
    }
}
