/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 hover-raft (tools4j), Anton Anufriev, Marco Terzer
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.dev4fx.raft.mmap.api;


import org.agrona.IoUtil;

import java.nio.channels.FileChannel;

/**
 * A region of a file that maps a certain block of file to a memory address.
 * Once mapped, client DirectBuffers can be wrapped to the pre-mapped memory.
 */
public interface Region extends RegionAccessor, RegionMapper {
    interface IoUnMapper {
        IoUnMapper DEFAULT = IoUtil::unmap;

        void unmap(FileChannel fileChannel, long address, long length);
    }

    interface IoMapper {
        IoMapper DEFAULT = IoUtil::map;

        long map(FileChannel fileChannel, FileChannel.MapMode mapMode, long offset, long length);
    }
}
