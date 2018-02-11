package org.dev4fx.raft.mmap.impl;

import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.def4fx.raft.mmap.api.AsyncRegion;
import org.def4fx.raft.mmap.api.FileSizeEnsurer;
import org.def4fx.raft.mmap.api.Region;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.tools4j.spockito.Spockito;

import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.*;

@Spockito.Unroll({
        "| testFactory                             |",
        "|-----------------------------------------|",
        "| VOLATILE_STATEMENT_MACHINE_REGION |",
        "| ATOMIC_STATEMENT_MACHINE_REGION   |",
        "| ATOMIC_EXCHANGE_REGION            |",
})
@Spockito.Name("[{row}]: {testFactory}")
@RunWith(Spockito.class)
public class AsyncRegionTest {

    interface AsyncRegionFactory {
        AsyncRegion create(final Supplier<FileChannel> fileChannelSupplier,
                      final Region.IoMapper ioMapper,
                      final Region.IoUnMapper ioUnMapper,
                      final FileSizeEnsurer fileSizeEnsurer,
                      final FileChannel.MapMode mapMode,
                      final int length,
                      final long timeout,
                      final TimeUnit timeUnits);
    }

    enum TestFactory {
        VOLATILE_STATEMENT_MACHINE_REGION(AsyncVolatileStateMachineRegion::new),
        ATOMIC_STATEMENT_MACHINE_REGION(AsyncAtomicStateMachineRegion::new),
        ATOMIC_EXCHANGE_REGION(AsyncAtomicExchangeRegion::new);

        private AsyncRegionFactory factory;

        TestFactory(final AsyncRegionFactory factory) {
            this.factory = Objects.requireNonNull(factory);
        }
    }

    @Spockito.Ref
    private TestFactory testFactory;

    @Mock
    private DirectBuffer directBuffer;
    @Mock
    private FileChannel fileChannel;
    @Mock
    private Region.IoMapper ioMapper;
    @Mock
    private Region.IoUnMapper ioUnMapper;
    @Mock
    private FileSizeEnsurer fileSizeEnsurer;

    private InOrder inOrder;

    private AsyncRegion region;

    private int length = 128;
    private FileChannel.MapMode mapMode = FileChannel.MapMode.READ_WRITE;
    private long timeoutMillis = 100;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        region = testFactory.factory.create(() -> fileChannel,
                ioMapper, ioUnMapper, fileSizeEnsurer,
                mapMode, length, timeoutMillis, TimeUnit.MILLISECONDS);
        inOrder = inOrder(directBuffer, fileChannel, ioMapper, ioUnMapper, fileSizeEnsurer);
    }

    @Test
    public void wrap_false_when_no_async_mapping() throws Exception {

        final boolean wrapped = region.wrap(10, directBuffer);

        assertThat(wrapped).isFalse();
        inOrder.verify(directBuffer, never()).wrap(anyLong(), anyInt());
        inOrder.verify(ioMapper, never()).map(same(fileChannel), same(mapMode), anyLong(), same(length));
    }

    @Test
    public void wrap_map_and_unmap() throws Exception {
        //given
        final AtomicBoolean wrapped = new AtomicBoolean();
        final long expectedAddress = 1024;
        final long position = 4567;
        final int positionInRegion = (int) (position % length);
        final long regionStartPosition = position - positionInRegion;

        when(ioMapper.map(fileChannel, mapMode, regionStartPosition, length)).thenReturn(expectedAddress);
        when(fileSizeEnsurer.ensureSize(regionStartPosition + length)).thenReturn(true);

        //check that there is nothing to process
        assertThat(region.process()).isFalse();

        //when - request mapping
        final Thread thread1 = new Thread(() -> {
            wrapped.set(region.wrap(position, directBuffer));
        });

        //and when - process mapping request
        final Thread thread2 = new Thread(() -> {
            boolean processed;
            do {
                processed = region.process();
            } while (!processed);
        });
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        //then
        assertThat(wrapped.get()).isTrue();
        inOrder.verify(ioMapper, times(1)).map(fileChannel, mapMode, regionStartPosition, length);
        inOrder.verify(directBuffer).wrap(expectedAddress + positionInRegion, length - positionInRegion);

        //check that there is nothing to process
        assertThat(region.process()).isFalse();
        //check that region is mapped
        assertThat(region.map(regionStartPosition)).isTrue();

        //when - wrap again within the same region
        final int offset = 4;
        region.wrap(regionStartPosition + offset, directBuffer);

        //then
        inOrder.verify(directBuffer).wrap(expectedAddress + offset, length - offset);
        inOrder.verify(ioMapper, times(0)).map(fileChannel, mapMode, regionStartPosition, length);

        //when
        //check that region is mapped
        assertThat(region.map(regionStartPosition)).isTrue();
        //then
        inOrder.verify(ioMapper, times(0)).map(fileChannel, mapMode, regionStartPosition, length);

        //check that there is nothing to process
        assertThat(region.process()).isFalse();

        //when send unmap request
        assertThat(region.unmap()).isFalse();

        //and when - process unmapping request
        final Thread thread3 = new Thread(() -> {
            boolean processed;
            do {
                processed = region.process();
            } while (!processed);
        });
        thread3.start();
        thread3.join();

        //then
        inOrder.verify(ioUnMapper, times(1)).unmap(fileChannel, expectedAddress, length);

        assertThat(region.unmap()).isTrue();
        inOrder.verify(ioUnMapper, times(0)).unmap(fileChannel, expectedAddress, length);
    }

    @Test
    public void map_and_unmap() throws Exception {
        //given
        final long expectedAddress = 1024;
        final long position = 4567;
        final int positionInRegion = (int) (position % length);
        final long regionStartPosition = position - positionInRegion;

        when(ioMapper.map(fileChannel, mapMode, regionStartPosition, length)).thenReturn(expectedAddress);
        when(fileSizeEnsurer.ensureSize(regionStartPosition + length)).thenReturn(true);

        //check that there is nothing to process
        assertThat(region.process()).isFalse();

        //when - request mapping
        assertThat(region.map(regionStartPosition)).isFalse();

        //and when - process mapping request
        final Thread thread1 = new Thread(() -> {
            boolean processed;
            do {
                processed = region.process();
            } while (!processed);
        });
        thread1.start();
        thread1.join();

        //then
        inOrder.verify(ioMapper, times(1)).map(fileChannel, mapMode, regionStartPosition, length);

        //check that there is nothing to process
        assertThat(region.process()).isFalse();

        //once region is mapped, wrap should be non-blocking
        assertThat(region.wrap(position, directBuffer)).isTrue();
        inOrder.verify(directBuffer).wrap(expectedAddress + positionInRegion, length - positionInRegion);

        //when - map again within the same region and check if had been mapped
        assertThat(region.map(regionStartPosition)).isTrue();

        //then
        inOrder.verify(ioMapper, times(0)).map(fileChannel, mapMode, regionStartPosition, length);

        //check that there is nothing to process
        assertThat(region.process()).isFalse();

        //when send unmap request
        assertThat(region.unmap()).isFalse();

        //and when - process unmapping request
        final Thread thread2 = new Thread(() -> {
            boolean processed;
            do {
                processed = region.process();
            } while (!processed);
        });
        thread2.start();
        thread2.join();

        //then
        inOrder.verify(ioUnMapper, times(1)).unmap(fileChannel, expectedAddress, length);

        assertThat(region.unmap()).isTrue();
        inOrder.verify(ioUnMapper, times(0)).unmap(fileChannel, expectedAddress, length);
    }

    @Test
    public void map_and_remap() throws Exception {
        //given
        final long expectedAddress = 1024;
        final long position = 4567;
        final int positionInRegion = (int) (position % length);
        final long regionStartPosition = position - positionInRegion;

        when(ioMapper.map(fileChannel, mapMode, regionStartPosition, length)).thenReturn(expectedAddress);
        when(fileSizeEnsurer.ensureSize(regionStartPosition + length)).thenReturn(true);

        //when - request mapping
        assertThat(region.map(regionStartPosition)).isFalse();

        //and when - process mapping request
        final Thread thread1 = new Thread(() -> {
            boolean processed;
            do {
                processed = region.process();
            } while (!processed);
        });
        thread1.start();
        thread1.join();

        //then
        inOrder.verify(ioMapper, times(1)).map(fileChannel, mapMode, regionStartPosition, length);


        //when send unmap request
        final long prevRegionStartPosition = regionStartPosition - length;
        final long prevExpectedAddress = expectedAddress - length;
        when(ioMapper.map(fileChannel, mapMode, prevRegionStartPosition, length)).thenReturn(prevExpectedAddress);
        when(fileSizeEnsurer.ensureSize(regionStartPosition)).thenReturn(true);

        assertThat(region.map(prevRegionStartPosition)).isFalse();

        //and when - process unmapping request
        final Thread thread2 = new Thread(() -> {
            boolean processed;
            do {
                processed = region.process();
            } while (!processed);
        });
        thread2.start();
        thread2.join();

        //then
        inOrder.verify(ioUnMapper, times(1)).unmap(fileChannel, expectedAddress, length);
        inOrder.verify(ioMapper, times(1)).map(fileChannel, mapMode, prevRegionStartPosition, length);
    }

    @Test
    public void map_and_close() throws Exception {
        //given
        final long expectedAddress = 1024;
        final long position = 4567;
        final int positionInRegion = (int) (position % length);
        final long regionStartPosition = position - positionInRegion;
        assertThat(region.size()).isEqualTo(length);

        when(ioMapper.map(fileChannel, mapMode, regionStartPosition, length)).thenReturn(expectedAddress);
        when(fileSizeEnsurer.ensureSize(regionStartPosition + length)).thenReturn(true);

        //when - request mapping
        assertThat(region.map(regionStartPosition)).isFalse();

        //and when - process mapping request
        final Thread thread1 = new Thread(() -> {
            boolean processed;
            do {
                processed = region.process();
            } while (!processed);
        });
        thread1.start();
        thread1.join();

        //then
        inOrder.verify(ioMapper, times(1)).map(fileChannel, mapMode, regionStartPosition, length);

        //when
        region.close();

        //and
        assertThat(region.process()).isTrue();

        //then
        inOrder.verify(ioUnMapper, times(1)).unmap(fileChannel, expectedAddress, length);
    }

}