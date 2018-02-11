package org.dev4fx.raft.mmap.impl;

import org.agrona.DirectBuffer;
import org.def4fx.raft.mmap.api.Region;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RegionRingAccessorTest {
    @Mock
    private Runnable onClose;
    @Mock
    private Region region1;
    @Mock
    private Region region2;
    @Mock
    private Region region3;
    @Mock
    private Region region4;

    private InOrder inOrder;

    @Mock
    private DirectBuffer directBuffer;

    private RegionRingAccessor regionRingAccessor;
    private int regionSize = 128;

    @Before
    public void setup() {
        inOrder = Mockito.inOrder(region1, region2, region3, region4, onClose);

        final Region[] regions = new Region[] {region1, region2, region3, region4};
        regionRingAccessor = new RegionRingAccessor(regions, regionSize, 3, onClose);
    }

    @Test
    public void wrap() throws Exception {
        //given
        when(region4.wrap(7 * regionSize + 45, directBuffer)).thenReturn(true);
        when(region4.wrap(7 * regionSize + 60, directBuffer)).thenReturn(true);

        //when
        regionRingAccessor.wrap(7 * regionSize + 45, directBuffer);
        regionRingAccessor.wrap(7 * regionSize + 60, directBuffer);
        regionRingAccessor.wrap(8 * regionSize + 20, directBuffer);

        //then
        inOrder.verify(region4).wrap(7 * regionSize + 45, directBuffer);

        inOrder.verify(region1).map(8 * regionSize);
        inOrder.verify(region2).map(9 * regionSize);
        inOrder.verify(region3).map(10 * regionSize);

        //when
        when(region4.wrap(7 * regionSize + 60, directBuffer)).thenReturn(true);
        regionRingAccessor.wrap(7 * regionSize + 60, directBuffer);

        inOrder.verify(region1, times(0)).map(8 * regionSize);
        inOrder.verify(region2, times(0)).map(9 * regionSize);
        inOrder.verify(region3, times(0)).map(10 * regionSize);

        //when
        when(region1.wrap(8 * regionSize + 20, directBuffer)).thenReturn(true);
        regionRingAccessor.wrap(8 * regionSize + 20, directBuffer);

        //then
        inOrder.verify(region2).map(9 * regionSize);
        inOrder.verify(region3).map(10 * regionSize);
        inOrder.verify(region4).map(11 * regionSize);

        //when - backwards
        when(region3.wrap(6 * regionSize + 80, directBuffer)).thenReturn(true);
        regionRingAccessor.wrap(6 * regionSize + 80, directBuffer);

        //then
        inOrder.verify(region2).map(5 * regionSize);
        inOrder.verify(region1).map(4 * regionSize);
        inOrder.verify(region4).map(3 * regionSize);
    }

    @Test
    public void close() throws Exception {
        //when
        regionRingAccessor.close();

        //verify
        inOrder.verify(region1).close();
        inOrder.verify(region2).close();
        inOrder.verify(region3).close();
        inOrder.verify(region4).close();
        inOrder.verify(onClose).run();
    }

    @Test
    public void size() throws Exception {
        assertThat(regionRingAccessor.size()).isEqualTo(regionSize);
    }
}