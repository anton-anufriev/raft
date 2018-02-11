package org.def4fx.raft.queue.util;

import org.HdrHistogram.Histogram;

/**
 * Test printout for histograms.
 */
public class HistogramPrinter {
    public static void printHistogram(final Histogram histogram) {
        System.out.println("Percentiles (micros)");
        System.out.println("\t90%    : " + histogram.getValueAtPercentile(90)/1000f);
        System.out.println("\t99%    : " + histogram.getValueAtPercentile(99)/1000f);
        System.out.println("\t99.9%  : " + histogram.getValueAtPercentile(99.9)/1000f);
        System.out.println("\t99.99% : " + histogram.getValueAtPercentile(99.99)/1000f);
        System.out.println("\t99.999%: " + histogram.getValueAtPercentile(99.999)/1000f);
        System.out.println("\tmax    : " + histogram.getMaxValue()/1000f);
        System.out.println();
        System.out.println("Histogram (micros):");
        histogram.outputPercentileDistribution(System.out, 1000.0);
    }
}
