package com.services.availability.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-23 17:53
 */
public class ThroughputMeter {
    private long startTime = -1;
    private long stopTime = -1;

    private AtomicLong counter = new AtomicLong(0);

    public void start() {
        if (startTime > 0) throw new IllegalStateException("ThroughputMeter was already started.");
        startTime = System.nanoTime();
    }

    public void stop() {
        if (stopTime > 0) throw new IllegalStateException("ThroughputMeter was already stopped.");
        stopTime = System.nanoTime();
    }

    public void reset() {
        startTime = stopTime = -1;
        counter = new AtomicLong(0);
    }

    public void inc() {
        counter.incrementAndGet();
    }

    public double getThroughput() {
        long reqNum = counter.get();
        long timePeriodMs = (stopTime - startTime) / 1000000;
        return ((1000.0 * reqNum) / timePeriodMs);
    }
}
