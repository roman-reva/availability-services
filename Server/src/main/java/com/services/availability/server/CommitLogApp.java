package com.services.availability.server;

import com.services.availability.server.storage.AvailabilityItem;
import com.services.availability.server.storage.CommitLog;

import java.io.IOException;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-16 14:47
 */
public class CommitLogApp {
    public static void main(String[] args) throws IOException {
        AvailabilityItem item = new AvailabilityItem(34643636, (short)463, 436435643);
        CommitLog commitLog = new CommitLog();

        // single record
        long startTime = System.nanoTime();
        commitLog.addPutRecord(item);
        long timeElapsed = System.nanoTime() - startTime;
        System.out.println("Time elapsed (1 record): " + timeElapsed + "ns");

        // many records
        startTime = System.currentTimeMillis();
        for (int i=0; i<10000; i++) {
            commitLog.addPutRecord(item);
        }
        timeElapsed = System.currentTimeMillis() - startTime;
        System.out.println("Time elapsed (multiple records): " + timeElapsed + "ms");

        // close streams
        commitLog.closeLogFiles();
    }
}
