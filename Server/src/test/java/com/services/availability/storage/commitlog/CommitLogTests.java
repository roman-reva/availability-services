package com.services.availability.storage.commitlog;


import com.services.availability.TestUtils;
import com.services.availability.model.AvailabilityItem;
import com.services.availability.storage.ccl.commitlog.CommitLog;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-15 19:51
 */
public class CommitLogTests {

    @Test
    public void writeToLogTest() throws IOException {
        AvailabilityItem item = new AvailabilityItem(TestUtils.getSku(), TestUtils.getStore(), TestUtils.getAmount());
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
