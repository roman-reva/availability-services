package com.services.availability.server.storage;


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
        CommitLog commitLog = new CommitLog();

        AvailabilityItem item = new AvailabilityItem(TestUtils.getSku(), TestUtils.getStore(), TestUtils.getAmount());
        long startTime = System.nanoTime();
        for (int i=0; i<1; i++) {
            commitLog.addPutRecord(item);
        }

        long timeElapsed = (System.nanoTime() - startTime) / 1000;

        commitLog.closeLogFiles();

        System.out.println("Time elapsed: " + timeElapsed + "mcs");
    }
}
