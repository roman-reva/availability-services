package com.services.availability.storage.commitlog;

import com.services.availability.model.AvailabilityItem;
import com.services.availability.utils.ByteUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.concurrent.Executors;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-15 17:44
 */
public class CommitLog {
    private static final int DEFAULT_THREAD_NUMBER = 1;

    private static Logger log = Logger.getLogger(CommitLog.class);

    private final int threadNumber;

    public CommitLog() {
        this.threadNumber = DEFAULT_THREAD_NUMBER;
    }

    public CommitLog(int threadNumber) {
        this.threadNumber = threadNumber;

        Executors.newFixedThreadPool(threadNumber);
    }

    public void addPutRecord(AvailabilityItem item) throws IOException {
        LogRecord record = new LogRecord(LogRecord.TYPE_PUT, item.key(), item.getSku(), item.getStore(), item.getAmount());
        writeRecord(record);
    }

    public void addRemoveRecord(long key) throws IOException {
        LogRecord record = new LogRecord(LogRecord.TYPE_REMOVE, key);
        writeRecord(record);
    }

    private void writeRecord(LogRecord record) throws IOException {
//        logFos.write(LogRecord.toByteArray(record));
//        logFos.flush();
    }

    private void writeHeader(int recordsCommitted) throws IOException {
//        byte[] header = new byte[32];
//        ByteUtils.putInt(recordsCommitted, header, 0);
//        logFos.write(header);
//        logFos.flush();
    }
}
