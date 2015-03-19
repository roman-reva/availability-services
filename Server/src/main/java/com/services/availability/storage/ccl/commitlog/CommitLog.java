package com.services.availability.storage.ccl.commitlog;

import com.services.availability.model.AvailabilityItem;
import com.services.availability.storage.ccl.LogRecord;
import com.services.availability.utils.ByteUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Commit log system.
 *
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-15 17:44
 */
public class CommitLog {
    private static final int DEFAULT_THREAD_NUMBER = 1;

    private static Logger log = Logger.getLogger(CommitLog.class);

    private static final String LOGFILE_PREFIX = "commit_";
    private static final String LOGFILE_POSTFIX = ".log";
    private static final String LOGFILE_GRP_A = "a";
    private static final String LOGFILE_GRP_B = "b";

    private final int threadNumber;
    private final BlockingQueue<LogDescriptor> availableLogs;
    private final List<LogDescriptor> busyLogs = new ArrayList<LogDescriptor>();

    private final ExecutorService executorService;
    private final LogManager logManager = new LogManager();
    private final AtomicInteger threadCounter = new AtomicInteger(0);

    public CommitLog() {
        threadNumber = DEFAULT_THREAD_NUMBER;
        executorService = Executors.newFixedThreadPool(threadNumber);
        availableLogs = new ArrayBlockingQueue<LogDescriptor>(threadNumber);

        openLogs(threadNumber);
    }

    public CommitLog(int threadNumber) {
        this.threadNumber = threadNumber;
        this.executorService = Executors.newFixedThreadPool(threadNumber);
        availableLogs = new ArrayBlockingQueue<LogDescriptor>(threadNumber);

        openLogs(threadNumber);
    }

    public void closeLogFiles() {
        for (LogDescriptor descriptor: availableLogs) {
            logManager.closeLogFiles(descriptor);
        }
    }

    /**
     * Current method opens for write necessary number of log files and
     * add all logs to availableLogs collection.
     *
     * @param logsNumber number of log files to open for write.
     */
    private void openLogs(int logsNumber) {
        try {
            for (int i=0; i<logsNumber; i++) {
                String filename = LOGFILE_PREFIX + currentGrp() + i + LOGFILE_POSTFIX;
                LogDescriptor descriptor = logManager.openLogFile(filename, LogDescriptor.MODE_WRITE);
                availableLogs.put(descriptor);
            }
        } catch (InterruptedException e) {
            log.error(e);
        }
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
        LogDescriptor descriptor = availableLogs.remove();
        descriptor.logFos.write(LogRecord.toByteArray(record));
        descriptor.logFos.flush();
        availableLogs.add(descriptor);
    }

//    private void writeHeader(FileOutputStream logFos, int recordsCommitted) throws IOException {
//        byte[] header = new byte[32];
//        ByteUtils.putInt(recordsCommitted, header, 0);
//        logFos.write(header, 0, 2);
//        logFos.flush();
//    }

//    private int readHeader(FileInputStream fis) throws IOException {
//        byte[] header = new byte[32];
//        fis.read(header, 0, 32);
//        int recNum = ByteUtils.getInt(header, 0);
//        return recNum;
//    }

    private String currentGrp() {
        return LOGFILE_GRP_A;
    }
}
