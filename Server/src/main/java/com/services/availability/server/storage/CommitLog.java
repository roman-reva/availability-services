package com.services.availability.server.storage;

import org.apache.log4j.Logger;

import java.io.*;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-15 17:44
 */
public class CommitLog {
    private Logger log = Logger.getLogger(CommitLog.class);

    private final static String FILE_PREFIX = "dblog_";
    private final static String FILE_EXTENTION = ".dat";

    private File logFile = new File(FILE_PREFIX + "0" + FILE_EXTENTION);

    private FileOutputStream fos = null;

//    private int logFilesNumber = 5;
//    private int currentLog = 0;

    public CommitLog() {
//        for (int idx = 0; idx < logFilesNumber; idx++) {
//            File file = new File(FILE_PREFIX + idx + FILE_EXTENTION);
//            if (file.exists()) currentLog = idx;
//        }

        try {
            fos = new FileOutputStream(logFile);
        } catch (FileNotFoundException e) {
            log.error(e);
            throw new RuntimeException("Not able to open log file", e);
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

    public void closeLogFiles() {
        try {
            fos.close();
        } catch (IOException e) {
            log.error(e);
        }
    }

    private void writeRecord(LogRecord record) throws IOException {
        fos.write(LogRecord.toByteArray(record));
        fos.flush();
    }

    private static class LogRecord {
        public static final byte TYPE_PUT = 1;
        public static final byte TYPE_REMOVE = 2;

        protected byte type;
        protected byte committed = 0;
        protected long key;
        protected int sku;
        protected short store;
        protected int amount;

        private LogRecord(byte type, long key) {
            this.type = type;
            this.key = key;
        }

        private LogRecord(byte type, byte committed, long key, int sku, short store, int amount) {
            this.type = type;
            this.committed = committed;
            this.key = key;
            this.sku = sku;
            this.store = store;
            this.amount = amount;
        }

        private LogRecord(byte type, long key, int sku, short store, int amount) {
            this.type = type;
            this.key = key;
            this.sku = sku;
            this.store = store;
            this.amount = amount;
        }

        public boolean isPutRecord() {
            return type == TYPE_PUT;
        }

        public boolean isRemoveRecord() {
            return type == TYPE_REMOVE;
        }

        public static byte[] toByteArray(LogRecord record) {
            byte[] byteRecord = new byte[32];

            byteRecord[0] = record.type;
            byteRecord[1] = record.committed;
            HashMMap.ByteUtils.putLong(record.key, byteRecord, 1);
            HashMMap.ByteUtils.putInt(record.sku, byteRecord, 9);
            HashMMap.ByteUtils.putShort(record.store, byteRecord, 13);
            HashMMap.ByteUtils.putInt(record.amount, byteRecord, 15);
            HashMMap.ByteUtils.putInt(record.getControlSum(), byteRecord, 28);

            return byteRecord;
        }

        public static LogRecord fromByteArray(byte[] byteRecord) {
            byte type = byteRecord[0];
            byte committed = byteRecord[1];
            long key = HashMMap.ByteUtils.getLong(byteRecord, 2);
            int sku = HashMMap.ByteUtils.getInt(byteRecord, 10);
            short store = HashMMap.ByteUtils.getShort(byteRecord, 14);
            int amount = HashMMap.ByteUtils.getInt(byteRecord, 16);

            LogRecord logRecord = new LogRecord(type, committed, key, sku, store, amount);

            int checkSum = HashMMap.ByteUtils.getInt(byteRecord, 28);
            if (logRecord.getControlSum() != checkSum) throw new RuntimeException("Corrupted log file");

            return logRecord;
        }

        private int getControlSum() {
            int result = (int) type;
            result = 31 * result + committed;
            result = 31 * result + (int) (key ^ (key >>> 32));
            result = 31 * result + sku;
            result = 31 * result + (int) store;
            result = 31 * result + amount;
            return result;
        }
    }

}
