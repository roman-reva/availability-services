package com.services.availability.storage.commitlog;

import com.services.availability.model.AvailabilityItem;
import com.services.availability.utils.ByteUtils;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-15 17:44
 */
public class CommitLog {
    private Logger log = Logger.getLogger(CommitLog.class);

    private final static int MAX_RECORDS_IN_LOG = 50000;

    private final static String META_FILE = "meta.dat";
    private final static String FILE_PREFIX = "dblog_";
    private final static String FILE_EXTENTION = ".dat";

    private final static int LOG_FILES_NUMBER = 20;

    private int recordCounter = 0;
    private int currentLog = 0;

    private File logFile;
    private FileInputStream logFis = null;
    private FileOutputStream logFos = null;

    private File metaFile;
    private FileInputStream metaFis = null;
    private FileOutputStream metaFos = null;

    public CommitLog() {
        initMetaFile();
        openLogFile();
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
            boolean logFisClosed=false, logFosClosed=false, metaFisClosed=false, metaFosClosed=false;
            if (logFis != null) {
                logFis.close();
                logFisClosed = true;
            }
            if (logFos != null) {
                logFos.close();
                logFosClosed = true;
            }
            if (metaFis != null) {
                metaFis.close();
                metaFisClosed = true;
            }
            if (metaFos != null) {
                metaFos.close();
                metaFosClosed = true;
            }

            log.debug("logFisClosed="+logFisClosed+", logFosClosed="+logFosClosed+", metaFisClosed="+metaFisClosed+", metaFosClosed="+metaFosClosed);
        } catch (IOException e) {
            log.error(e);
        }
    }

    private void initMetaFile() {
        metaFile = new File(META_FILE);
        try {
            boolean fileCreated = metaFile.createNewFile();
            metaFos = new FileOutputStream(metaFile);
            metaFis = new FileInputStream(metaFile);

            byte[] bytes = new byte[4];
            if (fileCreated) {
                ByteUtils.putInt(recordCounter, bytes, 0);
                metaFos.write(bytes, 0, 4);
                metaFos.flush();
            } else {
                if (metaFile.length() != 4) throw new IllegalStateException("Meta file data is corrupted, fileSize = " + metaFile.length());
                int readBytes = metaFis.read(bytes, 0, 4);
                if (readBytes != 4) throw new IllegalStateException("Meta file data is corrupted, readBytes = " + readBytes);

                currentLog = ByteUtils.getInt(bytes, 0);
                if (currentLog < 0) throw new IllegalStateException("Meta file data is corrupted");
            }
        } catch (FileNotFoundException e) {
            log.error(e);
            throw new RuntimeException("Not able to open log file", e);
        } catch (IOException e) {
            log.error(e);
        }
    }

    private void openLogFile() {
        logFile = new File(FILE_PREFIX + currentLog + FILE_EXTENTION);
        boolean fileExists = logFile.exists();
        try {
            if (!fileExists) {
                boolean created = logFile.createNewFile();
                if (!created) throw new IllegalStateException("Cannot create log file; filename=" + logFile.getName());
                log.debug("New log file created; filename = " + logFile.getName());
            }

            logFos = new FileOutputStream(logFile);
            logFis = new FileInputStream(logFile);

            if (fileExists) {
                verifyIntegrity();
            } else {
                initLogHeader();
            }
        } catch (FileNotFoundException e) {
            log.error(e);
            throw new RuntimeException("Not able to open log file", e);
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    private void verifyIntegrity() throws IOException {
        long fileSize = logFile.length();
        if (fileSize % 32 != 0) throw new IllegalStateException("File length is suspicious");
        if (fileSize > (long)Integer.MAX_VALUE) throw new IllegalStateException("Log file is extremely large");

        int recordsToVerify = 100;
        int contentSize = (int) fileSize - 32;
        int bufferSize = (contentSize < 32 * recordsToVerify) ? contentSize : 32 * recordsToVerify;

        int offset = (contentSize < 32 * recordsToVerify) ? 32 : 32 + contentSize - bufferSize;
        byte[] buffer = new byte[bufferSize];

        int bytesRead = logFis.read(buffer, offset, bufferSize);
        if (bytesRead != bufferSize) throw new IllegalStateException("Could not read last records, byteNum = " + bufferSize);

        byte[] record = new byte[32];
        for (int i=0; i<recordsToVerify; i++) {
            System.arraycopy(buffer, i * 32, record, 0, 32);
            LogRecord.fromByteArray(record);                    // verifying control sum
        }
    }

    private void initLogHeader() throws IOException {
        writeHeader(0);
    }

    private void writeRecord(LogRecord record) throws IOException {
        logFos.write(LogRecord.toByteArray(record));
        logFos.flush();
    }

    private void writeHeader(int recordsCommitted) throws IOException {
        byte[] header = new byte[32];
        ByteUtils.putInt(recordsCommitted, header, 0);
        logFos.write(header);
        logFos.flush();
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
            ByteUtils.putLong(record.key, byteRecord, 1);
            ByteUtils.putInt(record.sku, byteRecord, 9);
            ByteUtils.putShort(record.store, byteRecord, 13);
            ByteUtils.putInt(record.amount, byteRecord, 15);
            ByteUtils.putInt(record.getControlSum(), byteRecord, 28);

            return byteRecord;
        }

        public static LogRecord fromByteArray(byte[] byteRecord) {
            byte type = byteRecord[0];
            byte committed = byteRecord[1];
            long key = ByteUtils.getLong(byteRecord, 2);
            int sku = ByteUtils.getInt(byteRecord, 10);
            short store = ByteUtils.getShort(byteRecord, 14);
            int amount = ByteUtils.getInt(byteRecord, 16);

            LogRecord logRecord = new LogRecord(type, committed, key, sku, store, amount);

            int checkSum = ByteUtils.getInt(byteRecord, 28);
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
