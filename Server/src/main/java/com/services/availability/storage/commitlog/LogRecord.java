package com.services.availability.storage.commitlog;

import com.services.availability.utils.ByteUtils;

/**
 * Current class represents a record in a commit log. Each record
 * represents  either PUT, or REMOVE operation that should be
 * performed on a persistent storage.
 *
 * @author Roman Reva
 * @version 1.0
 * @since 2014-08-01 18:36
 */
public class LogRecord {
    public static final byte TYPE_PUT = 1;                  // put operation
    public static final byte TYPE_REMOVE = 2;               // remove operation

    protected byte type;                                    // operation type
    protected byte committed = 0;                           // if record is transferred to the persistent storage
    protected long timestamp = 0;                           // when the record was created

    protected long key;                                     // item key
    protected int sku;                                      // sku number
    protected short store;                                  // store number
    protected int amount;                                   // amount of sku left

    /**
     * Constructor. Sets current time as a timestamp, committed=0 and
     * the rest of params according to the arguments.
     *
     * @param type TYPE_PUT or TYPE_REMOVE constant
     * @param key item key
     */
    public LogRecord(byte type, long key) {
        if (type != TYPE_REMOVE) throw new IllegalArgumentException("Only type=TYPE_REMOVE is acceptible");

        this.timestamp = System.nanoTime();
        this.type = type;
        this.key = key;
    }

    /**
     * Constructor.
     *
     * @param type TYPE_PUT or TYPE_REMOVE constant
     * @param committed 1 if the record was submitted to the storage, 0 otherwise
     * @param key item key
     * @param sku item sku
     * @param store store number
     * @param amount amount of item left
     * @param timestamp record creation timestamp
     */
    public LogRecord(byte type, byte committed, long key, int sku, short store, int amount, long timestamp) {
        this.type = type;
        this.committed = committed;
        this.key = key;
        this.sku = sku;
        this.store = store;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    /**
     * Constructor. Sets current time as a timestamp, committed=0 and
     * the rest of params according to the arguments.
     *
     * @param type TYPE_PUT or TYPE_REMOVE constant
     * @param key item key
     * @param sku item sku
     * @param store store number
     * @param amount amount of item left
     */
    public LogRecord(byte type, long key, int sku, short store, int amount) {
        this.timestamp = System.nanoTime();
        this.type = type;
        this.key = key;
        this.sku = sku;
        this.store = store;
        this.amount = amount;
    }

    /**
     * Verifies record type.
     *
     * @return true, if record has type = TYPE_PUT
     */
    public boolean isPutRecord() {
        return type == TYPE_PUT;
    }

    /**
     * Verifies record type.
     *
     * @return true, if record has type = TYPE_REMOVE
     */
    public boolean isRemoveRecord() {
        return type == TYPE_REMOVE;
    }

    /**
     * Performs serialization of LogRecord object to byte array.
     *
     * @param record to convert
     * @return array of bytes
     */
    public static byte[] toByteArray(LogRecord record) {
        byte[] byteRecord = new byte[32];

        byteRecord[0] = record.type;
        byteRecord[1] = record.committed;
        ByteUtils.putLong(record.key, byteRecord, 2);
        ByteUtils.putInt(record.sku, byteRecord, 10);
        ByteUtils.putShort(record.store, byteRecord, 14);
        ByteUtils.putInt(record.amount, byteRecord, 16);

        ByteUtils.putLong(record.timestamp, byteRecord, 20);
        ByteUtils.putInt(record.getControlSum(), byteRecord, 28);

        return byteRecord;
    }

    /**
     * Performs deserialization of byte array to LogRecord object.
     *
     * @param byteRecord array of bytes
     * @return LogRecord object
     */
    public static LogRecord fromByteArray(byte[] byteRecord) {
        byte type = byteRecord[0];
        byte committed = byteRecord[1];
        long key = ByteUtils.getLong(byteRecord, 2);
        int sku = ByteUtils.getInt(byteRecord, 10);
        short store = ByteUtils.getShort(byteRecord, 14);
        int amount = ByteUtils.getInt(byteRecord, 16);
        long timestamp = ByteUtils.getLong(byteRecord, 20);

        LogRecord logRecord = new LogRecord(type, committed, key, sku, store, amount, timestamp);

        int checkSum = ByteUtils.getInt(byteRecord, 28);
        if (logRecord.getControlSum() != checkSum) throw new RuntimeException("Corrupted log file");

        return logRecord;
    }

    /**
     * Calculates control sum for the record. Is used to verify record
     * integrity after deserialization.
     *
     * @return control sum for the record
     */
    private int getControlSum() {
        int result = (int) type;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + committed;
        result = 31 * result + (int) (key ^ (key >>> 32));
        result = 31 * result + sku;
        result = 31 * result + (int) store;
        result = 31 * result + amount;
        return result;
    }
}
