package com.services.availability.storage.hashmmap;

import com.services.availability.utils.ByteUtils;

/**
 * Current class provides utility methods to operate with binary records
 * like with an object.
 *
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-17 17:12
 */
final class BinaryRecord {
    public static final int RECORD_SIZE = 8 + 4 + 2 + 4;    // key:long + sku:int + store:short + amount:int = 18

    public static final int RECORD_KEY_OFFSET = 0;          // long
    public static final int RECORD_SKU_OFFSET = 8;          // int
    public static final int RECORD_STORE_OFFSET = 12;       // short
    public static final int RECORD_AMOUNT_OFFSET = 14;      // int

    private BinaryRecord(){}

    public static void setKey(long value, byte [] record) {
        verifyRecord(record);
        ByteUtils.putLong(value, record, RECORD_KEY_OFFSET);
    }

    public static long getKey(byte [] record) {
        verifyRecord(record);
        return ByteUtils.getLong(record, RECORD_KEY_OFFSET);
    }

    public static void setAmount(int value, byte[] record) {
        verifyRecord(record);
        ByteUtils.putInt(value, record, RECORD_AMOUNT_OFFSET);
    }

    public static int getAmount(byte[] record) {
        verifyRecord(record);
        return ByteUtils.getInt(record, RECORD_AMOUNT_OFFSET);
    }

    public static void setSku(int value, byte[] record) {
        verifyRecord(record);
        ByteUtils.putInt(value, record, RECORD_SKU_OFFSET);
    }

    public static int getSku(byte[] record) {
        verifyRecord(record);
        return ByteUtils.getInt(record, RECORD_SKU_OFFSET);
    }

    public static void setStore(short value, byte[] record) {
        verifyRecord(record);
        ByteUtils.putShort(value, record, RECORD_STORE_OFFSET);
    }

    public static short getStore(byte[] record) {
        verifyRecord(record);
        return ByteUtils.getShort(record, RECORD_STORE_OFFSET);
    }

    private static void verifyRecord(byte[] record) {
        if (record.length != RECORD_SIZE)
            throw new IllegalArgumentException("Record size constraint violation (" + record.length + " instead of " + RECORD_SIZE + ")");
    }
}