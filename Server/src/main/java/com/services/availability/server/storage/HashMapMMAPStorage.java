package com.services.availability.server.storage;

import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-01 14:28
 */
public class HashMapMMAPStorage implements Storage {
    public static final int STORAGE_SIZE = 1024*1024*1024;  // 1 GB
    public static final int RECORD_SIZE = 8 + 4 + 2 + 4;    // key(long) + sku(int) + store(short) + amount(int) = 18

    public static final int STORAGE_HEADER_SIZE = 8;             // buckets number (int) + bucket capacity (int)

    public static final int INITIAL_BUCKET_NUMBER = 1024 * 1024;
    public static final int BUCKET_CAPACITY = 32;
    public static final int BUCKET_SIZE = BUCKET_CAPACITY * RECORD_SIZE + 4;    // 4 for number of records, total 580

    private boolean forceWrites = true;

    private Logger log = Logger.getLogger(HashMapMMAPStorage.class);

    private final MappedByteBuffer buffer;
    private int bucketNumber;

    public HashMapMMAPStorage() {
        this.bucketNumber = INITIAL_BUCKET_NUMBER;
        MappedByteBuffer buffer = null;
        try {
            RandomAccessFile file = new RandomAccessFile("data.dat", "rw");
            FileChannel fileChannel = file.getChannel();
            buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, STORAGE_SIZE);
        } catch (FileNotFoundException e) {
            log.error(e);
        } catch (IOException e) {
            log.error(e);
        }

        this.buffer = buffer;
        initBuckets();
    }

    /**
     * Current method creates initial number of buckets in the byte buffer.
     */
    private void initBuckets() {
        buffer.putInt(bucketNumber);
        buffer.putInt(BUCKET_CAPACITY);

        byte [] bucket = new byte[BUCKET_SIZE];
        BinaryBucket.setSize(0, bucket);
        for (int bktIdx=0; bktIdx<INITIAL_BUCKET_NUMBER; bktIdx++) {
            int bktOffset = STORAGE_HEADER_SIZE + bktIdx * BUCKET_SIZE;
            for (int byteIdx=0; byteIdx<bucket.length; byteIdx++)
                buffer.put(bktOffset + byteIdx, bucket[byteIdx]);
        }

        if (forceWrites) buffer.force();
    }

    @Override
    public void put(long key, AvailabilityItem value) {
        int bucketIdx = getBucketIdxByKey(key);
        byte[] bucket = readBucketByIdx(bucketIdx),
                record = new byte[RECORD_SIZE];

        int recordIdx = lookupInBucket(key, bucket, record);

        BinaryRecord.setKey(key, record);
        BinaryRecord.setSku(value.getSku(), record);
        BinaryRecord.setStore(value.getStore(), record);
        BinaryRecord.setAmount(value.getAmount(), record);

        if (recordIdx >= 0) {                               // if record with such key already exists
            writeRecordByIdx(bucketIdx, recordIdx, record); // then updating the record data
            if (forceWrites) buffer.force();
            return;
        }

        // otherwise, if record was not found
        int bucketSize = BinaryBucket.getSize(bucket);
        if (bucketSize >= BUCKET_CAPACITY) {
            throw new RuntimeException("Bucket(idx=" + bucketIdx + ") is overflowed");
        }
        writeRecordByIdx(bucketIdx, bucketSize, record);    // writing the record to the end of the bucket
        writeBucketSize(bucketIdx, bucketSize + 1);         // and updating the bucket size

        if (forceWrites) buffer.force();
    }

    @Override
    public AvailabilityItem get(long key) {
        int bucketIdx = getBucketIdxByKey(key);
        byte[] bucket = readBucketByIdx(bucketIdx),
                binaryRecord = new byte[RECORD_SIZE];

        int recordIdx = lookupInBucket(key, bucket, binaryRecord);

        // not found
        if (recordIdx < 0) return null;

        return new AvailabilityItem(
                BinaryRecord.getSku(binaryRecord),
                BinaryRecord.getStore(binaryRecord),
                BinaryRecord.getAmount(binaryRecord)
        );
    }


    @Override
    public AvailabilityItem remove(long key) {
        int bucketIdx = getBucketIdxByKey(key);
        byte[] bucket = readBucketByIdx(bucketIdx),
                record = new byte[RECORD_SIZE];

        int recordIdx = lookupInBucket(key, bucket, record);
        if (recordIdx < 0) return null;

        AvailabilityItem item = new AvailabilityItem(
                BinaryRecord.getSku(record),
                BinaryRecord.getStore(record),
                BinaryRecord.getAmount(record)
        );

        int bucketSize = BinaryBucket.getSize(bucket);
        if (recordIdx == bucketSize - 1) {                                      // if it's a last record in bucket
            writeRecordByIdx(bucketIdx, recordIdx, new byte[RECORD_SIZE]);      // replacing it with zeros
        } else {                                                                // otherwise
            moveRecordByIdx(bucketIdx, bucketSize - 1, recordIdx);              // moving the last record insted of this one
        }
        writeBucketSize(bucketIdx, bucketSize - 1);                             // and updating the bucket size
        if (forceWrites) buffer.force();

        return item;
    }

    /**
     * Performs lookup for a record in the provided bucket. If record exists,
     * performs copy of the record bytes into the targetBinaryRecord param
     * and returns record index in the bucket. Otherwise, returns -1.
     *
     * @param key key of the record you're looking for
     * @param bucket target bucket as byte array
     * @param targetBinaryRecord destination array
     * @return record index in the bucket
     */
    private int lookupInBucket(long key, byte[] bucket, byte[] targetBinaryRecord) {
        int bucketSize = BinaryBucket.getSize(bucket);
        byte[] record = new byte[RECORD_SIZE];
        for (int rcdIdx = 0; rcdIdx < bucketSize; rcdIdx++) {
            readRecordFromBucket(rcdIdx, bucket, record);
            if (BinaryRecord.getKey(record) == key) {
                System.arraycopy(record, 0, targetBinaryRecord, 0, RECORD_SIZE);
                return rcdIdx;
            }
        }

        return -1;
    }

    /**
     * Calculates bucket index for the provided key.
     *
     * @param key target key
     * @return bucket index
     */
    protected int getBucketIdxByKey(long key) {
        int hashCode = Math.abs(AvailabilityItem.keyToHashCode(key));
        return hashCode % bucketNumber;
    }

    /**
     * Performs copy of the target record with index `rcdIdx` from the `bucket`
     * into the array `targetRcd`.
     *
     * @param rcdIdx target record index
     * @param bucket source bucket
     * @param targetRcd target record array
     */
    private void readRecordFromBucket(int rcdIdx, byte[] bucket, byte [] targetRcd) {
        if (rcdIdx >= BUCKET_CAPACITY) throw new IllegalArgumentException("rcdIdx >= BUCKET_CAPACITY");
        if (bucket.length != BUCKET_SIZE) throw new IllegalArgumentException("Invalid bucket size");
        if (targetRcd.length != RECORD_SIZE) throw new IllegalArgumentException("Invalid record size");

        int recordOffset = BinaryBucket.BUCKET_FIRST_RECORD_OFFSET + RECORD_SIZE * rcdIdx;
        System.arraycopy(bucket, recordOffset, targetRcd, 0, RECORD_SIZE);
    }

    /**
     * Fetches a bucket in a binary format by it's index (ordinal number).
     *
     * @param bktIdx bucket ordinal number
     * @return bucket as a byte array
     */
    private byte[] readBucketByIdx(int bktIdx) {
        if (bktIdx >= bucketNumber) throw new IllegalArgumentException("bktIdx >= bucketNumber");

        int bktAddress = STORAGE_HEADER_SIZE + bktIdx * BUCKET_SIZE;
        return readBucket(bktAddress);
    }

    /**
     * Current method reads bucket by the specified bktAddress.
     *
     * @param bktAddress address of the record
     * @return byte array containing record in a binary format
     */
    private byte[] readBucket(int bktAddress) {
        byte[] byteBucket = new byte[BUCKET_SIZE];
        for (int bufAddress = bktAddress, byteIdx = 0; bufAddress < bktAddress + BUCKET_SIZE; bufAddress++) {
            try {
                byteBucket[byteIdx++] = buffer.get(bufAddress);
            } catch (IndexOutOfBoundsException e) {
                log.error("Error reading buffer at position " + bufAddress + " (bktAddress = " + bktAddress + ")", e);
                System.exit(0);
            }
        }

        return byteBucket;
    }

    /**
     * Writes the record into the specified bucket on the specified position.
     *
     * @param bucketIdx bucket index
     * @param rcdIdx record index
     * @param rcd record as a byte array
     */
    private void writeRecordByIdx(int bucketIdx, int rcdIdx, byte[] rcd) {
        int bktAddress = STORAGE_HEADER_SIZE + bucketIdx * BUCKET_SIZE;
        int rcdAddress = bktAddress + BinaryBucket.BUCKET_FIRST_RECORD_OFFSET + rcdIdx * RECORD_SIZE;
        writeRecord(rcdAddress, rcd);
    }

    /**
     * Writes the record into the buffer by the specified address.
     * @param rcdAddress record address
     * @param rcd record as a byte array
     */
    private void writeRecord(int rcdAddress, byte[] rcd) {
        if (rcd.length != RECORD_SIZE) throw new IllegalArgumentException("rcd.length != RECORD_SIZE");

        for (int bufAddress = rcdAddress, byteIdx = 0; bufAddress < rcdAddress + RECORD_SIZE; bufAddress++) {
            buffer.put(bufAddress, rcd[byteIdx++]);
        }
    }

    /**
     * Moves record from one position to another inside the same bucket,
     * identified by indexes.
     *
     * @param bktIdx bucket index
     * @param fromRcdIdx current record index
     * @param toRcdIdx new record index
     */
    private void moveRecordByIdx(int bktIdx, int fromRcdIdx, int toRcdIdx) {
        int bucketAddress = STORAGE_HEADER_SIZE + bktIdx * BUCKET_SIZE;
        int fromAddress = bucketAddress + BinaryBucket.BUCKET_FIRST_RECORD_OFFSET + fromRcdIdx * RECORD_SIZE;
        int toAddress = bucketAddress + BinaryBucket.BUCKET_FIRST_RECORD_OFFSET + toRcdIdx * RECORD_SIZE;

        moveRecord(fromAddress, toAddress);
    }

    /**
     * Moves the record from one address to another address in the buffer.
     * Old address is filled with zeros afterwards.
     *
     * @param fromAddress source record address
     * @param toAddress destination address
     */
    private void moveRecord(int fromAddress, int toAddress) {
        for (int byteIdx = 0; byteIdx < RECORD_SIZE; byteIdx++) {
            int currentFromAddress = fromAddress + byteIdx;
            buffer.put(toAddress + byteIdx, buffer.get(currentFromAddress));
            buffer.put(currentFromAddress, (byte)0);
        }
    }

    /**
     * Updates the bucket size in the buffer.
     *
     * @param bucketIdx bucket index
     * @param newSize size
     */
    private void writeBucketSize(int bucketIdx, int newSize) {
        int bktAddress = STORAGE_HEADER_SIZE + bucketIdx * BUCKET_SIZE;
        int bktSizeAddress = bktAddress + BinaryBucket.BUCKET_SIZE_OFFSET;
        buffer.putInt(bktSizeAddress, newSize);
    }

    /**
     * Current class provides utility methods to operate with binary buckets
     * like with an object.
     */
    protected final static class BinaryBucket {
        public static final int BUCKET_SIZE_OFFSET = 0;             // int
        public static final int BUCKET_FIRST_RECORD_OFFSET = 4;     // the rest

        private BinaryBucket(){}

        public static void setSize(int size, byte[] bucket) {
            verifyBucket(bucket);
            ByteUtils.putInt(size, bucket, BUCKET_SIZE_OFFSET);
        }

        public static int getSize(byte[] bucket) {
            verifyBucket(bucket);
            int bucketSize = ByteUtils.getInt(bucket, BUCKET_SIZE_OFFSET);
            return bucketSize;
        }

        private static void verifyBucket(byte[] bucket) {
            if (bucket.length != BUCKET_SIZE)
                throw new IllegalArgumentException("Record size constraint violation (" + bucket.length + " instead of " + RECORD_SIZE + ")");
        }
    }

    /**
     * Current class provides utility methods to operate with binary records
     * like with an object.
     */
    protected final static class BinaryRecord {
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

    /**
     * Current class provides utility methods to perform basic operations
     * on byte arrays.
     */
    protected final static class ByteUtils {
        public static void putInt(int value, byte[] record, int offset) {
            for (int i=offset, j=3; i<offset+4; i++, j--) {
                record[i] = (byte)(0xff & (value >> (j*8)));
            }
        }

        public static int getInt(byte[] record, int offset) {
            int value = 0;
            for (int i=offset, j=3; i<offset+4; i++, j--) {
                value |= ((record[i] & 0xff) << (j*8));
            }
            return value;
        }

        public static void putShort(short value, byte[] record, int offset) {
            for (int i=offset, j=1; i<offset+2; i++, j--) {
                record[i] = (byte)(0xff & (value >> (j*8)));
            }
        }

        public static short getShort(byte[] record, int offset) {
            short value = 0;
            for (int i=offset, j=1; i<offset+2; i++, j--) {
                value |= ((record[i] & 0xff) << (j*8));
            }
            return value;
        }

        public static void putLong(long value, byte[] record, int offset) {
            for (int i=offset, j=7; i<offset+8; i++, j--) {
                record[i]   = (byte)(0xff & (value >> (j*8)));
            }
        }

        public static long getLong(byte[] array, int offset) {
            long value = 0;
            for (int i=offset, j=7; i<offset+8; i++, j--) {
                value |= ((long)(array[i] & 0xff) << (j*8));
            }
            return value;
        }
    }

    private static byte[] getFirstBytes(MappedByteBuffer buffer, int n) {
        byte[] bytes = new byte[n];
        for (int i=0; i<n; i++) {
            bytes[i] = buffer.get(i);
        }
        return bytes;
    }
}
