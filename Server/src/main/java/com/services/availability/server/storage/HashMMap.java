package com.services.availability.server.storage;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-08 14:43
 */
public class HashMMap {
    public static final String STORAGE_FILE = "data.dat";       // storage filename

    public static final int STORAGE_SIZE = Integer.MAX_VALUE;   // 2 GB - 1b
    public static final int STORAGE_HEADER_SIZE = 8;            // bucketNumber:int + bucketCapacity:int
    public static final int STORAGE_BUCKET_NUM_OFFSET = 0;      // bucketNumber:int + bucketCapacity:int
    public static final int STORAGE_BUCKET_CAP_OFFSET = 4;      // bucketNumber:int + bucketCapacity:int

    public static final int DEFAULT_INITIAL_BUCKET_NUMBER = 256;
    public static final int DEFAULT_BUCKET_CAPACITY = 32;
    public static final int RESIZE_COEFFICIENT = 4;             // resize multiplier

    private static Logger log = Logger.getLogger(HashMMap.class);

    protected final MappedByteBuffer mappedBuffer;              // main storage buffer, mmaped to the file system
    protected ByteBuffer tmpBuffer;                             // temporary buffer, allocated during the resize operation

    protected final int bucketCapacity;                         // number of records in a bucket
    protected final int bucketSize;                             // number of bytes allocated for bucket
    protected volatile int bucketNumber;                        // current number of buckets in the storage

    /**
     * Initializes the storage. First an attempt to restore data from file
     * is made.
     *
     * If file exists its structure is verified. If errors in headers are found, an
     * exception is thrown. Otherwise, file is considered as valid and is used as storage.
     *
     * If file doesn't exist, it is been created, the data structure is initialized and
     * storage parameters to the default values.
     */
    public HashMMap() {
        boolean storageExists = storageFileExists();
        this.mappedBuffer = bindMappedBuffer();

        if (storageExists) {
            this.bucketNumber = readStorageBktNum(mappedBuffer);
            this.bucketCapacity = readStorageBktCapacity(mappedBuffer);
            this.bucketSize = bucketCapacity * BinaryRecord.RECORD_SIZE + BinaryBucket.BUCKET_HEADER_SIZE;

            verifyNonEmptyMappedBuffer();
        } else {
            this.bucketNumber = getDefaultInitialBucketNumber();
            this.bucketCapacity = DEFAULT_BUCKET_CAPACITY;
            this.bucketSize = bucketCapacity * BinaryRecord.RECORD_SIZE + BinaryBucket.BUCKET_HEADER_SIZE;

            verifyAllocatedSpace(bucketNumber);     // verifying that storage file has enough space for data structure
            initEmptyMappedBuffer();
        }
    }

    /**
     * Initializes the storage. First an attempt to restore data from file
     * is made.
     *
     * If file exists its structure is verified. If errors in headers are found, an
     * exception is thrown. Otherwise, file is considered as valid and is used as storage.
     *
     * If file doesn't exist, it is been created, the data structure is initialized and
     * storage parameters to the specified values.
     *
     * @param initialBucketNumber initial number of buckets
     * @param bucketCapacity number of records in a bucket
     */
    public HashMMap(int initialBucketNumber, int bucketCapacity) {
        boolean storageExists = storageFileExists();
        this.mappedBuffer = bindMappedBuffer();

        if (storageExists) {
            this.bucketNumber = readStorageBktNum(mappedBuffer);
            this.bucketCapacity = readStorageBktCapacity(mappedBuffer);
            this.bucketSize = bucketCapacity * BinaryRecord.RECORD_SIZE + BinaryBucket.BUCKET_HEADER_SIZE;

            verifyNonEmptyMappedBuffer();
        } else {
            this.bucketNumber = initialBucketNumber;
            this.bucketCapacity = bucketCapacity;
            this.bucketSize = bucketCapacity * BinaryRecord.RECORD_SIZE + BinaryBucket.BUCKET_HEADER_SIZE;

            verifyAllocatedSpace(bucketNumber);     // verifying that storage file has enough space for data structure
            initEmptyMappedBuffer();
        }
    }


    /**
     * Puts the element <i>value</i> into the collection by the specified <i>key</i>.
     *
     * @param key element key
     * @param value value to put
     */
    public void put(long key, AvailabilityItem value) {
        put(key, value, true);          // put element and flush buffer
    }

    /**
     * Performs put of all elements from the provided map.
     *
     * @param hashMap source map
     */
    public void putAll(Map<Long,AvailabilityItem> hashMap) {
        for (long key: hashMap.keySet()) {
            this.put(key, hashMap.get(key), false);
        }
//        mappedBuffer.force();
    }

    /**
     * Performs lookup of the provided key in the collection and
     * returns corresponding value if found. Otherwise, null is
     * returned.
     *
     * @param key requested key
     * @return AvailabilityItem corresponding to the current key
     */
    public AvailabilityItem get(long key) {
        int bucketIdx = getBucketIdxByKey(key);
        byte[] bucket = readBucketByIdx(bucketIdx, mappedBuffer),
                binaryRecord = new byte[BinaryRecord.RECORD_SIZE];

        int recordIdx = lookupInBucket(key, bucket, binaryRecord);

        // not found
        if (recordIdx < 0) return null;

        return new AvailabilityItem(
                BinaryRecord.getSku(binaryRecord),
                BinaryRecord.getStore(binaryRecord),
                BinaryRecord.getAmount(binaryRecord)
        );
    }

    /**
     * Performs lookup of the provided key in the collection. If found,
     * removes the corresponding value from the collection and returns it.
     * Otherwise, null is returned.
     *
     * @param key requested key
     * @return AvailabilityItem corresponding to the current key
     */
    public AvailabilityItem remove(long key) {
        int bucketIdx = getBucketIdxByKey(key);
        byte[] bucket = readBucketByIdx(bucketIdx, mappedBuffer),
                record = new byte[BinaryRecord.RECORD_SIZE];

        int recordIdx = lookupInBucket(key, bucket, record);
        if (recordIdx < 0) return null;

        AvailabilityItem item = new AvailabilityItem(
                BinaryRecord.getSku(record),
                BinaryRecord.getStore(record),
                BinaryRecord.getAmount(record)
        );

        int bucketSize = BinaryBucket.getSize(bucket);
        if (recordIdx == bucketSize - 1) {                                          // if it's a last record in bucket
            // replacing it with zeros
            writeRecordByIdx(bucketIdx, recordIdx, new byte[BinaryRecord.RECORD_SIZE], mappedBuffer);
        } else {                                                                    // otherwise
            moveRecordByIdx(bucketIdx, bucketSize - 1, recordIdx, mappedBuffer);    // moving the last record instead of this one
        }
        writeBucketSize(bucketIdx, bucketSize - 1, mappedBuffer);                   // and updating the bucket size
//        mappedBuffer.force();

        return item;
    }

    /**
     * Returns set of all containing keys in the HashMMap. If no
     * records are present, empty set is returned.
     *
     * Method iterates over the mmaped buffer and reads information
     * about each bucket, therefore is <i>extremely expensive</i>
     *
     * @return set of keys
     */
    public Set<Long> keySet() {
        Set<Long> keys = new HashSet<Long>();

        int bktSize, bktIdx, rcdIdx;
        byte[] bucket, record = new byte[BinaryRecord.RECORD_SIZE];
        for (bktIdx = 0; bktIdx < bucketNumber; bktIdx++) {
            bucket = readBucketByIdx(bktIdx, mappedBuffer);
            bktSize = BinaryBucket.getSize(bucket);
            for (rcdIdx = 0; rcdIdx < bktSize; rcdIdx++) {
                readRecordFromBucket(rcdIdx, bucket, record);
                keys.add(BinaryRecord.getKey(record));
            }
        }

        return keys;
    }

    /**
     * Removes all elements from the collection by reinitialization of
     * storage headers.
     */
    public void clear() {
        bucketNumber = getDefaultInitialBucketNumber();
        initEmptyMappedBuffer();

        log.debug("HashMMap cleared (bktNum = " + bucketNumber + ")");
    }


    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //                                                                                                          //
    //                                            INTERNAL METHODS                                              //
    //                                                                                                          //
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Verifies if storage file exists.
     * @return true, if exists
     */
    private boolean storageFileExists() {
        File file = new File(STORAGE_FILE);
        return file.exists();
    }

    /**
     * Allocates and initializes mmaped buffer.
     */
    private MappedByteBuffer bindMappedBuffer() {
        MappedByteBuffer buffer = null;
        try {
            RandomAccessFile file = new RandomAccessFile(STORAGE_FILE, "rw");
            FileChannel fileChannel = file.getChannel();
            buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, STORAGE_SIZE);
        } catch (FileNotFoundException e) {
            log.error(e);
        } catch (IOException e) {
            log.error(e);
        }

        return buffer;
    }

    /**
     * Verifies that there is enough space allocated in mapped buffer
     * to keep all required data.
     *
     * @param bucketNumber number of bucket
     */
    private void verifyAllocatedSpace(int bucketNumber) {
        int spaceRequired = bucketNumber * bucketSize + STORAGE_HEADER_SIZE;
        if (spaceRequired > STORAGE_SIZE) throw new IndexOutOfBoundsException("Not enough storage space for resize (" +
                spaceRequired + " bytes required, while storage has " + STORAGE_SIZE + " bytes allocated).");
    }

    /**
     * Verifies that storage has correct data structure and contains valid data.
     * Throws IllegalStateException in the case if the file is corrupted.
     */
    private void verifyNonEmptyMappedBuffer() {
        IllegalStateException corruptedException = new IllegalStateException("file `" + STORAGE_FILE + "` is corrupted");

        if (bucketNumber <= 0) {
            log.debug("storage file is corrupted");
            throw corruptedException;
        }
        if (bucketCapacity <= 0) {
            log.debug("storage file is corrupted");
            throw corruptedException;
        }

        int structureSize = STORAGE_HEADER_SIZE + bucketNumber * bucketSize;
        if (structureSize > STORAGE_SIZE) {
            log.debug("storage file is corrupted");
            throw corruptedException;
        }

        int bktSize, totalRcdNum = 0;
        byte[] bkt;
        for (int bktIdx=0; bktIdx<bucketNumber; bktIdx++) {
            bkt = readBucketByIdx(bktIdx, mappedBuffer);
            bktSize = BinaryBucket.getSize(bkt);

            if (bktSize < 0 || bktSize > bucketCapacity) {
                log.debug("storage file is corrupted");
                throw corruptedException;
            }
            totalRcdNum += bktSize;
        }

        log.debug("storage file is verified and looks fine (bktNum = " + bucketNumber + ", bktCap = " + bucketCapacity + ", rcdNum = " + totalRcdNum + ")");
    }


    /**
     * Current method initializes a an empty storage backed by mmaped buffer.
     * Sets up a storage header and a set of empty buckets in the buffer.
     */
    private void initEmptyMappedBuffer() {
        writeStorageHeader(mappedBuffer, bucketNumber, bucketCapacity);
        initBuckets(mappedBuffer, bucketNumber, STORAGE_HEADER_SIZE);           // initializing buckets

//        mappedBuffer.force();
    }

    /**
     * Current method initializes a storage header and a set of
     * buckets in the mapped byte buffer.
     *
     * @param buffer target buffer to initialize
     * @param bktNum number of buckets to initialize
     * @param bktCapacity bucket capacity
     */
    private void initEmptyByteBuffer(ByteBuffer buffer, int bktNum, int bktCapacity) {
        for (int i=0; i<buffer.capacity(); i++) buffer.put((byte)0);
        writeStorageHeader(buffer, bktNum, bktCapacity);
    }

    /**
     * Writes storage header data into the provided buffer.
     */
    private void writeStorageHeader(ByteBuffer buffer, int bktNum, int bktCapacity) {
        byte[] header = new byte[STORAGE_HEADER_SIZE];
        ByteUtils.putInt(bktNum, header, 0);
        ByteUtils.putInt(bktCapacity, header, 4);

        int byteIdx = 0;
        while (byteIdx < header.length) {
            byte b = header[byteIdx];
            buffer.put(byteIdx++, b);
        }
    }

    /**
     * Current method initializes a specified number of buckets in the provided
     * buffer starting from the offset.
     *
     * @param buffer target mapped buffer
     * @param bucketNumber number of buckets to initialize
     * @param offset write offset
     */
    private void initBuckets(ByteBuffer buffer, int bucketNumber, int offset) {
        byte [] bucket = new byte[bucketSize];
        BinaryBucket.setSize(0, bucket);
        for (int bktIdx = 0; bktIdx < bucketNumber; bktIdx++) {
            int bktOffset = offset + bktIdx * bucketSize;
            for (int byteIdx=0; byteIdx<bucket.length; byteIdx++)
                buffer.put(bktOffset + byteIdx, bucket[byteIdx]);
        }
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
        byte[] record = new byte[BinaryRecord.RECORD_SIZE];
        for (int rcdIdx = 0; rcdIdx < bucketSize; rcdIdx++) {
            readRecordFromBucket(rcdIdx, bucket, record);
            if (BinaryRecord.getKey(record) == key) {
                System.arraycopy(record, 0, targetBinaryRecord, 0, BinaryRecord.RECORD_SIZE);
                return rcdIdx;
            }
        }

        return -1;
    }

    protected void put(long key, AvailabilityItem value, boolean flush) {
        int bucketIdx = getBucketIdxByKey(key);
        byte[] bucket = readBucketByIdx(bucketIdx, mappedBuffer),
                record = new byte[BinaryRecord.RECORD_SIZE];

        int recordIdx = lookupInBucket(key, bucket, record);

        BinaryRecord.setKey(key, record);
        BinaryRecord.setSku(value.getSku(), record);
        BinaryRecord.setStore(value.getStore(), record);
        BinaryRecord.setAmount(value.getAmount(), record);

        if (recordIdx >= 0) {                                               // if record with such key already exists
            writeRecordByIdx(bucketIdx, recordIdx, record, mappedBuffer);   // then updating the record data
//            if (flush) mappedBuffer.force();
            return;
        }

        int bucketSize = BinaryBucket.getSize(bucket);                      // otherwise, if record was not found
        if (bucketSize < bucketCapacity) {                                  // and bucket is not full
            writeRecordByIdx(bucketIdx, bucketSize, record, mappedBuffer);  // writing the record to the end of the bucket
            writeBucketSize(bucketIdx, bucketSize + 1, mappedBuffer);       // and updating the bucket size

//            if (flush) mappedBuffer.force();
        } else {
            resize(bucketNumber * RESIZE_COEFFICIENT);      // increasing number of buckets in RESIZE_COEFFICIENT times
            put(key, value, flush);                         // and starting put() operation over
        }
    }

    /**
     * Calculates bucket index for the provided key.
     *
     * @param key target key
     * @return bucket index
     */
    protected int getBucketIdxByKey(long key) {
        return getBucketIdxByKey(key, bucketNumber);
    }

    /**
     * Calculates bucket index for the provided key and the specified
     * number of buckets.
     *
     * @param key target key
     * @param bucketNumber total number of buckets
     * @return bucket index
     */
    protected int getBucketIdxByKey(long key, int bucketNumber) {
        int hashCode = Math.abs(AvailabilityItem.keyToHashCode(key));
        return hashCode % bucketNumber;
    }

    /**
     * Increases number of buckets in the storage and redistributes all records
     * between the new buckets.
     *
     * Method creates a temporary byte buffer which is used as a temporary storage
     * during the resize operation. The <b>original mapped buffer remains untouched
     * until the all records are redistributed</b> between new buckets. Therefore it
     * can be used for reads when the resize is in progress.
     *
     * @param newBucketNumber new number of buckets
     */
    protected void resize(int newBucketNumber) {
        if (newBucketNumber <= bucketNumber) return;

        log.debug("Resize started");

//        mappedBuffer.force();
        verifyAllocatedSpace(newBucketNumber);
        prepareBuffers(newBucketNumber);

        long key;
        byte[] rcd = new byte[BinaryRecord.RECORD_SIZE], bkt, tgtBkt;
        int bktIdx, rcdIdx, tgtBktIdx, bktSize, tgtBktSize;

        for (bktIdx = 0; bktIdx < bucketNumber; bktIdx++) {                     // iterating over existing buckets
            bkt = readBucketByIdx(bktIdx, mappedBuffer);
            bktSize = BinaryBucket.getSize(bkt);
            for (rcdIdx = 0; rcdIdx < bktSize; rcdIdx++) {                      // iterating over current bucket records
                // and moving each record to a new bucket
                readRecordFromBucket(rcdIdx, bkt, rcd);
                key = BinaryRecord.getKey(rcd);
                tgtBktIdx = getBucketIdxByKey(key, newBucketNumber);

                ByteBuffer targetBuffer = (tgtBktIdx < bucketNumber) ? tmpBuffer : mappedBuffer;    // we should not override original buckets
                tgtBkt = readBucketByIdx(tgtBktIdx, targetBuffer);              // target bucket to place the record
                tgtBktSize = BinaryBucket.getSize(tgtBkt);                      // new record position
                writeRecordByIdx(tgtBktIdx, tgtBktSize, rcd, targetBuffer);     // writing record to the target bucket on targetBktSize position
                writeBucketSize(tgtBktIdx, tgtBktSize + 1, targetBuffer);       // and updating bucket size
            }
        }

        copyAndReleaseTmpBuffer();
        writeStorageHeader(mappedBuffer, newBucketNumber, bucketCapacity);
        bucketNumber = newBucketNumber;

//        mappedBuffer.force();

        log.debug("Resize finished (new bucket number is " + bucketNumber + ")");
    }

    /**
     * Current method prepares mapped buffer and temp buffer
     * for the resize operation.
     *
     * Required additional buckets in mapped buffer are initialized.
     * Temporary N buckets are created in temp buffer, where N is current
     * number of buckets in the mapped buffer.
     *
     * @param newBucketNumber total number of buckets in a buffer after resize
     */
    private void prepareBuffers(int newBucketNumber) {
        int mappedBufferSize = STORAGE_HEADER_SIZE + bucketNumber * bucketSize;
        int numOfBktToInit = newBucketNumber - bucketNumber;
        initBuckets(mappedBuffer, numOfBktToInit, mappedBufferSize);    // initializing additional buckets in the main buffer

        tmpBuffer = ByteBuffer.allocate(mappedBufferSize);              // additional buffer for existing buckets
        initEmptyByteBuffer(tmpBuffer, bucketNumber, bucketCapacity);       // initializing additional buffer
    }

    /**
     * Copies all data from the temp byte buffer into the mapped byte buffer
     * and releases temp buffer.
     */
    private void copyAndReleaseTmpBuffer() {
        int byteNum = tmpBuffer.capacity(),
                byteIdx = 0;
        tmpBuffer.flip();                       // prepare buffer for read
        while (byteIdx < byteNum) {             // copying bytes to the mapped buffer
            try {
                byte b = tmpBuffer.get(byteIdx);
                mappedBuffer.put(byteIdx++, b);
            } catch (IndexOutOfBoundsException e) {
                log.error("byteIdx = " + byteIdx + ", byteNum = " + byteNum, e);
                throw new RuntimeException(e);
            }
        }

        tmpBuffer.clear();
        tmpBuffer = null;                       // release temp buffer
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
        if (rcdIdx >= bucketCapacity) throw new IllegalArgumentException("rcdIdx >= bucketCapacity");
        if (bucket.length != bucketSize) throw new IllegalArgumentException("Invalid bucket size");
        if (targetRcd.length != BinaryRecord.RECORD_SIZE) throw new IllegalArgumentException("Invalid record size");

        int recordOffset = BinaryBucket.BUCKET_HEADER_SIZE + BinaryRecord.RECORD_SIZE * rcdIdx;
        System.arraycopy(bucket, recordOffset, targetRcd, 0, BinaryRecord.RECORD_SIZE);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    //                                                                                            //
    //                              PRIMITIVE OPERATIONS ON BUFFERS.                              //
    //                                                                                            //
    ////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Fetches a bucket in a binary format by it's index (ordinal number).
     *
     * @param bktIdx bucket ordinal number
     * @return bucket as a byte array
     */
    private byte[] readBucketByIdx(int bktIdx, ByteBuffer buffer) {
//        if (bktIdx >= bucketNumber) throw new IllegalArgumentException("bktIdx >= bucketNumber"); // todo: add check

        int bktAddress = STORAGE_HEADER_SIZE + bktIdx * bucketSize;
        return readBucket(bktAddress, buffer);
    }

    /**
     * Current method reads bucket by the specified bktAddress.
     *
     * @param bktAddress address of the record
     * @return byte array containing record in a binary format
     */
    private byte[] readBucket(int bktAddress, ByteBuffer buffer) {
        byte[] byteBucket = new byte[bucketSize];
        for (int bufAddress = bktAddress, byteIdx = 0; bufAddress < bktAddress + bucketSize; bufAddress++) {
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
     * Writes the record into the specified bucket on the specified position
     * to the specified buffer.
     *
     * @param bucketIdx bucket index
     * @param rcdIdx record index
     * @param rcd record as a byte array
     * @param buffer target buffer
     */
    private void writeRecordByIdx(int bucketIdx, int rcdIdx, byte[] rcd, ByteBuffer buffer) {
        int bktAddress = STORAGE_HEADER_SIZE + bucketIdx * bucketSize;
        int rcdAddress = bktAddress + BinaryBucket.BUCKET_HEADER_SIZE + rcdIdx * BinaryRecord.RECORD_SIZE;
        writeRecord(rcdAddress, rcd, buffer);
    }

    /**
     * Writes the record into the buffer by the specified address.
     * @param rcdAddress record address
     * @param rcd record as a byte array
     * @param buffer target buffer
     */
    private void writeRecord(int rcdAddress, byte[] rcd, ByteBuffer buffer) {
        if (rcd.length != BinaryRecord.RECORD_SIZE) throw new IllegalArgumentException("rcd.length != BinaryRecord.RECORD_SIZE");

        for (int bufAddress = rcdAddress, byteIdx = 0; bufAddress < rcdAddress + BinaryRecord.RECORD_SIZE; bufAddress++) {
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
    private void moveRecordByIdx(int bktIdx, int fromRcdIdx, int toRcdIdx, ByteBuffer buffer) {
        int bucketAddress = STORAGE_HEADER_SIZE + bktIdx * bucketSize;
        int fromAddress = bucketAddress + BinaryBucket.BUCKET_HEADER_SIZE + fromRcdIdx * BinaryRecord.RECORD_SIZE;
        int toAddress = bucketAddress + BinaryBucket.BUCKET_HEADER_SIZE + toRcdIdx * BinaryRecord.RECORD_SIZE;

        moveRecord(fromAddress, toAddress, buffer);
    }

    /**
     * Moves the record from one address to another address in the buffer.
     * Old address is filled with zeros afterwards.
     *
     * @param fromAddress source record address
     * @param toAddress destination address
     */
    private void moveRecord(int fromAddress, int toAddress, ByteBuffer buffer) {
        for (int byteIdx = 0; byteIdx < BinaryRecord.RECORD_SIZE; byteIdx++) {
            int currentFromAddress = fromAddress + byteIdx;
            buffer.put(toAddress + byteIdx, mappedBuffer.get(currentFromAddress));
            buffer.put(currentFromAddress, (byte) 0);
        }
    }

    /**
     * Updates the bucket size in the buffer.
     *
     * @param bucketIdx bucket index
     * @param newSize size
     */
    private void writeBucketSize(int bucketIdx, int newSize, ByteBuffer buffer) {
        int bktAddress = STORAGE_HEADER_SIZE + bucketIdx * bucketSize;
        int bktSizeAddress = bktAddress + BinaryBucket.BUCKET_SIZE_OFFSET;
        buffer.putInt(bktSizeAddress, newSize);
    }


    /**
     * Reads the number of buckets in storage from the specified byte buffer.
     *
     * @param buffer source byte buffer
     * @return number of initialized buckets
     */
    private int readStorageBktNum(ByteBuffer buffer) {
        return buffer.getInt(STORAGE_BUCKET_NUM_OFFSET);
    }

    /**
     * Reads the bucket capacity of storage from the specified byte buffer.
     *
     * @param buffer source byte buffer
     * @return bucket capacity
     */
    private int readStorageBktCapacity(ByteBuffer buffer) {
        return buffer.getInt(STORAGE_BUCKET_CAP_OFFSET);
    }

    /**
     * Getter to be redefined.
     *
     * @return default initial number of buckets
     */
    protected int getDefaultInitialBucketNumber() {
        return DEFAULT_INITIAL_BUCKET_NUMBER;
    }

    /**
     * Current class provides utility methods to operate with binary buckets
     * like with an object.
     */
    protected final static class BinaryBucket {
        public static final int BUCKET_SIZE_OFFSET = 0;             // int
        public static final int BUCKET_HEADER_SIZE = 4;             // the rest

        private BinaryBucket() {}

        public static void setSize(int size, byte[] bucket) {
            ByteUtils.putInt(size, bucket, BUCKET_SIZE_OFFSET);
        }

        public static int getSize(byte[] bucket) {
            return ByteUtils.getInt(bucket, BUCKET_SIZE_OFFSET);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    //                                                                                            //
    //       UTILITARIAN CLASSES CONTAINING METHODS TO PERFORM SOME PRIMITIVE BYTE OPERATION      //
    //                                                                                            //
    ////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Current class provides utility methods to operate with binary records
     * like with an object.
     */
    protected final static class BinaryRecord {
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

}
