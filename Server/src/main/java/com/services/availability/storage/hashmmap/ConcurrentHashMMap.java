package com.services.availability.storage.hashmmap;

import com.services.availability.model.AvailabilityItem;
import com.services.availability.storage.Storage;
import com.services.availability.storage.hashmmap.cache.*;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-09 13:32
 */
public class ConcurrentHashMMap extends HashMMap implements Storage {
    public static final int SYNC_INITIAL_BUCKET_NUMBER = 1024 * 1024 * 3;   // should be enough for ~ 5 * 10^7 elements

    private static Logger log = Logger.getLogger(ConcurrentHashMMap.class);

    private final StorageCache storageCache = new StorageCache();
    protected final Lock flushLock = new ReentrantLock();
    private Object[] bucketMonitor = null;

    private final ExecutorService batchJobExecutor = Executors.newSingleThreadExecutor();

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
    public ConcurrentHashMMap() {
        super();
        initMonitors();
    }

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
    public ConcurrentHashMMap(int initialBucketNumber, int bucketCapacity) {
        super(initialBucketNumber, bucketCapacity);
        initMonitors();
    }

    /**
     * Performs synchronized lookup of the provided  key in the
     * collection and cache and returns corresponding value if found.
     * Otherwise, null is returned.
     *
     * Thread safe operation.
     *
     * @param key requested key
     * @return AvailabilityItem corresponding to the current key
     */
    public AvailabilityItem get(long key) {
        synchronized (monitorForKey(key)) {         // cache.put() requires synchronization here
            OperationResult operationResult = storageCache.get(key);
            return operationResult.isFoundByKey() ? operationResult.getValue() : super.get(key);
        }
    }

    /**
     * Performs synchronized put of the specified element by the provided
     * key. Element is added first to the cache and then transferred to the
     * persistent storage.
     *
     * Thread safe operation.
     *
     * @param key requested key
     * @param value availability item to put
     */
    public void put(long key, AvailabilityItem value) {
        synchronized (monitorForKey(key)) {         // cache.put() requires synchronization here
            storageCache.put(key, value);
            verifyAndStartBatch();
        }
    }

    /**
     * Performs synchronized remove by key operation and returns the element
     * that was removed. Element is added first to the r-cache and then is
     * removed from the persistent storage by the batch-job thread.
     *
     * Thread safe operation.
     *
     * @param key requested key
     * @return availability item that was removed
     */
    public AvailabilityItem remove(long key) {
        synchronized (monitorForKey(key)) {
            OperationResult result = storageCache.remove(key);
            AvailabilityItem removedValue = result.isFoundByKey() ? result.getValue() : super.get(key);
            verifyAndStartBatch();
            return removedValue;
        }
    }

    /**
     * Method prepares storage for safe shutdown.
     *
     * Method waits for BatchJob thread to finish its activity and verifies that
     * there are no records in cache that are non persisted.
     */
    public void prepareForShutdown() {
        log.debug("Scheduling flush for the rest of records that are in cache");
        startBatchNow();

        log.debug("Sending shutdown signal to batchJobExecutor");
        batchJobExecutor.shutdown();

        if (!batchJobExecutor.isTerminated()) {
            try {
                boolean terminated = batchJobExecutor.awaitTermination(60, TimeUnit.SECONDS); // waiting for termination
                if (!terminated) throw new RuntimeException("Termination timeout elapsed.");  // termination goes too long

                storageCache.ensureEmpty();                // just to be sure
            } catch (InterruptedException e) {
                log.error(e);
            }
        }
        log.debug("Storage is ready for shutdown.");
    }

    /**
     * Put operation on the mmaped buffer.
     *
     * @param key long number to use as key
     * @param value availability item to put
     */
    protected void persistentPut(long key, AvailabilityItem value) {
        super.put(key, value);
    }

    /**
     * Remove operation on the mmaped buffer.
     *
     * @param key long number to use as key
     */
    protected AvailabilityItem persistentRemove(long key) {
        return super.remove(key);
    }

    /**
     * Initializes monitor objects for each bucket.
     */
    private void initMonitors() {
        Object[] bucketMonitors = new Object[bucketNumber];
        int bktNum = 0;

        if (this.bucketMonitor != null) {             // if some monitors were already created, just copying them
            System.arraycopy(this.bucketMonitor, 0, bucketMonitors, 0, this.bucketMonitor.length);
        }
        while (bktNum < bucketNumber) {             // creating the rest of monitors
            bucketMonitors[bktNum++] = new Object();
        }
        this.bucketMonitor = bucketMonitors;
    }

    /**
     * Verifies if number of cached changes has exceeded some threshold. If
     * cache is full and no batch job is running, then a new batch job is scheduled.
     * Otherwise, if cache is full but the job is already in progress, then
     * cache capacity is extended.
     */
    private void verifyAndStartBatch() {
        boolean lockAcquired = false;
        try {
            lockAcquired = flushLock.tryLock();
            if (lockAcquired) {             // no batch job is running, we can start new job if necessary
                Map<Long, CacheValue> cachedValues = storageCache.verifyAndSwap();
                if (cachedValues != null) {                 // cache was swapped
                    batchJobExecutor.submit(new BatchJobThread(this, cachedValues));
                }
            } else {
                storageCache.verifyAndExtend();    // batch job is already running, resize required
            }
        } finally {
            if (lockAcquired) flushLock.unlock();
        }
    }

    /**
     * Schedules a new batch job that start persisting all records that are
     * currently contained in cache.
     */
    private void startBatchNow() {
        Map<Long, CacheValue> cachedValues = storageCache.swap();
        if (cachedValues != null && cachedValues.size() > 0) {
            batchJobExecutor.submit(new BatchJobThread(this, cachedValues));
        }
    }

    /**
     * Tries to retrieve a monitor object for the specified key. Monitor
     * cannot be provided if the resize operation is in progress.
     *
     * @param key to retrieve monitor for
     * @return monitor object : Object
     */
    private Object monitorForKey(long key) {
        return bucketMonitor[getBucketIdxByKey(key)];
    }

    /**
     * Increases number of buckets in the storage and redistributes all records
     * between the new buckets <b>in a thread-safe manner</b>.
     *
     * Method creates a temporary byte buffer which is used as a temporary storage
     * during the resize operation. The <b>original mapped buffer remains untouched
     * until the all records are redistributed</b> between new buckets. Therefore it
     * can be used for reads when the resize is in progress.
     *
     * @param newBucketNumber new number of buckets
     */
    protected void resize(int newBucketNumber) {
        throw new UnsupportedOperationException("ConcurrentHashMMap cannot be resized.");
    }

    /**
     * Returns another default initial number of buckets that is big enough
     * to avoid resize operation.
     *
     * @return default initial number of buckets
     */
    protected int getDefaultInitialBucketNumber() {
        return SYNC_INITIAL_BUCKET_NUMBER;
    }

    /**
     * Getter for logger.
     *
     * @return logger
     */
    protected Logger getLogger() {
        return log;
    }

    /**
     * Class performs synchronization of cache with the persistent storage.
     */
    public static class BatchJobThread implements Runnable {
        private Logger logger = Logger.getLogger(BatchJobThread.class);
        private Map<Long, CacheValue> cachedValues;
        private ConcurrentHashMMap hashMMap;

        public BatchJobThread(ConcurrentHashMMap map, Map<Long, CacheValue> cacheValues) {
            cachedValues = cacheValues;
            hashMMap = map;
        }

        @Override
        public void run() {
            logger.debug("BatchJobThread started. Processing " + cachedValues.size() + " cached values");
            Lock lock = hashMMap.flushLock;
            boolean lockAcquired = false;
            try {
                lockAcquired = lock.tryLock(30, TimeUnit.SECONDS);
                logger.debug("lockAcquired = " + lockAcquired);

                if (lockAcquired) {
                    for (Long key: cachedValues.keySet()) {
                        AvailabilityItem value = cachedValues.get(key).value;
                        if (value == null) {
                            hashMMap.persistentRemove(key);
                        } else {
                            hashMMap.persistentPut(key, value);
                        }
                    }

                    logger.debug("flushing buffer");
                    hashMMap.flushMappedBuffer();
                    logger.debug("flush completed");
                } else throw new RuntimeException("BatchJobThread was not able to acquire the FLUSH LOCK. Looks like a deadlock.");
            } catch (InterruptedException e) {
                log.error(e);
            } finally {
                if (lockAcquired) {
                    lock.unlock();
                    logger.debug("lock released");
                }
                logger.debug("BatchJobThread terminated.");
            }
        }
    }
}
