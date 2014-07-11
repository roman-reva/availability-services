package com.services.availability.server.storage;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-09 13:32
 */
public class ConcurrentHashMMap extends HashMMap {
    public static final int SYNC_INITIAL_BUCKET_NUMBER = 1024 * 1024 * 3;   // should be enough for ~ 5 * 10^7 elements

    private final PRCache prCache = new PRCache();
    private Object[] bucketMonitor = null;

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
        synchronized (monitorForKey(key)) {
            AvailabilityItem cachedValue = prCache.get(key);
            if (cachedValue != null) return cachedValue;

            return super.get(key);
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
        synchronized (monitorForKey(key)) {
            prCache.put(key, value);
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
            return prCache.remove(key);
        }
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
     *
     */
    public static class PRCache {           // todo: extract cache as a separate class
        private final ConcurrentHashMap<Long, AvailabilityItem> cacheA = new ConcurrentHashMap<Long, AvailabilityItem>();
        private final ConcurrentHashMap<Long, AvailabilityItem> cacheB = new ConcurrentHashMap<Long, AvailabilityItem>();

        private boolean frontCacheA = true;

        /**
         * Puts object to the cache by the specified key.
         *
         * @param key object key
         * @param value availability item to put
         */
        public void put(long key, AvailabilityItem value) {
            getFrontEndCache().put(key, value);
        }

        /**
         * Performs search by the specified key in the cache.
         *
         * @param key requested key
         * @return corresponding AvailabilityItem if found, otherwise null
         */
        public synchronized AvailabilityItem get(long key) {
            getBackEndCache().get(key);
            getFrontEndCache().get(key);

            return null;
        }

        /**
         * Add object to the cache for remove operation. Returns false,
         * if object was already there, true otherwise.
         *
         * @param key requested key
         * @return true, if object was added to the r-cache. False, if it already was there
         */
        public AvailabilityItem remove(long key) {
            return getFrontEndCache().put(key, null);
        }

        /**
         * Returns back-end cache that is currently not in use.
         * @return
         */
        public synchronized ConcurrentHashMap<Long, AvailabilityItem> getBackEndCache() {
            return frontCacheA ? cacheB : cacheA;
        }

        /**
         *
         */
        public synchronized void switchCaches() {
            frontCacheA = !frontCacheA;
        }

        private ConcurrentHashMap<Long, AvailabilityItem> getFrontEndCache() {
            return frontCacheA ? cacheA : cacheB;
        }


    }

    /**
     * Class performs synchronization of p-cache and r-cache with the persistent
     * storage.
     */
    public static class BatchJobThread extends Thread {
        @Override
        public void run() {
            while (true) {
                // todo:
            }
        }
    }
}
