package com.services.availability.storage.hashmmap.cache;

import com.services.availability.model.AvailabilityItem;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-14 12:15
 */
public class StorageCache {
    private static final Logger log = Logger.getLogger(StorageCache.class);

    public static final int DEFAULT_CACHE_CAPACITY = 1000;          // default max number of elements in front cash
    private int cacheCapacity = DEFAULT_CACHE_CAPACITY;             // current max number of elements in front cash
    private volatile CacheContainer container;

    public StorageCache() {
        ConcurrentHashMap<Long, CacheValue> frontCache = new ConcurrentHashMap<Long, CacheValue>();
        ConcurrentHashMap<Long, CacheValue> backCache = new ConcurrentHashMap<Long, CacheValue>();
        container = new CacheContainer(frontCache, backCache);
    }

    public OperationResult put(long key, AvailabilityItem value) {
        Map<Long, CacheValue> frontCache = container.frontCache;

        boolean elementFound = frontCache.containsKey(key);
        AvailabilityItem oldValue = elementFound ? frontCache.get(key).value : null;
        frontCache.put(key, new CacheValue(value));

        return new OperationResult(elementFound, oldValue);
    }

    public OperationResult get(long key) {
        CacheContainer cacheContainer = this.container;
        if (cacheContainer.frontCache.containsKey(key)) {
            return new OperationResult(true, cacheContainer.frontCache.get(key).value);
        } else if (cacheContainer.backCache.containsKey(key)) {
            return new OperationResult(true, cacheContainer.backCache.get(key).value);
        }
        return new OperationResult(false, null);
    }

    public OperationResult remove(long key) {
        Map<Long, CacheValue> frontCache = container.frontCache;

        boolean containsItem = frontCache.containsKey(key);
        AvailabilityItem removedValue = containsItem ? frontCache.get(key).value : null;
        frontCache.put(key, new CacheValue(null));

        return new OperationResult(containsItem, removedValue);
    }

    /**
     * Verifies if cache is full.
     */
    public synchronized boolean verifyAndExtend() {
        // verify
        if (container.frontCache.size() < cacheCapacity) return false;

        cacheCapacity *= 2;
        log.debug("cacheCapacity extended. New capacity is " + cacheCapacity);

        return true;
    }

    /**
     * Verifies if cache is full. If it is, moves data from front cache to
     * the back cache and cleans up the front cache, and returns an
     * unmodifiable copy of back cache.
     *
     * @return back cache, if swap was performed, null otherwise.
     */
    public Map<Long, CacheValue> verifyAndSwap() {
        // verify
        if (container.frontCache.size() < cacheCapacity) return null;

        // swap
        return swap();
    }

    /**
     * Moves data from front cache to the back cache and cleans up the front cache,
     * and returns an unmodifiable copy of back cache.
     *
     * @return back cache
     */
    public Map<Long, CacheValue> swap() {
        // swap
        ConcurrentHashMap<Long, CacheValue> oldFrontCache = container.frontCache;
        container = new CacheContainer(new ConcurrentHashMap<Long, CacheValue>(), oldFrontCache);
        log.debug("cache swapped");

        // return values
        return Collections.unmodifiableMap(oldFrontCache);
    }

    /**
     * Ensures that there are not records left in the front cache.
     */
    public void ensureEmpty() {
        if (container.frontCache.size() > 0)
            throw new IllegalStateException("container.frontCache size is different from 0");

        log.debug("cache is empty");
    }

    /**
     * Container to hold front and back cache as final link for safe atomic cache publishing.
     */
    private static class CacheContainer {
        protected final ConcurrentHashMap<Long, CacheValue> frontCache;
        protected final ConcurrentHashMap<Long, CacheValue> backCache;

        public CacheContainer(ConcurrentHashMap<Long, CacheValue> fc, ConcurrentHashMap<Long, CacheValue> bc) {
            this.frontCache = fc;
            this.backCache = bc;
        }
    }
}
