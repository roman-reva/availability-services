package com.services.availability.server.storage;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-01 14:28
 */
public class HashMMapStorage implements Storage {
    public final static int RECORD_NUM_THRESHOLD = 5000;

    private HashMMap hashMMap = new HashMMap();
    private ConcurrentHashMap<Long, AvailabilityItem> cache = new ConcurrentHashMap<Long, AvailabilityItem>();

    @Override
    public void put(long key, AvailabilityItem value) {
        cache.put(key, value);
        if (cache.size() > RECORD_NUM_THRESHOLD) {
            hashMMap.putAll(cache);
            hashMMap.flushMappedBuffer();
            cache.clear();
        }
    }

    @Override
    public AvailabilityItem get(long key) {
        Long objKey = key;
        if (cache.containsKey(objKey)) {
            return cache.get(objKey);
        } else {
            return hashMMap.get(objKey);
        }
    }

    @Override
    public AvailabilityItem remove(long key) {
        AvailabilityItem mapValue = cache.remove(key);
        AvailabilityItem mmapValue = hashMMap.remove(key);
        return mapValue != null ? mapValue : mmapValue;
    }
}
