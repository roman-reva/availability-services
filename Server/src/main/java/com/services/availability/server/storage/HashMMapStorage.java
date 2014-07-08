package com.services.availability.server.storage;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-01 14:28
 */
public class HashMMapStorage implements Storage {
    public final static int RECORD_NUM_THRESHOLD = 5000;

    private HashMMap hashMMap = new HashMMap(false);
    private ConcurrentHashMap<Long, AvailabilityItem> hashMap = new ConcurrentHashMap<Long, AvailabilityItem>();

    @Override
    public void put(long key, AvailabilityItem value) {
        hashMap.put(key, value);
        if (hashMap.size() > RECORD_NUM_THRESHOLD) {
            hashMMap.putAll(hashMap);
            hashMap.clear();
        }
    }

    @Override
    public AvailabilityItem get(long key) {
        Long objKey = key;
        if (hashMap.containsKey(objKey)) {
            return hashMap.get(objKey);
        } else {
            return hashMMap.get(objKey);
        }
    }

    @Override
    public AvailabilityItem remove(long key) {
        AvailabilityItem mapValue = hashMap.remove(key);
        AvailabilityItem mmapValue = hashMMap.remove(key);
        return mapValue != null ? mapValue : mmapValue;
    }
}
