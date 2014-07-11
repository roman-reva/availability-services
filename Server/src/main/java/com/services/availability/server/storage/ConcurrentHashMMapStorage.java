package com.services.availability.server.storage;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-09 14:57
 */
public class ConcurrentHashMMapStorage implements Storage {
    private ConcurrentHashMMap hashMMap = new ConcurrentHashMMap();

    @Override
    public void put(long key, AvailabilityItem value) {
        hashMMap.put(key, value);
    }

    @Override
    public AvailabilityItem get(long key) {
        return hashMMap.get(key);
    }

    @Override
    public AvailabilityItem remove(long key) {
        return hashMMap.remove(key);
    }
}
