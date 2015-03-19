package com.services.availability.storage;

import com.services.availability.model.AvailabilityItem;
import com.services.availability.storage.hashmmap.ConcurrentHashMMap;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2015-03-17 14:57
 */
public class HashMMappedStorage implements Storage {
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

    @Override
    public void prepareForShutdown() {
        hashMMap.prepareForShutdown();
    }
}
