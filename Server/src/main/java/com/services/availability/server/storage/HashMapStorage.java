package com.services.availability.server.storage;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Roman Reva
 * @since  2014-06-19 18:35
 * @version 1.0
 */
public class HashMapStorage implements Storage{
    private final ConcurrentHashMap<Long, AvailabilityItem> storage = new ConcurrentHashMap<Long, AvailabilityItem>();

    public void put(long key, AvailabilityItem value) {
        storage.put(key, value);
    }

    public AvailabilityItem get(long key) {
        return storage.get(key);
    }

    public AvailabilityItem remove(long key) {
        return storage.remove(key);
    }
}
