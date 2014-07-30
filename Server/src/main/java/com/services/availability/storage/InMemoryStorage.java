package com.services.availability.storage;

import com.services.availability.model.AvailabilityItem;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-18 13:26
 */
public class InMemoryStorage implements Storage {

    private final int operationDuration;
    private final ConcurrentHashMap<Long, AvailabilityItem> map = new ConcurrentHashMap<Long, AvailabilityItem>();

    public InMemoryStorage() {
        this.operationDuration = 0;
    }

    public InMemoryStorage(int operationDuration) {
        this.operationDuration = operationDuration;
    }

    @Override
    public void put(long key, AvailabilityItem value) {
        try {
            if (operationDuration > 0) Thread.sleep(operationDuration);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        map.put(key, value);
    }

    @Override
    public AvailabilityItem get(long key) {
        try {
            if (operationDuration > 0) Thread.sleep(operationDuration);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return map.get(key);
    }

    @Override
    public AvailabilityItem remove(long key) {
        try {
            if (operationDuration > 0) Thread.sleep(operationDuration);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return map.remove(key);
    }

    @Override
    public void prepareForShutdown() { }
}
