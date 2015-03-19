package com.services.availability.storage.ccl.cache;

import com.services.availability.model.AvailabilityItem;
import com.services.availability.storage.ccl.LogRecord;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 3/19/15 12:40 PM
 */
public class CommitCache {
    private volatile Cache cache = new Cache();

    public void put(long key, AvailabilityItem value) {
        LogRecord record = new LogRecord(LogRecord.TYPE_PUT, value.key(), value.getSku(),
                value.getStore(), value.getAmount());
        cache.frontRecords.add(record);
    }

    public AvailabilityItem get(long key) {
        return null;
    }

    public AvailabilityItem remove(long key) {
//        LogRecord record = new LogRecord(LogRecord.TYPE_PUT, value.key(), value.getSku(),
//                value.getStore(), value.getAmount());
//        cache.frontRecords.add(record);
        return null;
    }

    public void swap() {
        cache = new Cache(cache.frontRecords);
    }

    private static class Cache {
        protected final List<LogRecord> frontRecords;
        protected final List<LogRecord> backRecords;

        public Cache() {
            frontRecords = new LinkedList<LogRecord>();
            backRecords = new LinkedList<LogRecord>();
        }

        public Cache(List<LogRecord> oldFrontCache) {
            frontRecords = new LinkedList<LogRecord>();
            backRecords = oldFrontCache;
        }
    }

}
