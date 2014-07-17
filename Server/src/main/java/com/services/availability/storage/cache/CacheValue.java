package com.services.availability.storage.cache;

import com.services.availability.model.AvailabilityItem;

/**
 * Wrapper for an availability item. Is required for cache to be able to hold NULL values.
 *
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-17 17:14
 */
public class CacheValue {
    public AvailabilityItem value;

    public CacheValue(AvailabilityItem value) {
        this.value = value;
    }
}