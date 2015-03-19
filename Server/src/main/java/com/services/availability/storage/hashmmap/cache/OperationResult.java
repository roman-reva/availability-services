package com.services.availability.storage.hashmmap.cache;

import com.services.availability.model.AvailabilityItem;

/**
 * Class describes an object that is returned by each PUT/GET/REMOVE operation on cache.
 *
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-17 17:09
 */
public class OperationResult {
    private boolean foundByKey;             // true, if cache contains the specified key
    private AvailabilityItem value;         // proper availability item to return

    public OperationResult(boolean foundByKey, AvailabilityItem value) {
        this.foundByKey = foundByKey;
        this.value = value;
    }

    public boolean isFoundByKey() {
        return foundByKey;
    }

    public AvailabilityItem getValue() {
        return value;
    }
}