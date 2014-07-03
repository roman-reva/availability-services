package com.services.availability.server.storage;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-27 13:08
 */
public interface Storage {

    public void put(long key, AvailabilityItem value);

    public AvailabilityItem get(long key);

    public AvailabilityItem remove(long key);

}
