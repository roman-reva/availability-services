package com.services.availability.server.storage;


import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-01 17:54
 */
public class AvailabilityItemTests {
    @Test
    public void hashCodeFromKeyTest() {
        int sku = 29841, amount = 35;
        short store = 325;
        AvailabilityItem item = new AvailabilityItem(sku, store, amount);
        assertEquals(item.hashCode(), AvailabilityItem.keyToHashCode(item.key()));

        sku = 23589732;
        amount = 0;
        store = 453;
        item = new AvailabilityItem(sku, store, amount);
        assertEquals(item.hashCode(), AvailabilityItem.keyToHashCode(item.key()));
    }
}
