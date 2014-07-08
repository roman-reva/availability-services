package com.services.availability.server.storage;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-08 19:11
 */
public class HashMMapStorageTests {
    private Random random = new Random();

    @Before
    public void init() {
        HashMMap hashMMap = new HashMMap();
        hashMMap.clear();
    }

    @Test
    public void simpleIntegrationTest() {
        HashMMapStorage storage = new HashMMapStorage();

        AvailabilityItem item = new AvailabilityItem(12509, (short)23, 399);

        AvailabilityItem restoredItem = storage.get(item.key());
        assertNull(restoredItem);

        storage.put(item.key(), item);
        restoredItem = storage.get(item.key());
        assertNotNull(restoredItem);
        assertEquals(item.getSku(), restoredItem.getSku());
        assertEquals(item.getStore(), restoredItem.getStore());
        assertEquals(item.getAmount(), restoredItem.getAmount());

        restoredItem = storage.remove(item.key());
        assertNotNull(restoredItem);
        assertEquals(item.getSku(), restoredItem.getSku());
        assertEquals(item.getStore(), restoredItem.getStore());
        assertEquals(item.getAmount(), restoredItem.getAmount());

        restoredItem = storage.get(item.key());
        assertNull(restoredItem);
    }

    @Test
    public void randomIntegrationTest() {
        HashMMapStorage storage = new HashMMapStorage();

        System.out.println("Initialized");
        List<AvailabilityItem> addedItems = new ArrayList<AvailabilityItem>(12000);
        List<AvailabilityItem> notAddedItems = new ArrayList<AvailabilityItem>(8000);

        for (int i=0; i<1000000; i++)
            notAddedItems.add(new AvailabilityItem(getSku(), getStore(), getAmount()));

        for (int i=0; i<1000000; i++) {
            if (i > 0 && i % 10000 == 0) System.out.println(i + " passed");
            AvailabilityItem item = new AvailabilityItem(getSku(), getStore(), getAmount());
            try {
                if (storage.get(item.key()) == null) {
                    storage.put(item.key(), item);
                    addedItems.add(item);
                }
            } catch (RuntimeException e) {
                e.printStackTrace();
                assert false;
            }
        }
        System.out.println("Assertions started");

        // optimistic test
        for (AvailabilityItem item: addedItems) {
            AvailabilityItem restoredItem = storage.get(item.key());
            assertNotNull(restoredItem);
            assertEquals(item.getSku(), restoredItem.getSku());
            assertEquals(item.getStore(), restoredItem.getStore());
            assertEquals(item.getAmount(), restoredItem.getAmount());
        }

        // pessimistic test
        for (AvailabilityItem item: notAddedItems) {
            AvailabilityItem restoredItem = storage.get(item.key());
            assertNull(restoredItem);
        }
    }

    private short getStore() {
        return (short)random.nextInt(1000);
    }

    private int getAmount() {
        return random.nextInt(50000);
    }

    private int getSku() {
        return Math.abs(random.nextInt());
    }
}

