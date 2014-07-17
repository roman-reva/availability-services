package com.services.availability.storage.hashmmap;

import com.services.availability.model.AvailabilityItem;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-01 15:00
 */
public class HashMMapTests {

    private Random random = new Random();

    @Before
    public void init() {
        HashMMap hashMMap = new HashMMap();
        hashMMap.clear();
    }

    @Test
    public void byteRecordTestsA() {
        byte [] record = new byte[BinaryRecord.RECORD_SIZE];
        long key = 2398436448724698204L;
        int sku = 436346343;
        short store = 892;
        int amount = 89235664;

        BinaryRecord.setKey(key, record);
        BinaryRecord.setSku(sku, record);
        BinaryRecord.setStore(store, record);
        BinaryRecord.setAmount(amount, record);

        long restoredKey = BinaryRecord.getKey(record);
        int restoredSku = BinaryRecord.getSku(record);
        short restoredStore = BinaryRecord.getStore(record);
        int restoredAmount = BinaryRecord.getAmount(record);

        Assert.assertEquals(key, restoredKey);
        Assert.assertEquals(sku, restoredSku);
        Assert.assertEquals(amount, restoredAmount);
        Assert.assertEquals(store, restoredStore);
    }

    @Test
    public void byteRecordTestsB() {
        byte [] record = new byte[BinaryRecord.RECORD_SIZE];
        long key = -2398436448724698204L;
        int sku = -436346343;
        short store = -235;
        int amount = -89235664;

        BinaryRecord.setKey(key, record);
        BinaryRecord.setSku(sku, record);
        BinaryRecord.setStore(store, record);
        BinaryRecord.setAmount(amount, record);

        long restoredKey = BinaryRecord.getKey(record);
        int restoredSku = BinaryRecord.getSku(record);
        short restoredStore = BinaryRecord.getStore(record);
        int restoredAmount = BinaryRecord.getAmount(record);

        Assert.assertEquals(key, restoredKey);
        Assert.assertEquals(sku, restoredSku);
        Assert.assertEquals(amount, restoredAmount);
        Assert.assertEquals(store, restoredStore);
    }

    @Test
    public void byteRecordTestsC() {
        byte [] record = new byte[BinaryRecord.RECORD_SIZE];
        long key = 1;
        int sku = -1;
        short store = 128;
        int amount = -1024;

        BinaryRecord.setKey(key, record);
        BinaryRecord.setSku(sku, record);
        BinaryRecord.setStore(store, record);
        BinaryRecord.setAmount(amount, record);

        long restoredKey = BinaryRecord.getKey(record);
        int restoredSku = BinaryRecord.getSku(record);
        short restoredStore = BinaryRecord.getStore(record);
        int restoredAmount = BinaryRecord.getAmount(record);

        Assert.assertEquals(key, restoredKey);
        Assert.assertEquals(sku, restoredSku);
        Assert.assertEquals(amount, restoredAmount);
        Assert.assertEquals(store, restoredStore);
    }

    @Test
    public void hashCodeDistributionTest() {
        HashMap<Integer, Integer> stat = new HashMap<Integer, Integer>();
        for (int i=0; i<8000000; i++) {
            AvailabilityItem item = new AvailabilityItem(getSku(), getStore(), getAmount());
            int hashCode = Math.abs(AvailabilityItem.keyToHashCode(item.key()));
            int bucketNum = hashCode % (1024 * 1024);

            if (!stat.containsKey(bucketNum)) stat.put(bucketNum, 0);
            stat.put(bucketNum, stat.get(bucketNum) + 1);
        }

        int min = 999999999, max = -1, sum = 0;
        for (int val : stat.values()) {
            if (min > val) min = val;
            if (max < val) max = val;
            sum += val;
        }
        int avg = sum / stat.size();

        System.out.println("Min: " + min);
        System.out.println("Max: " + max);
        System.out.println("Avg: " + avg);

    }

    @Test
    public void simpleIntegrationTest() {
        HashMMap storage = new HashMMap();

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
        HashMMap storage = new HashMMap();

        System.out.println("Initialized");
        List<AvailabilityItem> addedItems = new ArrayList<AvailabilityItem>(12000);
        List<AvailabilityItem> notAddedItems = new ArrayList<AvailabilityItem>(8000);

        for (int i=0; i<100000; i++)
            notAddedItems.add(new AvailabilityItem(getSku(), getStore(), getAmount()));

        for (int i=0; i<100000; i++) {
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
