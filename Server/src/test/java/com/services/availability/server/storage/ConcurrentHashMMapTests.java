package com.services.availability.server.storage;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static junit.framework.Assert.*;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-01 15:00
 */
public class ConcurrentHashMMapTests {

    private Random random = new Random();

    public ConcurrentHashMMap init() {
        ConcurrentHashMMap map = new ConcurrentHashMMap(256, 32);
        map.clear();
        return map;
    }

    @Test
    public void byteRecordTestsA() {
        byte [] record = new byte[ConcurrentHashMMap.BinaryRecord.RECORD_SIZE];
        long key = 2398436448724698204L;
        int sku = 436346343;
        short store = 892;
        int amount = 89235664;

        ConcurrentHashMMap.BinaryRecord.setKey(key, record);
        ConcurrentHashMMap.BinaryRecord.setSku(sku, record);
        ConcurrentHashMMap.BinaryRecord.setStore(store, record);
        ConcurrentHashMMap.BinaryRecord.setAmount(amount, record);

        long restoredKey = ConcurrentHashMMap.BinaryRecord.getKey(record);
        int restoredSku = ConcurrentHashMMap.BinaryRecord.getSku(record);
        short restoredStore = ConcurrentHashMMap.BinaryRecord.getStore(record);
        int restoredAmount = ConcurrentHashMMap.BinaryRecord.getAmount(record);

        Assert.assertEquals(key, restoredKey);
        Assert.assertEquals(sku, restoredSku);
        Assert.assertEquals(amount, restoredAmount);
        Assert.assertEquals(store, restoredStore);
    }

    @Test
    public void byteRecordTestsB() {
        byte [] record = new byte[ConcurrentHashMMap.BinaryRecord.RECORD_SIZE];
        long key = -2398436448724698204L;
        int sku = -436346343;
        short store = -235;
        int amount = -89235664;

        ConcurrentHashMMap.BinaryRecord.setKey(key, record);
        ConcurrentHashMMap.BinaryRecord.setSku(sku, record);
        ConcurrentHashMMap.BinaryRecord.setStore(store, record);
        ConcurrentHashMMap.BinaryRecord.setAmount(amount, record);

        long restoredKey = ConcurrentHashMMap.BinaryRecord.getKey(record);
        int restoredSku = ConcurrentHashMMap.BinaryRecord.getSku(record);
        short restoredStore = ConcurrentHashMMap.BinaryRecord.getStore(record);
        int restoredAmount = ConcurrentHashMMap.BinaryRecord.getAmount(record);

        Assert.assertEquals(key, restoredKey);
        Assert.assertEquals(sku, restoredSku);
        Assert.assertEquals(amount, restoredAmount);
        Assert.assertEquals(store, restoredStore);
    }

    @Test
    public void byteRecordTestsC() {
        byte [] record = new byte[ConcurrentHashMMap.BinaryRecord.RECORD_SIZE];
        long key = 1;
        int sku = -1;
        short store = 128;
        int amount = -1024;

        ConcurrentHashMMap.BinaryRecord.setKey(key, record);
        ConcurrentHashMMap.BinaryRecord.setSku(sku, record);
        ConcurrentHashMMap.BinaryRecord.setStore(store, record);
        ConcurrentHashMMap.BinaryRecord.setAmount(amount, record);

        long restoredKey = ConcurrentHashMMap.BinaryRecord.getKey(record);
        int restoredSku = ConcurrentHashMMap.BinaryRecord.getSku(record);
        short restoredStore = ConcurrentHashMMap.BinaryRecord.getStore(record);
        int restoredAmount = ConcurrentHashMMap.BinaryRecord.getAmount(record);

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
        ConcurrentHashMMap map = init();

        AvailabilityItem item = new AvailabilityItem(12509, (short)23, 399);

        AvailabilityItem restoredItem = map.get(item.key());
        assertNull(restoredItem);

        map.put(item.key(), item);
        restoredItem = map.get(item.key());
        assertNotNull(restoredItem);
        assertEquals(item.getSku(), restoredItem.getSku());
        assertEquals(item.getStore(), restoredItem.getStore());
        assertEquals(item.getAmount(), restoredItem.getAmount());

        restoredItem = map.remove(item.key());
        assertNotNull(restoredItem);
        assertEquals(item.getSku(), restoredItem.getSku());
        assertEquals(item.getStore(), restoredItem.getStore());
        assertEquals(item.getAmount(), restoredItem.getAmount());

        restoredItem = map.get(item.key());
        assertNull(restoredItem);
    }

    @Test
    public void randomIntegrationTest() {
        ConcurrentHashMMap map = init();

        System.out.println("Initialized");
        List<AvailabilityItem> addedItems = new ArrayList<AvailabilityItem>(12000);
        List<AvailabilityItem> notAddedItems = new ArrayList<AvailabilityItem>(8000);

        for (int i=0; i<100000; i++)
            notAddedItems.add(new AvailabilityItem(getSku(), getStore(), getAmount()));

        for (int i=0; i<100000; i++) {
            if (i > 0 && i % 10000 == 0) System.out.println(i + " passed");
            AvailabilityItem item = new AvailabilityItem(getSku(), getStore(), getAmount());
            try {
                if (map.get(item.key()) == null) {
                    map.put(item.key(), item);
                    addedItems.add(item);
                    if (random.nextInt(10) == 0) {
                        Thread.sleep(1);
                    }
                }
            } catch (RuntimeException e) {
                e.printStackTrace();
                assert false;
            } catch (InterruptedException e) {
                e.printStackTrace();
                assert false;
            }
        }

        System.out.println("Finalizing records");
        map.prepareForShutdown();

        System.out.println("Assertions started");

        // optimistic test
        for (AvailabilityItem item: addedItems) {
            try {
                AvailabilityItem restoredItem = map.get(item.key());
                assertNotNull(restoredItem);
                assertEquals(item.getSku(), restoredItem.getSku());
                assertEquals(item.getStore(), restoredItem.getStore());
                assertEquals(item.getAmount(), restoredItem.getAmount());
            } catch (AssertionFailedError e) {
                System.out.println("Error " + item.getSku() + " - " + item.getStore() + " - " + item.getAmount());
            }
        }

        // pessimistic test
        for (AvailabilityItem item: notAddedItems) {
            AvailabilityItem restoredItem = map.get(item.key());
            assertNull(restoredItem);
        }
        System.out.println("Assertions completed");
    }

    @Test
    public void syncTest() {
        Long value = 23897523985L, val2 = null;
        long start, total;

        start = System.nanoTime();
        synchronized (value) {
            val2 = 2L;
        }
        total = System.nanoTime() - start;
        System.out.println("Total: " + total);
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
