package com.services.availability.storage.hashmmap;

import com.services.availability.model.AvailabilityItem;
import com.services.availability.TestUtils;
import com.services.availability.storage.hashmmap.ConcurrentHashMMap;
import junit.framework.AssertionFailedError;
import org.junit.Test;

import java.util.*;

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
            notAddedItems.add(new AvailabilityItem(TestUtils.getSku(), TestUtils.getStore(), TestUtils.getAmount()));

        for (int i=0; i<100000; i++) {
            if (i > 0 && i % 10000 == 0) System.out.println(i + " passed");
            AvailabilityItem item = new AvailabilityItem(TestUtils.getSku(), TestUtils.getStore(), TestUtils.getAmount());
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

    /**
     * Current test starts up 20 threads that are perform concurrent put of
     * different values by the same key. In the same time the main thread reads
     * the value by the same key from the map and calculates how often each
     * value is return.
     */
    @Test
    public void concurrentUpdateTest() {
        int threadNum = 20;

        ConcurrentHashMMap map = new ConcurrentHashMMap();
        FlagHolder holder = new FlagHolder(true);

        Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
        List<TestRunner> threads = new LinkedList<TestRunner>();
        for (int i = 0; i < threadNum; i++) {
            TestRunner runner = new TestRunner(holder, map, i);
            threads.add(runner);
            runner.start();
            counts.put(i, 0);
        }

        holder.putStarted = true;

        long key = AvailabilityItem.key(TestRunner.SKU, TestRunner.STORE);
        long stopTime = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < stopTime) {
            AvailabilityItem item = map.get(key);
            if (item == null) continue;

            int amount = item.getAmount();
            counts.put(amount, counts.get(amount) + 1);
        }
        assert counts.size() == 20;

        holder.isRunning = false;

        boolean atLeastOneThreadIsRunning = true;
        outer: while (atLeastOneThreadIsRunning) {
            for (TestRunner t : threads) {
                if (t.isAlive()) {
                    continue outer;
                }
            }
            atLeastOneThreadIsRunning = false;
        }

        for (int num: counts.keySet()) {
            System.out.println(num + ": \t" + counts.get(num));
        }
    }

    static class FlagHolder {
        public volatile boolean isRunning;
        public volatile boolean putStarted = false;

        FlagHolder(boolean running) {
            isRunning = running;
        }
    }

    static class TestRunner extends Thread {
        private final FlagHolder holder;
        private final ConcurrentHashMMap map;
        private final int num;

        public static final int SKU = 982348;
        public static final short STORE = 1000;

        public TestRunner(FlagHolder holder, ConcurrentHashMMap map, int num) {
            this.holder = holder;
            this.map = map;
            this.num = num;
        }

        @Override
        public void run() {
            System.out.println("Thread " + Thread.currentThread().getId() + " is started");
            while (holder.isRunning) {
                AvailabilityItem item = new AvailabilityItem(SKU, STORE, num);
                if (holder.putStarted) {
                    map.put(item.key(), item);
                }
            }
            System.out.println("Thread " + Thread.currentThread().getId() + " is terminated");
        }
    }

}
