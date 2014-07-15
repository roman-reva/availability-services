package com.services.availability.server.storage;

import java.util.Random;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-15 19:52
 */
public class TestUtils {

    private static Random random = new Random();

    public static short getStore() {
        return (short)random.nextInt(1000);
    }

    public static int getAmount() {
        return random.nextInt(50000);
    }

    public static int getSku() {
        return Math.abs(random.nextInt());
    }
}
