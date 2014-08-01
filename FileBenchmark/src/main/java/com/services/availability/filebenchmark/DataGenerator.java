package com.services.availability.filebenchmark;

import java.util.Random;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-31 14:53
 */
public class DataGenerator {
    private static Random random = new Random();

    public static byte[] generateBytes(int n) {
        byte[] bytes = new byte[n];
        random.nextBytes(bytes);
        return bytes;
    }
}
