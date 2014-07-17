package com.services.availability.storage.hashmmap;

import com.services.availability.utils.ByteUtils;

/**
 * Current class provides utility methods to operate with binary buckets
 * like with an object.
 *
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-17 17:13
 */
final class BinaryBucket {
    public static final int BUCKET_SIZE_OFFSET = 0;             // int
    public static final int BUCKET_HEADER_SIZE = 4;             // the rest

    private BinaryBucket() {}

    public static void setSize(int size, byte[] bucket) {
        ByteUtils.putInt(size, bucket, BUCKET_SIZE_OFFSET);
    }

    public static int getSize(byte[] bucket) {
        return ByteUtils.getInt(bucket, BUCKET_SIZE_OFFSET);
    }
}