package com.services.availability.utils;

/**
 * Current class provides utility methods to perform basic operations
 * on byte arrays.
 *
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-17 14:39
 */
public class ByteUtils {
    public static void putInt(int value, byte[] record, int offset) {
        for (int i=offset, j=3; i<offset+4; i++, j--) {
            record[i] = (byte)(0xff & (value >> (j*8)));
        }
    }

    public static int getInt(byte[] record, int offset) {
        int value = 0;
        for (int i=offset, j=3; i<offset+4; i++, j--) {
            value |= ((record[i] & 0xff) << (j*8));
        }
        return value;
    }

    public static void putShort(short value, byte[] record, int offset) {
        for (int i=offset, j=1; i<offset+2; i++, j--) {
            record[i] = (byte)(0xff & (value >> (j*8)));
        }
    }

    public static short getShort(byte[] record, int offset) {
        short value = 0;
        for (int i=offset, j=1; i<offset+2; i++, j--) {
            value |= ((record[i] & 0xff) << (j*8));
        }
        return value;
    }

    public static void putLong(long value, byte[] record, int offset) {
        for (int i=offset, j=7; i<offset+8; i++, j--) {
            record[i]   = (byte)(0xff & (value >> (j*8)));
        }
    }

    public static long getLong(byte[] array, int offset) {
        long value = 0;
        for (int i=offset, j=7; i<offset+8; i++, j--) {
            value |= ((long)(array[i] & 0xff) << (j*8));
        }
        return value;
    }
}
