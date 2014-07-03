package com.services.availability.protocol.binary;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-26 13:13
 */
public enum BinaryErrorCodes {
    SUCCESS(0),
    IOEXCEPTION(1),
    ITEM_NOT_FOUND(2),
    UNKNOWN_REQUEST(3);
    BinaryErrorCodes(int code) {
        this.code = (byte) code;
    }
    private byte code;
    public byte getCode() {
        return code;
    }

    public static BinaryErrorCodes getByCode(int code) {
        switch (code) {
            case 0: return BinaryErrorCodes.SUCCESS;
            case 1: return BinaryErrorCodes.IOEXCEPTION;
            case 2: return BinaryErrorCodes.ITEM_NOT_FOUND;
            case 3: return BinaryErrorCodes.UNKNOWN_REQUEST;
            default: throw new IllegalArgumentException("Unknown code");
        }
    }
}
