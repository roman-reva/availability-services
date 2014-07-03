package com.services.availability.protocol.binary;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-26 12:54
 */
public enum BinaryRequestType {
    PUT(1),
    GET(2),
    REMOVE(3);

    public final byte code;
    private BinaryRequestType(int intCode) {
        code = (byte)intCode;
    }
    public byte getCode() {
        return code;
    }
    public static BinaryRequestType getByCode(int code) {
        switch (code) {
            case 1: return BinaryRequestType.PUT;
            case 2: return BinaryRequestType.GET;
            case 3: return BinaryRequestType.REMOVE;
            default: throw new IllegalArgumentException("Unknown code");
        }
    }

}
