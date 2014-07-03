package com.services.availability.protocol.binary;

import java.nio.ByteBuffer;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-26 12:52
 */
public class BinaryResponse {
    private BinaryRequestType requestType;
    private BinaryErrorCodes errorCode;
    private int sku = -1;
    private short store = -1;
    private int amount = -1;

    /**
     * Successful constructor. Is called when no error occurs during the processing of the request.
     * @param requestType type of request
     * @param sku requested sku
     * @param store requested store
     * @param amount requested amount
     */
    public BinaryResponse(BinaryRequestType requestType, int sku, short store, int amount) {
        if (requestType == null) throw new IllegalArgumentException("parameter `requestType` is null");

        this.requestType = requestType;
        this.errorCode = BinaryErrorCodes.SUCCESS;
        this.sku = sku;
        this.store = store;
        this.amount = amount;
    }

    /**
     * Pessimistic constructor. Is called when an error occurs during the processing of the request.
     * @param requestType type of request
     * @param errorCode byte code of error that occurred
     */
    public BinaryResponse(BinaryRequestType requestType, BinaryErrorCodes errorCode) {
        if (requestType == null) throw new IllegalArgumentException("parameter `requestType` is null");
        if (errorCode == null) throw new IllegalArgumentException("parameter `errorCode` is null");

        this.requestType = requestType;
        this.errorCode = errorCode;
    }

    /**
     * Pessimistic constructor. Is called when an error occurs during the processing of the request.
     * @param requestType type of request
     * @param errorCode byte code of error that occurred
     * @param sku requested sku
     * @param store requested store
     * @param amount requested amount
     */
    public BinaryResponse(BinaryRequestType requestType, BinaryErrorCodes errorCode, int sku, short store, int amount) {
        if (requestType == null) throw new IllegalArgumentException("parameter `requestType` is null");
        if (errorCode == null) throw new IllegalArgumentException("parameter `errorCode` is null");

        this.requestType = requestType;
        this.errorCode = errorCode;
        this.sku = sku;
        this.store = store;
        this.amount = amount;
    }

    public BinaryRequestType getRequestType() {
        return requestType;
    }

    public int getSku() {
        return sku;
    }

    public short getStore() {
        return store;
    }

    public int getAmount() {
        return amount;
    }

    public BinaryErrorCodes getErrorCode() {
        return errorCode;
    }

    public boolean isSuccess() {
        return errorCode == BinaryErrorCodes.SUCCESS;
    }

    public boolean isError() {
        return !isSuccess();
    }

    public void putToBuffer(ByteBuffer buffer) {
        buffer.put(requestType.getCode());
        buffer.put(errorCode.getCode());
        buffer.putInt(sku);
        buffer.putShort(store);
        buffer.putInt(amount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BinaryResponse response = (BinaryResponse) o;

        if (amount != response.amount) return false;
        if (sku != response.sku) return false;
        if (store != response.store) return false;
        if (errorCode != response.errorCode) return false;
        if (requestType != response.requestType) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = requestType.hashCode();
        result = 31 * result + errorCode.hashCode();
        result = 31 * result + sku;
        result = 31 * result + (int) store;
        result = 31 * result + amount;
        return result;
    }

    public static BinaryResponse fromByteBuffer(ByteBuffer buffer) {
        BinaryRequestType requestType = BinaryRequestType.getByCode(buffer.get());
        BinaryErrorCodes errorCode = BinaryErrorCodes.getByCode(buffer.get());

        int sku = buffer.getInt();
        short store = buffer.getShort();
        int amount = buffer.getInt();

        return new BinaryResponse(requestType, errorCode, sku, store, amount);
    }
}
