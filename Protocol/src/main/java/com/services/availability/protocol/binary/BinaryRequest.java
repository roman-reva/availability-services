package com.services.availability.protocol.binary;

import java.nio.ByteBuffer;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-26 12:52
 */
public class BinaryRequest {
    private BinaryRequestType requestType;
    private int sku;
    private short store;
    private int amount;

    /**
     * Request constructor.
     *
     * @param requestType type of request
     * @param sku requested sku
     * @param store requested store
     * @param amount requested amount
     */
    public BinaryRequest(BinaryRequestType requestType, int sku, short store, int amount) {
        if (requestType == null) throw new IllegalArgumentException("parameter `requestType` is null");
        this.requestType = requestType;
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

    public void putToBuffer(ByteBuffer buffer) {
        buffer.put(requestType.getCode());
        buffer.putInt(sku);
        buffer.putShort(store);
        buffer.putInt(amount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BinaryRequest request = (BinaryRequest) o;

        if (amount != request.amount) return false;
        if (sku != request.sku) return false;
        if (store != request.store) return false;
        if (requestType != request.requestType) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = requestType.hashCode();
        result = 31 * result + sku;
        result = 31 * result + (int) store;
        result = 31 * result + amount;
        return result;
    }

    public static BinaryRequest fromByteBuffer(ByteBuffer buffer) {
        BinaryRequestType requestType = BinaryRequestType.getByCode(buffer.get());
        int sku = buffer.getInt();
        short store = buffer.getShort();
        int amount = buffer.getInt();

        return new BinaryRequest(requestType, sku, store, amount);
    }
}
