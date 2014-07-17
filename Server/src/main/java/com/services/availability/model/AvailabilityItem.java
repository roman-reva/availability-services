package com.services.availability.model;

/**
 * @author Roman Reva
 * @since  2014-06-19 18:34
 * @version 1.0
 */
public class AvailabilityItem {
    private int sku;
    private short store;
    private int amount;

    public AvailabilityItem(int sku, short store, int amount) {
        this.sku = sku;
        this.store = store;
        this.amount = amount;
    }

    public int getSku() {
        return sku;
    }

    public void setSku(int sku) {
        this.sku = sku;
    }

    public short getStore() {
        return store;
    }

    public void setStore(short store) {
        this.store = store;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(short amount) {
        this.amount = amount;
    }

    public long key() {
        return key(sku, store);
    }

    public static long key(int sku, short store) {
        return (((long)sku) << 16) + store;
    }

    public static int keyToHashCode(long key) {
        short store = (short)key;
        int sku = (int) (key >> 16);
        int hashCode = 31 * sku + (int) store;
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AvailabilityItem that = (AvailabilityItem) o;

        if (amount != that.amount) return false;
        if (sku != that.sku) return false;
        if (store != that.store) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = sku;
        result = 31 * result + (int) store;
        return result;
    }
}
