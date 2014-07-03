package com.services.availability.protocol;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-24 13:22
 */
public abstract class AbstractAvailabilityResponse extends AbstractResponse {
    private int sku = -1;
    private short store = -1;
    private int amount = -1;

    public AbstractAvailabilityResponse(boolean error, ResponseErrorCodes errorCode) {
        super(error, errorCode);
    }

    protected AbstractAvailabilityResponse(int sku, short store, int amount) {
        this.sku = sku;
        this.store = store;
        this.amount = amount;
    }

    public int getAmount() {
        return amount;
    }

    public int getSku() {
        return sku;
    }

    public short getStore() {
        return store;
    }

}
