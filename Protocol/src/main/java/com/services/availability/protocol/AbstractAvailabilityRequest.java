package com.services.availability.protocol;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-24 13:24
 */
public abstract class AbstractAvailabilityRequest extends AbstractRequest {
    private int sku;
    private short store;

    protected AbstractAvailabilityRequest(int sku, short store) {
        super(RequestType.USER_REQUEST);
        this.sku = sku;
        this.store = store;
    }

    public int getSku() {
        return sku;
    }

    public short getStore() {
        return store;
    }

}
