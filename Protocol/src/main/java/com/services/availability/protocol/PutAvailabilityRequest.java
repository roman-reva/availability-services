package com.services.availability.protocol;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-19 18:58
 */
public class PutAvailabilityRequest extends AbstractAvailabilityRequest {
    private short amount;

    public PutAvailabilityRequest(int sku, short store, short amount) {
        super(sku, store);
        this.amount = amount;
    }

    public short getAmount() {
        return amount;
    }
}
