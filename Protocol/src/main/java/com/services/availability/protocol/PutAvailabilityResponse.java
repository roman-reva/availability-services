package com.services.availability.protocol;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-19 18:59
 */
public class PutAvailabilityResponse extends AbstractAvailabilityResponse {
    public PutAvailabilityResponse(boolean error, ResponseErrorCodes errorCode) {
        super(error, errorCode);
    }

    public PutAvailabilityResponse(int sku, short store, int amount) {
        super(sku, store, amount);
    }
}
