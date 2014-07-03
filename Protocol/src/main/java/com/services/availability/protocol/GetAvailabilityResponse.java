package com.services.availability.protocol;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-19 18:59
 */
public class GetAvailabilityResponse extends AbstractAvailabilityResponse {
    public GetAvailabilityResponse(boolean error, ResponseErrorCodes errorCode) {
        super(error, errorCode);
    }

    public GetAvailabilityResponse(int sku, short store, int amount) {
        super(sku, store, amount);
    }
}
