package com.services.availability.protocol;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-19 18:59
 */
public class GetAvailabilityRequest extends AbstractAvailabilityRequest {
    public GetAvailabilityRequest(int sku, short store) {
        super(sku, store);
    }
}
