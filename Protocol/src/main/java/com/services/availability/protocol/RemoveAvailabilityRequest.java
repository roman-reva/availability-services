package com.services.availability.protocol;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-19 18:59
 */
public class RemoveAvailabilityRequest extends AbstractAvailabilityRequest {
    public RemoveAvailabilityRequest(int sku, short store) {
        super(sku, store);
    }
}
