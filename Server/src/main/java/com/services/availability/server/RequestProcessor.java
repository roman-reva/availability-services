package com.services.availability.server;

import com.services.availability.protocol.binary.BinaryErrorCodes;
import com.services.availability.protocol.binary.BinaryRequest;
import com.services.availability.protocol.binary.BinaryRequestType;
import com.services.availability.protocol.binary.BinaryResponse;
import com.services.availability.model.AvailabilityItem;
import com.services.availability.common.ThroughputMeter;
import com.services.availability.storage.Storage;

/**
 * Current class fetches data from server requests, executes PUT, GET and REMOVE
 * business operations and returns a server response.
 *
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-19 19:14
 */
public class RequestProcessor {
    protected volatile long writesNumber = 0L;
    protected volatile long readsNumber = 0L;
    protected volatile long removesNumber = 0L;

    private final Storage storage;
    private final ThroughputMeter throughputMeter;

    public RequestProcessor(ThroughputMeter throughputMeter, Storage storage) {
        this.throughputMeter = throughputMeter;
        this.storage = storage;
    }

    /**
     * Performs processing of the provided request and counts number of
     * requests processed.
     *
     * @param request BinaryRequest to process
     * @return BinaryResponse
     */
    public BinaryResponse processRequest(BinaryRequest request) {
        throughputMeter.inc();

        if (request.getRequestType() == BinaryRequestType.GET) {
            readsNumber++;
            return processGet(request);
        } else if (request.getRequestType() == BinaryRequestType.PUT) {
            writesNumber++;
            return processPut(request);
        } else if (request.getRequestType() == BinaryRequestType.REMOVE) {
            removesNumber++;
            return processRemove(request);
        } else {
            return new BinaryResponse(request.getRequestType(), BinaryErrorCodes.UNKNOWN_REQUEST);
        }
    }

    /**
     * Converts request into Availability Item object and performs PUT
     * operation on storage.
     *
     * @param request request to process
     * @return server response
     */
    private BinaryResponse processPut(BinaryRequest request) {
        AvailabilityItem item = new AvailabilityItem(request.getSku(), request.getStore(), request.getAmount());
        storage.put(item.key(), item);
        return new BinaryResponse(request.getRequestType(), item.getSku(), item.getStore(), item.getAmount());
    }

    /**
     * Converts request into Availability Item object and performs GET
     * operation on storage.
     *
     * @param request request to process
     * @return server response
     */
    private BinaryResponse processGet(BinaryRequest request) {
        AvailabilityItem item = storage.get(AvailabilityItem.key(request.getSku(), request.getStore()));
        if (item == null) {
            return new BinaryResponse(request.getRequestType(), BinaryErrorCodes.ITEM_NOT_FOUND);
        }
        return new BinaryResponse(request.getRequestType(), item.getSku(), item.getStore(), item.getAmount());
    }

    /**
     * Converts request into Availability Item object and performs REMOVE
     * operation on storage.
     *
     * @param request request to process
     * @return server response
     */
    private BinaryResponse processRemove(BinaryRequest request) {
        AvailabilityItem item = storage.remove(AvailabilityItem.key(request.getSku(), request.getStore()));
        if (item == null) {
            return new BinaryResponse(request.getRequestType(), BinaryErrorCodes.ITEM_NOT_FOUND);
        }
        return new BinaryResponse(request.getRequestType(), item.getSku(), item.getStore(), item.getAmount());
    }
}
