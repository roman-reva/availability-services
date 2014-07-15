package com.services.availability.server.core;

import com.services.availability.protocol.*;
import com.services.availability.protocol.binary.BinaryErrorCodes;
import com.services.availability.protocol.binary.BinaryRequest;
import com.services.availability.protocol.binary.BinaryRequestType;
import com.services.availability.protocol.binary.BinaryResponse;
import com.services.availability.server.storage.AvailabilityItem;
import com.services.availability.server.storage.ConcurrentHashMMap;
import com.services.availability.common.SerializationUtils;
import com.services.availability.common.ThroughputMeter;

import java.io.IOException;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-19 19:14
 */
public class RequestProcessor {
    protected volatile long writesNumber = 0L;
    protected volatile long readsNumber = 0L;
    protected volatile long removesNumber = 0L;

//    private HashMapStorage storage = new HashMapStorage();
//    private HashMMapStorage storage = new HashMMapStorage();
    private ConcurrentHashMMap storage = new ConcurrentHashMMap();
    private ThroughputMeter throughputMeter;

    public RequestProcessor(ThroughputMeter throughputMeter) {
        this.throughputMeter = throughputMeter;
    }

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

    public byte[] processRequest(byte[] requestData) throws IOException{
        byte [] byteResponse = new byte[0];

        throughputMeter.inc();

        try {
            Object requestObject = SerializationUtils.deserialize(requestData);
            Object responseObject = null;

            if (requestObject instanceof PutAvailabilityRequest) {
                responseObject = processPut((PutAvailabilityRequest)requestObject);
                writesNumber++;
            } else if (requestObject instanceof  GetAvailabilityRequest) {
                responseObject = processGet((GetAvailabilityRequest) requestObject);
                readsNumber++;
            } else if (requestObject instanceof  RemoveAvailabilityRequest) {
                responseObject = processRemove((RemoveAvailabilityRequest)requestObject);
                removesNumber++;
            }

            if (responseObject != null) {
                byteResponse = SerializationUtils.serialize(responseObject);
            }

        } catch (ClassNotFoundException e) {
            throw new IOException("Serialization/deserialization error", e);
        }

        return byteResponse;
    }

    public void shutdownStorage() {
        storage.prepareForShutdown();
    }

    private PutAvailabilityResponse processPut(PutAvailabilityRequest request) {
        AvailabilityItem item = new AvailabilityItem(request.getSku(), request.getStore(), request.getAmount());
        storage.put(item.key(), item);
        return new PutAvailabilityResponse(item.getSku(), item.getStore(), item.getAmount());
    }

    private GetAvailabilityResponse processGet(GetAvailabilityRequest request) {
        AvailabilityItem item = storage.get(AvailabilityItem.key(request.getSku(), request.getStore()));
        if (item == null) {
            return new GetAvailabilityResponse(true, AbstractResponse.ResponseErrorCodes.ITEM_NOT_FOUND);
        }
        return new GetAvailabilityResponse(item.getSku(), item.getStore(), item.getAmount());
    }

    private RemoveAvailabilityResponse processRemove(RemoveAvailabilityRequest request) {
        AvailabilityItem item = storage.remove(AvailabilityItem.key(request.getSku(), request.getStore()));
        if (item == null) {
            return new RemoveAvailabilityResponse(true, AbstractResponse.ResponseErrorCodes.ITEM_NOT_FOUND);
        }
        return new RemoveAvailabilityResponse(item.getSku(), item.getStore(), item.getAmount());
    }

    private BinaryResponse processPut(BinaryRequest request) {
        AvailabilityItem item = new AvailabilityItem(request.getSku(), request.getStore(), request.getAmount());
        storage.put(item.key(), item);
        return new BinaryResponse(request.getRequestType(), item.getSku(), item.getStore(), item.getAmount());
    }

    private BinaryResponse processGet(BinaryRequest request) {
        AvailabilityItem item = storage.get(AvailabilityItem.key(request.getSku(), request.getStore()));
        if (item == null) {
            return new BinaryResponse(request.getRequestType(), BinaryErrorCodes.ITEM_NOT_FOUND);
        }
        return new BinaryResponse(request.getRequestType(), item.getSku(), item.getStore(), item.getAmount());
    }

    private BinaryResponse processRemove(BinaryRequest request) {
        AvailabilityItem item = storage.remove(AvailabilityItem.key(request.getSku(), request.getStore()));
        if (item == null) {
            return new BinaryResponse(request.getRequestType(), BinaryErrorCodes.ITEM_NOT_FOUND);
        }
        return new BinaryResponse(request.getRequestType(), item.getSku(), item.getStore(), item.getAmount());
    }
}
