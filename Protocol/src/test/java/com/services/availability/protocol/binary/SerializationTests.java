package com.services.availability.protocol.binary;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-26 13:24
 */
public class SerializationTests {
    @Test
    public void requestSerializationTest() {
        BinaryRequest request = new BinaryRequest(BinaryRequestType.GET, 124010, (short)1202, 5829);
        ByteBuffer byteBuffer = ByteBuffer.allocate(16);
        request.putToBuffer(byteBuffer);

        assertEquals(11, byteBuffer.position());

        byteBuffer.flip();
        BinaryRequest restoredRequest = BinaryRequest.fromByteBuffer(byteBuffer);

        assert request != restoredRequest;
        assert request.equals(restoredRequest);

        assertEquals(request.getRequestType(), restoredRequest.getRequestType());
        assertEquals(request.getSku(), restoredRequest.getSku());
        assertEquals(request.getStore(), restoredRequest.getStore());
        assertEquals(request.getAmount(), restoredRequest.getAmount());
    }

    @Test
    public void responseSerializationTest() {
        BinaryResponse response = new BinaryResponse(BinaryRequestType.GET, BinaryErrorCodes.ITEM_NOT_FOUND, 124010, (short)1202, -1);
        ByteBuffer byteBuffer = ByteBuffer.allocate(16);
        response.putToBuffer(byteBuffer);

        assertEquals(12, byteBuffer.position());

        byteBuffer.flip();
        BinaryResponse restoredResponse = BinaryResponse.fromByteBuffer(byteBuffer);

        assert response != restoredResponse;
        assert response.equals(restoredResponse);

        assertEquals(response.getRequestType(), restoredResponse.getRequestType());
        assertEquals(response.getErrorCode(), restoredResponse.getErrorCode());
        assertEquals(response.getSku(), restoredResponse.getSku());
        assertEquals(response.getStore(), restoredResponse.getStore());
        assertEquals(response.getAmount(), restoredResponse.getAmount());

        assert response.isError();
        assert !response.isSuccess();
    }
}
