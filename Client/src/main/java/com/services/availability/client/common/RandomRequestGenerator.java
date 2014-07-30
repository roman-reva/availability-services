package com.services.availability.client.common;

import com.services.availability.protocol.binary.BinaryRequest;
import com.services.availability.protocol.binary.BinaryRequestType;

import java.util.Random;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-25 18:25
 */
public class RandomRequestGenerator {
    private Random random = new Random();

    public BinaryRequest getBinaryRequest() {
        return new BinaryRequest(getRequestType(), getSku(), getStore(), getAmount());
    }

    private int getSku() {
        return random.nextInt();
    }

    private short getStore() {
        return (short)random.nextInt(1000);
    }

    private short getAmount() {
        return (short)random.nextInt(50000);
    }

    private BinaryRequestType getRequestType() {
        int code = random.nextInt(2) + 1;
        return BinaryRequestType.getByCode(code);
    }

}
