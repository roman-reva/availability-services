package com.services.availability.client.singlethread;

import com.services.availability.protocol.binary.BinaryRequest;
import com.services.availability.protocol.binary.BinaryResponse;

import java.io.IOException;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-29 14:22
 */
public interface Client {
    public abstract void initClient() throws IOException;
    public abstract void closeClient() throws IOException;
}
