package com.services.availability.client.singlethread;

import com.services.availability.common.ThroughputMeter;
import com.services.availability.protocol.binary.BinaryRequest;
import com.services.availability.protocol.binary.BinaryResponse;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-25 14:15
 */
public abstract class AbstractClient implements Client {
    protected final InetSocketAddress serverAddress;
    protected final ThroughputMeter throughputMeter;

    public AbstractClient(String host, int port) {
        serverAddress = new InetSocketAddress(host, port);
        throughputMeter = new ThroughputMeter();
    }

    public abstract BinaryResponse performRequest(BinaryRequest request) throws IOException;

    public abstract void initClient() throws IOException;
    public abstract void closeClient() throws IOException;

    public InetSocketAddress getServerAddress() {
        return serverAddress;
    }
}
