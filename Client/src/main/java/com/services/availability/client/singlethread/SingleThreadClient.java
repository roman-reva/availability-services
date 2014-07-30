package com.services.availability.client.singlethread;

import com.services.availability.protocol.binary.BinaryRequest;
import com.services.availability.protocol.binary.BinaryResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-20 18:59
 */
public class SingleThreadClient extends AbstractClient {
    private Selector selector = null;

    public SingleThreadClient(String host, int port) {
        super(host, port);
    }

    public void initClient() throws IOException {
        selector = Selector.open();
    }

    public void closeClient() throws IOException {
        if (selector != null && selector.isOpen()) {
            selector.close();
            selector = null;
        }
    }

    public BinaryResponse performRequest(BinaryRequest request) throws IOException {
        return processBinaryRequest(request);
    }

    private BinaryResponse processBinaryRequest(BinaryRequest request) throws IOException {
        if (selector == null || !selector.isOpen()) {
            throw new IllegalStateException("Selector either was not open, or is already closed");
        }

        BinaryResponse response = null;
        SocketChannel socketChannel = null;
        try {
            socketChannel = SocketChannel.open();
            configureChannel(socketChannel);
            socketChannel.connect(serverAddress);

            socketChannel.register(selector, SelectionKey.OP_CONNECT);

            ByteBuffer buffer = ByteBuffer.allocate(16);
            requestLoop: while (true) {
                if (selector.select() > 0) {
                    Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                    while (keys.hasNext()) {
                        SelectionKey key = keys.next();
                        if (key.isConnectable()) {
                            socketChannel.finishConnect();
                            socketChannel.register(selector, SelectionKey.OP_WRITE);
                        }
                        if (key.isWritable()) {
                            buffer.clear();
                            request.putToBuffer(buffer);
                            buffer.flip();
                            socketChannel.write(buffer);
                            socketChannel.register(selector, SelectionKey.OP_READ);
                        }
                        if (key.isReadable()) {
                            buffer.clear();
                            socketChannel.read(buffer);
                            buffer.flip();
                            response = BinaryResponse.fromByteBuffer(buffer);
                            break requestLoop;
                        }
                    }
                }
            }
        } finally {
            if (socketChannel != null && socketChannel.isOpen()) {
                socketChannel.close();
            }
        }

        throughputMeter.inc();

        return response;
    }

    private void configureChannel(SocketChannel channel) throws IOException {
        channel.configureBlocking(false);
        channel.socket().setSendBufferSize(0x100000);
        channel.socket().setReceiveBufferSize(0x100000);
        channel.socket().setKeepAlive(true);
        channel.socket().setReuseAddress(true);
        channel.socket().setSoLinger(false, 0);
        channel.socket().setSoTimeout(0);
        channel.socket().setTcpNoDelay(true);
    }
}
