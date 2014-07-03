package com.services.availability.server;

import com.services.availability.protocol.binary.BinaryRequest;
import com.services.availability.protocol.binary.BinaryResponse;
import com.services.availability.server.core.AbstractServer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-24 14:18
 */
public class NIOSingleThreadServer extends AbstractServer {
    private final InetSocketAddress serverAddress = new InetSocketAddress(8888);

    @Override
    protected void serverLoop() throws IOException {
        Selector selector = Selector.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        ByteBuffer buffer = ByteBuffer.allocate(512);

        while (true) {
            int readyChannels = selector.select();
            if (readyChannels == 0) continue;

            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
            while (keyIterator.hasNext()) {
                SelectionKey key = keyIterator.next();
                if(key.isAcceptable()) {
                    SocketChannel client = serverSocketChannel.accept();
                    client.configureBlocking(false);
                    client.socket().setTcpNoDelay(true);
                    client.register(selector, SelectionKey.OP_READ);
                } else if (key.isReadable()) {
                    buffer.clear();
                    SocketChannel clientChannel = ((SocketChannel)key.channel());
                    if (clientChannel.read(buffer) == -1) {
                        throw new IOException("Read on closed key");
                    }

                    clientChannel.read(buffer);

                    byte[] byteResponse = requestProcessor.processRequest(buffer.array());
                    buffer.clear();

                    buffer.put(byteResponse);
                    buffer.flip();
                    clientChannel.write(buffer);
                    clientChannel.close();
                }
                keyIterator.remove();
            }
        }

    }

    @Override
    protected InetSocketAddress getBindAddress() {
        return serverAddress;
    }

    @Override
    protected Logger getLogger() {
        return Logger.getLogger(NIOSingleThreadServer.class);
    }

    public static void main(String[] args) throws IOException {
        new NIOSingleThreadServer().startup();
    }
}