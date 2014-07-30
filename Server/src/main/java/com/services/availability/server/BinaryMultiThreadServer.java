package com.services.availability.server;

import com.services.availability.protocol.binary.BinaryRequest;
import com.services.availability.protocol.binary.BinaryResponse;
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
 * @since 2014-07-17 17:50
 */
public class BinaryMultiThreadServer extends AbstractServer {
    private final static int DEFAULT_THREAD_NUMBER = 4;

    private final InetSocketAddress serverAddress = new InetSocketAddress(8888);
    private AsyncRequestProcessor asyncRequestProcessor;
    private int threadNumber;

    private int counter = 0;

    /**
     * Default client constructor.
     */
    public BinaryMultiThreadServer() {
        super();
        this.threadNumber = DEFAULT_THREAD_NUMBER;
    }

    /**
     * Client constructor that allows to set up a definite number of server threads.
     */
    public BinaryMultiThreadServer(int threadNumber) {
        super();
        this.threadNumber = threadNumber;
    }

    /**
     * Current constructor allows to provide ServerShutdownHook entity that
     * will contain some actions that should be executed during the client
     * shutdown.
     *
     * @param hook implementation of a ServerShutdownHook interface
     */
    public BinaryMultiThreadServer(ServerShutdownHook hook, int threadNumber) {
        super(hook);
        this.threadNumber = threadNumber;
    }

    @Override
    protected void serverLoop() throws IOException {
        Selector selector = Selector.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        ByteBuffer buffer = ByteBuffer.allocate(16);

        while (isRunning) {
            int readyChannels = selector.select();
            if (readyChannels == 0) continue;

            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
            while (keyIterator.hasNext()) {
                SelectionKey key = keyIterator.next();
                if (key.isValid() && key.isAcceptable()) {
                    SocketChannel client = serverSocketChannel.accept();
                    client.configureBlocking(false);
                    client.socket().setTcpNoDelay(true);
                    client.register(selector, SelectionKey.OP_READ);
                } else if (key.isValid() && key.isReadable()) {
                    buffer.clear();
                    SocketChannel clientChannel = ((SocketChannel)key.channel());
                    try {
                        if (clientChannel.read(buffer) == -1) {
                            logger.error("cannot read client channel");
                            clientChannel.close();
                            keyIterator.remove();
                            continue;
                        }
                    } catch (Exception e) {
                        logger.error("Error in client channel: " + clientChannel.hashCode(), e);
                        throw new RuntimeException(e);
                    }

                    clientChannel.read(buffer);
//                    int bytesRead = clientChannel.read(buffer);
//                    if (bytesRead != BinaryRequest.REQUEST_SIZE)
//                        logger.warn("bytesRead != BinaryRequest.REQUEST_SIZE; bytesRead = " + bytesRead);

                    buffer.flip();

                    clientChannel.register(selector, 0);        // remove channel from selector's interest set

                    BinaryRequest request = BinaryRequest.fromByteBuffer(buffer);
                    asyncRequestProcessor.scheduleRequestProcessing(request, buildCallback(clientChannel));
                    counter++;

                }
                keyIterator.remove();
            }
        }
    }

    private AsyncRequestProcessor.ResponseWriteOutCallback buildCallback(final SocketChannel clientChannel) {
        return new AsyncRequestProcessor.ResponseWriteOutCallback() {
            @Override
            public void writeOut(BinaryResponse response) {
                ByteBuffer buffer = ByteBuffer.allocate(16);
                response.putToBuffer(buffer);
                buffer.flip();
                try {
                    clientChannel.write(buffer);
                    clientChannel.close();
//                    logger.debug("Client channel " + clientChannel.hashCode() + "is closed!");
                } catch (IOException e) {
                    logger.error("IOException during writing out the response", e);
                }
            }
        };
    }

    public void setRequestProcessor(RequestProcessor requestProcessor) {
        super.setRequestProcessor(requestProcessor);
        asyncRequestProcessor = new AsyncRequestProcessor(requestProcessor, threadNumber);
    }

    @Override
    protected InetSocketAddress getBindAddress() {
        return serverAddress;
    }

    @Override
    protected Logger getLogger() {
        return Logger.getLogger(BinaryMultiThreadServer.class);
    }
}
