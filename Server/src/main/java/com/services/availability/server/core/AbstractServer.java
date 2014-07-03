package com.services.availability.server.core;

import com.services.availability.common.ThroughputMeter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-19 19:14
 */
public abstract class AbstractServer {
    protected final Logger logger;

    protected volatile boolean isRunning = false;

    protected HeartbeatThread heartbeatThread;
    protected RequestProcessor requestProcessor;
    protected ThroughputMeter throughputMeter;
    protected ServerSocketChannel serverSocketChannel;

    protected AbstractServer() {
        logger = getLogger();
    }

    public void startup() throws IOException {
        isRunning = true;

        initComponents();
        openServerSocket();
        startHeartbeat();
        addShutdownHook();

        serverLoop();
    }

    public void shutdown() throws IOException {
        isRunning = false;
        if (serverSocketChannel != null) {
            serverSocketChannel.close();
        }
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }
    }

    protected abstract void serverLoop() throws IOException;

    protected void initComponents() {
        throughputMeter = new ThroughputMeter();
        requestProcessor = new RequestProcessor(throughputMeter);
    }

    protected void openServerSocket() throws IOException {
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(getBindAddress());
    }

    protected void startHeartbeat() {
        heartbeatThread = new HeartbeatThread(this);
        heartbeatThread.start();
    }

    protected abstract InetSocketAddress getBindAddress();

    protected abstract Logger getLogger();

    protected void addShutdownHook() {
        final AbstractServer serverObject = this;
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.debug("SHUTDOWN HOOK : Server shutdown event registered.");
                try {
                    serverObject.shutdown();
                } catch (IOException e) {
                    logger.error("Error during server shutdown", e);
                }
            }
        });
    }
}
