package com.services.availability.server;

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

    protected ServerShutdownHook serverShutdownHook = null;

    protected AbstractServer() {
        throughputMeter = new ThroughputMeter();

        logger = getLogger();
    }

    protected AbstractServer(ServerShutdownHook hook) {
        throughputMeter = new ThroughputMeter();

        logger = getLogger();
        serverShutdownHook = hook;
    }

    public void startup() throws IOException {
        isRunning = true;

        verifyRequestProcessor();
        openServerSocket();
        startHeartbeat();

        serverLoop();
    }

    public void shutdown() throws IOException {
        if (serverShutdownHook != null) serverShutdownHook.beforeShutdownAction();

        isRunning = false;
        if (serverSocketChannel != null) {
            serverSocketChannel.close();
        }
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }

        if (serverShutdownHook != null) serverShutdownHook.afterShutdownAction();
    }

    protected abstract void serverLoop() throws IOException;

    protected void verifyRequestProcessor() {
        if (requestProcessor == null)
            throw  new IllegalStateException("No RequestProcessor is attached");
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

    public ThroughputMeter getThroughputMeter() {
        return throughputMeter;
    }

    public void setRequestProcessor(RequestProcessor requestProcessor) {
        this.requestProcessor = requestProcessor;
    }

    /**
     * An implementation of current interface could be provided to the server,
     * so that corresponding methods will be executed just before and after
     * the server shut down.
     */
    public static interface ServerShutdownHook {
        public void beforeShutdownAction();
        public void afterShutdownAction();
    }
}
