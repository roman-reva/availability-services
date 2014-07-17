package com.services.availability.server;

import com.services.availability.common.ThroughputMeter;
import org.apache.log4j.Logger;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-23 13:49
 */
public class HeartbeatThread extends Thread {
    public final static long HEARTBEAT_DELAY = 2000; // each 10 second

    protected final Logger log = Logger.getLogger(HeartbeatThread.class);

    private final AbstractServer server;
    private final RequestProcessor requestProcessor;
    private final ThroughputMeter throughputMeter;

    public HeartbeatThread(AbstractServer server) {
        if (server == null) throw new IllegalArgumentException("Server object cannot be null");
        this.server = server;
        this.requestProcessor = server.requestProcessor;
        this.throughputMeter = server.throughputMeter;
    }

    public void run() {
        double throughput = 0;
        log.debug("HEARTBEAT : thread started.");
        while (server.isRunning) {
            throughputMeter.start();
            log.debug("HEARTBEAT : server.readsNumber=" + requestProcessor.readsNumber + "; server.writesNumber=" + requestProcessor.writesNumber + "; throughput=" + throughput);
            try {
                Thread.sleep(HEARTBEAT_DELAY);
            } catch (InterruptedException e) {
                log.debug("HEARTBEAT : server shutdown event fired.");
                break;
            }
            throughputMeter.stop();
            throughput = throughputMeter.getThroughput();
            throughputMeter.reset();
        }
        log.debug("HEARTBEAT : thread stopped.");
    }
}
