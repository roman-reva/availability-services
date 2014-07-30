package com.services.availability.client.singlethread;

import com.services.availability.common.ThroughputMeter;
import org.apache.log4j.Logger;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-25 18:36
 */
public class ClientHeartbeatThread extends Thread {
    private final static int HEARTBEAT_DELAY = 2000;

    private volatile boolean alive = true;


    private Logger log = Logger.getLogger(ClientHeartbeatThread.class);
    private ThroughputMeter throughputMeter;

    public ClientHeartbeatThread(AbstractClient client) {
        throughputMeter = client.throughputMeter;
    }

    @Override
    public void run() {
        while (alive) {
            throughputMeter.reset();
            throughputMeter.start();

            try {
                Thread.sleep(HEARTBEAT_DELAY);
            } catch (InterruptedException e) {
                break;
            }

            throughputMeter.stop();
            log.debug("CLIENT HEARTBEAT : throughput = " + throughputMeter.getThroughput());
        }
        log.debug("CLIENT HEARTBEAT : stopped.");
    }

    public void killHeartbeat() {
        alive = false;
    }
}
