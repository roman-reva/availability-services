package com.services.availability.client.multithread;

import com.services.availability.client.common.RandomRequestGenerator;
import com.services.availability.client.singlethread.SingleThreadClient;
import com.services.availability.protocol.binary.BinaryRequest;
import com.services.availability.protocol.binary.BinaryResponse;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-29 13:45
 */
public class MultithreadLoadGenerator {
    private static final Logger logger = Logger.getLogger(MultithreadLoadGenerator.class);
    private static final int THREAD_NUMBER = 16;

    private final ThreadPoolExecutor executorService = (ThreadPoolExecutor)Executors.newFixedThreadPool(THREAD_NUMBER);
    private final AtomicInteger counter = new AtomicInteger(0);
    private final RandomRequestGenerator generator = new RandomRequestGenerator();

    private final String host;
    private final int port;
    private final double requiredThroughput;

    private final List<Worker> workers = new ArrayList<Worker>();

    private double delay;
    private volatile int delayMs;
    private volatile int delayNs;

    public MultithreadLoadGenerator(String host, int port, double requiredThroughput) {
        if (requiredThroughput > 5000) throw new IllegalArgumentException("Cannot generate more than 5000 req/sec");

        this.host = host;
        this.port = port;
        this.requiredThroughput = requiredThroughput;

    }

    public void start() {
        double singleThreadThroughput = requiredThroughput / THREAD_NUMBER;
        delay = 1000 / singleThreadThroughput;
        setThreadDelays();

        for (int i=0; i<THREAD_NUMBER; i++) {
            workers.add(new Worker());
        }

        for (Worker w: workers) {
            executorService.execute(w);
        }

        while (true) {
            try {
                int requestsBefore = counter.get();
                Thread.sleep(1000);
                int requestsAfter = counter.get();
                int throughput = requestsAfter - requestsBefore;

                logger.debug("throughput = " + throughput);

                double delta = requiredThroughput / throughput;
                logger.debug("delta = " + delta);
                if (Math.abs(delta - 1) > 0.05) {       // throughput correction
                    delay /= delta;
                    logger.debug("delay= " + delay);
                    setThreadDelays();
                }

            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    private void setThreadDelays() {
        delayMs = (int) delay;
        delayNs = (int) ((delay - delayMs) * 1000000);
    }

    private BinaryRequest generateRequest() {
        return generator.getBinaryRequest();
    }

    private class Worker implements Runnable {
        private volatile boolean isRunning = true;

        private final SingleThreadClient client;

        public Worker() {
            this.client = new SingleThreadClient(host, port);
        }

        @Override
        public void run() {
            try {
                client.initClient();
                while (isRunning) {
                    BinaryResponse response = client.performRequest(generateRequest());
                    counter.getAndIncrement();

                    if (response == null) throw new RuntimeException("Response is null");
                    Thread.sleep(delayMs, delayNs);
                }
            } catch (IOException e) {
                logger.error(e);
            } catch (InterruptedException e) {
                logger.error(e);
            } finally {
                try {
                    if (client != null) client.closeClient();
                } catch (IOException e) {
                    logger.error(e);
                }
            }
        }
    }
}
