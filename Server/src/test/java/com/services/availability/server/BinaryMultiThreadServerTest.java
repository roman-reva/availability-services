package com.services.availability.server;

import com.services.availability.TestUtils;
import com.services.availability.client.singlethread.SingleThreadClient;
import com.services.availability.protocol.binary.BinaryRequest;
import com.services.availability.protocol.binary.BinaryRequestType;
import com.services.availability.protocol.binary.BinaryResponse;
import com.services.availability.storage.InMemoryStorage;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-18 13:18
 */
public class BinaryMultiThreadServerTest {
    private Logger log = Logger.getLogger(BinaryMultiThreadServerTest.class);

    private AbstractServer server;
    private Thread serverThread;

    public void setup(AbstractServer serverImpl, int storageDelay) throws IOException, InterruptedException {
        server = serverImpl;
        server.setRequestProcessor(new RequestProcessor(server.getThroughputMeter(), new InMemoryStorage(storageDelay)));

        log.debug("Starting server...");
        serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.startup();
                } catch (IOException e) {
                    log.error("Error in server loop", e);
                }
            }
        });
        serverThread.start();
        log.debug("Waiting 2000 ms for start up...");

        Thread.sleep(2000);
        log.debug("Server started");
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        log.debug("Shutting down server...");
        server.shutdown();
        log.debug("Server stopped!");
    }

    @Test
    public void singleRequestTest() throws IOException, InterruptedException {
        setup(new BinaryMultiThreadServer(), 0);

        log.debug("singleRequestTest(): started!");

        log.debug("Initializing client...");
        SingleThreadClient client = new SingleThreadClient("localhost", 8888);
        client.initClient();

        log.debug("Performing request...");
        BinaryResponse response = client.performRequest(new BinaryRequest(BinaryRequestType.PUT, 1000, (short) 325, 10));
        assert response != null;

        log.debug("singleRequestTest(): assertions passed!");
    }

    @Test
    public void hundredRequestTest() throws IOException, InterruptedException {
        setup(new BinaryMultiThreadServer(), 0);
        log.debug("hundredRequestTest(): started!");

        log.debug("Initializing client...");
        SingleThreadClient client = new SingleThreadClient("localhost", 8888);
        client.initClient();

        List<Long> latencies = new LinkedList<Long>();
        long avgLatency = 0, reqNum = 100;

        log.debug("Performing requests...");
        for (int i=0; i<reqNum; i++) {
            BinaryRequest request = new BinaryRequest(BinaryRequestType.PUT, TestUtils.getSku(), TestUtils.getStore(), TestUtils.getAmount());

            long startTime = System.nanoTime();
            BinaryResponse response = client.performRequest(request);
            long latency = System.nanoTime() - startTime;

            assert response != null;
            latencies.add(latency);

            avgLatency += latency;
        }

        BinaryRequest request = new BinaryRequest(BinaryRequestType.PUT, TestUtils.getSku(), TestUtils.getStore(), TestUtils.getAmount());
        BinaryResponse response = client.performRequest(request);

        log.debug("hundredRequestTest(): assertions passed!");

        avgLatency = avgLatency / reqNum;
        log.debug("Avg ltc: " + avgLatency);

        Thread.sleep(2000);
    }
}
