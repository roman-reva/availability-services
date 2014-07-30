package com.services.availability.client;

import com.services.availability.client.common.RandomRequestGenerator;
import com.services.availability.client.singlethread.AbstractClient;
import com.services.availability.client.singlethread.ClientHeartbeatThread;
import com.services.availability.client.singlethread.SingleThreadClient;
import com.services.availability.common.ArgumentsExtractor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-25 17:34
 */
public class ClientApp {
    public final static int DEFAULT_PORT = 8888;
    public final static String DEFAULT_HOST = "localhost";

    private ClientHeartbeatThread heartbeatThread;
    private AbstractClient client;

    private RandomRequestGenerator requestGenerator = new RandomRequestGenerator();
    private Logger log = Logger.getLogger(ClientApp.class);

    public ClientApp(String host, int port) {
        client = new SingleThreadClient(host, port);
        try {
            client.initClient();
        } catch (IOException e) {
            log.error("Could not initialize client", e);
            System.exit(0);
        }

        heartbeatThread = new ClientHeartbeatThread(client);
        addShutdownHook();
    }

    public void startClient() {
        heartbeatThread.start();

        while (true) {
            try {
                client.performRequest(requestGenerator.getBinaryRequest());
//                if (random.nextInt(30) == 0) Thread.sleep(1);
            } catch (IOException e) {
                log.error("Cannot perform request", e);
//            } catch (InterruptedException e) {
//                log.error("Interrupted");
            }
        }
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.debug("SHUTDOWN HOOK : client is going to shut down.");

                heartbeatThread.killHeartbeat();
                heartbeatThread.interrupt();
            }
        });
    }

    public static void main(String[] args) {
        Map<String, String> argValues = ArgumentsExtractor.extract(new String[]{"port", "host"}, args);
        int port = argValues.containsKey("port") ? Integer.parseInt(argValues.get("port")) : DEFAULT_PORT;
        String host = argValues.containsKey("host") ? argValues.get("host") : DEFAULT_HOST;

        ClientApp app = new ClientApp(host, port);
        app.startClient();
    }
}
