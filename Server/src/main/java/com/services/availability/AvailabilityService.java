package com.services.availability;

import com.services.availability.server.BinarySingleThreadServer;
import com.services.availability.server.RequestProcessor;
import com.services.availability.storage.CachedLoggedStorage;
import com.services.availability.storage.Storage;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-17 14:47
 */
public class AvailabilityService {
    private Logger logger = Logger.getLogger(AvailabilityService.class);

    private final BinarySingleThreadServer server;
    private final Storage storage;

    /**
     * Default constructor. Builds up all main components.
     */
    private AvailabilityService() {
        storage = new CachedLoggedStorage();
        server = new BinarySingleThreadServer();

        RequestProcessor requestProcessor = new RequestProcessor(server.getThroughputMeter(), storage);
        server.setRequestProcessor(requestProcessor);

        addShutdownHook();
    }

    /**
     * Method adds a shutdown hook that shuts down the server and prepares
     * storage for safe shut down.
     */
    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.debug("shutdown event registered.");
                try {
                    server.shutdown();
                    logger.debug("server is shut down");
                    storage.prepareForShutdown();
                    logger.debug("storage is ready for shut down");
                } catch (IOException e) {
                    logger.error("Error during server shutdown", e);
                }
            }
        });
    }

    /**
     * Starts service.
     *
     * @throws IOException
     */
    public void startService() throws IOException {
        server.startup();
    }

    /**
     * Main method.
     *
     * @param args application arguments
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        new AvailabilityService().startService();
    }
}
