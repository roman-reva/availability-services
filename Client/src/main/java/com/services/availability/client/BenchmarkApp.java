package com.services.availability.client;

import com.services.availability.client.multithread.MultithreadLoadGenerator;
import com.services.availability.common.ArgumentsExtractor;

import java.util.Map;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-29 19:14
 */
public class BenchmarkApp {
    public final static int DEFAULT_LOAD = 1000;
    public final static int DEFAULT_PORT = 8888;
    public final static String DEFAULT_HOST = "localhost";

    private MultithreadLoadGenerator generator;

    public BenchmarkApp() {
        generator = new MultithreadLoadGenerator(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_LOAD);
    }

    public BenchmarkApp(String host, int port, int throughput) {
        generator = new MultithreadLoadGenerator(host, port, throughput);
    }

    public void start() {
        generator.start();
    }

    public static void main(String[] args) {
        Map<String, String> argValues = ArgumentsExtractor.extract(new String[]{"port", "host", "load"}, args);
        String host = argValues.containsKey("host") ? argValues.get("host") : DEFAULT_HOST;
        int port = argValues.containsKey("port") ? Integer.parseInt(argValues.get("port")) : DEFAULT_PORT;
        int load = argValues.containsKey("load") ? Integer.parseInt(argValues.get("load")) : DEFAULT_LOAD;

        BenchmarkApp app = new BenchmarkApp(host, port, load);
        app.start();
    }
}
