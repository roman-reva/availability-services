package com.services.availability.filebenchmark;

import com.services.availability.common.ArgumentsExtractor;
import com.services.availability.storage.commitlog.LogDescriptor;
import com.services.availability.storage.commitlog.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-31 17:24
 */
public class Benchmark {
    private final static int DEFAULT_SIZE = 1;
    private final static int DEFAULT_THREADS = 4;

    private static Logger log = Logger.getLogger(Benchmark.class);

    private final String multiLogPrefix = "logs/multilog_";
    private final String multiLogPostfix = ".log";

    private final int dataSize;
    private final int threads;

    private final LogManager logManager = new LogManager();
    private final DataChunk[] dataChunks;

    private final Map<Integer, Long> results = new HashMap<Integer, Long>();

    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, String> argValues = ArgumentsExtractor.extract(new String[]{"dataSize", "maxThreads"}, args);
        int dataSize = argValues.containsKey("dataSize") ? Integer.parseInt(argValues.get("dataSize")) : DEFAULT_SIZE;
        int maxThreads = argValues.containsKey("maxThreads") ? Integer.parseInt(argValues.get("maxThreads")) : DEFAULT_THREADS;

        new Benchmark(dataSize, maxThreads).start();
    }

    public Benchmark(int dataSize, int threads) {
        this.dataSize = dataSize * 1024 * 1024;
        this.threads = threads;
        this.dataChunks = new DataChunk[this.dataSize];
    }

    public void start() throws IOException, InterruptedException {
        generateData();

        for (int threadNum=1; threadNum<=threads; threadNum*=2) {
            runMultipleThreadWrite(threadNum);
        }

        printResults();
    }

    private void generateData() {
        log.debug("Data generation started");
        for (int i=0; i<dataSize; i++) {
            dataChunks[i] = new DataChunk(DataGenerator.generateBytes(32));
        }
        log.debug("Data generated");
    }

    private void printResults() {
        log.debug("");
        log.debug("==============================================");
        log.debug("================== RESULTS ===================");
        for (Integer key: results.keySet()) {
            log.debug(key + ": " + results.get(key));
        }
    }

    private void runMultipleThreadWrite(int threadNum) throws InterruptedException {
        log.debug("runMultipleThreadWrite(): threadNum=" + threadNum);

        int dataLength = dataSize / threadNum;
        DataChunk[] chunks = new DataChunk[dataLength];
        System.arraycopy(dataChunks, 0, chunks, 0, dataLength);

        // opening log files and creating threads
        List<WriterThread> threads = new ArrayList<WriterThread>();
        for (int i=0; i<threadNum; i++) {
            String logFilename = multiLogPrefix + threadNum + "_" + i + multiLogPostfix;
            LogDescriptor descriptor = logManager.openLogFile(logFilename, LogDescriptor.MODE_WRITE);
            WriterThread threadA = new WriterThread(logManager, descriptor, chunks);
            threads.add(threadA);
        }

        // starting threads
        long startTime = System.currentTimeMillis();
        for (WriterThread thread: threads) {
            thread.start();
        }

        // waiting for threads to complete
        while (true) {
            boolean threadAlive = false;
            for (WriterThread thread: threads) {
                if (!thread.completed) threadAlive = true;
            }
            if (!threadAlive) break;

            Thread.sleep(250);
        }

        // closing logs and getting max time
        long stopTime = 0;
        for (int i=0; i<threadNum; i++) {
            WriterThread thread = threads.get(i);

            logManager.closeLogFiles(thread.logDescriptor);
            stopTime = Math.max(stopTime, thread.stopTime);
        }

        // printing result
        long result = (stopTime - startTime);
        results.put(threadNum, result);
        log.debug("..............................");
        log.debug(threadNum + " threads: " + result + "ms");

        int i=0;
        for (WriterThread thread: threads) {
            long avgLtc = thread.avgLatency();
            long ttlLatency = thread.ttlLatency();
            log.debug("       " + ++i + " thread avgLtc = " + avgLtc+ "ns, ltcSum=" + ttlLatency);
        }

        log.debug("runMultipleThreadWrite():END");
        log.debug("..............................");
    }

    private class WriterThread extends Thread {
        private final LogDescriptor logDescriptor;
        private final DataChunk[] data;
        private final LogManager writer;
        private volatile boolean completed = false;
        private volatile long stopTime = 0;

        private final long[] latencies;

        private WriterThread(LogManager writer, LogDescriptor logDescriptor, DataChunk[] data) {
            super();
            this.logDescriptor = logDescriptor;
            this.data = data;
            this.writer = writer;
            this.latencies = new long[data.length];
        }

        public void run() {
            try {
                int i=0;
                for (DataChunk chunk: data) {
                    long startTime = System.nanoTime();
                    writer.writeAndFlush(logDescriptor, chunk.data);
                    long ltc = System.nanoTime() - startTime;
                    latencies[i++] = ltc;
                }
//                writer.flush(log);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                stopTime = System.currentTimeMillis();
                completed = true;
            }
        }

        public long ttlLatency() {
            long total = 0;
            for (long ltc: latencies) {
                total+=ltc;
            }
            return total;
        }

        public int avgLatency() {
            long total = ttlLatency();
            return  (int) (total / latencies.length);
        }
    }

    private static class DataChunk {
        byte[] data;
        private DataChunk(byte[] data) {
            this.data = data;
        }
    }
}
