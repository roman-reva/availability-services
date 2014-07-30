package com.services.availability.server;

import com.services.availability.protocol.binary.BinaryRequest;
import com.services.availability.protocol.binary.BinaryResponse;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-18 13:10
 */
public class AsyncRequestProcessor {
    private Logger logger = Logger.getLogger(AsyncRequestProcessor.class);
    private final ThreadPoolExecutor executorService;
    private final RequestProcessor processor;

    public AsyncRequestProcessor(RequestProcessor processor, int threadNumber) {
        this.processor = processor;
        this.executorService = (ThreadPoolExecutor)Executors.newFixedThreadPool(threadNumber);
    }

    public void scheduleRequestProcessing(BinaryRequest request, ResponseWriteOutCallback callback) {
        RequestData requestData = new RequestData(request, callback);
        executorService.execute(new RunnableRequestHandler(processor, requestData));

//        logger.debug("Job scheduled, active count = " + executorService.getActiveCount());
    }

    public static class RunnableRequestHandler implements Runnable {
        private final RequestData requestData;
        private final RequestProcessor processor;

        public RunnableRequestHandler(RequestProcessor processor, RequestData requestData) {
            this.processor = processor;
            this.requestData = requestData;
        }

        @Override
        public void run() {
            BinaryResponse response = processor.processRequest(requestData.getRequest());
            ResponseWriteOutCallback callback = requestData.getCallback();
            callback.writeOut(response);
        }
    }

    public static class RequestData {
        private final BinaryRequest request;
        private final ResponseWriteOutCallback callback;

        public RequestData(BinaryRequest request, ResponseWriteOutCallback callback) {
            this.request = request;
            this.callback = callback;
        }

        public BinaryRequest getRequest() {
            return request;
        }

        public ResponseWriteOutCallback getCallback() {
            return callback;
        }
    }

    public static interface ResponseWriteOutCallback {
        public void writeOut(BinaryResponse response);
    }
}
