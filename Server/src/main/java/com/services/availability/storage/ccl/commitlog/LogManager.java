package com.services.availability.storage.ccl.commitlog;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Current class contains a collection of log files and a set of primary operations
 * that could be performs on this log files.
 *
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-31 14:52
 */
public class LogManager {
    private Logger log = Logger.getLogger(LogManager.class);

    /**
     * Set of log files identified by filename.
     */
    private Map<String, LogDescriptor> files = new ConcurrentHashMap<String, LogDescriptor>();

    /**
     * Opens the specified log file in the specified mode, creates file
     * descriptor and adds it to the collection.
     *
     * @param filename log file name
     * @param mode LogDescriptor.MODE_READ, LogDescriptor.MODE_WRITE, LogDescriptor.MODE_RW
     */
    public LogDescriptor openLogFile(String filename, int mode) {
        synchronized (filename.intern()) {
            LogDescriptor logDescriptor = new LogDescriptor(filename, mode);
            File logFile = logDescriptor.logFile;
            boolean fileExists = logFile.exists();
            try {
                if (!fileExists) {
                    boolean created = logFile.createNewFile();
                    if (!created) throw new IllegalStateException("Cannot create log file; filename=" + logFile.getName());
                    log.debug("New log file created; filename = " + logFile.getName());
                }

                if ((mode & LogDescriptor.MODE_WRITE) == LogDescriptor.MODE_WRITE)
                    logDescriptor.logFos = new FileOutputStream(logFile);

                if ((mode & LogDescriptor.MODE_READ) == LogDescriptor.MODE_READ)
                    logDescriptor.logFis = new FileInputStream(logFile);

                files.put(filename, logDescriptor);

                return logDescriptor;
            } catch (FileNotFoundException e) {
                log.error(e);
                throw new RuntimeException("Not able to open log file", e);
            } catch (IOException e) {
                log.error(e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Closes specified file and removes corresponding descriptor from the
     * log file set.
     *
     * @param logDescriptor file to close
     */
    public void closeLogFiles(LogDescriptor logDescriptor) {
        synchronized (logDescriptor.monitor()) {
            verifyFileOpen(logDescriptor);

            files.remove(logDescriptor.filename);

            try {
                boolean logFisClosed=false, logFosClosed=false;
                if (logDescriptor.fileOpenForRead()) {
                    logDescriptor.logFis.close();
                    logFisClosed = true;
                }
                if (logDescriptor.fileOpenForWrite()) {
                    logDescriptor.logFos.close();
                    logFosClosed = true;
                }

                log.debug("logFisClosed="+logFisClosed+", logFosClosed="+logFosClosed);
            } catch (IOException e) {
                log.error(e);
            }
        }
    }

    /**
     * Verifies that specified file is open for write and puts
     * the <i>data</i> into output stream.
     *
     * @param logDescriptor log file
     * @param data data to write
     * @throws IOException
     */
    public void write(LogDescriptor logDescriptor, byte[] data) throws IOException {
        verifyFileOpen(logDescriptor);
        synchronized (logDescriptor.monitor()) {
            if (!logDescriptor.fileOpenForWrite()) throw new IllegalStateException("File `" + logDescriptor + "` is not open for write");

            logDescriptor.logFos.write(data);
        }
    }

    /**
     * Performs flush of the specified file's output stream.
     *
     * @param logDescriptor log file name
     * @throws IOException
     */
    public void flush(LogDescriptor logDescriptor) throws IOException {
        verifyFileOpen(logDescriptor);
        synchronized (logDescriptor.monitor()) {
            if (!logDescriptor.fileOpenForWrite()) throw new IllegalStateException("File `" + logDescriptor + "` is not open for write");

            logDescriptor.logFos.flush();
        }
    }

    /**
     * Verifies that specified file is open for write, puts the <i>data</i>
     * into output stream and flushes the changes.
     *
     * @param logDescriptor log file name
     * @param data data
     * @throws IOException
     */
    public void writeAndFlush(LogDescriptor logDescriptor, byte[] data) throws IOException {
        verifyFileOpen(logDescriptor);
        synchronized (logDescriptor.monitor()) {
            if (!logDescriptor.fileOpenForWrite()) throw new IllegalStateException("File `" + logDescriptor + "` is not open for write");

            logDescriptor.logFos.write(data);
            logDescriptor.logFos.flush();
        }
    }

    /**
     * Throws IllegalArgumentException if specified file is not open.
     *
     * @param logDescriptor log file to verify
     */
    private void verifyFileOpen(LogDescriptor logDescriptor) {
        if (!files.containsValue(logDescriptor)) {
            throw new IllegalArgumentException("No open file found (" + logDescriptor + ")");
        }
    }
}
