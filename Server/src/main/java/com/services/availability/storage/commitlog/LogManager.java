package com.services.availability.storage.commitlog;

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
    private Map<String, FileData> files = new ConcurrentHashMap<String, FileData>();

    /**
     * Opens the specified log file in the specified mode, creates file
     * descriptor and adds it to the collection.
     *
     * @param filename log file name
     * @param mode FileData.MODE_READ, FileData.MODE_WRITE, FileData.MODE_RW
     */
    public void openLogFile(String filename, int mode) {
        synchronized (filename.intern()) {
            FileData fileData = new FileData(filename, mode);
            File logFile = fileData.logFile;
            boolean fileExists = logFile.exists();
            try {
                if (!fileExists) {
                    boolean created = logFile.createNewFile();
                    if (!created) throw new IllegalStateException("Cannot create log file; filename=" + logFile.getName());
                    log.debug("New log file created; filename = " + logFile.getName());
                }

                if ((mode & FileData.MODE_WRITE) == FileData.MODE_WRITE)
                    fileData.logFos = new FileOutputStream(logFile);

                if ((mode & FileData.MODE_READ) == FileData.MODE_READ)
                    fileData.logFis = new FileInputStream(logFile);

                files.put(filename, fileData);
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
     * @param filename file to close
     */
    public void closeLogFiles(String filename) {
        synchronized (filename.intern()) {
            verifyFileOpen(filename);

            FileData fileData = files.remove(filename);

            try {
                boolean logFisClosed=false, logFosClosed=false;
                if (fileData.fileOpenForRead()) {
                    fileData.logFis.close();
                    logFisClosed = true;
                }
                if (fileData.fileOpenForWrite()) {
                    fileData.logFos.close();
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
     * @param filename log file
     * @param data data to write
     * @throws IOException
     */
    public void write(String filename, byte[] data) throws IOException {
        verifyFileOpen(filename);
        synchronized (filename.intern()) {
            FileData fileData = files.get(filename);
            if (!fileData.fileOpenForWrite()) throw new IllegalStateException("File `" + fileData + "` is not open for write");

            fileData.logFos.write(data);
        }
    }

    /**
     * Performs flush of the specified file's output stream.
     *
     * @param filename log file name
     * @throws IOException
     */
    public void flush(String filename) throws IOException {
        verifyFileOpen(filename);
        synchronized (filename.intern()) {
            FileData fileData = files.get(filename);
            if (!fileData.fileOpenForWrite()) throw new IllegalStateException("File `" + fileData + "` is not open for write");

            fileData.logFos.flush();
        }
    }

    /**
     * Verifies that specified file is open for write, puts the <i>data</i>
     * into output stream and flushes the changes.
     *
     * @param filename log file name
     * @param data data
     * @throws IOException
     */
    public void writeAndFlush(String filename, byte[] data) throws IOException {
        verifyFileOpen(filename);
        synchronized (filename.intern()) {
            FileData fileData = files.get(filename);
            if (!fileData.fileOpenForWrite()) throw new IllegalStateException("File `" + fileData + "` is not open for write");

            fileData.logFos.write(data);
            fileData.logFos.flush();
        }
    }

    /**
     * Throws IllegalArgumentException if specified file is not open.
     *
     * @param filename log file to verify
     */
    private void verifyFileOpen(String filename) {
        if (!files.containsKey(filename)) {
            throw new IllegalArgumentException("No open file found with name = " + filename);
        }
    }
}
