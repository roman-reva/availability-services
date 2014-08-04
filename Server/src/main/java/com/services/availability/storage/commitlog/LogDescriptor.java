package com.services.availability.storage.commitlog;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-07-31 16:05
 */
public class LogDescriptor {
    public static final int MODE_READ = 1;
    public static final int MODE_WRITE = 1 << 1;
    public static final int MODE_RW = MODE_READ | MODE_WRITE;

    protected final String filename;
    protected final int mode;
    protected final File logFile;
    protected FileInputStream logFis = null;
    protected FileOutputStream logFos = null;

    public LogDescriptor(String filename, int mode) {
        this.filename = filename;
        this.mode = mode;
        this.logFile = new File(filename);
    }

    public boolean fileOpenForRead() {
        return (mode & MODE_READ) == MODE_READ && logFis != null;
    }

    public boolean fileOpenForWrite() {
        return (mode & MODE_WRITE) == MODE_WRITE && logFos != null;
    }

    public String toString() {
        return filename + " (mode=" + (fileOpenForRead() ? "R" : "") + (fileOpenForWrite() ? "W" : "") + ")";
    }

    public String monitor() {
        return filename.intern();
    }
}