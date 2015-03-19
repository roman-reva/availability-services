package com.services.availability.storage.ccl;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 3/19/15 12:57 PM
 */
public class LogRecordTest {

    @Test
    public void serializationTest() {
        LogRecord record = new LogRecord(LogRecord.TYPE_PUT, 35623L, 100, (short)10, 50);
        LogRecord restored = LogRecord.fromByteArray(LogRecord.toByteArray(record));

        assertEquals(record.key, restored.key);
        assertEquals(record.sku, restored.sku);
        assertEquals(record.store, restored.store);
        assertEquals(record.amount, restored.amount);

        assertEquals(record.type, restored.type);
        assertEquals(record.timestamp, restored.timestamp);
        assertEquals(record.committed, restored.committed);
    }
}
