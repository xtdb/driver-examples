package com.xtdb.debezium.common;

import java.util.Map;

/**
 * Parsed CDC record with extracted metadata.
 */
public class CdcRecord {
    private final String table;
    private final String operation;  // c=create, u=update, d=delete, r=read
    private final Object id;
    private final long timestampMs;
    private final Map<String, Object> data;
    private final boolean deleted;

    public CdcRecord(String table, String operation, Object id, long timestampMs,
                     Map<String, Object> data, boolean deleted) {
        this.table = table;
        this.operation = operation;
        this.id = id;
        this.timestampMs = timestampMs;
        this.data = data;
        this.deleted = deleted;
    }

    public String getTable() { return table; }
    public String getOperation() { return operation; }
    public Object getId() { return id; }
    public long getTimestampMs() { return timestampMs; }
    public Map<String, Object> getData() { return data; }
    public boolean isDeleted() { return deleted; }
    public boolean isDelete() { return deleted || "d".equals(operation); }
    public boolean isCreate() { return "c".equals(operation); }
}
