package com.xtdb.debezium.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Writes CDC records to XTDB using the PostgreSQL wire protocol.
 * Handles INSERT/UPDATE (as upsert) and DELETE with bitemporal support.
 */
public class XtdbWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(XtdbWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private final Connection connection;
    private final String idField;

    // Statistics
    private long insertCount = 0;
    private long updateCount = 0;
    private long deleteCount = 0;

    public XtdbWriter(String url, String user, String password, String idField) throws SQLException {
        LOG.info("Connecting to XTDB at {}", url);
        this.connection = DriverManager.getConnection(url, user, password);
        this.connection.setAutoCommit(false);
        this.idField = idField;
        LOG.info("Connected to XTDB successfully");
    }

    public XtdbWriter(String url, String user, String password) throws SQLException {
        this(url, user, password, "id");
    }

    /**
     * Write a CDC record to XTDB.
     */
    public void write(CdcRecord record) throws SQLException, IOException {
        if (record.isDelete()) {
            deleteRecord(record);
            deleteCount++;
        } else {
            upsertRecord(record);
            if (record.isCreate()) {
                insertCount++;
            } else {
                updateCount++;
            }
        }
    }

    private void upsertRecord(CdcRecord record) throws SQLException, IOException {
        String validFrom = Instant.ofEpochMilli(record.getTimestampMs())
                .atOffset(ZoneOffset.UTC)
                .format(ISO_FORMATTER);

        // Build XTDB record
        Map<String, Object> xtdbRecord = new HashMap<>();
        xtdbRecord.put("_id", record.getId());
        xtdbRecord.put("_valid_from", validFrom);

        // Copy data fields (exclude Debezium metadata and id field)
        for (Map.Entry<String, Object> entry : record.getData().entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("__") || key.equals(idField)) {
                continue;
            }
            xtdbRecord.put(key, entry.getValue());
        }

        String recordJson = MAPPER.writeValueAsString(xtdbRecord);
        String sql = String.format("INSERT INTO %s RECORDS ?", sanitizeIdentifier(record.getTable()));

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            PGobject jsonObject = new PGobject();
            jsonObject.setType("json");
            jsonObject.setValue(recordJson);

            stmt.setObject(1, jsonObject);
            stmt.executeUpdate();
            LOG.info("[INSERT/UPDATE] {}.{}", record.getTable(), record.getId());
        }
    }

    private void deleteRecord(CdcRecord record) throws SQLException {
        String validFrom = Instant.ofEpochMilli(record.getTimestampMs())
                .atOffset(ZoneOffset.UTC)
                .format(ISO_FORMATTER);

        String idClause;
        Object id = record.getId();
        if (id instanceof String) {
            idClause = String.format("_id = '%s'", sanitizeValue((String) id));
        } else {
            idClause = String.format("_id = %s", id);
        }

        String sql = String.format(
                "DELETE FROM %s FOR PORTION OF VALID_TIME FROM TIMESTAMP '%s' TO NULL WHERE %s",
                sanitizeIdentifier(record.getTable()), validFrom, idClause);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.executeUpdate();
            LOG.info("[DELETE] {}.{}", record.getTable(), record.getId());
        }
    }

    public void commit() throws SQLException {
        connection.commit();
    }

    public void rollback() throws SQLException {
        connection.rollback();
    }

    @Override
    public void close() throws SQLException {
        LOG.info("Closing XTDB connection. Stats: inserts={}, updates={}, deletes={}",
                insertCount, updateCount, deleteCount);
        if (connection != null) {
            connection.close();
        }
    }

    public long getInsertCount() { return insertCount; }
    public long getUpdateCount() { return updateCount; }
    public long getDeleteCount() { return deleteCount; }

    private String sanitizeIdentifier(String identifier) {
        return identifier.replaceAll("[^a-zA-Z0-9_]", "");
    }

    private String sanitizeValue(String value) {
        return value.replace("'", "''");
    }
}
