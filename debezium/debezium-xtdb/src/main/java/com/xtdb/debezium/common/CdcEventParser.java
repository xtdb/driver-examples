package com.xtdb.debezium.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Parses Debezium CDC events from both:
 * - Full envelope format (embedded engine without transforms)
 * - Flat format (after ExtractNewRecordState transform)
 */
public class CdcEventParser {

    private static final Logger LOG = LoggerFactory.getLogger(CdcEventParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String idField;

    public CdcEventParser(String idField) {
        this.idField = idField;
    }

    public CdcEventParser() {
        this("id");
    }

    /**
     * Parse a CDC event JSON string into a CdcRecord.
     * Returns null if the event should be skipped (schema events, no data, etc.)
     */
    public CdcRecord parse(String json) throws IOException {
        if (json == null || json.isEmpty()) {
            return null;
        }

        Map<String, Object> envelope = MAPPER.readValue(json, new TypeReference<>() {});
        return parse(envelope);
    }

    /**
     * Parse a CDC event map into a CdcRecord.
     */
    @SuppressWarnings("unchecked")
    public CdcRecord parse(Map<String, Object> envelope) {
        if (envelope == null) {
            return null;
        }

        String table;
        String op;
        long tsMs;
        Map<String, Object> data;
        boolean deleted;

        if (envelope.containsKey("source")) {
            // Full Debezium envelope format
            Map<String, Object> source = (Map<String, Object>) envelope.get("source");
            if (source == null) {
                LOG.debug("No source in event, skipping");
                return null;
            }

            // Skip DDL/schema events
            if (envelope.containsKey("ddl") || envelope.containsKey("tableChanges")) {
                LOG.debug("Skipping DDL/schema event");
                return null;
            }

            // Skip events without data payload
            if (!envelope.containsKey("after") && !envelope.containsKey("before")) {
                LOG.debug("No data payload in event, skipping");
                return null;
            }

            // Extract table from source
            String db = getStringField(source, "db", "");
            String tbl = getStringField(source, "table", "");
            if (tbl == null || tbl.isEmpty()) {
                LOG.debug("No table in source, skipping");
                return null;
            }
            table = tbl;  // Just use table name, not schema-qualified
            tsMs = getLongField(source, "ts_ms", System.currentTimeMillis());

            // Get operation
            op = getStringField(envelope, "op", "r");

            // Extract data from after/before
            if ("d".equals(op)) {
                data = (Map<String, Object>) envelope.get("before");
                deleted = true;
            } else {
                data = (Map<String, Object>) envelope.get("after");
                deleted = false;
            }
        } else {
            // Flat format (ExtractNewRecordState applied)
            op = getStringField(envelope, "__op", "c");
            String tableFullName = getStringField(envelope, "__table", "");
            tsMs = getLongField(envelope, "__source_ts_ms", System.currentTimeMillis());
            deleted = "true".equals(getStringField(envelope, "__deleted", "false"));

            // Extract table name (remove schema prefix)
            table = tableFullName.contains(".")
                    ? tableFullName.substring(tableFullName.lastIndexOf('.') + 1)
                    : tableFullName;

            data = envelope;
        }

        if (table.isEmpty()) {
            LOG.debug("No table name, skipping");
            return null;
        }

        if (data == null) {
            LOG.debug("No data for table {}, skipping", table);
            return null;
        }

        // Get record ID
        Object recordId = data.get(idField);
        if (recordId == null) {
            LOG.debug("No '{}' field in event for table {}", idField, table);
            return null;
        }

        return new CdcRecord(table, op, recordId, tsMs, data, deleted);
    }

    private String getStringField(Map<String, Object> map, String field, String defaultValue) {
        Object value = map.get(field);
        return value != null ? value.toString() : defaultValue;
    }

    private long getLongField(Map<String, Object> map, String field, long defaultValue) {
        Object value = map.get(field);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }
}
