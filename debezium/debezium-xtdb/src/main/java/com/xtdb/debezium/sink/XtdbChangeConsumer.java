package com.xtdb.debezium.sink;

import com.xtdb.debezium.common.CdcEventParser;
import com.xtdb.debezium.common.CdcRecord;
import com.xtdb.debezium.common.XtdbWriter;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Debezium Server sink connector for XTDB.
 * Uses shared XtdbWriter and CdcEventParser for CDC processing.
 *
 * Configuration:
 *   debezium.sink.type=xtdb
 *   debezium.sink.xtdb.url=jdbc:postgresql://localhost:5432/xtdb
 *   debezium.sink.xtdb.user=xtdb
 *   debezium.sink.xtdb.password=xtdb
 *   debezium.sink.xtdb.id.field=id
 */
@Named("xtdb")
@Dependent
public class XtdbChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOG = LoggerFactory.getLogger(XtdbChangeConsumer.class);
    private static final String PROP_PREFIX = "debezium.sink.xtdb.";

    private XtdbWriter writer;
    private CdcEventParser parser;

    private synchronized void ensureConnected() {
        if (writer != null) return;

        Config config = ConfigProvider.getConfig();
        String url = config.getValue(PROP_PREFIX + "url", String.class);
        String user = config.getOptionalValue(PROP_PREFIX + "user", String.class).orElse("xtdb");
        String password = config.getOptionalValue(PROP_PREFIX + "password", String.class).orElse("xtdb");
        String idField = config.getOptionalValue(PROP_PREFIX + "id.field", String.class).orElse("id");

        try {
            writer = new XtdbWriter(url, user, password, idField);
            parser = new CdcEventParser(idField);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to connect to XTDB: " + e.getMessage(), e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                           DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        ensureConnected();
        LOG.debug("Processing batch of {} records", records.size());

        for (ChangeEvent<Object, Object> record : records) {
            try {
                String value = (String) record.value();
                CdcRecord cdcRecord = parser.parse(value);
                if (cdcRecord != null) {
                    writer.write(cdcRecord);
                }
                committer.markProcessed(record);
            } catch (Exception e) {
                LOG.error("Error processing record: {}", e.getMessage(), e);
                throw new RuntimeException("Failed to process CDC record", e);
            }
        }

        try {
            writer.commit();
            committer.markBatchFinished();
            LOG.debug("Batch committed successfully");
        } catch (SQLException e) {
            LOG.error("Failed to commit batch", e);
            throw new RuntimeException("Failed to commit batch", e);
        }
    }
}
