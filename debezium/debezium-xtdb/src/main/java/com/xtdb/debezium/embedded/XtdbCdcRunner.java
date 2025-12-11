package com.xtdb.debezium.embedded;

import com.xtdb.debezium.common.CdcEventParser;
import com.xtdb.debezium.common.CdcRecord;
import com.xtdb.debezium.common.XtdbWriter;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Standalone Debezium Embedded CDC for MySQL to XTDB replication.
 * Uses Debezium Embedded Engine directly - no Kafka, no Debezium Server.
 *
 * Usage:
 *   java -jar debezium-xtdb.jar [config.properties]
 */
public class XtdbCdcRunner {

    private static final Logger LOG = LoggerFactory.getLogger(XtdbCdcRunner.class);

    private final Properties config;
    private final CdcEventParser parser;
    private XtdbWriter writer;
    private long errorCount = 0;

    public XtdbCdcRunner(Properties config) {
        this.config = config;
        this.parser = new CdcEventParser(config.getProperty("xtdb.id.field", "id"));
    }

    public static void main(String[] args) throws Exception {
        Properties config = loadConfig(args.length > 0 ? args[0] : null);
        XtdbCdcRunner runner = new XtdbCdcRunner(config);
        runner.run();
    }

    private static Properties loadConfig(String configPath) throws IOException {
        Properties props = new Properties();

        if (configPath != null) {
            LOG.info("Loading config from: {}", configPath);
            try (FileInputStream fis = new FileInputStream(configPath)) {
                props.load(fis);
            }
        } else {
            LOG.info("Using default configuration");

            // XTDB settings
            String xtdbHost = getEnv("XTDB_HOST", "xtdb");
            props.setProperty("xtdb.url", "jdbc:postgresql://" + xtdbHost + ":5432/xtdb");
            props.setProperty("xtdb.user", getEnv("XTDB_USER", "xtdb"));
            props.setProperty("xtdb.password", getEnv("XTDB_PASSWORD", "xtdb"));
            props.setProperty("xtdb.id.field", "id");

            // Debezium source settings
            props.setProperty("name", "xtdb-cdc-connector");
            props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
            props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
            props.setProperty("offset.storage.file.filename", getEnv("OFFSET_FILE", "data/offsets.dat"));
            props.setProperty("offset.flush.interval.ms", "1000");

            // MySQL connection
            props.setProperty("database.hostname", getEnv("MYSQL_HOST", "127.0.0.1"));
            props.setProperty("database.port", getEnv("MYSQL_PORT", "3306"));
            props.setProperty("database.user", getEnv("MYSQL_USER", "cdc_user"));
            props.setProperty("database.password", getEnv("MYSQL_PASSWORD", "cdc_password"));
            props.setProperty("database.server.id", "184056");
            props.setProperty("topic.prefix", "mysql");

            // Tables
            props.setProperty("database.include.list", getEnv("MYSQL_DATABASE", "accounts"));
            props.setProperty("table.include.list", getEnv("MYSQL_TABLES", "accounts.users"));

            // Schema history
            props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
            props.setProperty("schema.history.internal.file.filename", getEnv("SCHEMA_HISTORY_FILE", "data/schema-history.dat"));

            // JSON converter settings
            props.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
            props.setProperty("key.converter.schemas.enable", "false");
            props.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
            props.setProperty("value.converter.schemas.enable", "false");
        }

        return props;
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    public void run() throws Exception {
        // Connect to XTDB
        writer = new XtdbWriter(
                config.getProperty("xtdb.url"),
                config.getProperty("xtdb.user", "xtdb"),
                config.getProperty("xtdb.password", "xtdb"),
                config.getProperty("xtdb.id.field", "id")
        );

        // Shutdown hook
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown requested...");
            latch.countDown();
        }));

        // Build and run Debezium engine
        LOG.info("Starting Debezium CDC engine...");

        try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(config)
                .notifying(this::handleBatch)
                .build()) {

            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(engine);

            LOG.info("CDC engine started. Press Ctrl+C to stop");
            latch.await();

            LOG.info("Stopping CDC engine...");
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }

        LOG.info("Final stats: inserts={}, updates={}, deletes={}, errors={}",
                writer.getInsertCount(), writer.getUpdateCount(), writer.getDeleteCount(), errorCount);

        writer.close();
    }

    private void handleBatch(List<ChangeEvent<String, String>> records,
                             DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer)
            throws InterruptedException {

        LOG.debug("Processing batch of {} records", records.size());

        for (ChangeEvent<String, String> record : records) {
            try {
                CdcRecord cdcRecord = parser.parse(record.value());
                if (cdcRecord != null) {
                    writer.write(cdcRecord);
                }
                committer.markProcessed(record);
            } catch (Exception e) {
                LOG.error("Error processing record: {}", e.getMessage(), e);
                errorCount++;
                try {
                    writer.rollback();
                } catch (SQLException ex) {
                    LOG.error("Error rolling back", ex);
                }
            }
        }

        try {
            writer.commit();
            committer.markBatchFinished();
        } catch (SQLException e) {
            LOG.error("Failed to commit batch", e);
            throw new RuntimeException("Failed to commit batch", e);
        }
    }
}
