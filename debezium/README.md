# Debezium CDC Demos for XTDB

This directory contains demos showing how XTDB can ingest Debezium-style CDC (Change Data Capture) events, demonstrating schema-less ingestion and bitemporal capabilities.

## Demos

### [debezium-static-json](./debezium-static-json/)

**Static JSON demo** - Uses pre-generated Debezium JSON events to demonstrate XTDB's CDC capabilities without requiring any external infrastructure.

- No MySQL, Kafka, or other dependencies
- Good for understanding the data format and XTDB behavior
- Quick to run and explore

```bash
cd debezium-static-json
mise run
```

### [debezium-xtdb](./debezium-xtdb/)

**Live MySQL CDC to XTDB** - A Java-based Debezium embedded engine that captures changes from a real MySQL/MariaDB database and writes them to XTDB with full bitemporal support.

- Real MySQL/MariaDB CDC (binlog-based)
- Single JVM process (no Kafka required)
- Full bitemporal support (`_valid_from`, `FOR PORTION OF VALID_TIME` deletes)
- Includes helper scripts for testing (mysql-writer, xtdb-poller)

```bash
cd debezium-xtdb
mise run demo    # Installs MariaDB, starts it, runs CDC
```

The module also includes a Debezium Server sink connector for deployment with standalone Debezium Server.

## Key Concepts

### Schema-less Ingestion

XTDB accepts records with varying column sets without requiring DDL changes. When your source schema evolves (new columns added), XTDB handles it automatically.

### Bitemporal Tracking

CDC event timestamps become `_valid_from` in XTDB, enabling:
- Point-in-time queries: "What was the state at time X?"
- History queries: "Show all versions of record Y"
- Deleted record visibility: Records aren't lost, they have `_valid_to` set

### Debezium Event Format

The demos handle both full Debezium envelope format and the flattened format (via `ExtractNewRecordState` transform):

```json
{
  "id": 1,
  "email": "alice@example.com",
  "username": "alice",
  "__op": "c",
  "__table": "accounts.users",
  "__source_ts_ms": 1704067200000
}
```

## Comparison

| Feature | Static JSON | Live CDC (debezium-xtdb) |
|---------|-------------|--------------------------|
| Real database | No | Yes (MySQL/MariaDB) |
| Kafka required | No | No |
| CDC engine | None | Debezium Embedded |
| Latency | N/A | Sub-second |
| Setup complexity | Minimal | Medium (MariaDB install) |
| Best for | Learning | Development/Testing |

## Architecture

```
                        debezium-xtdb (Embedded Mode)
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  ┌──────────────────┐    ┌────────────────┐    ┌───────────────────┐   │
│  │ MySQL/MariaDB    │───►│ Debezium       │───►│  XtdbWriter       │   │
│  │ (binlog)         │    │ Embedded Engine│    │  (JDBC)           │   │
│  └──────────────────┘    └────────────────┘    └─────────┬─────────┘   │
│                                                          │             │
└──────────────────────────────────────────────────────────┼─────────────┘
                                                           │
                                                           ▼
                                                   ┌──────────────┐
                                                   │     XTDB     │
                                                   │ (bitemporal) │
                                                   └──────────────┘
```

## Production Deployment

For production CDC:

1. **Embedded mode** (`debezium-xtdb`): Single JAR, good for simpler deployments
2. **Debezium Server + Sink**: Deploy the XTDB sink JAR with Debezium Server for more complex setups with multiple connectors

See the [debezium-xtdb directory](./debezium-xtdb/) for detailed instructions.
