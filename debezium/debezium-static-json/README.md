# Debezium CDC Demo for XTDB

This demo shows how XTDB can ingest Debezium CDC (Change Data Capture) events from MySQL, handling schema evolution (new columns, type changes) without any schema migrations.

## What This Demonstrates

1. **Schema-less ingestion**: XTDB accepts records with varying column sets - no DDL required
2. **Schema evolution**: New columns appear in CDC events over time, XTDB handles them automatically
3. **Bitemporality**: CDC event timestamps become `_valid_from`, enabling time-travel queries
4. **Full CDC support**: Handles inserts, updates, and deletes from Debezium

## Scenario

The demo simulates a MySQL "accounts" database with three evolving tables:

| Table | Original Schema | Evolved Schema (new columns) |
|-------|-----------------|------------------------------|
| `users` | id, email, username, created_at | + phone_number, verified_at |
| `profiles` | id, user_id, display_name | + avatar_url, bio |
| `sessions` | id, user_id, token, created_at | + device_type, ip_address |

The `cdc/events.json` file contains 22 Debezium events spanning 4 days:
- Initial inserts with original schema
- Schema evolution (new columns appear in events)
- Updates to existing records
- Deletes (user deactivation, session logout)

## Running the Demo

```bash
cd debezium-static-json

# Install dependencies and run ingestion
mise run

# Or step by step:
mise run deps    # Install Go dependencies
mise run run     # Ingest CDC events into XTDB

# Run example queries
mise run query

# Check record counts
mise run test

# Clean and re-run
mise run reset
```

## How It Works

### Debezium Event Format

Each CDC event follows the Debezium format:

```json
{
  "payload": {
    "op": "c",                    // c=create, u=update, d=delete
    "ts_ms": 1704067200000,       // Event timestamp (milliseconds)
    "source": {
      "db": "accounts",
      "table": "users"
    },
    "before": null,               // Previous state (for updates/deletes)
    "after": {                    // New state
      "id": 1,
      "email": "alice@example.com",
      "username": "alice"
    }
  }
}
```

### Transformation to XTDB

The Go script transforms each event:

| Debezium | XTDB |
|----------|------|
| `source.table` | Table name |
| `after.id` | `_id` |
| `ts_ms` | `_valid_from` |
| `after.*` | Record fields (dynamic) |

Operations:
- **create/update** → `INSERT INTO table RECORDS {...}`
- **delete** → `DELETE FROM table FOR PORTION OF VALID_TIME ...`

### Schema Evolution Handling

XTDB's schema-less design means:

1. **Event 1** (Jan 1): `{id: 1, email: "alice@example.com"}`
2. **Event 2** (Jan 2): `{id: 4, email: "diana@example.com", phone_number: "+1-555-0104"}`

No `ALTER TABLE` needed! XTDB stores each record with its actual columns.

## Example Queries

After ingestion, you can run time-travel queries:

```sql
-- Current state of users
SELECT * FROM users;

-- See all historical versions of Alice
SELECT * FROM users FOR ALL VALID_TIME WHERE _id = 1;

-- Users as of Jan 1, 2024 (before schema evolution)
SELECT * FROM users FOR VALID_TIME AS OF TIMESTAMP '2024-01-01T12:00:00Z';

-- See deleted users
SELECT * FROM users FOR ALL VALID_TIME WHERE _valid_to IS NOT NULL;
```

## Files

```
debezium-static-json/
├── .mise.toml          # Task definitions
├── go.mod              # Go module
├── main.go             # Ingestion script (~150 lines)
├── cdc/
│   └── events.json     # Static Debezium CDC events (22 events)
├── sql/
│   └── queries.sql     # Example queries
└── README.md           # This file
```

## Why Not a Live Kafka/Debezium Setup?

This demo uses static JSON files to:
- Keep the demo simple and self-contained
- Focus on XTDB's schema evolution capabilities
- Avoid requiring Kafka, Zookeeper, MySQL, and Debezium containers

For production, you would connect XTDB to Kafka using a similar ingestion approach, or use the XTDB Kafka module directly.

## Production Considerations

For real-world CDC ingestion:

1. **Kafka Consumer**: Replace file reading with a Kafka consumer (e.g., Sarama for Go)
2. **Batching**: Batch inserts for better throughput
3. **Exactly-once**: Track Kafka offsets in XTDB for exactly-once semantics
4. **Error handling**: Dead letter queues for failed events
5. **Monitoring**: Metrics for lag, throughput, and errors

Alternatively, a sample XTDB Kafka Connect Sink is available (which may be further adapted to support MySQL-compatible Debezium output): https://github.com/egg-juxt/xtdb-kafka-connect
