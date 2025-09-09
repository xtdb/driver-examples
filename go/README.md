# XTDB Go Example

This example demonstrates how to connect to XTDB using Go with the PostgreSQL wire protocol.

## Prerequisites

- Go 1.21 or later
- XTDB server running on port 5432

## Dependencies

This example uses:
- `github.com/jackc/pgx/v5` - PostgreSQL driver for Go

## Running the Example

```bash
./run.sh
```

Or manually:

```bash
go mod download
go run main.go
```

## Features Demonstrated

1. **Connection**: Connecting to XTDB using the PostgreSQL protocol
2. **Data Insertion**: Using XTDB's RECORDS syntax to insert data
3. **Queries**: Querying data with WHERE clauses
4. **JSON Support**: Working with JSONB data types
5. **Transactions**: Demonstrating transaction support
6. **Aggregations**: Using COUNT and other aggregate functions

## Example Operations

The example performs the following operations:

1. Inserts sample trade records with nested JSON data
2. Queries trades with quantity > 100
3. Demonstrates XTDB's RECORDS syntax for bulk inserts
4. Shows transaction support with commit/rollback
5. Counts total records in the database

## Notes

- XTDB automatically creates tables on first insert (no CREATE TABLE needed)
- The RECORDS syntax is XTDB-specific and allows for convenient bulk inserts
- JSON data is handled natively through the JSONB type