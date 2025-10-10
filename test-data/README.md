# Test Data

This directory contains shared sample data for testing XTDB driver examples across all languages.

## Files

### `sample-users.json`
Standard JSON format with user records for testing basic INSERT/SELECT operations and JSON roundtripping.

### `sample-users-transit.json`
Transit-JSON format (one record per line) for testing transit-json type handling (OID 16384).
Transit-JSON preserves richer type information including:
- Keywords (prefixed with `~:`)
- Dates (prefixed with `~t`)
- Timestamps with timezone
- Sets, maps, and other EDN types

## Usage in Tests

### Transit-JSON (via COPY or direct insert)
```sql
-- Using COPY FROM STDIN
COPY users FROM STDIN WITH (FORMAT 'transit-json')

-- Or direct insert with OID 16384
INSERT INTO users RECORDS $1  -- where $1 has OID 16384
```
