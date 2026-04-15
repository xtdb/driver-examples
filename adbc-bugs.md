# XTDB ADBC / Flight SQL — Known Issues

Running log of issues discovered while exercising XTDB's ADBC/FlightSQL surface
from non-JVM clients. Updated as new issues land or resolve.

**Tracking issue upstream:** [xtdb/xtdb#5132](https://github.com/xtdb/xtdb/issues/5132)

## Environment

| Item | Value |
|---|---|
| XTDB image | `ghcr.io/xtdb/xtdb-aws:edge` |
| Tested build | nightly `75472e4` (2026-04-14 rebuild) |
| FlightSQL endpoint | `grpc://xtdb:9833` |
| Python ADBC driver | `adbc-driver-flightsql v1.11.0` |
| Go ADBC driver | `github.com/apache/arrow-adbc/go/adbc/driver/flightsql` |

---

## Open

### 1. Malformed Arrow IPC on column-schema responses

**Symptom.** Both `GetTableSchema` and `GetObjects(depth=all)` (the call paths
that emit per-column schema information) return corrupt IPC bytes:

```
arrow/ipc: could not read message schema: could not read continuation indicator: EOF
```

**Reproducer (Python):**

```python
import adbc_driver_flightsql.dbapi as f
c = f.connect("grpc://xtdb:9833")
cur = c.cursor()
cur.executemany("INSERT INTO t (_id, n) VALUES (?, ?)", [(1, 42)])
cur.close()

# Both raise the IPC error:
c.adbc_get_table_schema("t", db_schema_filter="public")
c.adbc_get_objects(depth="all", table_name_filter="t").read_all()
```

**Impact.** Any ADBC client calling `GetTableSchema` or enumerating columns
via `GetObjects` gets a hard error. BI/notebook tools that introspect schemas
before querying (e.g. DBeaver, DataGrip, Trino-style metadata scans) will fail.

**Test coverage.** `python/tests/test_adbc_metadata.py` has two `xfail`-marked
cases (`test_returns_arrow_schema`, `test_all_depth_returns_columns`) that flip
to XPASS when fixed.

**Suspect.** Same IPC encoding path shared by both endpoints. Other shallower
`GetObjects` depths (`catalogs`, `db_schemas`, `tables`) serialize fine.

---

### 2. Literal DML via `cursor.execute()` returns `INTERNAL` instead of a useful error

**Symptom.** DML statements (INSERT/UPDATE/DELETE/ERASE) passed to
`cursor.execute()` hit the FlightSQL DoGet (query) path and fail:

```
INTERNAL: [FlightSQL] There was an error servicing your request. (Internal; ExecuteQuery). Vendor code: 13
```

**Correct client usage.** Python ADBC exposes `cursor.executescript()` for the
update path (as discussed on xtdb/xtdb#5082). Python's DB-API doesn't
distinguish execute-query vs execute-update, so `executescript()` is the
designated update route for literal DML.

```python
# Fails:
cur.execute("INSERT INTO products RECORDS {_id: 1, name: 'Widget'}")

# Works:
cur.executescript("INSERT INTO products RECORDS {_id: 1, name: 'Widget'}")

# Also works (parameterized path goes through DoPut automatically):
cur.executemany(
    "INSERT INTO products (_id, name) VALUES (?, ?)", [(1, "Widget")]
)
```

**Server-side ask.** The error message is useless. XTDB should either propagate
a structured message distinguishing "DML submitted via query endpoint" from
genuine internal faults, or accept the statement on DoGet and route it
internally. Tracked in xtdb/xtdb#5082.

**Test coverage.** `python/flight_sql_example.py` section `4b` exercises the
`executescript` path as the canonical shape.

---

### 3. Parser errors surface as `INTERNAL` (Prepare) instead of `INVALID_ARGUMENT`

**Symptom.** Syntactically-unrecognized SQL — e.g. `DROP TABLE IF EXISTS t`,
which XTDB has no parser rule for — comes back from ADBC as:

```
InternalError: INTERNAL: [FlightSQL] There was an error servicing your
  request. (Internal; Prepare). Vendor code: 13
```

The same error appears via both `cursor.adbc_prepare(sql)` and
`cursor.execute(sql)` (which prepares under the hood).

**Reproducer.**

```python
import adbc_driver_flightsql.dbapi as f
c = f.connect("grpc://xtdb:9833")
cur = c.cursor()
cur.adbc_prepare("SELECT 1")              # OK
cur.adbc_prepare("SELECT ? + 1")          # OK (binds supported)
cur.adbc_prepare("DROP TABLE IF EXISTS t")  # raises INTERNAL
```

**Impact.** The entire `adbc-drivers/validation` `test_get_objects_table_*`
setup path hits this because the suite's default `try_drop_table` quirk
issues `DROP TABLE`. Client error handling can't distinguish real server
faults from grammar mismatches.

**Server-side ask.** Classify parser errors as `INVALID_ARGUMENT` /
`UNIMPLEMENTED` rather than `INTERNAL`. Separately, either accept `DROP TABLE`
as a no-op (XTDB has no schemas) or surface a helpful "use ERASE instead"
message.

**Test coverage.** Visible in `python/validation/tests/` under the
`test_get_objects_*` errors.

---

### 3b. FlightSQL bulk ingest (`ExecuteIngest`) not implemented

**Symptom.** `cursor.adbc_ingest(table, arrow_table, mode=…)` for any of
`create`, `append`, `replace`, `create_append`:

```
NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] Not implemented.
  (Unimplemented; ExecuteIngest). Vendor code: 12
```

**Impact.** Loss of the headline Arrow ergonomic: loading a `pyarrow.Table`
directly into XTDB in one call. Clients have to manually shred the Arrow
table into rows and call `executemany("INSERT ... VALUES (?, …)")`, which
round-trips through parameterized INSERT and is much slower for wide tables.

**Reproducer.**

```python
import pyarrow as pa, adbc_driver_flightsql.dbapi as f
c = f.connect("grpc://xtdb:9833")
cur = c.cursor()
t = pa.table({"_id": [1, 2], "n": [10, 20]})
cur.adbc_ingest("ingest_probe", t, mode="create_append")  # raises
```

**Server-side ask.** Implement FlightSQL's `CommandStatementIngest` (added in
FlightSQL v13). Especially valuable for loading parquet/arrow snapshots into
XTDB for time-travel analysis.

**Test coverage.** `python/validation/tests/test_ingest.py` (via task b)
surfaces this already as `NotSupportedError`.

---

### 4. `GetCurrentCatalog` / `GetCurrentDbSchema` not exposed over FlightSQL

**Symptom.**

```
ProgrammingError: NOT_FOUND: [Flight SQL] failed to get current catalog:
  Not Found: [Flight SQL] current catalog not supported
```

Same for `adbc_current_db_schema`.

**Reproducer.**

```python
import adbc_driver_flightsql.dbapi as f
c = f.connect("grpc://xtdb:9833")
c.adbc_current_catalog       # raises
c.adbc_current_db_schema     # raises
```

**Impact.** Clients that auto-scope DDL/DML to the current catalog/schema (e.g.
DBeaver, Tableau) get a hard error rather than falling back. Per jarohen's
comment on xtdb/xtdb#5132 these were part of the stage-4 work; the landed
changes appear to cover `GetObjects`/`GetInfo` but not the FlightSQL
`CommandGetCurrentCatalog` / `CommandGetCurrentDbSchema` extensions.

**Test coverage.** Captured implicitly — `validation/xtdb.py` sets
`current_catalog=None`/`current_schema=None` so the validation suite does not
assert specific values. Flip back to the real values once server-side support
lands.

---

## Resolved

*(none yet — this file is brand new)*

---

## Notes

- Non-JVM clients use the stock Apache ADBC FlightSQL driver (Go/C/Python/C#).
  There is no bespoke XTDB ADBC driver outside the JVM.
- In-process JVM ADBC is a separate codepath with its own residual items on
  issue #5132 (e.g. `AdbcStatement.prepare()` still a TODO). This file
  focuses on over-the-wire FlightSQL issues visible to non-JVM clients.
- Conformance target is
  [adbc-drivers/validation](https://github.com/adbc-drivers/validation). Wired
  into this repo under `python/validation/` — see
  [`python/validation/README.md`](python/validation/README.md) for the
  16 pass / 49 fail / 77 skip / 13 error breakdown and failure categories.
