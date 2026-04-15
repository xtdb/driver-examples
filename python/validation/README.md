# XTDB × adbc-drivers/validation

Wiring for the upstream [adbc-drivers/validation](https://github.com/adbc-drivers/validation)
conformance suite, pointed at XTDB's Flight SQL server.

## Layout

```
validation/
├── xtdb.py                  # XtdbQuirks — feature matrix + SQL dialect overrides
├── tests/
│   ├── conftest.py          # driver + driver_path fixtures; re-exports suite fixtures
│   ├── test_connection.py   # thin wrapper around suite's connection tests
│   ├── test_query.py        # thin wrapper around suite's query tests
│   ├── test_ingest.py       # thin wrapper around suite's ingest tests
│   └── test_statement.py    # thin wrapper around suite's statement tests
└── queries/
    └── type/select/
        └── int32.txtcase    # XTDB-specific override demonstrating the pattern
```

## Running

```bash
# With venv + XTDB live on grpc://localhost:9833
XTDB_HOST=localhost \
XTDB_FLIGHT_SQL_URI=grpc://localhost:9833 \
  venv/bin/python -m pytest validation/tests/ -q --tb=no
```

## Current status (XTDB nightly `75472e4`, validation suite HEAD)

| bucket | count |
|---|---|
| passed | 16 |
| skipped (declared-unsupported feature) | 77 |
| failed | 49 |
| errored (setup) | 13 |

### Failure categories

1. **`CREATE TABLE` / standard DDL not supported** — drives most
   `test_connection.py::test_get_objects_*` errors and the 13-case
   `type/select/*` failures. XTDB has implicit tables; every row requires
   `_id`. Each base fixture needs a per-driver `.txtcase` override (see
   `queries/type/select/int32.txtcase` for the pattern).
2. **Value-level dynamic typing** — `INT` columns that declare `int32` come
   back as `int64` because XTDB promotes numeric literals. The suite's
   schema-strict comparator rejects this. Either the override widens the
   expected schema to int64, or the server preserves declared widths.
3. **`GetCurrentCatalog` / `GetCurrentDbSchema` not wired** — see
   [`adbc-bugs.md`](../../adbc-bugs.md) #3.
4. **`AdbcStatement.prepare()` returns INTERNAL** — covers the 3 statement
   failures (`test_prepare`, `test_parameter_execute`, `test_parameter_schema`)
   and bubbles through `try_drop_table` (which prepares under the hood). This
   matches the "must-have for release" item on xtdb/xtdb#5132.
5. **`type/bind/*` — parameter-binding type coverage** — 28 cases. Need
   targeted probes to split these into driver/server/suite layers.

### Paths to reduce the failure count

- Fast win: override a handful of `type/select/*.txtcase` to use
  `INSERT RECORDS` + widen expected schemas. This bumps the pass rate without
  server changes.
- Medium: override the `get_objects_table` fixture so it doesn't call
  `drop_table` (XTDB uses `ERASE`).
- Server-side: fix `AdbcStatement.prepare` and wire
  `GetCurrentCatalog`/`GetCurrentDbSchema`. Both are explicitly tracked on
  xtdb/xtdb#5132.

## Not submoduled

The validation suite is pip-installed editable from a local clone
(`pip install --editable /tmp/adbc-validation` in setup). For CI or
`requirements.txt`, prefer the git URL form once upstream publishes releases:

```
adbc-drivers-validation @ git+https://github.com/adbc-drivers/validation.git
```

See the top-level README for why we don't submodule.
