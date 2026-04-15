"""
XTDB ADBC Metadata-Endpoint Tests

Exercises the catalog-introspection surface of ADBC over Flight SQL:
  - adbc_get_info()          (SqlInfo / driver+vendor identification)
  - adbc_get_table_types()   (FlightSQL CommandGetTableTypes)
  - adbc_get_objects()       (CommandGetCatalogs / DBSchemas / Tables)
  - adbc_get_table_schema()  (CommandGetTableSchema for one table)

These map to the stage-4 endpoints tracked in xtdb/xtdb#5132. Tests marked
xfail(strict=False) capture known server-side issues; they flip to XPASS
when fixed so regressions/resolutions are both visible.

Known residual bug (as of XTDB nightly 75472e4, 2026-04-14):
  GetTableSchema and GetObjects(depth=all) emit malformed Arrow IPC:
    "arrow/ipc: could not read message schema: could not read continuation
     indicator: EOF"
  Both call sites touch the column-schema serialization path.
"""

import os
import random
import time
import warnings

import pyarrow as pa
import pytest

import adbc_driver_flightsql.dbapi as flight_sql

warnings.filterwarnings("ignore", message="Cannot disable autocommit")

FLIGHT_SQL_HOST = os.environ.get("XTDB_HOST", "xtdb")
FLIGHT_SQL_URI = f"grpc://{FLIGHT_SQL_HOST}:9833"

IPC_SCHEMA_BUG = (
    "XTDB emits malformed Arrow IPC for column-schema responses — "
    "'could not read continuation indicator: EOF'. Affects both "
    "GetTableSchema and GetObjects(depth=all)."
)


@pytest.fixture
def connection():
    conn = flight_sql.connect(FLIGHT_SQL_URI)
    yield conn
    conn.close()


@pytest.fixture
def seeded_table(connection):
    """Create a table with one row so metadata lookups have something to find."""
    name = f"meta_probe_{int(time.time())}_{random.randint(1000, 9999)}"
    cur = connection.cursor()
    cur.executemany(
        f"INSERT INTO {name} (_id, label, qty, price) VALUES (?, ?, ?, ?)",
        [(1, "alpha", 42, 9.99)],
    )
    cur.close()
    yield name
    cur = connection.cursor()
    cur.executemany(f"ERASE FROM {name} WHERE _id = ?", [(1,)])
    cur.close()


class TestAdbcGetInfo:
    """ADBC GetInfo — driver/vendor identification SqlInfo codes."""

    def test_returns_dict_with_driver_keys(self, connection):
        info = connection.adbc_get_info()
        assert isinstance(info, dict)
        for key in ("driver_name", "driver_version", "driver_arrow_version",
                    "driver_adbc_version"):
            assert key in info, f"expected SqlInfo key {key!r}"

    def test_driver_identifies_as_flightsql(self, connection):
        info = connection.adbc_get_info()
        assert "flight sql" in info["driver_name"].lower()

    def test_vendor_info_populated(self, connection):
        info = connection.adbc_get_info()
        assert info.get("vendor_name"), "vendor_name should be populated"
        # XTDB may report its own name here once fully wired; for now just
        # assert it isn't the placeholder string.
        assert info["vendor_version"] != "(unknown or development build)", (
            f"vendor_version placeholder still present: {info['vendor_version']!r}"
        )


class TestAdbcGetTableTypes:
    """ADBC GetTableTypes — FlightSQL CommandGetTableTypes."""

    def test_returns_table_types(self, connection):
        types = connection.adbc_get_table_types()
        assert isinstance(types, list)
        assert any(t.upper() in ("TABLE", "BASE TABLE") for t in types), types


class TestAdbcGetObjects:
    """ADBC GetObjects — hierarchical catalog/schema/table/column listing."""

    def test_catalogs_depth(self, connection):
        rdr = connection.adbc_get_objects(depth="catalogs")
        rows = rdr.read_all().to_pylist()
        assert len(rows) >= 1
        assert "catalog_name" in rows[0]

    def test_db_schemas_depth(self, connection):
        rdr = connection.adbc_get_objects(depth="db_schemas")
        rows = rdr.read_all().to_pylist()
        assert len(rows) >= 1
        all_schemas = [
            s["db_schema_name"]
            for cat in rows
            for s in (cat.get("catalog_db_schemas") or [])
        ]
        assert "public" in all_schemas, all_schemas
        assert "information_schema" in all_schemas, all_schemas

    def test_tables_depth_finds_seeded_table(self, connection, seeded_table):
        rdr = connection.adbc_get_objects(
            depth="tables", table_name_filter=seeded_table
        )
        rows = rdr.read_all().to_pylist()
        found = [
            (t["table_name"], t.get("table_type"))
            for cat in rows
            for s in (cat.get("catalog_db_schemas") or [])
            for t in (s.get("db_schema_tables") or [])
        ]
        assert any(name == seeded_table for name, _ in found), found
        assert all(typ == "TABLE" for _, typ in found if typ is not None), found

    @pytest.mark.xfail(reason=IPC_SCHEMA_BUG, strict=False)
    def test_all_depth_returns_columns(self, connection, seeded_table):
        rdr = connection.adbc_get_objects(
            depth="all", table_name_filter=seeded_table
        )
        rows = rdr.read_all().to_pylist()
        col_names = [
            c["column_name"]
            for cat in rows
            for s in (cat.get("catalog_db_schemas") or [])
            for t in (s.get("db_schema_tables") or [])
            if t["table_name"] == seeded_table
            for c in (t.get("table_columns") or [])
        ]
        assert "_id" in col_names, col_names
        assert "label" in col_names, col_names


class TestAdbcGetTableSchema:
    """ADBC GetTableSchema — Arrow schema for one table."""

    @pytest.mark.xfail(reason=IPC_SCHEMA_BUG, strict=False)
    def test_returns_arrow_schema(self, connection, seeded_table):
        schema = connection.adbc_get_table_schema(
            seeded_table, db_schema_filter="public"
        )
        assert isinstance(schema, pa.Schema)
        names = {f.name for f in schema}
        assert "_id" in names, names
        assert "label" in names, names
