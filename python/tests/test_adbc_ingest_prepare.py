"""
XTDB ADBC Bulk-Ingest & Prepared-Statement Tests

Exercises two ADBC features distinct from parameterized-query DML:
  - cursor.adbc_ingest(table, arrow_table, mode=...) — FlightSQL
    CommandStatementIngest. Currently NOT_IMPLEMENTED on XTDB; each mode is
    xfail(strict=False) so the tests flip to XPASS when support lands.
  - cursor.adbc_prepare(sql) — exposes the FlightSQL CreatePreparedStatement
    handshake separately from execute(). Works for SELECT + parameterized
    queries; DDL-shaped SQL XTDB can't parse currently returns INTERNAL
    (see adbc-bugs.md #3).
"""

import os
import random
import time
import warnings

import pyarrow as pa
import pytest
from adbc_driver_manager import InternalError, NotSupportedError

import adbc_driver_flightsql.dbapi as flight_sql

warnings.filterwarnings("ignore", message="Cannot disable autocommit")

FLIGHT_SQL_HOST = os.environ.get("XTDB_HOST", "xtdb")
FLIGHT_SQL_URI = f"grpc://{FLIGHT_SQL_HOST}:9833"


@pytest.fixture
def connection():
    conn = flight_sql.connect(FLIGHT_SQL_URI)
    yield conn
    conn.close()


@pytest.fixture
def cursor(connection):
    cur = connection.cursor()
    yield cur
    cur.close()


def unique_table() -> str:
    return f"ingest_probe_{int(time.time())}_{random.randint(1000, 9999)}"


class TestAdbcBulkIngest:
    """Arrow-first bulk load via cursor.adbc_ingest()."""

    @pytest.fixture
    def arrow_data(self):
        return pa.table(
            {
                "_id": [1, 2, 3],
                "name": ["alpha", "beta", "gamma"],
                "score": [10.5, 20.5, 30.5],
            }
        )

    @pytest.mark.parametrize(
        "mode", ["create", "append", "replace", "create_append"]
    )
    @pytest.mark.xfail(
        reason="XTDB does not implement FlightSQL CommandStatementIngest "
               "(adbc-bugs.md #3b). All modes return NOT_IMPLEMENTED.",
        strict=False,
        raises=NotSupportedError,
    )
    def test_ingest_mode(self, cursor, arrow_data, mode):
        table = unique_table()
        cursor.adbc_ingest(table, arrow_data, mode=mode)

        cursor.execute(f"SELECT COUNT(*) AS n FROM {table}")
        result = cursor.fetch_arrow_table().to_pylist()
        assert result == [{"n": 3}]

        # Cleanup (only reached if the call succeeds)
        cursor.executemany(
            f"ERASE FROM {table} WHERE _id = ?", [(1,), (2,), (3,)]
        )


class TestAdbcPrepare:
    """Prepared-statement handshake via cursor.adbc_prepare()."""

    def test_prepare_simple_select(self, cursor):
        cursor.adbc_prepare("SELECT 1 AS x")
        cursor.execute("SELECT 1 AS x")
        assert cursor.fetch_arrow_table().to_pylist() == [{"x": 1}]

    def test_prepare_parameterized(self, cursor):
        """Parameter binding through prepared statement path."""
        cursor.adbc_prepare("SELECT ? + 1 AS r")
        cursor.execute("SELECT ? + 1 AS r", parameters=(41,))
        assert cursor.fetch_arrow_table().to_pylist() == [{"r": 42}]

    def test_prepare_where_clause(self, cursor):
        """Prepared DELETE-shape — validates bind on DML statements."""
        table = unique_table()
        cursor.executemany(
            f"INSERT INTO {table} (_id, name) VALUES (?, ?)",
            [(1, "a"), (2, "b")],
        )
        cursor.adbc_prepare(f"DELETE FROM {table} WHERE _id = ?")
        cursor.executemany(f"ERASE FROM {table} WHERE _id = ?", [(1,), (2,)])

    @pytest.mark.xfail(
        reason="XTDB has no DDL (no DROP TABLE). Parser error surfaces as "
               "INTERNAL rather than INVALID_ARGUMENT — adbc-bugs.md #3.",
        strict=False,
        raises=InternalError,
    )
    def test_prepare_unparseable_sql_classifies_as_invalid_argument(self, cursor):
        # DROP TABLE isn't in XTDB's grammar. When the parser rejects it,
        # the client should ideally see InvalidArgument / NotSupported,
        # not Internal.
        with pytest.raises(NotSupportedError):
            cursor.adbc_prepare("DROP TABLE IF EXISTS does_not_exist")
