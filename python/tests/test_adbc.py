"""
XTDB ADBC Tests

Tests for connecting to XTDB via Arrow Flight SQL protocol using ADBC.
Demonstrates DML operations (INSERT, UPDATE, DELETE, ERASE) and temporal queries.

Requirements:
    pip install adbc-driver-flightsql pyarrow pandas pytest
"""

import pytest
import time
import random
import os
import adbc_driver_flightsql.dbapi as flight_sql


FLIGHT_SQL_HOST = os.environ.get("XTDB_HOST", "xtdb")
FLIGHT_SQL_URI = f"grpc://{FLIGHT_SQL_HOST}:9833"


@pytest.fixture
def connection():
    """Create a Flight SQL connection for each test."""
    conn = flight_sql.connect(FLIGHT_SQL_URI)
    yield conn
    conn.close()


@pytest.fixture
def cursor(connection):
    """Create a cursor from the connection."""
    cursor = connection.cursor()
    yield cursor
    cursor.close()


def get_clean_table():
    """Generate a unique table name for test isolation."""
    return f"test_adbc_{int(time.time())}_{random.randint(1000, 9999)}"


class TestAdbcConnection:
    """Test basic ADBC connectivity."""

    def test_connection(self, connection):
        """Verify connection to Flight SQL server."""
        assert connection is not None

    def test_simple_query(self, cursor):
        """Test simple SELECT query returning Arrow table."""
        cursor.execute("SELECT 1 AS x, 'hello' AS greeting")
        table = cursor.fetch_arrow_table()

        assert table.num_rows == 1
        assert table.column_names == ["x", "greeting"]
        assert table.column("x").to_pylist() == [1]
        assert table.column("greeting").to_pylist() == ["hello"]

    def test_query_with_expressions(self, cursor):
        """Test query with computed expressions."""
        cursor.execute("SELECT 2 + 2 AS sum, UPPER('hello') AS upper_greeting")
        table = cursor.fetch_arrow_table()

        assert table.num_rows == 1
        assert table.column("sum").to_pylist() == [4]
        assert table.column("upper_greeting").to_pylist() == ["HELLO"]

    def test_system_tables(self, cursor):
        """Test querying information_schema."""
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' LIMIT 10"
        )
        table = cursor.fetch_arrow_table()
        # Should return without error, may have 0 or more rows
        assert table is not None


class TestAdbcDML:
    """Test DML operations via Flight SQL."""

    def test_insert_and_query(self, cursor):
        """Test INSERT and SELECT operations."""
        table_name = get_clean_table()

        # INSERT using executemany with parameters
        cursor.executemany(
            f"INSERT INTO {table_name} (_id, name, price, category) VALUES (?, ?, ?, ?)",
            [
                (1, "Widget", 19.99, "gadgets"),
                (2, "Gizmo", 29.99, "gadgets"),
                (3, "Thingamajig", 9.99, "misc"),
            ],
        )


        # Query the inserted data
        cursor.execute(f"SELECT * FROM {table_name} ORDER BY _id")
        table = cursor.fetch_arrow_table()

        assert table.num_rows == 3
        assert table.column("_id").to_pylist() == [1, 2, 3]
        assert table.column("name").to_pylist() == ["Widget", "Gizmo", "Thingamajig"]

        # Cleanup
        cursor.executemany(f"ERASE FROM {table_name} WHERE _id = ?", [(1,), (2,), (3,)])

    def test_update(self, cursor):
        """Test UPDATE operation."""
        table_name = get_clean_table()

        # Insert initial data
        cursor.executemany(
            f"INSERT INTO {table_name} (_id, name, price) VALUES (?, ?, ?)",
            [(1, "Widget", 19.99)],
        )

        # Update the price
        cursor.executemany(
            f"UPDATE {table_name} SET price = ? WHERE _id = ?",
            [(24.99, 1)],
        )

        # Verify update
        cursor.execute(f"SELECT price FROM {table_name} WHERE _id = 1")
        table = cursor.fetch_arrow_table()

        assert table.num_rows == 1
        assert abs(table.column("price").to_pylist()[0] - 24.99) < 0.01

        # Cleanup
        cursor.executemany(f"ERASE FROM {table_name} WHERE _id = ?", [(1,)])

    def test_delete(self, cursor):
        """Test DELETE operation (soft delete - data still in history)."""
        table_name = get_clean_table()

        # Insert data
        cursor.executemany(
            f"INSERT INTO {table_name} (_id, name) VALUES (?, ?)",
            [(1, "ToDelete"), (2, "ToKeep")],
        )

        # Delete one record
        cursor.executemany(f"DELETE FROM {table_name} WHERE _id = ?", [(1,)])

        # Verify only one record remains in current view
        cursor.execute(f"SELECT * FROM {table_name}")
        table = cursor.fetch_arrow_table()

        assert table.num_rows == 1
        assert table.column("_id").to_pylist() == [2]

        # Cleanup
        cursor.executemany(f"ERASE FROM {table_name} WHERE _id = ?", [(1,), (2,)])

    def test_historical_query(self, cursor):
        """Test FOR ALL VALID_TIME to see historical data."""
        table_name = get_clean_table()

        # Insert initial data
        cursor.executemany(
            f"INSERT INTO {table_name} (_id, name, price) VALUES (?, ?, ?)",
            [(1, "Widget", 19.99)],
        )

        # Update the price (creates new version)
        cursor.executemany(
            f"UPDATE {table_name} SET price = ? WHERE _id = ?",
            [(24.99, 1)],
        )

        # Query historical data
        cursor.execute(
            f"SELECT *, _valid_from, _valid_to FROM {table_name} "
            f"FOR ALL VALID_TIME ORDER BY _id, _valid_from"
        )
        table = cursor.fetch_arrow_table()

        # Should have 2 versions of the record
        assert table.num_rows == 2
        prices = table.column("price").to_pylist()
        assert abs(prices[0] - 19.99) < 0.01  # Original price
        assert abs(prices[1] - 24.99) < 0.01  # Updated price

        # Cleanup
        cursor.executemany(f"ERASE FROM {table_name} WHERE _id = ?", [(1,)])

    def test_erase(self, cursor):
        """Test ERASE operation (hard delete - removes from history)."""
        table_name = get_clean_table()

        # Insert data
        cursor.executemany(
            f"INSERT INTO {table_name} (_id, name) VALUES (?, ?)",
            [(1, "ToErase"), (2, "ToKeep")],
        )

        # Update to create history
        cursor.executemany(
            f"UPDATE {table_name} SET name = ? WHERE _id = ?",
            [("UpdatedErase", 1)],
        )

        # Erase record 1 completely
        cursor.executemany(f"ERASE FROM {table_name} WHERE _id = ?", [(1,)])

        # Verify erased record is gone from all history
        cursor.execute(
            f"SELECT * FROM {table_name} FOR ALL VALID_TIME ORDER BY _id"
        )
        table = cursor.fetch_arrow_table()

        # Only record 2 should remain
        assert table.num_rows == 1
        assert table.column("_id").to_pylist() == [2]

        # Cleanup
        cursor.executemany(f"ERASE FROM {table_name} WHERE _id = ?", [(2,)])


class TestAdbcDataTypes:
    """Test Arrow data type handling."""

    def test_to_pandas(self, cursor):
        """Test converting Arrow table to pandas DataFrame."""
        cursor.execute("SELECT 1 AS int_col, 'text' AS str_col, 3.14 AS float_col")
        table = cursor.fetch_arrow_table()
        df = table.to_pandas()

        assert len(df) == 1
        assert df["int_col"].iloc[0] == 1
        assert df["str_col"].iloc[0] == "text"
        assert abs(df["float_col"].iloc[0] - 3.14) < 0.01

    def test_arrow_schema(self, cursor):
        """Test Arrow schema information."""
        table_name = get_clean_table()

        cursor.executemany(
            f"INSERT INTO {table_name} (_id, name, count, price, active) VALUES (?, ?, ?, ?, ?)",
            [(1, "Test", 42, 19.99, True)],
        )

        cursor.execute(f"SELECT * FROM {table_name}")
        table = cursor.fetch_arrow_table()

        # Verify schema has expected columns
        schema = table.schema
        column_names = [field.name for field in schema]
        assert "_id" in column_names
        assert "name" in column_names

        # Cleanup
        cursor.executemany(f"ERASE FROM {table_name} WHERE _id = ?", [(1,)])
