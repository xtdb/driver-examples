"""
XTDB Flight SQL Example

Demonstrates connecting to XTDB via Arrow Flight SQL protocol using ADBC.

Note: XTDB's Flight SQL implementation currently supports queries (SELECT).
DML operations (INSERT/UPDATE/DELETE) and some metadata operations are
not yet fully implemented. Use the PostgreSQL wire protocol for full
functionality.

Requirements:
    pip install adbc-driver-flightsql pyarrow pandas

Usage:
    python flight_sql_example.py
"""

import adbc_driver_flightsql.dbapi as flight_sql


def main():
    # Connect to XTDB Flight SQL server using ADBC
    # Use "xtdb" as host when running inside Docker, "localhost" from host machine
    uri = "grpc://localhost:9833"

    with flight_sql.connect(uri) as conn:
        with conn.cursor() as cursor:
            print("Connected to XTDB Flight SQL server")

            # Simple query
            print("\n1. Simple SELECT query:")
            cursor.execute("SELECT 1 AS x, 'hello' AS greeting")
            print(cursor.fetch_arrow_table().to_pandas())

            # Query with expressions
            print("\n2. Query with expressions:")
            cursor.execute("SELECT 2 + 2 AS sum, UPPER('hello') AS upper_greeting")
            print(cursor.fetch_arrow_table().to_pandas())

            # Query system tables
            print("\n3. List tables (information_schema):")
            cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' LIMIT 10"
            )
            print(cursor.fetch_arrow_table().to_pandas())

            # Query with more expressions
            print("\n4. More expressions:")
            cursor.execute("SELECT 10 * 5 AS product, 'foo' || 'bar' AS concat")
            print(cursor.fetch_arrow_table().to_pandas())

            # If there's existing data, query it
            print("\n5. Query existing data (if any):")
            try:
                cursor.execute("SELECT * FROM foo LIMIT 5")
                result = cursor.fetch_arrow_table()
                if result.num_rows > 0:
                    print(result.to_pandas())
                else:
                    print("   (table 'foo' is empty)")
            except Exception as e:
                print(f"   (table 'foo' doesn't exist or error: {e})")

            print("\n✓ Flight SQL query examples completed successfully")
            print("\nNote: For INSERT/UPDATE/DELETE operations, use the PostgreSQL")
            print("wire protocol (port 5432) instead of Flight SQL.")


if __name__ == "__main__":
    main()
