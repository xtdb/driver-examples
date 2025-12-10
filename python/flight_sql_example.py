"""
XTDB Flight SQL Example

Demonstrates connecting to XTDB via Arrow Flight SQL protocol using ADBC,
including DML operations (INSERT, UPDATE, DELETE, ERASE).

Note: DML operations require parameterized queries via executemany().
Literal SQL values in DML statements are not yet supported.

Requirements:
    pip install adbc-driver-flightsql pyarrow pandas

Usage:
    python flight_sql_example.py
"""

import adbc_driver_flightsql.dbapi as flight_sql
import os


def main():
    # Use XTDB_HOST env var or default to 'xtdb' for container, 'localhost' for local
    host = os.environ.get("XTDB_HOST", "xtdb")
    uri = f"grpc://{host}:9833"

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

            # DML: INSERT (using executemany with parameters)
            # Note: Literal SQL values don't work yet, use parameterized queries instead
            #cursor.execute(
            #    "INSERT INTO products (_id, name, price, category) VALUES "
            #    "(1, 'Widget', 19.99, 'gadgets'), "
            #    "(2, 'Gizmo', 29.99, 'gadgets'), "
            #    "(3, 'Thingamajig', 9.99, 'misc')"
            #)
            print("\n4. DML - INSERT:")
            cursor.executemany(
                "INSERT INTO products (_id, name, price, category) VALUES (?, ?, ?, ?)",
                [
                    (1, "Widget", 19.99, "gadgets"),
                    (2, "Gizmo", 29.99, "gadgets"),
                    (3, "Thingamajig", 9.99, "misc"),
                ],
            )
            print("   Inserted 3 rows into 'products'")


            print("\n5. Query inserted data:")
            cursor.execute("SELECT * FROM products ORDER BY _id")
            print(cursor.fetch_arrow_table().to_pandas())

            # DML: UPDATE
            print("\n6. DML - UPDATE:")
            cursor.executemany(
                "UPDATE products SET price = ? WHERE _id = ?",
                [(24.99, 1)],
            )
            print("   Updated price for product _id=1")


            print("\n7. Query after UPDATE:")
            cursor.execute("SELECT * FROM products ORDER BY _id")
            print(cursor.fetch_arrow_table().to_pandas())

            # DML: DELETE (sets valid_to, data still visible in history)
            print("\n8. DML - DELETE:")
            cursor.executemany("DELETE FROM products WHERE _id = ?", [(3,)])
            print("   Deleted product _id=3")


            print("\n9. Query after DELETE:")
            cursor.execute("SELECT * FROM products ORDER BY _id")
            print(cursor.fetch_arrow_table().to_pandas())

            # Query historical data using FOR ALL VALID_TIME
            print("\n10. Query historical data (FOR ALL VALID_TIME):")
            cursor.execute(
                "SELECT *, _valid_from, _valid_to FROM products "
                "FOR ALL VALID_TIME ORDER BY _id, _valid_from"
            )
            print(cursor.fetch_arrow_table().to_pandas())

            # DML: ERASE (completely removes from history)
            print("\n11. DML - ERASE:")
            cursor.executemany("ERASE FROM products WHERE _id = ?", [(2,)])
            print("   Erased product _id=2 from all history")


            print("\n12. Query after ERASE (FOR ALL VALID_TIME):")
            cursor.execute(
                "SELECT *, _valid_from, _valid_to FROM products "
                "FOR ALL VALID_TIME ORDER BY _id, _valid_from"
            )
            print(cursor.fetch_arrow_table().to_pandas())

            # Cleanup
            print("\n13. Cleanup - ERASE remaining data:")
            cursor.executemany("ERASE FROM products WHERE _id = ?", [(1,), (3,)])
            print("   Erased remaining products")

            print("\n✓ Flight SQL DML examples completed successfully")


if __name__ == "__main__":
    main()
