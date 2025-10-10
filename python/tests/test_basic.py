import pytest
import json

@pytest.mark.asyncio
async def test_connection(conn):
    """Test basic database connectivity."""
    cursor = await conn.execute("SELECT 1 as test")
    result = await cursor.fetchone()
    assert result[0] == 1

@pytest.mark.asyncio
async def test_insert_and_query(conn, clean_table):
    """Test basic INSERT and SELECT operations."""
    table = clean_table

    await conn.execute(
        f"INSERT INTO {table} RECORDS {{_id: 'test1', value: 'hello'}}, {{_id: 'test2', value: 'world'}}"
    )

    cursor = await conn.execute(f"SELECT * FROM {table} ORDER BY _id")
    rows = await cursor.fetchall()

    assert len(rows) == 2
    assert rows[0][0] == "test1"
    assert rows[0][1] == "hello"
    assert rows[1][0] == "test2"
    assert rows[1][1] == "world"

@pytest.mark.asyncio
async def test_parameterized_query(conn, clean_table):
    """Test parameterized queries with prepared statements."""
    table = clean_table

    # Insert with parameters
    cursor = await conn.execute(
        f"INSERT INTO {table} (_id, name, age) VALUES (%s, %s, %s)",
        ("user1", "Alice", 30)
    )

    # Query with parameters (select specific columns in order)
    cursor = await conn.execute(
        f"SELECT _id, name, age FROM {table} WHERE _id = %s",
        ("user1",)
    )
    result = await cursor.fetchone()

    assert result[0] == "user1"
    assert result[1] == "Alice"
    assert result[2] == 30

@pytest.mark.asyncio
async def test_count_query(conn, clean_table):
    """Test COUNT queries."""
    table = clean_table

    await conn.execute(
        f"INSERT INTO {table} RECORDS {{_id: 1}}, {{_id: 2}}, {{_id: 3}}"
    )

    cursor = await conn.execute(f"SELECT COUNT(*) FROM {table}")
    result = await cursor.fetchone()

    assert result[0] == 3

@pytest.mark.asyncio
async def test_where_clause(conn, clean_table):
    """Test WHERE clause filtering."""
    table = clean_table

    await conn.execute(
        f"INSERT INTO {table} (_id, age) VALUES (1, 25), (2, 35), (3, 45)"
    )

    cursor = await conn.execute(f"SELECT _id FROM {table} WHERE age > 30 ORDER BY _id")
    rows = await cursor.fetchall()

    assert len(rows) == 2
    assert rows[0][0] == 2
    assert rows[1][0] == 3
