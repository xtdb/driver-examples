import pytest
import json
from psycopg.types.json import Json

@pytest.mark.asyncio
async def test_json_roundtrip(conn, clean_table):
    """Test JSON data roundtripping."""
    table = clean_table

    json_data = {
        "nested": {
            "key": "value",
            "array": [1, 2, 3],
            "bool": True
        }
    }

    await conn.execute(
        f"INSERT INTO {table} (_id, data) VALUES (%s, %s)",
        ("json1", Json(json_data))
    )

    cursor = await conn.execute(f"SELECT data FROM {table} WHERE _id = %s", ("json1",))
    result = await cursor.fetchone()

    # Result comes back as string, parse it
    returned_data = json.loads(result[0]) if isinstance(result[0], str) else result[0]

    assert returned_data == json_data
    assert returned_data["nested"]["key"] == "value"
    assert returned_data["nested"]["array"] == [1, 2, 3]
    assert returned_data["nested"]["bool"] is True

@pytest.mark.asyncio
async def test_load_sample_data(conn):
    """Test loading and querying sample data from test-data directory."""
    import os

    test_data_path = os.path.join(os.path.dirname(__file__), "../../test-data/sample-users.json")

    with open(test_data_path) as f:
        users = json.load(f)

    # Insert sample users
    for user in users:
        await conn.execute(
            """INSERT INTO sample_users (_id, name, age, email, active, tags, metadata)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (
                user["_id"],
                user["name"],
                user["age"],
                user["email"],
                user["active"],
                Json(user["tags"]),
                Json(user["metadata"])
            )
        )

    # Query back
    cursor = await conn.execute("SELECT _id, name, age FROM sample_users ORDER BY _id")
    rows = await cursor.fetchall()

    assert len(rows) == 3
    assert rows[0][0] == "alice"
    assert rows[0][1] == "Alice Smith"
    assert rows[0][2] == 30

@pytest.mark.asyncio
async def test_json_records_syntax(conn, clean_table):
    """Test RECORDS syntax with JSON objects (similar to Node.js pattern)."""
    import os

    table = clean_table
    test_data_path = os.path.join(os.path.dirname(__file__), "../../test-data/sample-users.json")

    with open(test_data_path) as f:
        users = json.load(f)

    # Insert using RECORDS syntax with JSON-encoded strings
    # Note: psycopg doesn't have sql.types.json() like postgres.js
    # We build the RECORDS syntax manually with JSON-encoded values
    for user in users:
        # Build RECORDS clause with properly formatted JSON
        record_str = f"""{{
            _id: '{user["_id"]}',
            name: '{user["name"]}',
            age: {user["age"]},
            email: '{user["email"]}',
            active: {str(user["active"]).lower()}
        }}"""
        await conn.execute(f"INSERT INTO {table} RECORDS {record_str}")

    # Query back
    cursor = await conn.execute(f"SELECT _id, name, age, active FROM {table} ORDER BY _id")
    rows = await cursor.fetchall()

    assert len(rows) == 3
    assert rows[0][0] == "alice"
    assert rows[0][1] == "Alice Smith"
    assert rows[0][2] == 30
    assert rows[0][3] is True
