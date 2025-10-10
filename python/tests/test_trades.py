"""
Original trades example from main.py, preserved as tests.
"""
import pytest
from psycopg.types.json import Json

@pytest.mark.asyncio
async def test_trades_insert_and_query(conn, clean_table):
    """Test the original trades example."""
    table = clean_table

    trades = [
        {"_id": 1, "name": "Trade1", "quantity": 1001, "info": {"some_nested": ["json", 42, {"data": ["hello"]}]}},
        {"_id": 2, "name": "Trade2", "quantity": 15, "info": 2},
        {"_id": 3, "name": "Trade3", "quantity": 200, "info": 3},
    ]

    # Insert trades
    for trade in trades:
        await conn.execute(
            f"INSERT INTO {table} (_id, name, quantity, info) VALUES (%s, %s, %s, %s)",
            (trade["_id"], trade["name"], trade["quantity"], Json(trade["info"]))
        )

    # Query trades with quantity > 100
    cursor = await conn.execute(f"SELECT _id, name, quantity, info FROM {table} WHERE quantity > %s ORDER BY _id", (100,))
    result = await cursor.fetchall()

    assert len(result) == 2
    assert result[0][1] == "Trade1"
    assert result[0][2] == 1001
    assert result[1][1] == "Trade3"
    assert result[1][2] == 200

@pytest.mark.asyncio
async def test_complex_json_structure(conn, clean_table):
    """Test complex nested JSON structures."""
    table = clean_table

    complex_data = {
        "some_nested": [
            "json",
            42,
            {
                "data": ["hello"],
                "more": {
                    "deeply": {
                        "nested": True
                    }
                }
            }
        ]
    }

    await conn.execute(
        f"INSERT INTO {table} (_id, info) VALUES (%s, %s)",
        (1, Json(complex_data))
    )

    cursor = await conn.execute(f"SELECT info FROM {table} WHERE _id = 1")
    result = await cursor.fetchone()

    # Parse if string
    import json
    returned = json.loads(result[0]) if isinstance(result[0], str) else result[0]

    assert returned["some_nested"][0] == "json"
    assert returned["some_nested"][1] == 42
    assert returned["some_nested"][2]["data"] == ["hello"]
    assert returned["some_nested"][2]["more"]["deeply"]["nested"] is True
