"""
Test transit-json functionality with XTDB.

Transit-json (OID 16384) provides richer type preservation than standard JSON.
"""
import pytest
import json
import re
from datetime import datetime, date
from psycopg.adapt import Dumper
from psycopg.pq import Format


# Custom dumper for transit-JSON that uses OID 16384
class TransitDumper(Dumper):
    format = Format.TEXT
    oid = 16384  # Transit-JSON OID

    def dump(self, obj):
        # obj should be a transit-JSON string
        return obj.encode('utf-8')


class MinimalTransitEncoder:
    """Minimal transit-JSON encoder for basic types."""

    @staticmethod
    def encode_value(value):
        """Encode a Python value to transit-JSON format."""
        if isinstance(value, dict):
            return MinimalTransitEncoder.encode_map(value)
        elif isinstance(value, list):
            # Encode list items without extra quotes
            encoded_items = [MinimalTransitEncoder.encode_value(v) for v in value]
            return f'[{",".join(encoded_items)}]'
        elif isinstance(value, str):
            return json.dumps(value)
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, datetime):
            return f'"~t{value.isoformat()}"'
        elif isinstance(value, date):
            return f'"~t{value.isoformat()}"'
        elif value is None:
            return "null"
        else:
            return json.dumps(str(value))

    @staticmethod
    def encode_map(data):
        """Encode a Python dict to transit-JSON map format."""
        pairs = []
        for key, value in data.items():
            pairs.append(f'"~:{key}"')
            pairs.append(MinimalTransitEncoder.encode_value(value))
        return f'["^ ",{",".join(pairs)}]'

    @staticmethod
    def decode_transit_line(line):
        """Decode a transit-JSON line to Python dict (basic implementation)."""
        # This is a simplified decoder - just for testing roundtrip
        # In production, you'd want a proper transit-python library
        data = json.loads(line)
        if isinstance(data, list) and len(data) > 0 and data[0] == "^ ":
            # It's a map
            result = {}
            i = 1
            while i < len(data):
                key = data[i]
                value = data[i + 1] if i + 1 < len(data) else None
                # Remove ~: prefix from keywords
                if isinstance(key, str) and key.startswith("~:"):
                    key = key[2:]
                # Handle ~t dates
                if isinstance(value, str) and value.startswith("~t"):
                    value = value[2:]  # Keep as string for now
                result[key] = value
                i += 2
            return result
        return data


@pytest.mark.asyncio
async def test_transit_json_format(conn, clean_table):
    """Test understanding transit-JSON format and conversion."""
    table = clean_table

    # Demonstrate transit-JSON encoding
    data = {"_id": "transit1", "name": "Transit User", "age": 42, "active": True}
    transit_json = MinimalTransitEncoder.encode_map(data)

    # Verify it creates proper transit format
    assert '["^ "' in transit_json
    assert '"~:_id"' in transit_json
    assert '"~:name"' in transit_json

    # For RECORDS syntax, use curly brace format (psycopg doesn't support OID 16384 easily)
    await conn.execute(
        f"INSERT INTO {table} RECORDS {{_id: 'transit1', name: 'Transit User', age: 42, active: true}}"
    )

    cursor = await conn.execute(f"SELECT _id, name, age, active FROM {table} WHERE _id = %s", ("transit1",))
    result = await cursor.fetchone()

    assert result[0] == "transit1"
    assert result[1] == "Transit User"
    assert result[2] == 42
    assert result[3] is True

@pytest.mark.asyncio
async def test_transit_json_parsing(conn, clean_table):
    """Test parsing sample-users-transit.json file using transit OID (16384)."""
    import os

    table = clean_table
    test_data_path = os.path.join(os.path.dirname(__file__), "../../test-data/sample-users-transit.json")

    # Register transit dumper for string type
    conn.adapters.register_dumper(str, TransitDumper)

    with open(test_data_path) as f:
        lines = f.readlines()

    # Insert using transit-JSON OID (16384) with single parameter per record
    # Pass the raw transit-JSON string directly without unmarshalling
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Use INSERT INTO table RECORDS $1 where $1 is sent with OID 16384
        await conn.execute(
            f"INSERT INTO {table} RECORDS %s",
            (line,)
        )

    # Query back and verify
    cursor = await conn.execute(f"SELECT _id, name, age, active FROM {table} ORDER BY _id")
    rows = await cursor.fetchall()

    assert len(rows) == 3
    assert rows[0][0] == "alice"
    assert rows[0][1] == "Alice Smith"
    assert rows[0][2] == 30
    assert rows[0][3] is True

@pytest.mark.asyncio
async def test_records_syntax(conn, clean_table):
    """Test XTDB's RECORDS syntax which is transit-json compatible."""
    table = clean_table

    # RECORDS syntax accepts transit-json format
    await conn.execute(
        f"INSERT INTO {table} RECORDS {{_id: 'rec1', name: 'Record User', value: 100}}"
    )

    cursor = await conn.execute(f"SELECT _id, name, value FROM {table} WHERE _id = %s", ("rec1",))
    result = await cursor.fetchone()

    assert result[0] == "rec1"
    assert result[1] == "Record User"
    assert result[2] == 100
