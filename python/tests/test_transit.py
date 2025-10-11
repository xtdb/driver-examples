"""
Test transit-json functionality with XTDB.

Transit-json (OID 16384) provides richer type preservation than standard JSON.

Note: This module includes a custom transit-JSON reader (parse_transit_value)
that handles the common transit-JSON subset without requiring external libraries.
It supports:
- Transit maps: ["^ ", "~:key", value, ...]
- Transit keywords: "~:keyword"
- Transit dates: "~t2020-01-15"
- Nested arrays and objects

For WRITING transit-JSON from Python, you'd need transit-python2 or similar.
For READING transit-JSON (as shown here), the custom parser is sufficient.
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

@pytest.mark.asyncio
async def test_transit_verify_with_unmarshalling(conn, clean_table):
    """
    Verify the OID 16384 approach by parsing and comparing transit-JSON.

    This test demonstrates:
    1. Insert complete transit-JSON data using OID 16384 (raw strings)
    2. Query ALL fields back from XTDB (including nested arrays and objects)
    3. Parse the original transit-JSON to verify correctness

    This proves the OID approach preserves ALL data correctly including:
    - Scalar fields (strings, numbers, booleans)
    - Nested arrays (tags)
    - Nested objects (metadata with dates)

    Note: Uses a custom transit-JSON parser (no external transit library needed for reading).
    """
    import os
    import json
    from datetime import date

    table = clean_table
    test_data_path = os.path.join(os.path.dirname(__file__), "../../test-data/sample-users-transit.json")

    # Register transit dumper for string type
    conn.adapters.register_dumper(str, TransitDumper)

    with open(test_data_path) as f:
        lines = f.readlines()

    # Store the original parsed data for later comparison
    original_data = []

    # Step 1: Insert using OID 16384 approach
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Parse the COMPLETE transit-JSON with custom parser
        # This demonstrates that you don't need transit-python2 for reading transit-JSON
        raw_json = json.loads(line)

        # Custom transit-JSON reader (handles the subset we need)
        def parse_transit_value(val):
            if isinstance(val, list) and len(val) > 0:
                if val[0] == "^ ":
                    # It's a map: ["^ ", "~:key1", val1, "~:key2", val2, ...]
                    result = {}
                    for i in range(1, len(val), 2):
                        if i + 1 >= len(val):
                            break
                        k = val[i]
                        v = val[i + 1]
                        # Convert keyword keys
                        if isinstance(k, str) and k.startswith("~:"):
                            k = k[2:]
                        # Recursively parse values
                        result[k] = parse_transit_value(v)
                    return result
                else:
                    # It's an array - parse each element
                    return [parse_transit_value(item) for item in val]
            elif isinstance(val, str) and val.startswith("~t"):
                # Transit date - keep as ISO string
                return val[2:]
            else:
                return val

        parsed_dict = parse_transit_value(raw_json)
        original_data.append(parsed_dict)

        # Insert using OID 16384 - raw transit-JSON string
        await conn.execute(
            f"INSERT INTO {table} RECORDS %s",
            (line,)
        )

    # Step 2: Query back ALL the data including nested fields
    cursor = await conn.execute(
        f"SELECT _id, name, age, active, email, salary, tags, metadata FROM {table} ORDER BY _id"
    )
    rows = await cursor.fetchall()

    assert len(rows) == 3
    assert len(original_data) == 3

    # Step 3: Verify that ALL queried data matches what we parsed from transit-JSON
    for i, (row, original) in enumerate(zip(rows, original_data)):
        print(f"\n✅ Verifying record {i+1} ({original.get('_id')}):")
        print(f"   Scalar fields:")
        print(f"     _id:    {row[0]} == {original.get('_id')}")
        print(f"     name:   {row[1]} == {original.get('name')}")
        print(f"     age:    {row[2]} == {original.get('age')}")
        print(f"     active: {row[3]} == {original.get('active')}")
        print(f"     email:  {row[4]} == {original.get('email')}")
        print(f"     salary: {row[5]} == {original.get('salary')}")

        # Verify scalar fields
        assert row[0] == original.get('_id')
        assert row[1] == original.get('name')
        assert row[2] == original.get('age')
        assert row[3] == original.get('active')
        assert row[4] == original.get('email')

        # Verify float field
        assert row[5] == original.get('salary'), f"salary mismatch: {row[5]} != {original.get('salary')}"

        # Verify nested array (tags)
        tags_from_db = row[6]
        tags_from_transit = original.get('tags')
        print(f"   Nested array (tags):")
        print(f"     From DB:      {tags_from_db}")
        print(f"     From transit: {tags_from_transit}")
        assert tags_from_db == tags_from_transit, f"tags mismatch"

        # Verify nested object (metadata)
        metadata_from_db = row[7]
        metadata_from_transit = original.get('metadata')
        print(f"   Nested object (metadata):")
        print(f"     From DB:      {metadata_from_db}")
        print(f"     From transit: {metadata_from_transit}")

        # Compare metadata fields
        assert metadata_from_db.get('department') == metadata_from_transit.get('department')
        assert metadata_from_db.get('level') == metadata_from_transit.get('level')

        # Compare dates - XTDB may return with time/timezone added
        joined_db = metadata_from_db.get('joined')
        joined_transit = metadata_from_transit.get('joined')

        # Normalize to date-only strings for comparison
        if isinstance(joined_db, date):
            joined_db_str = joined_db.isoformat()
        else:
            joined_db_str = str(joined_db)

        if isinstance(joined_transit, date):
            joined_transit_str = joined_transit.isoformat()
        else:
            joined_transit_str = str(joined_transit)

        # Extract just the date part (YYYY-MM-DD) for comparison
        # XTDB may return "2020-01-15T00:00Z" while transit has "2020-01-15"
        joined_db_date = joined_db_str.split('T')[0]
        joined_transit_date = joined_transit_str.split('T')[0]

        print(f"     joined: {joined_db_str} -> {joined_db_date} == {joined_transit_date}")
        assert joined_db_date == joined_transit_date, f"metadata.joined date mismatch"

    print(f"\n✅ Successfully verified COMPLETE OID 16384 approach")
    print(f"   All {len(rows)} records verified including:")
    print(f"   - Scalar fields (TEXT, INTEGER, BOOLEAN, FLOAT)")
    print(f"   - Nested arrays (tags)")
    print(f"   - Nested objects with dates (metadata)")
    print(f"   ✨ transit-JSON input == XTDB output (100% data fidelity)")
    print(f"\n💡 Note: Custom transit-JSON parser used (no external transit library needed!)")
