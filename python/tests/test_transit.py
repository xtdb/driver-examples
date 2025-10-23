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


class TransitDecoder:
    """Decode transit-JSON strings to Python objects."""

    @staticmethod
    def decode(value):
        """Decode a value that might be transit-JSON."""
        if not isinstance(value, str):
            return value

        # Try to parse as JSON first
        try:
            data = json.loads(value)
            return TransitDecoder.decode_value(data)
        except (json.JSONDecodeError, ValueError):
            return value

    @staticmethod
    def decode_value(data):
        """Recursively decode transit-JSON structures."""
        if isinstance(data, list):
            if len(data) > 0 and data[0] == "^ ":
                # Transit map: ["^ ", key1, val1, key2, val2, ...]
                return TransitDecoder.decode_map(data)
            elif len(data) == 2 and isinstance(data[0], str) and data[0].startswith('~#'):
                # Transit tagged value: ["~#tag", value]
                # For dates like ["~#time/zoned-date-time", "2020-01-15T00:00Z[UTC]"]
                # Extract just the ISO date string
                return data[1]
            else:
                # Regular array
                return [TransitDecoder.decode_value(item) for item in data]
        elif isinstance(data, str):
            if data.startswith('~:'):
                # Keyword - remove prefix
                return data[2:]
            elif data.startswith('~t'):
                # Date - remove prefix
                return data[2:]
            else:
                return data
        else:
            return data

    @staticmethod
    def decode_map(data):
        """Decode a transit map: ["^ ", k1, v1, k2, v2, ...]"""
        result = {}
        i = 1
        while i < len(data):
            key = TransitDecoder.decode_value(data[i])
            value = TransitDecoder.decode_value(data[i + 1]) if i + 1 < len(data) else None
            result[key] = value
            i += 2
        return result


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
async def test_transit_verify_with_unmarshalling(conn_transit, clean_table_transit):
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
    Uses conn_transit with fallback_output_format=transit for proper nested data typing.
    """
    import os
    import json
    from datetime import date

    table = clean_table_transit
    test_data_path = os.path.join(os.path.dirname(__file__), "../../test-data/sample-users-transit.json")

    # Register transit dumper for string type
    conn_transit.adapters.register_dumper(str, TransitDumper)

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
        await conn_transit.execute(
            f"INSERT INTO {table} RECORDS %s",
            (line,)
        )

    # Step 2: Query back ALL the data including nested fields
    cursor = await conn_transit.execute(
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

        # Verify nested array (tags) - With transit output format, properly typed
        tags_from_db = row[6]
        tags_from_transit = original.get('tags')
        print(f"   Nested array (tags):")
        print(f"     From DB:      {tags_from_db} (type: {type(tags_from_db).__name__})")
        print(f"     From transit: {tags_from_transit} (type: {type(tags_from_transit).__name__})")

        # Validate it's a proper list, not a string
        assert isinstance(tags_from_db, list), f"tags should be list, got {type(tags_from_db)}"
        assert tags_from_db == tags_from_transit, f"tags mismatch"

        # Verify nested object (metadata) - With transit output format, decode transit string
        metadata_from_db_raw = row[7]
        metadata_from_db = TransitDecoder.decode(metadata_from_db_raw)
        metadata_from_transit = original.get('metadata')
        print(f"   Nested object (metadata):")
        print(f"     From DB (raw):     {metadata_from_db_raw} (type: {type(metadata_from_db_raw).__name__})")
        print(f"     From DB (decoded): {metadata_from_db} (type: {type(metadata_from_db).__name__})")
        print(f"     From transit:      {metadata_from_transit} (type: {type(metadata_from_transit).__name__})")

        # Validate it's a proper dict after decoding
        assert isinstance(metadata_from_db, dict), f"metadata should be dict after decoding, got {type(metadata_from_db)}"

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

@pytest.mark.asyncio
async def test_transit_msgpack_parsing(conn_transit, clean_table_transit):
    """Test parsing sample-users-transit.msgpack file using COPY FROM with transit-msgpack format."""
    import os

    table = clean_table_transit
    test_data_path = os.path.join(os.path.dirname(__file__), "../../test-data/sample-users-transit.msgpack")

    # Read the msgpack file as binary data
    with open(test_data_path, 'rb') as f:
        msgpack_data = f.read()

    # Use COPY FROM STDIN with transit-msgpack format
    async with conn_transit.cursor() as cur:
        # Start a COPY operation with transit-msgpack format
        async with cur.copy(f"COPY {table} FROM STDIN WITH (FORMAT 'transit-msgpack')") as copy:
            await copy.write(msgpack_data)

    # Query back and verify - get ALL columns including nested data
    cursor = await conn_transit.execute(
        f"SELECT _id, name, age, active, email, salary, tags, metadata FROM {table} ORDER BY _id"
    )
    rows = await cursor.fetchall()

    assert len(rows) == 3

    # Verify first record (alice)
    alice_row = rows[0]
    assert alice_row[0] == "alice"
    assert alice_row[1] == "Alice Smith"
    assert alice_row[2] == 30
    assert alice_row[3] is True
    assert alice_row[4] == "alice@example.com"
    assert alice_row[5] == 125000.5

    # Verify nested array (tags) - With transit output format, properly typed
    tags_from_db = alice_row[6]
    assert isinstance(tags_from_db, list), f"tags should be list, got {type(tags_from_db)}"
    assert tags_from_db == ["admin", "developer"], f"tags mismatch"

    # Verify nested object (metadata) - With transit output format, decode transit string
    metadata_from_db_raw = alice_row[7]
    metadata_from_db = TransitDecoder.decode(metadata_from_db_raw)
    assert isinstance(metadata_from_db, dict), f"metadata should be dict after decoding"
    assert metadata_from_db.get('department') == 'Engineering'
    assert metadata_from_db.get('level') == 5

    print(f"\n✅ Successfully tested transit-msgpack with COPY FROM!")
    print(f"   All {len(rows)} records loaded and verified from msgpack binary format")

@pytest.mark.asyncio
async def test_transit_json_copy_parsing(conn_transit, clean_table_transit):
    """Test parsing sample-users-transit.json file using COPY FROM with transit-json format."""
    import os

    table = clean_table_transit
    test_data_path = os.path.join(os.path.dirname(__file__), "../../test-data/sample-users-transit.json")

    # Read the JSON file as text data (newline-delimited JSON)
    with open(test_data_path, 'r') as f:
        json_data = f.read()

    # Use COPY FROM STDIN with transit-json format
    async with conn_transit.cursor() as cur:
        # Start a COPY operation with transit-json format
        async with cur.copy(f"COPY {table} FROM STDIN WITH (FORMAT 'transit-json')") as copy:
            await copy.write(json_data.encode('utf-8'))

    # Query back and verify - get ALL columns including nested data
    cursor = await conn_transit.execute(
        f"SELECT _id, name, age, active, email, salary, tags, metadata FROM {table} ORDER BY _id"
    )
    rows = await cursor.fetchall()

    assert len(rows) == 3

    # Verify first record (alice)
    alice_row = rows[0]
    assert alice_row[0] == "alice"
    assert alice_row[1] == "Alice Smith"
    assert alice_row[2] == 30
    assert alice_row[3] is True
    assert alice_row[4] == "alice@example.com"
    assert alice_row[5] == 125000.5

    # Verify nested array (tags) - With transit output format, properly typed
    tags_from_db = alice_row[6]
    assert isinstance(tags_from_db, list), f"tags should be list, got {type(tags_from_db)}"
    assert tags_from_db == ["admin", "developer"], f"tags mismatch"

    # Verify nested object (metadata) - With transit output format, decode transit string
    metadata_from_db_raw = alice_row[7]
    metadata_from_db = TransitDecoder.decode(metadata_from_db_raw)
    assert isinstance(metadata_from_db, dict), f"metadata should be dict after decoding"
    assert metadata_from_db.get('department') == 'Engineering'
    assert metadata_from_db.get('level') == 5

    print(f"\n✅ Successfully tested transit-json with COPY FROM!")
    print(f"   All {len(rows)} records loaded and verified from JSON format")

@pytest.mark.asyncio
async def test_transit_nest_one_full_record(conn_transit, clean_table_transit):
    """
    Test NEST_ONE() with transit fallback to decode an entire record as a nested object.

    This demonstrates that transit decoding works for entire records, not just nested fields.
    """
    import os

    table = clean_table_transit
    test_data_path = os.path.join(os.path.dirname(__file__), "../../test-data/sample-users-transit.json")

    # Register transit dumper for string type
    conn_transit.adapters.register_dumper(str, TransitDumper)

    with open(test_data_path) as f:
        lines = f.readlines()

    # Insert using transit-JSON OID (16384)
    for line in lines:
        line = line.strip()
        if not line:
            continue

        await conn_transit.execute(
            f"INSERT INTO {table} RECORDS %s",
            (line,)
        )

    # Query using NEST_ONE to get entire record as a single nested object
    cursor = await conn_transit.execute(
        f"SELECT NEST_ONE(FROM {table} WHERE _id = 'alice') AS r"
    )
    result = await cursor.fetchone()

    assert result is not None, "Expected one result"

    # The entire record comes back as a transit-JSON string that needs to be decoded
    record_raw = result[0]
    print(f"\n✅ NEST_ONE returned entire record: {type(record_raw).__name__}")
    print(f"   Raw record: {record_raw}")

    # Decode the transit-JSON string
    record = TransitDecoder.decode(record_raw)
    print(f"   Decoded record: {type(record).__name__}")

    # With transit fallback, the entire record should be properly typed
    assert isinstance(record, dict), f"Expected dict after decoding, got {type(record)}"

    # Verify all fields are accessible as native types
    assert record['_id'] == 'alice'
    assert record['name'] == 'Alice Smith'
    assert record['age'] == 30
    assert record['active'] is True
    assert record['email'] == 'alice@example.com'
    assert record['salary'] == 125000.5

    # Nested array should be native list
    assert isinstance(record['tags'], list), f"tags should be list, got {type(record['tags'])}"
    assert 'admin' in record['tags']
    assert 'developer' in record['tags']
    print(f"   ✅ Nested array (tags) properly typed: {record['tags']}")

    # Nested object should be native dict
    assert isinstance(record['metadata'], dict), f"metadata should be dict, got {type(record['metadata'])}"
    assert record['metadata']['department'] == 'Engineering'
    assert record['metadata']['level'] == 5

    # Verify joined date - after transit decoding, tagged values like ["~#time/zoned-date-time", "..."]
    # are decoded to just the value string
    joined_raw = record['metadata']['joined']
    print(f"   Joined raw value: {joined_raw} (type: {type(joined_raw).__name__})")

    if isinstance(joined_raw, str):
        # The transit decoder extracts the value from ["~#time/zoned-date-time", "2020-01-15T00:00Z[UTC]"]
        # leaving us with just "2020-01-15T00:00Z[UTC]"
        # Remove the [UTC] timezone annotation and parse the ISO datetime string
        date_str = joined_raw.split('[')[0]  # Remove [UTC] suffix
        try:
            parsed_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            print(f"   ✅ Decoded joined date to datetime: {parsed_date}")

            # Verify it's the expected date
            assert parsed_date.year == 2020
            assert parsed_date.month == 1
            assert parsed_date.day == 15
            print(f"   ✅ Transit tagged date successfully decoded and verified")
        except ValueError as e:
            pytest.fail(f"Failed to parse date {date_str}: {e}")
    else:
        pytest.fail(f"Expected joined to be string, got {type(joined_raw)}: {joined_raw}")

    print(f"   ✅ Nested object (metadata) properly typed: {record['metadata']}")

    print(f"\n✅ NEST_ONE with transit fallback successfully decoded entire record!")
    print(f"   All fields accessible as native Python types")


@pytest.mark.asyncio
async def test_zzz_feature_report():
    """Report unsupported features for matrix generation. Runs last due to zzz prefix."""
    # Python supports all features - nothing to report
    pass
