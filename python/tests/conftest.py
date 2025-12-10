import pytest
import pytest_asyncio
import psycopg as pg
import asyncio
import os
import time
import random

XTDB_HOST = os.environ.get("XTDB_HOST", "xtdb")

def _generate_table_name():
    """Generate a unique table name using timestamp and random suffix."""
    return f"test_table_{int(time.time() * 1000)}_{random.randint(0, 99999)}"

# Default DB params without transit fallback (for JSON and basic tests)
DB_PARAMS = {
    "host": XTDB_HOST,
    "port": 5432,
    "dbname": "xtdb"
}

# DB params with transit fallback (for transit-specific tests only)
DB_PARAMS_TRANSIT = {
    "host": XTDB_HOST,
    "port": 5432,
    "dbname": "xtdb",
    "options": "-c fallback_output_format=transit"
}

@pytest_asyncio.fixture
async def conn():
    """Create a database connection for testing (without transit fallback)."""
    connection = await pg.AsyncConnection.connect(**DB_PARAMS, autocommit=True)
    # Register dumpers with proper type hints for XTDB
    connection.adapters.register_dumper(str, pg.types.string.StrDumperVarchar)
    connection.adapters.register_dumper(int, pg.types.numeric.IntDumper)
    yield connection
    await connection.close()

@pytest_asyncio.fixture
async def conn_transit():
    """Create a database connection with transit fallback (for transit tests only)."""
    connection = await pg.AsyncConnection.connect(**DB_PARAMS_TRANSIT, autocommit=True)
    # Register dumpers with proper type hints for XTDB
    connection.adapters.register_dumper(str, pg.types.string.StrDumperVarchar)
    connection.adapters.register_dumper(int, pg.types.numeric.IntDumper)
    yield connection
    await connection.close()

@pytest_asyncio.fixture
async def clean_table(conn):
    """Create a clean test table."""
    table_name = _generate_table_name()
    yield table_name
    # Cleanup happens automatically in XTDB (ephemeral container)

@pytest_asyncio.fixture
async def clean_table_transit(conn_transit):
    """Create a clean test table for transit connection."""
    table_name = _generate_table_name()
    yield table_name
    # Cleanup happens automatically in XTDB (ephemeral container)

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
