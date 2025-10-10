import pytest
import pytest_asyncio
import psycopg as pg
import asyncio

DB_PARAMS = {
    "host": "xtdb",
    "port": 5432,
    "dbname": "xtdb"
}

@pytest_asyncio.fixture
async def conn():
    """Create a database connection for testing."""
    connection = await pg.AsyncConnection.connect(**DB_PARAMS, autocommit=True)
    # Register dumpers with proper type hints for XTDB
    connection.adapters.register_dumper(str, pg.types.string.StrDumperVarchar)
    connection.adapters.register_dumper(int, pg.types.numeric.IntDumper)
    yield connection
    await connection.close()

@pytest_asyncio.fixture
async def clean_table(conn):
    """Create a clean test table."""
    table_name = f"test_table_{id(conn)}"
    yield table_name
    # Cleanup happens automatically in XTDB (ephemeral container)

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
