import pytest
import psycopg as pg

DB_PARAMS = {
    "host": "xtdb",
    "port": 5432,
    "dbname": "xtdb"
}

@pytest.mark.asyncio
async def test_transaction_commit():
    """Test transaction commit."""
    conn = await pg.AsyncConnection.connect(**DB_PARAMS, autocommit=False)
    conn.adapters.register_dumper(str, pg.types.string.StrDumperVarchar)

    try:
        await conn.execute("INSERT INTO tx_test RECORDS {_id: 'tx1', value: 'committed'}")
        await conn.commit()

        cursor = await conn.execute("SELECT value FROM tx_test WHERE _id = 'tx1'")
        result = await cursor.fetchone()

        assert result is not None
        assert result[0] == "committed"
    finally:
        await conn.close()

@pytest.mark.asyncio
async def test_transaction_rollback():
    """Test transaction rollback."""
    conn = await pg.AsyncConnection.connect(**DB_PARAMS, autocommit=False)
    conn.adapters.register_dumper(str, pg.types.string.StrDumperVarchar)

    try:
        await conn.execute("INSERT INTO tx_test RECORDS {_id: 'tx2', value: 'should_rollback'}")
        await conn.rollback()

        cursor = await conn.execute("SELECT value FROM tx_test WHERE _id = 'tx2'")
        result = await cursor.fetchone()

        # Should not exist after rollback
        assert result is None
    finally:
        await conn.close()

@pytest.mark.asyncio
async def test_transaction_context_manager():
    """Test transaction using async context manager."""
    conn = await pg.AsyncConnection.connect(**DB_PARAMS, autocommit=False)
    conn.adapters.register_dumper(str, pg.types.string.StrDumperVarchar)

    try:
        async with conn.transaction():
            await conn.execute("INSERT INTO tx_test RECORDS {_id: 'tx3', value: 'context'}")

        cursor = await conn.execute("SELECT value FROM tx_test WHERE _id = 'tx3'")
        result = await cursor.fetchone()

        assert result is not None
        assert result[0] == "context"
    finally:
        await conn.close()
