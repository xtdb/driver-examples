import asyncio
import psycopg as pg
from psycopg.types.json import Json

DB_PARAMS = {
    "host": "xtdb",
    "port": 5432,
    "dbname": "xtdb"
}

async def insert_trades(conn, trades):
    query = r"""
    INSERT INTO trades (_id, name, quantity, info) VALUES (%s, %s, %s, %s)
    """.strip()

    async with conn.cursor() as cur:
        for trade in trades:
            trade_values = (trade["_id"], trade["name"], trade["quantity"], Json(trade["info"]))
            await cur.execute(query, trade_values)

async def get_trades_over(conn, quantity):
    query = """
    SELECT * FROM trades WHERE quantity > %s
    """
    async with conn.cursor() as cur:
        await cur.execute(query, (quantity,))
        return await cur.fetchall()

async def main():
    trades = [
        {"_id": 1, "name": "Trade1", "quantity": 1001, "info": {"some_nested":
                                                                ["json", 42,
                                                                 {"data": ["hello"]}]}},
        {"_id": 2, "name": "Trade2", "quantity": 15, "info": 2},
        {"_id": 3, "name": "Trade3", "quantity": 200, "info": 3},
    ]

    try:
        async with await pg.AsyncConnection.connect(**DB_PARAMS, autocommit=True) as conn:
            # register_dumper required for now https://github.com/xtdb/xtdb/issues/3589 (TODO: reopen?)
            conn.adapters.register_dumper(str, pg.types.string.StrDumperVarchar)

            await insert_trades(conn, trades)
            print("Trades inserted successfully")

            result = await get_trades_over(conn, 100)
            print(result)
    except Exception as error:
        print(f"Driver error occurred: {error}")

if __name__ == "__main__":
    asyncio.run(main())
