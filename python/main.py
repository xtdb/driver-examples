import asyncio
import psycopg as pg

async def main():
    conn = await pg.AsyncConnection.connect(
        host="xtdb", port=5432, dbname="xtdb", autocommit=True
    )

    await conn.execute(
        "INSERT INTO python_users RECORDS {_id: 'alice', name: 'Alice'}, {_id: 'bob', name: 'Bob'}"
    )

    cursor = await conn.execute("SELECT _id, name FROM python_users")
    rows = await cursor.fetchall()

    print("Users:")
    for row in rows:
        print(f"  * {row[0]}: {row[1]}")

    await conn.close()
    print("\n✓ XTDB connection successful")

if __name__ == "__main__":
    asyncio.run(main())
