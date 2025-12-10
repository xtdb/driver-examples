import { describe, it, after, beforeEach } from "node:test";
import assert from "node:assert";
import postgres from "postgres";

const xtdbHost = process.env.XTDB_HOST || "xtdb";

const sql = postgres({
  host: xtdbHost,
  port: 5432,
  database: "xtdb",
  fetch_types: false,
  types: {
    bool: { to: 16 },
    int: {
      to: 20,
      from: [23, 20],
      parse: parseInt,
    },
  },
});

let tableCounter = 0;

function getCleanTable() {
  return `test_table_${Date.now()}_${process.hrtime.bigint()}_${tableCounter++}`;
}

after(async () => {
  await sql.end();
});

describe("Basic Operations", () => {
  it("should connect to database", async () => {
    const result = await sql`SELECT 1 as test`;
    assert.strictEqual(result[0].test, 1);
  });

  it("should insert and query records", async () => {
    const table = getCleanTable();

    await sql`INSERT INTO ${sql(table)} RECORDS {_id: 'test1', value: 'hello'}, {_id: 'test2', value: 'world'}`;

    const rows = await sql`SELECT _id, value FROM ${sql(table)} ORDER BY _id`;

    assert.strictEqual(rows.length, 2);
    assert.strictEqual(rows[0]._id, "test1");
    assert.strictEqual(rows[0].value, "hello");
    assert.strictEqual(rows[1]._id, "test2");
    assert.strictEqual(rows[1].value, "world");
  });

  it("should handle WHERE clause queries", async () => {
    const table = getCleanTable();

    await sql`INSERT INTO ${sql(table)} RECORDS {_id: 'user1', name: 'Alice', age: 30}`;

    const result = await sql`SELECT _id, name, age FROM ${sql(table)} WHERE _id = ${"user1"}`;

    assert.strictEqual(result[0]._id, "user1");
    assert.strictEqual(result[0].name, "Alice");
    assert.strictEqual(result[0].age, 30);
  });

  it("should count records", async () => {
    const table = getCleanTable();

    await sql`INSERT INTO ${sql(table)} RECORDS {_id: 1}, {_id: 2}, {_id: 3}`;

    const result = await sql`SELECT COUNT(*) as count FROM ${sql(table)}`;

    assert.strictEqual(Number(result[0].count), 3);
  });

  it("should filter with WHERE clause", async () => {
    const table = getCleanTable();

    await sql`INSERT INTO ${sql(table)} (_id, age) VALUES (1, 25), (2, 35), (3, 45)`;

    const rows = await sql`SELECT _id FROM ${sql(table)} WHERE age > 30 ORDER BY _id`;

    assert.strictEqual(rows.length, 2);
    assert.strictEqual(rows[0]._id, 2);
    assert.strictEqual(rows[1]._id, 3);
  });
});
