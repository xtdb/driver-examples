import { describe, it, after } from "node:test";
import assert from "node:assert";
import postgres from "postgres";
import { readFile } from "fs/promises";

const xtdbHost = process.env.XTDB_HOST || "xtdb";

const sql = postgres({
  host: xtdbHost,
  port: 5432,
  database: "xtdb",
  fetch_types: false,
  types: {
    json: {
      to: 114,
      serialize: (v) => JSON.stringify(v),
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

describe("JSON Operations", () => {
  it("should handle records with multiple fields", async () => {
    const table = getCleanTable();

    await sql`INSERT INTO ${sql(table)} RECORDS {_id: 'user1', name: 'Alice', age: 30, active: true}`;

    const result = await sql`SELECT _id, name, age, active FROM ${sql(table)} WHERE _id = ${"user1"}`;

    assert.strictEqual(result[0]._id, "user1");
    assert.strictEqual(result[0].name, "Alice");
    // Age may be returned as string or number depending on type inference
    assert.strictEqual(Number(result[0].age), 30);
    assert.strictEqual(result[0].active, true);
  });

  it("should roundtrip sample data from sample-users.json", async () => {
    const table = getCleanTable();
    const sampleData = JSON.parse(
      await readFile("../test-data/sample-users.json", "utf8")
    );

    // Use sql.types.json() to pass entire record objects as JSON (OID 114)
    await sql`INSERT INTO ${sql(table)} RECORDS
      ${sql.types.json(sampleData[0])},
      ${sql.types.json(sampleData[1])},
      ${sql.types.json(sampleData[2])}`;

    const result = await sql`SELECT _id, name, age, active FROM ${sql(table)} ORDER BY _id`;

    assert.strictEqual(result.length, 3);
    assert.strictEqual(result[0]._id, "alice");
    assert.strictEqual(result[0].name, "Alice Smith");
    assert.strictEqual(Number(result[0].age), 30);
    assert.strictEqual(result[0].active, true);
  });
});
