import { describe, it, after, before } from "node:test";
import assert from "node:assert";
import postgres from "postgres";
import transit from "transit-js";

const OID = {
  boolean: 16,
  int64: 20,
  int32: 23,
  text: 25,
  float64: 701,
  transit: 16384,
};

const transitReader = transit.reader("json");
const transitWriter = transit.writer("json");

const sql = postgres({
  host: "xtdb",
  port: 5432,
  database: "xtdb",
  fetch_types: false,

  connection: {
    // Record objects will be returned fully typed using the transit format:
    fallback_output_format: "transit",
  },

  types: {
    // Add support for the transit format:
    transit: {
      to: 16384,
      from: [16384],
      serialize: (v) => transitWriter.write(v),
      parse: (v) => transitReader.read(v),
    },

    // By default, int64 values are handled as text.
    // Reading int64 values as a number, ensuring no loss of precision:
    int64: {
      from: [20],
      parse: (x) => {
        const res = parseInt(x);
        if (!Number.isSafeInteger(res))
          throw Error(`Could not convert to number: ${x}`);
        return res;
      },
    },

    bool: { to: 16 },
    int: {
      to: 20,
      from: [23, 20],
      parse: parseInt,
    },
  },
});

after(async () => {
  await sql.end();
});

describe("Transit-JSON with RECORDS", () => {
  it("should insert using RECORDS with transit type", async () => {
    await sql`
      INSERT INTO transit_records RECORDS
        ${sql.types.transit({ _id: 1, name: "Alice", age: 30 })},
        ${sql.types.transit({ _id: 2, name: "Bob", age: 25 })}
    `;

    const result = await sql`SELECT _id, name, age FROM transit_records ORDER BY _id`;

    assert.strictEqual(result.length, 2);
    assert.strictEqual(result[0]._id, 1);
    assert.strictEqual(result[0].name, "Alice");
    assert.strictEqual(result[0].age, 30);
  });

  it("should roundtrip sample data from test-data directory", async () => {
    const { readFile } = await import("fs/promises");
    const transitData = await readFile("../test-data/sample-users-transit.json", "utf8");

    // Parse each line as transit-JSON
    const lines = transitData.trim().split('\n');
    const users = lines.map(line => transitReader.read(line));

    for (const user of users) {
      await sql`
        INSERT INTO transit_samples RECORDS
          ${sql.types.transit(user)}
      `;
    }

    const result = await sql`SELECT _id, name, age, active FROM transit_samples ORDER BY _id`;

    assert.strictEqual(result.length, 3);
    assert.strictEqual(result[0]._id, "alice");
    assert.strictEqual(result[0].name, "Alice Smith");
    assert.strictEqual(result[0].age, 30);
    assert.strictEqual(result[0].active, true);
  });

  it("should insert typed values using VALUES syntax", async () => {
    await sql`
      INSERT INTO transit_typed (_id, name, age) VALUES
      (${sql.typed(10, OID.int32)}, ${sql.typed("James", OID.text)}, ${sql.typed(35, OID.int32)}),
      (${sql.typed(20, OID.int32)}, ${sql.typed("Jeremy", OID.text)}, ${sql.typed(40, OID.int32)})
    `;

    const result = await sql`SELECT _id, name, age FROM transit_typed ORDER BY _id`;

    assert.strictEqual(result.length, 2);
    assert.strictEqual(result[0]._id, 10);
    assert.strictEqual(result[0].name, "James");
    assert.strictEqual(result[0].age, 35);
  });
});
