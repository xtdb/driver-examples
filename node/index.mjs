"use strict";

import postgres from "postgres";
import transit from "transit-js";

const writer = transit.writer("json");

const sql = postgres({
  host: "xtdb",
  port: 5432,
  database: "xtdb",
  fetch_types: false, // Required for XTDB compatibility
  types: {
    bool: { to: 16 },
    int: {
      to: 20,
      from: [23, 20], // int4, int8
      parse: parseInt,
    },
  },
});

const profile_record = {
  _id: 3,
  name: 'bob',
  age: 42,
  location: {
    city: "Berlin",
    zip: "10115"
  },
  misc: ['a', 'b', {c: ['d']}],
  last_contacted: new Date("2025-05-21T09:47:09.892Z")
};

const transit_oid = 16384;

async function main() {
  try {
    // Insert data into the "users" table
    await sql`INSERT INTO users (_id, name) VALUES (${sql.typed.int(1)}, 'James'), (${sql.typed.int(2)}, 'Jeremy')`;

    // Insert transit-js record into the "users" table
    await sql`INSERT INTO users RECORDS ${sql.typed(writer.write(profile_record), transit_oid)}`;

    // Fetch data from the "users" table
    const users = await sql`SELECT * FROM users`;
    console.dir([...users], { depth: null });
  } catch (err) {
    console.error("Error occurred:", err);
  } finally {
    await sql.end(); // Close the connection
  }
}

main();