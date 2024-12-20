"use strict";

import postgres from "postgres";

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

async function main() {
  try {
    // Insert data into the "users" table
    await sql`INSERT INTO users (_id, name) VALUES (${sql.typed.int(1)}, 'James'), (${sql.typed.int(2)}, 'Jeremy')`;

    // Fetch data from the "users" table
    const users = await sql`SELECT _id, name FROM users`;
    console.log([...users]); // => [{_id: 1, name: "James"}, {_id: 2, name: "Jeremy"}]
  } catch (err) {
    console.error("Error occurred:", err);
  } finally {
    await sql.end(); // Close the connection
  }
}

main();