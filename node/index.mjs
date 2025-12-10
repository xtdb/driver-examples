import postgres from "postgres";

const xtdbHost = process.env.XTDB_HOST || "xtdb";

const sql = postgres({
  host: xtdbHost,
  port: 5432,
  database: "xtdb",
  fetch_types: false,
});

try {
  await sql`INSERT INTO node_users RECORDS {_id: 'alice', name: 'Alice'}, {_id: 'bob', name: 'Bob'}`;

  const users = await sql`SELECT _id, name FROM node_users`;

  console.log("Users:");
  for (const user of users) {
    console.log(`  * ${user._id}: ${user.name}`);
  }

  console.log("\n✓ XTDB connection successful");
} catch (err) {
  console.error("Error:", err);
  process.exit(1);
} finally {
  await sql.end();
}
