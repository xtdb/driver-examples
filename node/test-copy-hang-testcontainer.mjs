import postgres from "postgres";
import { readFile } from "fs/promises";
import { Readable } from "stream";
import { pipeline } from "stream/promises";
import { GenericContainer } from "testcontainers";

console.log("=".repeat(80));
console.log("POSTGRES.JS COPY FROM STDIN BUG REPRODUCTION");
console.log("=".repeat(80));

async function testCopyHang() {
  console.log("\n1. Starting PostgreSQL container...");
  const container = await new GenericContainer("postgres:16-alpine")
    .withEnvironment({
      POSTGRES_USER: "testuser",
      POSTGRES_PASSWORD: "testpass",
      POSTGRES_DB: "testdb",
    })
    .withExposedPorts(5432)
    .start();

  const host = container.getHost();
  const port = container.getMappedPort(5432);

  console.log(`   ✅ Container started at ${host}:${port}`);

  try {
    console.log("\n2. Connecting to PostgreSQL...");
    const sql = postgres({
      host,
      port,
      database: "testdb",
      username: "testuser",
      password: "testpass",
    });

    console.log("   ✅ Connected successfully");

    console.log("\n3. Creating test table...");
    await sql`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL
      )
    `;
    console.log("   ✅ Table created");

    console.log("\n4. Reading CSV data...");
    const csvData = await readFile("test-data-simple.csv", "utf8");
    console.log(`   ✅ Loaded ${csvData.length} bytes`);
    console.log(`   CSV content:\n${csvData.split('\n').map(l => `      ${l}`).join('\n')}`);

    console.log("\n5. Performing COPY FROM STDIN...");
    const stream = Readable.from([csvData]);
    const writable = await sql`COPY users FROM STDIN WITH (FORMAT CSV, HEADER true)`.writable();

    console.log("   ⏳ Awaiting pipeline completion...");
    await pipeline(stream, writable);

    console.log("   ✅ Pipeline completed successfully!");
    console.log("   ✅ COPY operation finished on server side");

    console.log("\n6. Attempting to query the data back...");
    console.log("   ⚠️  THIS IS WHERE THE HANG OCCURS");
    console.log("   ⏳ Executing: SELECT * FROM users ORDER BY id");

    // This will hang indefinitely
    const results = await sql`SELECT * FROM users ORDER BY id`;

    console.log(`\n   ✅ Query completed! Got ${results.length} rows:`);
    results.forEach(row => {
      console.log(`      - ID ${row.id}: ${row.name}, age ${row.age}`);
    });

    console.log("\n🎉 If you see this message, the bug has been fixed!");

    await sql.end();
    console.log("\n✅ Connection closed successfully");

  } catch (error) {
    console.error("\n❌ Error occurred:", error.message);
    throw error;
  } finally {
    console.log("\n7. Stopping container...");
    await container.stop();
    console.log("   ✅ Container stopped");
  }
}

console.log("\nStarting test (will hang after 'Pipeline completed')...\n");

testCopyHang().catch(err => {
  console.error("\n❌ Test failed:", err);
  process.exit(1);
});

// Add timeout warning
setTimeout(() => {
  console.log("\n⚠️  Test has been running for 10 seconds");
  console.log("⚠️  If you see 'Pipeline completed successfully' but no query results,");
  console.log("⚠️  then the bug is confirmed - the connection is stuck.");
  console.log("⚠️  Press Ctrl+C to exit.");
}, 10000);
