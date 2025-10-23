# COPY FROM STDIN leaves connection stuck after pipeline completes

## Summary

When using `COPY FROM STDIN` with the `.writable()` method and `await pipeline()`, the COPY operation completes successfully on the server side (data is inserted), but subsequent queries on the same connection hang indefinitely. The connection appears to be left in an incomplete protocol state.

## Expected Behavior

After `await pipeline(stream, writable)` completes successfully, the connection should be usable for subsequent queries without hanging.

## Actual Behavior

1. `await pipeline(stream, writable)` completes successfully
2. Data is inserted on the server (confirmed via separate connection)
3. **Any subsequent query on the same connection hangs indefinitely**

## Minimal Reproduction

### Setup

```bash
npm install postgres testcontainers
```

Create `test-data.csv`:
```csv
id,name,age
1,Alice,30
2,Bob,25
3,Charlie,35
```

### Test Code

```javascript
import postgres from "postgres";
import { readFile } from "fs/promises";
import { Readable } from "stream";
import { pipeline } from "stream/promises";
import { GenericContainer } from "testcontainers";

async function testCopyHang() {
  // Start PostgreSQL container
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

  try {
    const sql = postgres({
      host,
      port,
      database: "testdb",
      username: "testuser",
      password: "testpass",
    });

    // Create table
    await sql`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL
      )
    `;

    // Perform COPY FROM STDIN
    const csvData = await readFile("test-data.csv", "utf8");
    const stream = Readable.from([csvData]);
    const writable = await sql`COPY users FROM STDIN WITH (FORMAT CSV, HEADER true)`.writable();

    console.log("⏳ Awaiting pipeline...");
    await pipeline(stream, writable);
    console.log("✅ Pipeline completed!");

    // THIS HANGS INDEFINITELY
    console.log("⏳ Querying data...");
    const results = await sql`SELECT * FROM users ORDER BY id`;
    console.log(`✅ Got ${results.length} rows`); // Never reached

    await sql.end();
  } finally {
    await container.stop();
  }
}

testCopyHang();
```

### Run the test

```bash
node test-copy-hang.mjs
```

### Expected Output

```
⏳ Awaiting pipeline...
✅ Pipeline completed!
⏳ Querying data...
✅ Got 3 rows
```

### Actual Output

```
⏳ Awaiting pipeline...
✅ Pipeline completed!
⏳ Querying data...
[HANGS INDEFINITELY - must Ctrl+C to exit]
```

## Verification: Data Was Actually Inserted

You can verify the COPY succeeded by querying from a **separate connection**:

```javascript
// Open new connection after COPY
await sql1.end(); // Close first connection
const sql2 = postgres({ /* same config */ });

// This works!
const results = await sql2`SELECT * FROM users`;
console.log(results); // Shows 3 rows

await sql2.end();
```

## Workaround

Close and reopen the connection after each COPY operation:

```javascript
// Connection 1: COPY
const sql1 = postgres(config);
const writable = await sql1`COPY table FROM STDIN`.writable();
await pipeline(stream, writable);
await sql1.end(); // ← Close immediately

// Connection 2: Query
const sql2 = postgres(config);
const results = await sql2`SELECT * FROM table`;
await sql2.end();
```

## Environment

- **postgres.js version**: 3.4.5
- **Node.js version**: v22.13.1
- **PostgreSQL version**: 16-alpine (tested via Docker)
- **OS**: Linux (also reproduced on macOS)

## Additional Notes

- The issue occurs with any format (CSV, TEXT, BINARY, custom formats like transit-json)
- The issue occurs with any data size (even single-row inserts)
- Server-side logs show the COPY completed successfully
- No error messages are produced
- The connection is not closed (no socket errors)
- Network traffic shows the connection is open but idle

## Impact

This makes `COPY FROM STDIN` effectively unusable in postgres.js unless you accept the performance penalty of reconnecting after every COPY operation (50-100ms overhead per operation).

## Related

This may be related to how postgres.js handles the COPY protocol completion sequence. The PostgreSQL COPY protocol requires specific message exchanges to properly close the COPY mode and return to normal query mode.
