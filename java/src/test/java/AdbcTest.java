import org.apache.arrow.adbc.core.*;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * XTDB ADBC Tests
 *
 * Tests for connecting to XTDB via Arrow Flight SQL protocol using ADBC.
 * Demonstrates DML operations (INSERT, UPDATE, DELETE, ERASE) and temporal queries.
 */
public class AdbcTest {

    private static final String FLIGHT_SQL_URI = getFlightSqlUri();

    private static String getFlightSqlUri() {
        String host = System.getenv("XTDB_HOST");
        if (host == null || host.isEmpty()) {
            host = "xtdb";
        }
        return "grpc+tcp://" + host + ":9833";
    }

    private BufferAllocator allocator;
    private AdbcDatabase database;
    private AdbcConnection connection;

    private static int tableCounter = 0;

    @BeforeEach
    void setUp() throws Exception {
        allocator = new RootAllocator();
        database = new FlightSqlDriver(allocator).open(Map.of("uri", FLIGHT_SQL_URI));
        connection = database.connect();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null) connection.close();
        if (database != null) database.close();
        if (allocator != null) allocator.close();
    }

    private String getCleanTable() {
        tableCounter++;
        return "test_adbc_" + System.currentTimeMillis() + "_" + tableCounter;
    }

    // === Connection Tests ===

    @Test
    void testConnection() {
        assertNotNull(connection, "Connection should be established");
    }

    @Test
    void testSimpleQuery() throws Exception {
        try (AdbcStatement stmt = connection.createStatement()) {
            stmt.setSqlQuery("SELECT 1 AS x, 'hello' AS greeting");

            AdbcStatement.QueryResult result = stmt.executeQuery();
            try (ArrowReader reader = result.getReader()) {
                assertTrue(reader.loadNextBatch(), "Should have at least one batch");
                VectorSchemaRoot root = reader.getVectorSchemaRoot();

                assertEquals(1, root.getRowCount());
                assertEquals(2, root.getFieldVectors().size());

                // Verify column names
                List<String> columnNames = new ArrayList<>();
                for (FieldVector vector : root.getFieldVectors()) {
                    columnNames.add(vector.getName());
                }
                assertTrue(columnNames.contains("x"));
                assertTrue(columnNames.contains("greeting"));
            }
        }
    }

    @Test
    void testQueryWithExpressions() throws Exception {
        try (AdbcStatement stmt = connection.createStatement()) {
            stmt.setSqlQuery("SELECT 2 + 2 AS sum, UPPER('hello') AS upper_greeting");

            AdbcStatement.QueryResult result = stmt.executeQuery();
            try (ArrowReader reader = result.getReader()) {
                assertTrue(reader.loadNextBatch());
                VectorSchemaRoot root = reader.getVectorSchemaRoot();

                assertEquals(1, root.getRowCount());

                // Find the sum column and verify value
                for (FieldVector vector : root.getFieldVectors()) {
                    if (vector.getName().equals("sum")) {
                        Object value = vector.getObject(0);
                        assertEquals(4L, ((Number) value).longValue());
                    }
                    if (vector.getName().equals("upper_greeting")) {
                        Object value = vector.getObject(0);
                        assertEquals("HELLO", value.toString());
                    }
                }
            }
        }
    }

    @Test
    void testSystemTables() throws Exception {
        try (AdbcStatement stmt = connection.createStatement()) {
            stmt.setSqlQuery(
                "SELECT table_name FROM information_schema.tables " +
                "WHERE table_schema = 'public' LIMIT 10"
            );

            AdbcStatement.QueryResult result = stmt.executeQuery();
            try (ArrowReader reader = result.getReader()) {
                // Should execute without error
                assertNotNull(reader);
            }
        }
    }

    // === DML Tests ===

    @Test
    void testInsertAndQuery() throws Exception {
        String table = getCleanTable();

        try (AdbcStatement stmt = connection.createStatement()) {
            // INSERT using RECORDS syntax
            stmt.setSqlQuery(
                String.format(
                    "INSERT INTO %s RECORDS " +
                    "{_id: 1, name: 'Widget', price: 19.99, category: 'gadgets'}, " +
                    "{_id: 2, name: 'Gizmo', price: 29.99, category: 'gadgets'}, " +
                    "{_id: 3, name: 'Thingamajig', price: 9.99, category: 'misc'}",
                    table
                )
            );
            stmt.executeUpdate();


            // Query the inserted data
            stmt.setSqlQuery(String.format("SELECT * FROM %s ORDER BY _id", table));
            AdbcStatement.QueryResult result = stmt.executeQuery();
            try (ArrowReader reader = result.getReader()) {
                assertTrue(reader.loadNextBatch());
                VectorSchemaRoot root = reader.getVectorSchemaRoot();

                assertEquals(3, root.getRowCount());
            }
        } finally {
            cleanup(table, 1, 2, 3);
        }
    }

    @Test
    void testUpdate() throws Exception {
        String table = getCleanTable();

        try (AdbcStatement stmt = connection.createStatement()) {
            // Insert initial data
            stmt.setSqlQuery(
                String.format("INSERT INTO %s RECORDS {_id: 1, name: 'Widget', price: 19.99}", table)
            );
            stmt.executeUpdate();

            // Update the price using RECORDS syntax (simpler for single updates)
            stmt.setSqlQuery(
                String.format("UPDATE %s SET price = 24.99 WHERE _id = 1", table)
            );
            stmt.executeUpdate();

            // Verify update
            stmt.setSqlQuery(String.format("SELECT price FROM %s WHERE _id = 1", table));
            AdbcStatement.QueryResult result = stmt.executeQuery();
            try (ArrowReader reader = result.getReader()) {
                assertTrue(reader.loadNextBatch());
                VectorSchemaRoot root = reader.getVectorSchemaRoot();

                assertEquals(1, root.getRowCount());
                FieldVector priceVector = root.getVector("price");
                double price = ((Number) priceVector.getObject(0)).doubleValue();
                assertEquals(24.99, price, 0.01);
            }
        } finally {
            cleanup(table, 1);
        }
    }

    @Test
    void testDelete() throws Exception {
        String table = getCleanTable();

        try (AdbcStatement stmt = connection.createStatement()) {
            // Insert data
            stmt.setSqlQuery(
                String.format("INSERT INTO %s RECORDS {_id: 1, name: 'ToDelete'}, {_id: 2, name: 'ToKeep'}", table)
            );
            stmt.executeUpdate();

            // Delete one record
            stmt.setSqlQuery(String.format("DELETE FROM %s WHERE _id = 1", table));
            stmt.executeUpdate();

            // Verify only one record remains
            stmt.setSqlQuery(String.format("SELECT * FROM %s", table));
            AdbcStatement.QueryResult result = stmt.executeQuery();
            try (ArrowReader reader = result.getReader()) {
                assertTrue(reader.loadNextBatch());
                VectorSchemaRoot root = reader.getVectorSchemaRoot();

                assertEquals(1, root.getRowCount());
            }
        } finally {
            cleanup(table, 1, 2);
        }
    }

    @Test
    void testHistoricalQuery() throws Exception {
        String table = getCleanTable();

        try (AdbcStatement stmt = connection.createStatement()) {
            // Insert initial data
            stmt.setSqlQuery(
                String.format("INSERT INTO %s RECORDS {_id: 1, name: 'Widget', price: 19.99}", table)
            );
            stmt.executeUpdate();

            // Update (creates new version)
            stmt.setSqlQuery(
                String.format("UPDATE %s SET price = 24.99 WHERE _id = 1", table)
            );
            stmt.executeUpdate();

            // Query historical data
            stmt.setSqlQuery(
                String.format(
                    "SELECT *, _valid_from, _valid_to FROM %s FOR ALL VALID_TIME ORDER BY _id, _valid_from",
                    table
                )
            );
            AdbcStatement.QueryResult result = stmt.executeQuery();
            try (ArrowReader reader = result.getReader()) {
                assertTrue(reader.loadNextBatch());
                VectorSchemaRoot root = reader.getVectorSchemaRoot();

                // Should have 2 versions
                assertEquals(2, root.getRowCount());

                FieldVector priceVector = root.getVector("price");
                double price1 = ((Number) priceVector.getObject(0)).doubleValue();
                double price2 = ((Number) priceVector.getObject(1)).doubleValue();

                assertEquals(19.99, price1, 0.01); // Original
                assertEquals(24.99, price2, 0.01); // Updated
            }
        } finally {
            cleanup(table, 1);
        }
    }

    @Test
    void testErase() throws Exception {
        String table = getCleanTable();

        try (AdbcStatement stmt = connection.createStatement()) {
            // Insert data
            stmt.setSqlQuery(
                String.format("INSERT INTO %s RECORDS {_id: 1, name: 'ToErase'}, {_id: 2, name: 'ToKeep'}", table)
            );
            stmt.executeUpdate();

            // Update to create history
            stmt.setSqlQuery(
                String.format("UPDATE %s SET name = 'UpdatedErase' WHERE _id = 1", table)
            );
            stmt.executeUpdate();

            // Erase record 1 completely
            stmt.setSqlQuery(String.format("ERASE FROM %s WHERE _id = 1", table));
            stmt.executeUpdate();

            // Verify erased from all history
            stmt.setSqlQuery(
                String.format("SELECT * FROM %s FOR ALL VALID_TIME ORDER BY _id", table)
            );
            AdbcStatement.QueryResult result = stmt.executeQuery();
            try (ArrowReader reader = result.getReader()) {
                assertTrue(reader.loadNextBatch());
                VectorSchemaRoot root = reader.getVectorSchemaRoot();

                // Only record 2 should remain
                assertEquals(1, root.getRowCount());
            }
        } finally {
            cleanup(table, 2);
        }
    }

    // === Helper Methods ===

    private void cleanup(String table, int... ids) {
        try (AdbcStatement stmt = connection.createStatement()) {
            for (int id : ids) {
                stmt.setSqlQuery(String.format("ERASE FROM %s WHERE _id = %d", table, id));
                try {
                    stmt.executeUpdate();
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }
}
