import org.apache.arrow.adbc.core.*
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.ipc.ArrowReader
import org.junit.jupiter.api.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * XTDB ADBC Tests
 *
 * Tests for connecting to XTDB via Arrow Flight SQL protocol using ADBC.
 * Demonstrates DML operations (INSERT, UPDATE, DELETE, ERASE) and temporal queries.
 */
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class AdbcTest {

    private lateinit var allocator: BufferAllocator
    private lateinit var database: AdbcDatabase
    private lateinit var connection: AdbcConnection

    companion object {
        private val FLIGHT_SQL_URI: String = run {
            val host = System.getenv("XTDB_HOST") ?: "xtdb"
            "grpc+tcp://$host:9833"
        }
        private var tableCounter = 0
    }

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        database = FlightSqlDriver(allocator).open(mapOf("uri" to FLIGHT_SQL_URI))
        connection = database.connect()
    }

    @AfterEach
    fun tearDown() {
        connection.close()
        database.close()
        allocator.close()
    }

    private fun getCleanTable(): String {
        tableCounter++
        return "test_adbc_${System.currentTimeMillis()}_$tableCounter"
    }

    // === Connection Tests ===

    @Test
    fun testConnection() {
        assertNotNull(connection, "Connection should be established")
    }

    @Test
    fun testSimpleQuery() {
        connection.createStatement().use { stmt ->
            stmt.setSqlQuery("SELECT 1 AS x, 'hello' AS greeting")

            val result = stmt.executeQuery()
            result.reader.use { reader ->
                assertTrue(reader.loadNextBatch(), "Should have at least one batch")
                val root = reader.vectorSchemaRoot

                assertEquals(1, root.rowCount)
                assertEquals(2, root.fieldVectors.size)

                val columnNames = root.fieldVectors.map { it.name }
                assertTrue("x" in columnNames)
                assertTrue("greeting" in columnNames)
            }
        }
    }

    @Test
    fun testQueryWithExpressions() {
        connection.createStatement().use { stmt ->
            stmt.setSqlQuery("SELECT 2 + 2 AS sum, UPPER('hello') AS upper_greeting")

            val result = stmt.executeQuery()
            result.reader.use { reader ->
                assertTrue(reader.loadNextBatch())
                val root = reader.vectorSchemaRoot

                assertEquals(1, root.rowCount)

                for (vector in root.fieldVectors) {
                    when (vector.name) {
                        "sum" -> assertEquals(4L, (vector.getObject(0) as Number).toLong())
                        "upper_greeting" -> assertEquals("HELLO", vector.getObject(0).toString())
                    }
                }
            }
        }
    }

    @Test
    fun testSystemTables() {
        connection.createStatement().use { stmt ->
            stmt.setSqlQuery(
                "SELECT table_name FROM information_schema.tables " +
                "WHERE table_schema = 'public' LIMIT 10"
            )

            val result = stmt.executeQuery()
            result.reader.use { reader ->
                assertNotNull(reader)
            }
        }
    }

    // === DML Tests ===

    @Test
    fun testInsertAndQuery() {
        val table = getCleanTable()

        try {
            // Use single statement for both INSERT and SELECT (like Java version)
            connection.createStatement().use { stmt ->
                stmt.setSqlQuery(
                    "INSERT INTO $table RECORDS " +
                    "{_id: 1, name: 'Widget', price: 19.99, category: 'gadgets'}, " +
                    "{_id: 2, name: 'Gizmo', price: 29.99, category: 'gadgets'}, " +
                    "{_id: 3, name: 'Thingamajig', price: 9.99, category: 'misc'}"
                )
                stmt.executeUpdate()


                stmt.setSqlQuery("SELECT * FROM $table ORDER BY _id")
                val result = stmt.executeQuery()
                result.reader.use { reader ->
                    assertTrue(reader.loadNextBatch())
                    val root = reader.vectorSchemaRoot

                    assertEquals(3, root.rowCount)
                }
            }
        } finally {
            cleanup(table, 1, 2, 3)
        }
    }

    @Test
    fun testUpdate() {
        val table = getCleanTable()

        try {
            connection.createStatement().use { stmt ->
                // Insert initial data
                stmt.setSqlQuery("INSERT INTO $table RECORDS {_id: 1, name: 'Widget', price: 19.99}")
                stmt.executeUpdate()

                // Update the price
                stmt.setSqlQuery("UPDATE $table SET price = 24.99 WHERE _id = 1")
                stmt.executeUpdate()

                // Verify update
                stmt.setSqlQuery("SELECT price FROM $table WHERE _id = 1")
                val result = stmt.executeQuery()
                result.reader.use { reader ->
                    assertTrue(reader.loadNextBatch())
                    val root = reader.vectorSchemaRoot

                    assertEquals(1, root.rowCount)
                    val priceVector = root.getVector("price")
                    val price = (priceVector.getObject(0) as Number).toDouble()
                    assertEquals(24.99, price, 0.01)
                }
            }
        } finally {
            cleanup(table, 1)
        }
    }

    @Test
    fun testDelete() {
        val table = getCleanTable()

        try {
            connection.createStatement().use { stmt ->
                // Insert data
                stmt.setSqlQuery("INSERT INTO $table RECORDS {_id: 1, name: 'ToDelete'}, {_id: 2, name: 'ToKeep'}")
                stmt.executeUpdate()

                // Delete one record
                stmt.setSqlQuery("DELETE FROM $table WHERE _id = 1")
                stmt.executeUpdate()

                // Verify only one record remains
                stmt.setSqlQuery("SELECT * FROM $table")
                val result = stmt.executeQuery()
                result.reader.use { reader ->
                    assertTrue(reader.loadNextBatch())
                    val root = reader.vectorSchemaRoot

                    assertEquals(1, root.rowCount)
                }
            }
        } finally {
            cleanup(table, 1, 2)
        }
    }

    @Test
    fun testHistoricalQuery() {
        val table = getCleanTable()

        try {
            // Insert initial data
            connection.createStatement().use { stmt ->
                stmt.setSqlQuery("INSERT INTO $table RECORDS {_id: 1, name: 'Widget', price: 19.99}")
                stmt.executeUpdate()
            }

            // Update (creates new version)
            connection.createStatement().use { stmt ->
                stmt.setSqlQuery("UPDATE $table SET price = 24.99 WHERE _id = 1")
                stmt.executeUpdate()
            }

            // Query historical data with fresh statement
            connection.createStatement().use { stmt ->
                stmt.setSqlQuery(
                    "SELECT *, _valid_from, _valid_to FROM $table FOR ALL VALID_TIME ORDER BY _id, _valid_from"
                )
                val result = stmt.executeQuery()
                result.reader.use { reader ->
                    assertTrue(reader.loadNextBatch())
                    val root = reader.vectorSchemaRoot

                    // Should have 2 versions
                    assertEquals(2, root.rowCount)

                    val priceVector = root.getVector("price")
                    val price1 = (priceVector.getObject(0) as Number).toDouble()
                    val price2 = (priceVector.getObject(1) as Number).toDouble()

                    assertEquals(19.99, price1, 0.01) // Original
                    assertEquals(24.99, price2, 0.01) // Updated
                }
            }
        } finally {
            cleanup(table, 1)
        }
    }

    @Test
    fun testErase() {
        val table = getCleanTable()

        try {
            // Insert data
            connection.createStatement().use { stmt ->
                stmt.setSqlQuery("INSERT INTO $table RECORDS {_id: 1, name: 'ToErase'}, {_id: 2, name: 'ToKeep'}")
                stmt.executeUpdate()
            }

            // Update to create history
            connection.createStatement().use { stmt ->
                stmt.setSqlQuery("UPDATE $table SET name = 'UpdatedErase' WHERE _id = 1")
                stmt.executeUpdate()
            }

            // Erase record 1 completely
            connection.createStatement().use { stmt ->
                stmt.setSqlQuery("ERASE FROM $table WHERE _id = 1")
                stmt.executeUpdate()
            }

            // Verify erased from all history with fresh statement
            connection.createStatement().use { stmt ->
                stmt.setSqlQuery("SELECT * FROM $table FOR ALL VALID_TIME ORDER BY _id")
                val result = stmt.executeQuery()
                result.reader.use { reader ->
                    assertTrue(reader.loadNextBatch())
                    val root = reader.vectorSchemaRoot

                    // Only record 2 should remain
                    assertEquals(1, root.rowCount)
                }
            }
        } finally {
            cleanup(table, 2)
        }
    }

    // === Helper Methods ===

    private fun cleanup(table: String, vararg ids: Int) {
        try {
            connection.createStatement().use { stmt ->
                for (id in ids) {
                    stmt.setSqlQuery("ERASE FROM $table WHERE _id = $id")
                    try {
                        stmt.executeUpdate()
                    } catch (e: Exception) {
                        // Ignore cleanup errors
                    }
                }
            }
        } catch (e: Exception) {
            // Ignore cleanup errors
        }
    }
}
