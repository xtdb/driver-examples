import org.junit.jupiter.api.*
import java.sql.DriverManager
import java.sql.Connection
import java.io.File
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertNotNull
import com.cognitect.transit.TransitFactory
import com.fasterxml.jackson.databind.ObjectMapper
import org.postgresql.util.PGobject
import kotlin.random.Random

class XtdbTest {

    private lateinit var connection: Connection

    companion object {
        private val DB_URL: String = run {
            val host = System.getenv("XTDB_HOST") ?: "xtdb"
            "jdbc:xtdb://$host:5432/xtdb"
        }
        private const val DB_USER = "xtdb"
        private const val DB_PASS = ""
    }

    @BeforeEach
    fun setUp() {
        connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)
    }

    @AfterEach
    fun tearDown() {
        connection.close()
    }

    private fun getCleanTable(): String {
        return "test_table_${System.currentTimeMillis()}_${Random.nextInt(10000)}"
    }

    // Basic Operations Tests

    @Test
    fun testConnection() {
        connection.createStatement().use { stmt ->
            stmt.executeQuery("SELECT 1 as test").use { rs ->
                assertTrue(rs.next())
                assertEquals(1, rs.getInt("test"))
            }
        }
    }

    @Test
    fun testInsertAndQuery() {
        val table = getCleanTable()

        connection.createStatement().use { stmt ->
            stmt.execute(
                "INSERT INTO $table RECORDS {_id: 'test1', value: 'hello'}, {_id: 'test2', value: 'world'}"
            )

            stmt.executeQuery("SELECT _id, value FROM $table ORDER BY _id").use { rs ->
                assertTrue(rs.next())
                assertEquals("test1", rs.getString("_id"))
                assertEquals("hello", rs.getString("value"))

                assertTrue(rs.next())
                assertEquals("test2", rs.getString("_id"))
                assertEquals("world", rs.getString("value"))
            }
        }
    }

    @Test
    fun testWhereClause() {
        val table = getCleanTable()

        connection.createStatement().use { stmt ->
            stmt.execute("INSERT INTO $table (_id, age) VALUES (1, 25), (2, 35), (3, 45)")

            stmt.executeQuery("SELECT _id FROM $table WHERE age > 30 ORDER BY _id").use { rs ->
                var count = 0
                while (rs.next()) {
                    count++
                }
                assertEquals(2, count)
            }
        }
    }

    @Test
    fun testCountQuery() {
        val table = getCleanTable()

        connection.createStatement().use { stmt ->
            stmt.execute("INSERT INTO $table RECORDS {_id: 1}, {_id: 2}, {_id: 3}")

            stmt.executeQuery("SELECT COUNT(*) as count FROM $table").use { rs ->
                assertTrue(rs.next())
                assertEquals(3, rs.getLong("count"))
            }
        }
    }

    @Test
    fun testParameterizedQuery() {
        val table = getCleanTable()

        connection.createStatement().use { stmt ->
            stmt.execute(
                "INSERT INTO $table RECORDS {_id: 'param1', name: 'Test User', age: 30}"
            )

            connection.prepareStatement("SELECT _id, name, age FROM $table WHERE _id = ?").use { pstmt ->
                pstmt.setString(1, "param1")
                pstmt.executeQuery().use { rs ->
                    assertTrue(rs.next())
                    assertEquals("Test User", rs.getString("name"))
                    assertEquals(30, rs.getInt("age"))
                }
            }
        }
    }

    // JSON Tests

    @Test
    fun testJSONRecords() {
        val table = getCleanTable()

        connection.createStatement().use { stmt ->
            stmt.execute(
                "INSERT INTO $table RECORDS {_id: 'user1', name: 'Alice', age: 30, active: true}"
            )

            stmt.executeQuery("SELECT _id, name, age, active FROM $table WHERE _id = 'user1'").use { rs ->
                assertTrue(rs.next())
                assertEquals("user1", rs.getString("_id"))
                assertEquals("Alice", rs.getString("name"))
                assertEquals(30, rs.getInt("age"))
                assertTrue(rs.getBoolean("active"))
            }
        }
    }

    @Test
    fun testLoadSampleJSON() {
        val table = getCleanTable()

        // Load sample-users.json
        val mapper = ObjectMapper()
        val jsonPath = "../test-data/sample-users.json"
        val users = mapper.readValue(File(jsonPath), List::class.java) as List<Map<String, Any>>

        // Insert using JSON OID (114) with single parameter per record
        // Use PGobject to specify the type as 'json'
        connection.prepareStatement("INSERT INTO $table RECORDS ?").use { pstmt ->
            for (user in users) {
                val userJSON = mapper.writeValueAsString(user)

                val jsonObject = PGobject()
                jsonObject.type = "json"
                jsonObject.value = userJSON

                pstmt.setObject(1, jsonObject)
                pstmt.execute()
            }
        }

        // Query back and verify - get ALL columns including nested data
        connection.createStatement().use { stmt ->
            stmt.executeQuery("SELECT * FROM $table ORDER BY _id").use { rs ->
                // Verify first record (alice)
                assertTrue(rs.next())
                assertEquals("alice", rs.getString("_id"))
                assertEquals("Alice Smith", rs.getString("name"))
                assertEquals(30, rs.getInt("age"))
                assertTrue(rs.getBoolean("active"))
                assertEquals("alice@example.com", rs.getString("email"))

                // Verify salary (float field)
                assertEquals(125000.5, rs.getDouble("salary"), 0.01)

                // Verify nested array (tags)
                val tagsArray = rs.getArray("tags")
                assertNotNull(tagsArray)
                val tags = tagsArray.array as Array<*>
                assertEquals(2, tags.size)
                assertEquals("admin", tags[0])
                assertEquals("developer", tags[1])

                // Verify nested object (metadata) exists
                val metadata = rs.getObject("metadata")
                assertNotNull(metadata)
                println("✅ Alice record verified with all fields including nested data")

                // Count all records
                var count = 1
                while (rs.next()) {
                    count++
                }
                assertEquals(3, count)
            }
        }
    }

    // Transit-JSON Tests

    @Test
    fun testTransitJSONFormat() {
        val table = getCleanTable()

        // Create transit writer
        val out = ByteArrayOutputStream()
        val writer: com.cognitect.transit.Writer<Any> = TransitFactory.writer(TransitFactory.Format.JSON, out)

        // Create transit map
        val data = mapOf(
            TransitFactory.keyword("_id") to "transit1",
            TransitFactory.keyword("name") to "Transit User",
            TransitFactory.keyword("age") to 42,
            TransitFactory.keyword("active") to true
        )

        writer.write(data)
        val transitJSON = out.toString()

        // Verify it contains transit markers
        assertTrue(transitJSON.contains("~:_id"))
        assertTrue(transitJSON.contains("~:name"))

        // Insert using RECORDS syntax (JDBC doesn't easily support OID 16384)
        connection.createStatement().use { stmt ->
            stmt.execute(
                "INSERT INTO $table RECORDS {_id: 'transit1', name: 'Transit User', age: 42, active: true}"
            )

            stmt.executeQuery("SELECT _id, name, age, active FROM $table WHERE _id = 'transit1'").use { rs ->
                assertTrue(rs.next())
                assertEquals("transit1", rs.getString("_id"))
                assertEquals("Transit User", rs.getString("name"))
                assertEquals(42, rs.getInt("age"))
                assertTrue(rs.getBoolean("active"))
            }
        }
    }

    @Test
    fun testParseTransitMsgpack() {
        val table = getCleanTable()

        // Load transit-msgpack file (binary)
        val msgpackPath = "../test-data/sample-users-transit.msgpack"
        val msgpackData = File(msgpackPath).readBytes()

        // Use COPY FROM STDIN with transit-msgpack format
        val pgConn = connection.unwrap(org.postgresql.PGConnection::class.java)
        val copyManager = pgConn.copyAPI

        copyManager.copyIn(
            "COPY $table FROM STDIN WITH (FORMAT 'transit-msgpack')",
            msgpackData.inputStream()
        )

        // Query back and verify
        connection.createStatement().use { stmt ->
            stmt.executeQuery("SELECT _id, name, age FROM $table ORDER BY _id").use { rs ->
                var count = 0
                while (rs.next()) {
                    count++
                    if (count == 1) {
                        assertEquals("alice", rs.getString("_id"))
                        assertEquals("Alice Smith", rs.getString("name"))
                        assertEquals(30, rs.getInt("age"))
                    }
                }
                assertEquals(3, count)
            }
        }
    }

    @Test
    fun testTransitJsonCopyFrom() {
        val table = getCleanTable()

        // Read ../test-data/sample-users-transit.json as text
        val transitJsonPath = "../test-data/sample-users-transit.json"
        val transitJsonData = File(transitJsonPath).readText()

        // Use COPY FROM STDIN with transit-json format
        val pgConn = connection.unwrap(org.postgresql.PGConnection::class.java)
        val copyManager = pgConn.copyAPI

        copyManager.copyIn(
            "COPY $table FROM STDIN WITH (FORMAT 'transit-json')",
            transitJsonData.byteInputStream()
        )

        // Query back and verify
        connection.createStatement().use { stmt ->
            stmt.executeQuery("SELECT * FROM $table ORDER BY _id").use { rs ->
                var count = 0
                while (rs.next()) {
                    count++
                    if (count == 1) {
                        // Verify the alice record has correct fields
                        assertEquals("alice", rs.getString("_id"))
                        assertEquals("Alice Smith", rs.getString("name"))
                        assertEquals(30, rs.getInt("age"))
                        assertEquals("alice@example.com", rs.getString("email"))
                        assertTrue(rs.getBoolean("active"))
                        assertEquals(125000.5, rs.getDouble("salary"), 0.01)

                        // Verify nested array (tags)
                        val tagsArray = rs.getArray("tags")
                        assertNotNull(tagsArray)
                        val tags = tagsArray.array as Array<*>
                        assertEquals(2, tags.size)
                        assertEquals("admin", tags[0])
                        assertEquals("developer", tags[1])
                    }
                }
                // Verify 3 records are loaded
                assertEquals(3, count)
                println("Successfully loaded and verified 3 records from transit-json using COPY FROM")
            }
        }
    }

    @Test
    fun testParseTransitJSON() {
        val table = getCleanTable()

        // Load sample-users-transit.json
        val transitPath = "../test-data/sample-users-transit.json"
        val lines = File(transitPath).readLines()

        // Insert using transit OID (16384) with single parameter per record
        // Use PGobject to specify the type as 'transit'
        connection.prepareStatement("INSERT INTO $table RECORDS ?").use { pstmt ->
            for (line in lines) {
                if (line.trim().isEmpty()) continue

                val transitObject = PGobject()
                transitObject.type = "transit"
                transitObject.value = line.trim()

                pstmt.setObject(1, transitObject)
                pstmt.execute()
            }
        }

        // Query back and verify - get ALL columns including nested data
        connection.createStatement().use { stmt ->
            stmt.executeQuery("SELECT * FROM $table ORDER BY _id").use { rs ->
                // Verify first record (alice)
                assertTrue(rs.next())
                assertEquals("alice", rs.getString("_id"))
                assertEquals("Alice Smith", rs.getString("name"))
                assertEquals(30, rs.getInt("age"))
                assertTrue(rs.getBoolean("active"))
                assertEquals("alice@example.com", rs.getString("email"))

                // Verify salary (float field from transit)
                assertEquals(125000.5, rs.getDouble("salary"), 0.01)

                // Verify nested array (tags)
                val tagsArray = rs.getArray("tags")
                assertNotNull(tagsArray)
                val tags = tagsArray.array as Array<*>
                assertEquals(2, tags.size)
                assertEquals("admin", tags[0])
                assertEquals("developer", tags[1])

                // Verify nested object (metadata) exists
                val metadata = rs.getObject("metadata")
                assertNotNull(metadata)
                println("✅ Alice record verified with all transit fields including nested data")

                // Count all records
                var count = 1
                while (rs.next()) {
                    count++
                }
                assertEquals(3, count)
            }
        }
    }

    @Test
    fun testTransitJSONEncoding() {
        // Test transit-java encoding capabilities
        val out = ByteArrayOutputStream()
        val writer: com.cognitect.transit.Writer<Any> = TransitFactory.writer(TransitFactory.Format.JSON, out)

        // Create complex data structure
        val data = mapOf(
            TransitFactory.keyword("string") to "hello",
            TransitFactory.keyword("number") to 42,
            TransitFactory.keyword("bool") to true,
            TransitFactory.keyword("array") to listOf(1, 2, 3)
        )

        writer.write(data)
        val transitJSON = out.toString()

        // Verify encoding
        assertTrue(transitJSON.contains("hello"))
        assertTrue(transitJSON.contains("42"))
        assertTrue(transitJSON.contains("true"))

        // Parse it back
        val input = ByteArrayInputStream(transitJSON.toByteArray())
        val reader = TransitFactory.reader(TransitFactory.Format.JSON, input)
        @Suppress("UNCHECKED_CAST")
        val parsed = reader.read() as Map<Any, Any>

        assertEquals("hello", parsed[TransitFactory.keyword("string")])
        assertEquals(42L, parsed[TransitFactory.keyword("number")])
        assertTrue(parsed[TransitFactory.keyword("bool")] as Boolean)
    }

    @Test
    fun testNestOneFullRecord() {
        val table = getCleanTable()

        // Load sample-users-transit.json
        val transitPath = "../test-data/sample-users-transit.json"
        val lines = File(transitPath).readLines()

        // Insert using transit OID (16384) with single parameter per record
        connection.prepareStatement("INSERT INTO $table RECORDS ?").use { pstmt ->
            for (line in lines) {
                if (line.trim().isEmpty()) continue

                val transitObject = PGobject()
                transitObject.type = "transit"
                transitObject.value = line.trim()

                pstmt.setObject(1, transitObject)
                pstmt.execute()
            }
        }

        // Query using NEST_ONE to get entire record as a single nested object
        connection.prepareStatement("SELECT NEST_ONE(FROM $table WHERE _id = ?) AS r").use { pstmt ->
            pstmt.setString(1, "alice")
            pstmt.executeQuery().use { rs ->
                assertTrue(rs.next())

                // The entire record comes back as a nested object (PGobject with transit type)
                val record = rs.getObject("r")
                assertNotNull(record)
                println("\n✅ NEST_ONE returned entire record: ${record.javaClass.simpleName}")

                // NEST_ONE returns the record, but JDBC doesn't automatically parse it
                // In production, you would parse the transit-encoded result
                // For now, verify it's not null and is a valid object
                val recordStr = record.toString()
                assertTrue(recordStr.contains("alice") || recordStr.contains("Alice"))
                println("   Record contains expected data: ${recordStr.substring(0, minOf(100, recordStr.length))}...")

                println("\n✅ NEST_ONE successfully retrieved entire record!")
                println("   Note: JDBC returns the raw result; production code should parse with transit-java")
            }
        }
    }

    @Test
    fun testZzzFeatureReport() {
        // Report unsupported features for matrix generation. Runs last due to Zzz prefix.
    }
}
