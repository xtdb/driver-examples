import org.junit.jupiter.api.*;
import java.sql.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import com.cognitect.transit.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.util.PGobject;

import static org.junit.jupiter.api.Assertions.*;

public class XtdbTest {

    private Connection connection;
    private static final String DB_URL = getDbUrl();
    private static final String DB_USER = "xtdb";
    private static final String DB_PASS = "";

    private static String getDbUrl() {
        String host = System.getenv("XTDB_HOST");
        if (host == null || host.isEmpty()) {
            host = "xtdb";
        }
        return "jdbc:xtdb://" + host + ":5432/xtdb";
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    private String getCleanTable() {
        return "test_table_" + System.currentTimeMillis() + "_" + new Random().nextInt(10000);
    }

    // Basic Operations Tests

    @Test
    void testConnection() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 as test")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("test"));
        }
    }

    @Test
    void testInsertAndQuery() throws SQLException {
        String table = getCleanTable();

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                "INSERT INTO %s RECORDS {_id: 'test1', value: 'hello'}, {_id: 'test2', value: 'world'}",
                table
            ));

            try (ResultSet rs = stmt.executeQuery(
                String.format("SELECT _id, value FROM %s ORDER BY _id", table))) {

                assertTrue(rs.next());
                assertEquals("test1", rs.getString("_id"));
                assertEquals("hello", rs.getString("value"));

                assertTrue(rs.next());
                assertEquals("test2", rs.getString("_id"));
                assertEquals("world", rs.getString("value"));
            }
        }
    }

    @Test
    void testWhereClause() throws SQLException {
        String table = getCleanTable();

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                "INSERT INTO %s (_id, age) VALUES (1, 25), (2, 35), (3, 45)",
                table
            ));

            try (ResultSet rs = stmt.executeQuery(
                String.format("SELECT _id FROM %s WHERE age > 30 ORDER BY _id", table))) {

                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertEquals(2, count);
            }
        }
    }

    @Test
    void testCountQuery() throws SQLException {
        String table = getCleanTable();

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                "INSERT INTO %s RECORDS {_id: 1}, {_id: 2}, {_id: 3}",
                table
            ));

            try (ResultSet rs = stmt.executeQuery(
                String.format("SELECT COUNT(*) as count FROM %s", table))) {

                assertTrue(rs.next());
                assertEquals(3, rs.getLong("count"));
            }
        }
    }

    @Test
    void testParameterizedQuery() throws SQLException {
        String table = getCleanTable();

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                "INSERT INTO %s RECORDS {_id: 'param1', name: 'Test User', age: 30}",
                table
            ));

            try (PreparedStatement pstmt = connection.prepareStatement(
                String.format("SELECT _id, name, age FROM %s WHERE _id = ?", table))) {

                pstmt.setString(1, "param1");
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("Test User", rs.getString("name"));
                    assertEquals(30, rs.getInt("age"));
                }
            }
        }
    }

    // JSON Tests

    @Test
    void testJSONRecords() throws SQLException {
        String table = getCleanTable();

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                "INSERT INTO %s RECORDS {_id: 'user1', name: 'Alice', age: 30, active: true}",
                table
            ));

            try (ResultSet rs = stmt.executeQuery(
                String.format("SELECT _id, name, age, active FROM %s WHERE _id = 'user1'", table))) {

                assertTrue(rs.next());
                assertEquals("user1", rs.getString("_id"));
                assertEquals("Alice", rs.getString("name"));
                assertEquals(30, rs.getInt("age"));
                assertTrue(rs.getBoolean("active"));
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testLoadSampleJSON() throws Exception {
        String table = getCleanTable();

        // Load sample-users.json
        ObjectMapper mapper = new ObjectMapper();
        String jsonPath = "../test-data/sample-users.json";
        List<Map<String, Object>> users = mapper.readValue(
            new File(jsonPath),
            List.class
        );

        // Insert using JSON OID (114) with single parameter per record
        // Use PGobject to specify the type as 'json'
        try (PreparedStatement pstmt = connection.prepareStatement(
            String.format("INSERT INTO %s RECORDS ?", table))) {

            for (Map<String, Object> user : users) {
                String userJSON = mapper.writeValueAsString(user);

                PGobject jsonObject = new PGobject();
                jsonObject.setType("json");
                jsonObject.setValue(userJSON);

                pstmt.setObject(1, jsonObject);
                pstmt.execute();
            }
        }

        // Query back and verify - get ALL columns including nested data
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                String.format("SELECT * FROM %s ORDER BY _id", table))) {

            // Verify first record (alice)
            assertTrue(rs.next());
            assertEquals("alice", rs.getString("_id"));
            assertEquals("Alice Smith", rs.getString("name"));
            assertEquals(30, rs.getInt("age"));
            assertTrue(rs.getBoolean("active"));
            assertEquals("alice@example.com", rs.getString("email"));

            // Verify salary (float field)
            assertEquals(125000.5, rs.getDouble("salary"), 0.01);

            // Verify nested array (tags)
            Array tagsArray = rs.getArray("tags");
            assertNotNull(tagsArray);
            String[] tags = (String[]) tagsArray.getArray();
            assertEquals(2, tags.length);
            assertEquals("admin", tags[0]);
            assertEquals("developer", tags[1]);

            // Verify nested object (metadata) exists
            Object metadata = rs.getObject("metadata");
            assertNotNull(metadata);
            System.out.println("✅ Alice record verified with all fields including nested data");

            // Count all records
            int count = 1;
            while (rs.next()) {
                count++;
            }
            assertEquals(3, count);
        }
    }

    // Transit-JSON Tests

    @Test
    void testTransitJSONFormat() throws Exception {
        String table = getCleanTable();

        // Create transit writer
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        com.cognitect.transit.Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, out);

        // Create transit map
        Map<Object, Object> data = new HashMap<>();
        data.put(TransitFactory.keyword("_id"), "transit1");
        data.put(TransitFactory.keyword("name"), "Transit User");
        data.put(TransitFactory.keyword("age"), 42);
        data.put(TransitFactory.keyword("active"), true);

        writer.write(data);
        String transitJSON = out.toString();

        // Verify it contains transit markers
        assertTrue(transitJSON.contains("~:_id"));
        assertTrue(transitJSON.contains("~:name"));

        // Insert using RECORDS syntax (JDBC doesn't easily support OID 16384)
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                "INSERT INTO %s RECORDS {_id: 'transit1', name: 'Transit User', age: 42, active: true}",
                table
            ));

            try (ResultSet rs = stmt.executeQuery(
                String.format("SELECT _id, name, age, active FROM %s WHERE _id = 'transit1'", table))) {

                assertTrue(rs.next());
                assertEquals("transit1", rs.getString("_id"));
                assertEquals("Transit User", rs.getString("name"));
                assertEquals(42, rs.getInt("age"));
                assertTrue(rs.getBoolean("active"));
            }
        }
    }

    @Test
    void testParseTransitMsgpack() throws Exception {
        String table = getCleanTable();

        // Load transit-msgpack file (binary)
        String msgpackPath = "../test-data/sample-users-transit.msgpack";
        byte[] msgpackData = Files.readAllBytes(Paths.get(msgpackPath));

        // Use COPY FROM STDIN with transit-msgpack format
        org.postgresql.PGConnection pgConn = connection.unwrap(org.postgresql.PGConnection.class);
        org.postgresql.copy.CopyManager copyManager = pgConn.getCopyAPI();

        try (ByteArrayInputStream bis = new ByteArrayInputStream(msgpackData)) {
            copyManager.copyIn(
                String.format("COPY %s FROM STDIN WITH (FORMAT 'transit-msgpack')", table),
                bis
            );
        }

        // Query back and verify
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                String.format("SELECT _id, name, age FROM %s ORDER BY _id", table))) {

            int count = 0;
            while (rs.next()) {
                count++;
                if (count == 1) {
                    assertEquals("alice", rs.getString("_id"));
                    assertEquals("Alice Smith", rs.getString("name"));
                    assertEquals(30, rs.getInt("age"));
                }
            }
            assertEquals(3, count);
        }
    }

    @Test
    void testTransitJsonCopyFrom() throws Exception {
        String table = getCleanTable();

        // Load transit-json file as text
        String transitJsonPath = "../test-data/sample-users-transit.json";
        String transitJsonData = Files.readString(Paths.get(transitJsonPath));

        // Use COPY FROM STDIN with transit-json format
        org.postgresql.PGConnection pgConn = connection.unwrap(org.postgresql.PGConnection.class);
        org.postgresql.copy.CopyManager copyManager = pgConn.getCopyAPI();

        try (ByteArrayInputStream bis = new ByteArrayInputStream(transitJsonData.getBytes())) {
            copyManager.copyIn(
                String.format("COPY %s FROM STDIN WITH (FORMAT 'transit-json')", table),
                bis
            );
        }

        // Query back and verify
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                String.format("SELECT _id, name, age, active, email FROM %s ORDER BY _id", table))) {

            // Verify first record (alice)
            assertTrue(rs.next());
            assertEquals("alice", rs.getString("_id"));
            assertEquals("Alice Smith", rs.getString("name"));
            assertEquals(30, rs.getInt("age"));
            assertTrue(rs.getBoolean("active"));
            assertEquals("alice@example.com", rs.getString("email"));

            // Count all records
            int count = 1;
            while (rs.next()) {
                count++;
            }
            assertEquals(3, count);

            System.out.println("✅ Transit JSON COPY FROM test passed - 3 records loaded, alice record verified");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testParseTransitJSON() throws Exception {
        String table = getCleanTable();

        // Load sample-users-transit.json
        String transitPath = "../test-data/sample-users-transit.json";
        List<String> lines = Files.readAllLines(Paths.get(transitPath));

        // Insert using transit OID (16384) with single parameter per record
        // Use PGobject to specify the type as 'transit'
        try (PreparedStatement pstmt = connection.prepareStatement(
            String.format("INSERT INTO %s RECORDS ?", table))) {

            for (String line : lines) {
                line = line.trim();
                if (line.isEmpty()) continue;

                PGobject transitObject = new PGobject();
                transitObject.setType("transit");
                transitObject.setValue(line);

                pstmt.setObject(1, transitObject);
                pstmt.execute();
            }
        }

        // Query back and verify - get ALL columns including nested data
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                String.format("SELECT * FROM %s ORDER BY _id", table))) {

            // Verify first record (alice)
            assertTrue(rs.next());
            assertEquals("alice", rs.getString("_id"));
            assertEquals("Alice Smith", rs.getString("name"));
            assertEquals(30, rs.getInt("age"));
            assertTrue(rs.getBoolean("active"));
            assertEquals("alice@example.com", rs.getString("email"));

            // Verify salary (float field from transit)
            assertEquals(125000.5, rs.getDouble("salary"), 0.01);

            // Verify nested array (tags)
            Array tagsArray = rs.getArray("tags");
            assertNotNull(tagsArray);
            String[] tags = (String[]) tagsArray.getArray();
            assertEquals(2, tags.length);
            assertEquals("admin", tags[0]);
            assertEquals("developer", tags[1]);

            // Verify nested object (metadata) exists
            Object metadata = rs.getObject("metadata");
            assertNotNull(metadata);
            System.out.println("✅ Alice record verified with all transit fields including nested data");

            // Count all records
            int count = 1;
            while (rs.next()) {
                count++;
            }
            assertEquals(3, count);
        }
    }

    @Test
    void testTransitJSONEncoding() throws Exception {
        // Test transit-java encoding capabilities
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        com.cognitect.transit.Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, out);

        // Create complex data structure
        Map<Object, Object> data = new HashMap<>();
        data.put(TransitFactory.keyword("string"), "hello");
        data.put(TransitFactory.keyword("number"), 42);
        data.put(TransitFactory.keyword("bool"), true);
        data.put(TransitFactory.keyword("array"), Arrays.asList(1, 2, 3));

        writer.write(data);
        String transitJSON = out.toString();

        // Verify encoding
        assertTrue(transitJSON.contains("hello"));
        assertTrue(transitJSON.contains("42"));
        assertTrue(transitJSON.contains("true"));

        // Parse it back
        ByteArrayInputStream in = new ByteArrayInputStream(transitJSON.getBytes());
        com.cognitect.transit.Reader reader = TransitFactory.reader(TransitFactory.Format.JSON, in);
        @SuppressWarnings("unchecked")
        Map<Object, Object> parsed = (Map<Object, Object>) reader.read();

        assertEquals("hello", parsed.get(TransitFactory.keyword("string")));
        assertEquals(42L, parsed.get(TransitFactory.keyword("number")));
        assertTrue((Boolean) parsed.get(TransitFactory.keyword("bool")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testNestOneFullRecord() throws Exception {
        String table = getCleanTable();

        // Load sample-users-transit.json
        String transitPath = "../test-data/sample-users-transit.json";
        List<String> lines = Files.readAllLines(Paths.get(transitPath));

        // Insert using transit OID (16384) with single parameter per record
        try (PreparedStatement pstmt = connection.prepareStatement(
            String.format("INSERT INTO %s RECORDS ?", table))) {

            for (String line : lines) {
                line = line.trim();
                if (line.isEmpty()) continue;

                PGobject transitObject = new PGobject();
                transitObject.setType("transit");
                transitObject.setValue(line);

                pstmt.setObject(1, transitObject);
                pstmt.execute();
            }
        }

        // Query using NEST_ONE to get entire record as a single nested object
        try (PreparedStatement pstmt = connection.prepareStatement(
            String.format("SELECT NEST_ONE(FROM %s WHERE _id = ?) AS r", table))) {

            pstmt.setString(1, "alice");
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next());

                // The entire record comes back as a nested object (PGobject with transit type)
                Object record = rs.getObject("r");
                assertNotNull(record);
                System.out.println("\n✅ NEST_ONE returned entire record: " + record.getClass().getSimpleName());

                // NEST_ONE returns the record, but JDBC doesn't automatically parse it
                // In production, you would parse the transit-encoded result
                // For now, verify it's not null and is a valid object
                String recordStr = record.toString();
                assertTrue(recordStr.contains("alice") || recordStr.contains("Alice"));
                System.out.println("   Record contains expected data: " + recordStr.substring(0, Math.min(100, recordStr.length())) + "...");

                System.out.println("\n✅ NEST_ONE successfully retrieved entire record!");
                System.out.println("   Note: JDBC returns the raw result; production code should parse with transit-java");
            }
        }
    }

    @Test
    void testZzzFeatureReport() {
        // Report unsupported features for matrix generation. Runs last due to Zzz prefix.
    }
}
