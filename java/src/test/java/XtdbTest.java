import org.junit.jupiter.api.*;
import java.sql.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import com.cognitect.transit.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.*;

public class XtdbTest {

    private Connection connection;
    private static final String DB_URL = "jdbc:postgresql://xtdb:5432/xtdb";
    private static final String DB_USER = "xtdb";
    private static final String DB_PASS = "";

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

        // Insert each user
        try (Statement stmt = connection.createStatement()) {
            for (Map<String, Object> user : users) {
                String id = (String) user.get("_id");
                String name = (String) user.get("name");
                int age = (Integer) user.get("age");
                boolean active = (Boolean) user.get("active");

                stmt.execute(String.format(
                    "INSERT INTO %s RECORDS {_id: '%s', name: '%s', age: %d, active: %s}",
                    table, id, name, age, active
                ));
            }

            // Query back and verify
            try (ResultSet rs = stmt.executeQuery(
                String.format("SELECT _id, name, age, active FROM %s ORDER BY _id", table))) {

                assertTrue(rs.next());
                assertEquals("alice", rs.getString("_id"));
                assertEquals("Alice Smith", rs.getString("name"));
                assertEquals(30, rs.getInt("age"));
                assertTrue(rs.getBoolean("active"));

                int count = 1;
                while (rs.next()) {
                    count++;
                }
                assertEquals(3, count);
            }
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
    @SuppressWarnings("unchecked")
    void testParseTransitJSON() throws Exception {
        String table = getCleanTable();

        // Load sample-users-transit.json
        String transitPath = "../test-data/sample-users-transit.json";
        List<String> lines = Files.readAllLines(Paths.get(transitPath));

        try (Statement stmt = connection.createStatement()) {
            for (String line : lines) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // Parse transit-JSON
                ByteArrayInputStream in = new ByteArrayInputStream(line.getBytes());
                com.cognitect.transit.Reader reader = TransitFactory.reader(TransitFactory.Format.JSON, in);
                Map<Object, Object> userData = (Map<Object, Object>) reader.read();

                // Extract values (keywords are returned as Keyword objects)
                String id = userData.get(TransitFactory.keyword("_id")).toString();
                String name = userData.get(TransitFactory.keyword("name")).toString();
                Object ageObj = userData.get(TransitFactory.keyword("age"));
                int age = (ageObj instanceof Long) ? ((Long) ageObj).intValue() : (Integer) ageObj;
                boolean active = (Boolean) userData.get(TransitFactory.keyword("active"));

                // Insert using RECORDS syntax
                stmt.execute(String.format(
                    "INSERT INTO %s RECORDS {_id: '%s', name: '%s', age: %d, active: %s}",
                    table, id, name, age, active
                ));
            }

            // Query back and verify
            try (ResultSet rs = stmt.executeQuery(
                String.format("SELECT _id, name, age, active FROM %s ORDER BY _id", table))) {

                assertTrue(rs.next());
                assertEquals("alice", rs.getString("_id"));
                assertEquals("Alice Smith", rs.getString("name"));
                assertEquals(30, rs.getInt("age"));
                assertTrue(rs.getBoolean("active"));

                int count = 1;
                while (rs.next()) {
                    count++;
                }
                assertEquals(3, count);
            }
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
}
