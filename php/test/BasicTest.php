<?php

use PHPUnit\Framework\TestCase;

class BasicTest extends TestCase
{
    private $connection;

    protected function setUp(): void
    {
        $connection_string = "host=xtdb port=5432 dbname=xtdb user=xtdb password=";
        $this->connection = pg_connect($connection_string);

        if (!$this->connection) {
            $this->fail("Connection failed: " . pg_last_error());
        }
    }

    protected function tearDown(): void
    {
        if ($this->connection) {
            pg_close($this->connection);
        }
    }

    private function getCleanTable(): string
    {
        return 'test_table_' . round(microtime(true) * 1000) . '_' . rand(0, 9999);
    }

    private function buildTransitJson(array $data): string
    {
        $pairs = [];
        foreach ($data as $key => $value) {
            $pairs[] = '"~:' . $key . '"';
            $pairs[] = $this->encodeTransitValue($value);
        }
        return '["^ ",' . implode(',', $pairs) . ']';
    }

    private function encodeTransitValue($value): string
    {
        if (is_string($value)) {
            return json_encode($value);
        } elseif (is_bool($value)) {
            return $value ? 'true' : 'false';
        } elseif (is_numeric($value)) {
            return (string)$value;
        } elseif (is_array($value)) {
            if (array_keys($value) === range(0, count($value) - 1)) {
                // Indexed array
                $encoded = array_map([$this, 'encodeTransitValue'], $value);
                return '[' . implode(',', $encoded) . ']';
            } else {
                // Associative array (map)
                return $this->buildTransitJson($value);
            }
        }
        return json_encode((string)$value);
    }

    // Basic Operations Tests

    public function testConnection(): void
    {
        $result = pg_query($this->connection, "SELECT 1 as test");
        $this->assertNotFalse($result);

        $row = pg_fetch_assoc($result);
        $this->assertEquals(1, $row['test']);

        pg_free_result($result);
    }

    public function testInsertAndQuery(): void
    {
        $table = $this->getCleanTable();

        $insert_query = "INSERT INTO $table RECORDS {_id: 'test1', value: 'hello'}, {_id: 'test2', value: 'world'}";
        $result = pg_query($this->connection, $insert_query);
        $this->assertNotFalse($result);

        $select_query = "SELECT _id, value FROM $table ORDER BY _id";
        $result = pg_query($this->connection, $select_query);
        $this->assertNotFalse($result);

        $rows = pg_fetch_all($result);
        $this->assertCount(2, $rows);
        $this->assertEquals('test1', $rows[0]['_id']);
        $this->assertEquals('hello', $rows[0]['value']);
        $this->assertEquals('test2', $rows[1]['_id']);
        $this->assertEquals('world', $rows[1]['value']);

        pg_free_result($result);
    }

    public function testWhereClause(): void
    {
        $table = $this->getCleanTable();

        $insert_query = "INSERT INTO $table (_id, age) VALUES (1, 25), (2, 35), (3, 45)";
        $result = pg_query($this->connection, $insert_query);
        $this->assertNotFalse($result);

        $select_query = "SELECT _id FROM $table WHERE age > 30 ORDER BY _id";
        $result = pg_query($this->connection, $select_query);
        $this->assertNotFalse($result);

        $count = pg_num_rows($result);
        $this->assertEquals(2, $count);

        pg_free_result($result);
    }

    public function testCountQuery(): void
    {
        $table = $this->getCleanTable();

        $insert_query = "INSERT INTO $table RECORDS {_id: 1}, {_id: 2}, {_id: 3}";
        $result = pg_query($this->connection, $insert_query);
        $this->assertNotFalse($result);

        $select_query = "SELECT COUNT(*) as count FROM $table";
        $result = pg_query($this->connection, $select_query);
        $this->assertNotFalse($result);

        $row = pg_fetch_assoc($result);
        $this->assertEquals(3, $row['count']);

        pg_free_result($result);
    }

    public function testParameterizedQuery(): void
    {
        $table = $this->getCleanTable();

        $insert_query = "INSERT INTO $table RECORDS {_id: 'param1', name: 'Test User', age: 30}";
        $result = pg_query($this->connection, $insert_query);
        $this->assertNotFalse($result);

        $select_query = "SELECT _id, name, age FROM $table WHERE _id = $1";
        $result = pg_query_params($this->connection, $select_query, ['param1']);
        $this->assertNotFalse($result);

        $row = pg_fetch_assoc($result);
        $this->assertEquals('Test User', $row['name']);
        $this->assertEquals(30, $row['age']);

        pg_free_result($result);
    }

    // JSON Tests

    public function testJsonRecords(): void
    {
        $table = $this->getCleanTable();

        $insert_query = "INSERT INTO $table RECORDS {_id: 'user1', name: 'Alice', age: 30, active: true}";
        $result = pg_query($this->connection, $insert_query);
        $this->assertNotFalse($result);

        $select_query = "SELECT _id, name, age, active FROM $table WHERE _id = 'user1'";
        $result = pg_query($this->connection, $select_query);
        $this->assertNotFalse($result);

        $row = pg_fetch_assoc($result);
        $this->assertEquals('user1', $row['_id']);
        $this->assertEquals('Alice', $row['name']);
        $this->assertEquals(30, $row['age']);
        $this->assertEquals('t', $row['active']); // PostgreSQL returns 't' for true

        pg_free_result($result);
    }

    public function testLoadSampleJson(): void
    {
        $table = $this->getCleanTable();

        // Load sample-users.json
        $json_content = file_get_contents('../test-data/sample-users.json');
        $users = json_decode($json_content, true);

        // Insert each user
        foreach ($users as $user) {
            $active = $user['active'] ? 'true' : 'false';
            $insert_query = "INSERT INTO $table RECORDS {_id: '{$user['_id']}', name: '{$user['name']}', age: {$user['age']}, active: {$active}}";
            $result = pg_query($this->connection, $insert_query);
            $this->assertNotFalse($result);
        }

        // Query back and verify
        $select_query = "SELECT _id, name, age, active FROM $table ORDER BY _id";
        $result = pg_query($this->connection, $select_query);
        $this->assertNotFalse($result);

        $rows = pg_fetch_all($result);
        $this->assertCount(3, $rows);
        $this->assertEquals('alice', $rows[0]['_id']);
        $this->assertEquals('Alice Smith', $rows[0]['name']);
        $this->assertEquals(30, $rows[0]['age']);
        $this->assertEquals('t', $rows[0]['active']);

        pg_free_result($result);
    }

    // Transit-JSON Tests

    public function testTransitJsonFormat(): void
    {
        $table = $this->getCleanTable();

        // Create transit-JSON
        $data = [
            '_id' => 'transit1',
            'name' => 'Transit User',
            'age' => 42,
            'active' => true
        ];
        $transit_json = $this->buildTransitJson($data);

        // Verify it contains transit markers
        $this->assertStringContainsString('~:', $transit_json);

        // Insert using RECORDS syntax
        $insert_query = "INSERT INTO $table RECORDS {_id: 'transit1', name: 'Transit User', age: 42, active: true}";
        $result = pg_query($this->connection, $insert_query);
        $this->assertNotFalse($result);

        $select_query = "SELECT _id, name, age, active FROM $table WHERE _id = 'transit1'";
        $result = pg_query($this->connection, $select_query);
        $this->assertNotFalse($result);

        $row = pg_fetch_assoc($result);
        $this->assertEquals('transit1', $row['_id']);
        $this->assertEquals('Transit User', $row['name']);
        $this->assertEquals(42, $row['age']);
        $this->assertEquals('t', $row['active']);

        pg_free_result($result);
    }

    public function testParseTransitJson(): void
    {
        $table = $this->getCleanTable();

        // Load sample-users-transit.json
        $content = file_get_contents('../test-data/sample-users-transit.json');
        $lines = explode("\n", trim($content));

        foreach ($lines as $line) {
            if (empty(trim($line))) continue;

            // Parse transit-JSON
            $user_data = json_decode($line, true);

            // Transit format: ["^ ", "~:_id", "alice", "~:name", "Alice Smith", ...]
            array_shift($user_data); // Remove "^ "
            $map = [];
            for ($i = 0; $i < count($user_data); $i += 2) {
                $key = $user_data[$i];
                $value = $user_data[$i + 1];
                $map[$key] = $value;
            }

            $id = $map['~:_id'];
            $name = $map['~:name'];
            $age = $map['~:age'];
            $active = $map['~:active'] ? 'true' : 'false';

            $insert_query = "INSERT INTO $table RECORDS {_id: '$id', name: '$name', age: $age, active: $active}";
            $result = pg_query($this->connection, $insert_query);
            $this->assertNotFalse($result);
        }

        // Query back and verify
        $select_query = "SELECT _id, name, age, active FROM $table ORDER BY _id";
        $result = pg_query($this->connection, $select_query);
        $this->assertNotFalse($result);

        $rows = pg_fetch_all($result);
        $this->assertCount(3, $rows);
        $this->assertEquals('alice', $rows[0]['_id']);
        $this->assertEquals('Alice Smith', $rows[0]['name']);
        $this->assertEquals(30, $rows[0]['age']);
        $this->assertEquals('t', $rows[0]['active']);

        pg_free_result($result);
    }

    public function testTransitJsonEncoding(): void
    {
        // Test transit encoding capabilities
        $data = [
            'string' => 'hello',
            'number' => 42,
            'bool' => true,
            'array' => [1, 2, 3]
        ];

        $transit_json = $this->buildTransitJson($data);

        // Verify encoding
        $this->assertStringContainsString('hello', $transit_json);
        $this->assertStringContainsString('42', $transit_json);
        $this->assertStringContainsString('true', $transit_json);

        // Verify it can be parsed as JSON
        $parsed = json_decode($transit_json);
        $this->assertNotNull($parsed);
    }
}
