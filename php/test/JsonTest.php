<?php

use PHPUnit\Framework\TestCase;
use pq\Connection;
use pq\Result;

class JsonTest extends TestCase
{
    private Connection $connection;

    protected function setUp(): void
    {
        $this->connection = new Connection("host=xtdb port=5432 dbname=xtdb user=xtdb password=");
    }

    protected function tearDown(): void
    {
        unset($this->connection);
    }

    private function getCleanTable(): string
    {
        return 'test_table_' . round(microtime(true) * 1000) . '_' . rand(0, 9999);
    }

    // Helper to parse PostgreSQL array format: {val1,val2} to PHP array
    private function parsePgArray(string $str): array
    {
        if (!str_starts_with($str, '{') || !str_ends_with($str, '}')) {
            return [$str];
        }

        $content = substr($str, 1, -1);
        if (empty($content)) {
            return [];
        }

        // Split by comma and strip quotes from each element
        return array_map(
            fn($v) => trim($v, '"'),
            explode(',', $content)
        );
    }

    public function testJsonRecords(): void
    {
        $table = $this->getCleanTable();

        $insert_query = "INSERT INTO $table RECORDS {_id: 'user1', name: 'Alice', age: 30, active: true}";
        $this->connection->exec($insert_query);

        $result = $this->connection->exec("SELECT _id, name, age, active FROM $table WHERE _id = 'user1'");
        $row = $result->fetchRow(Result::FETCH_ASSOC);

        $this->assertEquals('user1', $row['_id']);
        $this->assertEquals('Alice', $row['name']);
        $this->assertEquals(30, $row['age']);
        $this->assertEquals('t', $row['active']); // PostgreSQL returns 't' for true
    }

    public function testLoadSampleJson(): void
    {
        $table = $this->getCleanTable();

        // Load sample-users.json
        $json_content = file_get_contents('../test-data/sample-users.json');
        $users = json_decode($json_content, true);

        // Insert each user using JSON OID (114)
        foreach ($users as $user) {
            $json_str = json_encode($user);

            // Use execParams with JSON OID to send parameter as JSON type
            $this->connection->execParams(
                "INSERT INTO $table RECORDS \$1",
                [$json_str],
                [114]  // OID 114 = JSON type
            );
        }

        // Query back and verify with ALL fields including nested data
        $result = $this->connection->exec("SELECT _id, name, age, active, email, salary, tags, metadata FROM $table ORDER BY _id");

        $rows = [];
        while ($row = $result->fetchRow(Result::FETCH_ASSOC)) {
            $rows[] = $row;
        }

        $this->assertCount(3, $rows);

        // Verify first record (alice) with all fields
        $alice = $rows[0];
        $this->assertEquals('alice', $alice['_id']);
        $this->assertEquals('Alice Smith', $alice['name']);
        $this->assertEquals(30, $alice['age']);
        $this->assertEquals('t', $alice['active']);
        $this->assertEquals('alice@example.com', $alice['email']);
        $this->assertEqualsWithDelta(125000.5, (float)$alice['salary'], 0.01);

        // Verify nested array (tags) - May come as PostgreSQL array string, parse if needed
        $tags_raw = $alice['tags'];
        $tags = is_string($tags_raw) ? $this->parsePgArray($tags_raw) : $tags_raw;

        $this->assertIsArray($tags);
        $this->assertContains('admin', $tags);
        $this->assertContains('developer', $tags);
        $this->assertCount(2, $tags);

        // Verify nested object (metadata) - May come as JSON string, parse if needed
        $metadata_raw = $alice['metadata'];
        $metadata = is_string($metadata_raw) ? json_decode($metadata_raw, true) : $metadata_raw;

        $this->assertIsArray($metadata);
        $this->assertEquals('Engineering', $metadata['department']);
        $this->assertEquals(5, $metadata['level']);
        $this->assertEquals('2020-01-15', $metadata['joined']);
    }
}
