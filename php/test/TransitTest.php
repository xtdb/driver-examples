<?php

use PHPUnit\Framework\TestCase;
use pq\Connection;
use pq\Result;
use Xtdb\Example\Transit;

class TransitTest extends TestCase
{
    private Connection $connection;

    protected function setUp(): void
    {
        // Connect with transit fallback output format
        $this->connection = new Connection("host=xtdb port=5432 dbname=xtdb user=xtdb password= options='fallback_output_format=transit'");
    }

    protected function tearDown(): void
    {
        unset($this->connection);
    }

    private function getCleanTable(): string
    {
        return 'test_table_' . round(microtime(true) * 1000) . '_' . rand(0, 9999);
    }

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
        $transit_json = Transit::encodeMap($data);

        // Verify it contains transit markers
        $this->assertStringContainsString('~:', $transit_json);
        $this->assertStringContainsString('"^ "', $transit_json);

        // Insert using RECORDS syntax with transit OID
        $this->connection->execParams(
            "INSERT INTO $table RECORDS \$1",
            [$transit_json],
            [16384]  // OID 16384 = transit type
        );

        $result = $this->connection->exec("SELECT _id, name, age, active FROM $table WHERE _id = 'transit1'");
        $row = $result->fetchRow(Result::FETCH_ASSOC);

        $this->assertEquals('transit1', $row['_id']);
        $this->assertEquals('Transit User', $row['name']);
        $this->assertEquals(42, $row['age']);
        $this->assertEquals('t', $row['active']);
    }

    public function testParseTransitJson(): void
    {
        $table = $this->getCleanTable();

        // Load transit-JSON file
        $transit_path = '../test-data/sample-users-transit.json';
        $lines = file($transit_path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);

        // Insert each line using transit OID (16384)
        foreach ($lines as $line) {
            $this->connection->execParams(
                "INSERT INTO $table RECORDS \$1",
                [$line],
                [16384]  // OID 16384 = transit type
            );
        }

        // Query back and verify - use Sequel for querying
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

        // Verify nested array (tags) - With transit output format, properly typed
        $tags = Transit::decode($alice['tags']);

        $this->assertIsArray($tags);
        $this->assertContains('admin', $tags);
        $this->assertContains('developer', $tags);
        $this->assertCount(2, $tags);

        // Verify nested object (metadata) - With transit output format, properly typed
        $metadata = Transit::decode($alice['metadata']);

        $this->assertIsArray($metadata);
        $this->assertEquals('Engineering', $metadata['department']);
        $this->assertEquals(5, $metadata['level']);
        $this->assertStringContainsString('2020-01-15', $metadata['joined']);
    }

    public function testTransitEncoding(): void
    {
        // Test transit encoding capabilities
        $data = [
            'string' => 'hello',
            'number' => 42,
            'bool' => true,
            'array' => [1, 2, 3]
        ];

        $transit_json = Transit::encodeMap($data);

        // Verify encoding
        $this->assertStringContainsString('hello', $transit_json);
        $this->assertStringContainsString('42', $transit_json);
        $this->assertStringContainsString('true', $transit_json);
        $this->assertStringContainsString('[1,2,3]', $transit_json);

        // Verify it can be parsed as JSON
        $parsed = json_decode($transit_json, true);
        $this->assertNotNull($parsed);
        $this->assertEquals('^ ', $parsed[0]);
    }

    public function testNestOneWithTransit(): void
    {
        $table = $this->getCleanTable();

        // Load transit-JSON file
        $transit_path = '../test-data/sample-users-transit.json';
        $lines = file($transit_path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);

        // Insert each line using transit OID (16384)
        foreach ($lines as $line) {
            $this->connection->execParams(
                "INSERT INTO $table RECORDS \$1",
                [$line],
                [16384]
            );
        }

        // Query using NEST_ONE to get entire record as a single nested object
        $result = $this->connection->execParams(
            "SELECT NEST_ONE(FROM $table WHERE _id = \$1) AS r",
            ['alice']
        );

        $row = $result->fetchRow(Result::FETCH_ASSOC);
        $this->assertNotNull($row);

        // The entire record comes back - ext-pq may have already decoded it
        $record_raw = $row['r'];

        echo "\n✅ NEST_ONE returned entire record\n";

        // Decode the transit-JSON (or use as-is if already decoded)
        $record = Transit::decode($record_raw);
        echo "   Record type: " . gettype($record) . "\n";
        echo "   Decoded record keys: " . implode(', ', array_keys($record)) . "\n";

        // With transit fallback, the entire record should be properly typed
        $this->assertIsArray($record);

        // Verify all fields are accessible as native types
        $this->assertEquals('alice', $record['_id']);
        $this->assertEquals('Alice Smith', $record['name']);
        $this->assertEquals(30, $record['age']);
        $this->assertEquals(true, $record['active']);
        $this->assertEquals('alice@example.com', $record['email']);
        $this->assertEqualsWithDelta(125000.5, $record['salary'], 0.01);

        // Nested array should be native array
        $this->assertIsArray($record['tags']);
        $this->assertContains('admin', $record['tags']);
        $this->assertContains('developer', $record['tags']);
        echo "   ✅ Nested array (tags) properly typed: [" . implode(', ', $record['tags']) . "]\n";

        // Nested object should be native array (PHP uses arrays for objects)
        $this->assertIsArray($record['metadata']);
        $this->assertEquals('Engineering', $record['metadata']['department']);
        $this->assertEquals(5, $record['metadata']['level']);

        // Verify joined date
        $joined = $record['metadata']['joined'];
        $this->assertStringContainsString('2020-01-15', $joined);
        echo "   ✅ Joined date: $joined\n";

        echo "   ✅ Nested object (metadata) properly typed\n";
        echo "\n✅ NEST_ONE with transit fallback successfully decoded entire record!\n";
        echo "   All fields accessible as native PHP types\n";
    }
}
