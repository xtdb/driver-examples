<?php

use PHPUnit\Framework\TestCase;
use pq\Connection;
use pq\Result;

class BasicTest extends TestCase
{
    private Connection $connection;

    protected function setUp(): void
    {
        $this->connection = new Connection("host=xtdb port=5432 dbname=xtdb user=xtdb password=");
    }

    protected function tearDown(): void
    {
        // Connection is auto-closed on destruction
        unset($this->connection);
    }

    private function getCleanTable(): string
    {
        return 'test_table_' . round(microtime(true) * 1000) . '_' . rand(0, 9999);
    }

    // Basic Operations Tests

    public function testConnection(): void
    {
        $result = $this->connection->exec("SELECT 1 as test");
        $row = $result->fetchRow(Result::FETCH_ASSOC);

        $this->assertEquals(1, $row['test']);
    }

    public function testInsertAndQuery(): void
    {
        $table = $this->getCleanTable();

        $insert_query = "INSERT INTO $table RECORDS {_id: 'test1', value: 'hello'}, {_id: 'test2', value: 'world'}";
        $this->connection->exec($insert_query);

        $result = $this->connection->exec("SELECT _id, value FROM $table ORDER BY _id");

        $rows = [];
        while ($row = $result->fetchRow(Result::FETCH_ASSOC)) {
            $rows[] = $row;
        }

        $this->assertCount(2, $rows);
        $this->assertEquals('test1', $rows[0]['_id']);
        $this->assertEquals('hello', $rows[0]['value']);
        $this->assertEquals('test2', $rows[1]['_id']);
        $this->assertEquals('world', $rows[1]['value']);
    }

    public function testWhereClause(): void
    {
        $table = $this->getCleanTable();

        $insert_query = "INSERT INTO $table (_id, age) VALUES (1, 25), (2, 35), (3, 45)";
        $this->connection->exec($insert_query);

        $result = $this->connection->exec("SELECT _id FROM $table WHERE age > 30 ORDER BY _id");

        $count = 0;
        while ($result->fetchRow(Result::FETCH_ASSOC)) {
            $count++;
        }

        $this->assertEquals(2, $count);
    }

    public function testCountQuery(): void
    {
        $table = $this->getCleanTable();

        $insert_query = "INSERT INTO $table RECORDS {_id: 1}, {_id: 2}, {_id: 3}";
        $this->connection->exec($insert_query);

        $result = $this->connection->exec("SELECT COUNT(*) as count FROM $table");
        $row = $result->fetchRow(Result::FETCH_ASSOC);

        $this->assertEquals(3, $row['count']);
    }

    public function testParameterizedQuery(): void
    {
        $table = $this->getCleanTable();

        $insert_query = "INSERT INTO $table RECORDS {_id: 'param1', name: 'Test User', age: 30}";
        $this->connection->exec($insert_query);

        // Use execParams with parameter
        $result = $this->connection->execParams(
            "SELECT _id, name, age FROM $table WHERE _id = \$1",
            ['param1']
        );

        $row = $result->fetchRow(Result::FETCH_ASSOC);

        $this->assertEquals('Test User', $row['name']);
        $this->assertEquals(30, $row['age']);
    }
}
