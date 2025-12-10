using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Drivers.Interop.FlightSql;
using Xunit;

namespace XtdbTests;

/// <summary>
/// XTDB ADBC Tests
///
/// Tests for connecting to XTDB via Arrow Flight SQL protocol using ADBC.
/// Uses the Interop driver which wraps the mature Go Flight SQL implementation.
/// Demonstrates DML operations (INSERT, UPDATE, DELETE, ERASE) and temporal queries.
/// </summary>
public class AdbcTest : IDisposable
{
    private static readonly string FlightSqlUri = GetFlightSqlUri();
    private static int _tableCounter = 0;

    private static string GetFlightSqlUri()
    {
        var host = Environment.GetEnvironmentVariable("XTDB_HOST") ?? "xtdb";
        return $"grpc://{host}:9833";
    }

    private readonly AdbcDriver _driver;
    private readonly AdbcDatabase _database;
    private readonly AdbcConnection _connection;

    public AdbcTest()
    {
        // Load the Go-based Flight SQL driver via interop
        _driver = FlightSqlDriverLoader.LoadDriver();

        var parameters = new Dictionary<string, string>
        {
            { "uri", FlightSqlUri }
        };
        _database = _driver.Open(parameters);
        // Connect() takes no parameters for the Interop driver
        _connection = _database.Connect(new Dictionary<string, string>());
    }

    public void Dispose()
    {
        _connection?.Dispose();
        _database?.Dispose();
    }

    private static string GetCleanTable()
    {
        Interlocked.Increment(ref _tableCounter);
        return $"test_adbc_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}_{_tableCounter}";
    }

    private void Cleanup(string table, params int[] ids)
    {
        foreach (var id in ids)
        {
            try
            {
                using var stmt = _connection.CreateStatement();
                stmt.SqlQuery = $"ERASE FROM {table} WHERE _id = {id}";
                stmt.ExecuteUpdate();
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    // === Connection Tests ===

    [Fact]
    public void TestConnection()
    {
        Assert.NotNull(_connection);
    }

    [Fact]
    public void TestSimpleQuery()
    {
        using var stmt = _connection.CreateStatement();
        stmt.SqlQuery = "SELECT 1 AS x, 'hello' AS greeting";

        var result = stmt.ExecuteQuery();
        Assert.NotNull(result.Stream);

        var batch = result.Stream.ReadNextRecordBatchAsync().Result;
        Assert.NotNull(batch);
        Assert.Equal(1, batch.Length);
        Assert.Equal(2, batch.ColumnCount);

        // Verify column names
        var schema = batch.Schema;
        var columnNames = new List<string>();
        foreach (var field in schema.FieldsList)
        {
            columnNames.Add(field.Name);
        }
        Assert.Contains("x", columnNames);
        Assert.Contains("greeting", columnNames);
    }

    [Fact]
    public void TestQueryWithExpressions()
    {
        using var stmt = _connection.CreateStatement();
        stmt.SqlQuery = "SELECT 2 + 2 AS sum, UPPER('hello') AS upper_greeting";

        var result = stmt.ExecuteQuery();
        var batch = result.Stream.ReadNextRecordBatchAsync().Result;

        Assert.NotNull(batch);
        Assert.Equal(1, batch.Length);
    }

    [Fact]
    public void TestSystemTables()
    {
        using var stmt = _connection.CreateStatement();
        stmt.SqlQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' LIMIT 10";

        var result = stmt.ExecuteQuery();
        Assert.NotNull(result.Stream);
    }

    // === DML Tests ===

    [Fact]
    public void TestInsertAndQuery()
    {
        var table = GetCleanTable();

        try
        {
            // INSERT using RECORDS syntax
            using (var stmt = _connection.CreateStatement())
            {
                stmt.SqlQuery = $"INSERT INTO {table} RECORDS " +
                    "{_id: 1, name: 'Widget', price: 19.99, category: 'gadgets'}, " +
                    "{_id: 2, name: 'Gizmo', price: 29.99, category: 'gadgets'}, " +
                    "{_id: 3, name: 'Thingamajig', price: 9.99, category: 'misc'}";
                stmt.ExecuteUpdate();
            }


            // Query the inserted data
            using var queryStmt = _connection.CreateStatement();
            queryStmt.SqlQuery = $"SELECT * FROM {table} ORDER BY _id";
            var result = queryStmt.ExecuteQuery();
            var batch = result.Stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            Assert.Equal(3, batch.Length);
        }
        finally
        {
            Cleanup(table, 1, 2, 3);
        }
    }

    [Fact]
    public void TestUpdate()
    {
        var table = GetCleanTable();

        try
        {
            // Insert initial data
            using (var stmt = _connection.CreateStatement())
            {
                stmt.SqlQuery = $"INSERT INTO {table} RECORDS {{_id: 1, name: 'Widget', price: 19.99}}";
                stmt.ExecuteUpdate();
            }

            // Update the price
            using (var stmt = _connection.CreateStatement())
            {
                stmt.SqlQuery = $"UPDATE {table} SET price = 24.99 WHERE _id = 1";
                stmt.ExecuteUpdate();
            }

            // Verify update
            using var queryStmt = _connection.CreateStatement();
            queryStmt.SqlQuery = $"SELECT price FROM {table} WHERE _id = 1";
            var result = queryStmt.ExecuteQuery();
            var batch = result.Stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
        }
        finally
        {
            Cleanup(table, 1);
        }
    }

    [Fact]
    public void TestDelete()
    {
        var table = GetCleanTable();

        try
        {
            // Insert data
            using (var stmt = _connection.CreateStatement())
            {
                stmt.SqlQuery = $"INSERT INTO {table} RECORDS {{_id: 1, name: 'ToDelete'}}, {{_id: 2, name: 'ToKeep'}}";
                stmt.ExecuteUpdate();
            }

            // Delete one record
            using (var stmt = _connection.CreateStatement())
            {
                stmt.SqlQuery = $"DELETE FROM {table} WHERE _id = 1";
                stmt.ExecuteUpdate();
            }

            // Verify only one record remains
            using var queryStmt = _connection.CreateStatement();
            queryStmt.SqlQuery = $"SELECT * FROM {table}";
            var result = queryStmt.ExecuteQuery();
            var batch = result.Stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
        }
        finally
        {
            Cleanup(table, 1, 2);
        }
    }

    [Fact]
    public void TestHistoricalQuery()
    {
        var table = GetCleanTable();

        try
        {
            // Insert initial data
            using (var stmt = _connection.CreateStatement())
            {
                stmt.SqlQuery = $"INSERT INTO {table} RECORDS {{_id: 1, name: 'Widget', price: 19.99}}";
                stmt.ExecuteUpdate();
            }

            // Update (creates new version)
            using (var stmt = _connection.CreateStatement())
            {
                stmt.SqlQuery = $"UPDATE {table} SET price = 24.99 WHERE _id = 1";
                stmt.ExecuteUpdate();
            }

            // Query historical data
            using var queryStmt = _connection.CreateStatement();
            queryStmt.SqlQuery = $"SELECT *, _valid_from, _valid_to FROM {table} FOR ALL VALID_TIME ORDER BY _id, _valid_from";
            var result = queryStmt.ExecuteQuery();
            var batch = result.Stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            // Should have 2 versions
            Assert.Equal(2, batch.Length);
        }
        finally
        {
            Cleanup(table, 1);
        }
    }

    [Fact]
    public void TestErase()
    {
        var table = GetCleanTable();

        try
        {
            // Insert data
            using (var stmt = _connection.CreateStatement())
            {
                stmt.SqlQuery = $"INSERT INTO {table} RECORDS {{_id: 1, name: 'ToErase'}}, {{_id: 2, name: 'ToKeep'}}";
                stmt.ExecuteUpdate();
            }

            // Update to create history
            using (var stmt = _connection.CreateStatement())
            {
                stmt.SqlQuery = $"UPDATE {table} SET name = 'UpdatedErase' WHERE _id = 1";
                stmt.ExecuteUpdate();
            }

            // Erase record 1 completely
            using (var stmt = _connection.CreateStatement())
            {
                stmt.SqlQuery = $"ERASE FROM {table} WHERE _id = 1";
                stmt.ExecuteUpdate();
            }

            // Verify erased from all history
            using var queryStmt = _connection.CreateStatement();
            queryStmt.SqlQuery = $"SELECT * FROM {table} FOR ALL VALID_TIME ORDER BY _id";
            var result = queryStmt.ExecuteQuery();
            var batch = result.Stream.ReadNextRecordBatchAsync().Result;

            Assert.NotNull(batch);
            // Only record 2 should remain
            Assert.Equal(1, batch.Length);
        }
        finally
        {
            Cleanup(table, 2);
        }
    }
}
