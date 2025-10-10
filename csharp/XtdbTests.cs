using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace XtdbTests
{
    public class XtdbTest : IAsyncLifetime
    {
        private const string ConnectionString = "Host=xtdb;Port=5432;Database=xtdb;Username=xtdb;Password=;";
        private NpgsqlDataSource? _dataSource;

        public async Task InitializeAsync()
        {
            var builder = new NpgsqlDataSourceBuilder(ConnectionString);
            builder.ConfigureTypeLoading(sb =>
            {
                sb.EnableTypeLoading(false);
                sb.EnableTableCompositesLoading(false);
            });
            _dataSource = builder.Build();
            await Task.CompletedTask;
        }

        public async Task DisposeAsync()
        {
            if (_dataSource != null)
            {
                await _dataSource.DisposeAsync();
            }
        }

        private string GetCleanTable()
        {
            return $"test_table_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}_{Random.Shared.Next(10000)}";
        }

        private string BuildTransitJson(Dictionary<string, object> data)
        {
            var pairs = new List<string>();
            foreach (var kvp in data)
            {
                pairs.Add($"\"~:{kvp.Key}\"");
                pairs.Add(EncodeTransitValue(kvp.Value));
            }
            return $"[\"^ \",{string.Join(",", pairs)}]";
        }

        private string EncodeTransitValue(object value)
        {
            return value switch
            {
                string s => JsonSerializer.Serialize(s),
                bool b => b.ToString().ToLower(),
                int or long or double or float => value.ToString()!,
                DateTime dt => $"\"~t{dt:yyyy-MM-dd}\"",
                DateTimeOffset dto => $"\"~t{dto:yyyy-MM-ddTHH:mm:ss.fffZ}\"",
                IEnumerable<object> list => $"[{string.Join(",", list.Select(EncodeTransitValue))}]",
                Dictionary<string, object> dict => BuildTransitJson(dict),
                _ => JsonSerializer.Serialize(value?.ToString() ?? "null")
            };
        }

        // Basic Operations Tests

        [Fact]
        public async Task TestConnection()
        {
            await using var conn = await _dataSource!.OpenConnectionAsync();
            await using var cmd = new NpgsqlCommand("SELECT 1 as test", conn);
            var result = await cmd.ExecuteScalarAsync();
            Assert.Equal(1, Convert.ToInt32(result));
        }

        [Fact]
        public async Task TestInsertAndQuery()
        {
            var table = GetCleanTable();
            await using var conn = await _dataSource!.OpenConnectionAsync();

            await using (var cmd = new NpgsqlCommand(
                $"INSERT INTO {table} RECORDS {{_id: 'test1', value: 'hello'}}, {{_id: 'test2', value: 'world'}}", conn))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            await using (var cmd = new NpgsqlCommand($"SELECT _id, value FROM {table} ORDER BY _id", conn))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                Assert.True(await reader.ReadAsync());
                Assert.Equal("test1", reader.GetString(0));
                Assert.Equal("hello", reader.GetString(1));

                Assert.True(await reader.ReadAsync());
                Assert.Equal("test2", reader.GetString(0));
                Assert.Equal("world", reader.GetString(1));
            }
        }

        [Fact]
        public async Task TestWhereClause()
        {
            var table = GetCleanTable();
            await using var conn = await _dataSource!.OpenConnectionAsync();

            await using (var cmd = new NpgsqlCommand(
                $"INSERT INTO {table} (_id, age) VALUES (1, 25), (2, 35), (3, 45)", conn))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            await using (var cmd = new NpgsqlCommand($"SELECT _id FROM {table} WHERE age > 30 ORDER BY _id", conn))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                var count = 0;
                while (await reader.ReadAsync()) count++;
                Assert.Equal(2, count);
            }
        }

        [Fact]
        public async Task TestCountQuery()
        {
            var table = GetCleanTable();
            await using var conn = await _dataSource!.OpenConnectionAsync();

            await using (var cmd = new NpgsqlCommand(
                $"INSERT INTO {table} RECORDS {{_id: 1}}, {{_id: 2}}, {{_id: 3}}", conn))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            await using (var cmd = new NpgsqlCommand($"SELECT COUNT(*) as count FROM {table}", conn))
            {
                var result = await cmd.ExecuteScalarAsync();
                Assert.Equal(3L, Convert.ToInt64(result));
            }
        }

        [Fact]
        public async Task TestParameterizedQuery()
        {
            var table = GetCleanTable();
            await using var conn = await _dataSource!.OpenConnectionAsync();

            await using (var cmd = new NpgsqlCommand(
                $"INSERT INTO {table} RECORDS {{_id: 'param1', name: 'Test User', age: 30}}", conn))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            await using (var cmd = new NpgsqlCommand($"SELECT _id, name, age FROM {table} WHERE _id = @id", conn))
            {
                cmd.Parameters.AddWithValue("@id", "param1");
                await using var reader = await cmd.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync());
                Assert.Equal("Test User", reader.GetString(1));
                Assert.Equal(30, reader.GetInt32(2));
            }
        }

        // JSON Tests

        [Fact]
        public async Task TestJsonRecords()
        {
            var table = GetCleanTable();
            await using var conn = await _dataSource!.OpenConnectionAsync();

            await using (var cmd = new NpgsqlCommand(
                $"INSERT INTO {table} RECORDS {{_id: 'user1', name: 'Alice', age: 30, active: true}}", conn))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            await using (var cmd = new NpgsqlCommand(
                $"SELECT _id, name, age, active FROM {table} WHERE _id = 'user1'", conn))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                Assert.True(await reader.ReadAsync());
                Assert.Equal("user1", reader.GetString(0));
                Assert.Equal("Alice", reader.GetString(1));
                Assert.Equal(30, reader.GetInt32(2));
                Assert.True(reader.GetBoolean(3));
            }
        }

        [Fact]
        public async Task TestLoadSampleJson()
        {
            var table = GetCleanTable();
            await using var conn = await _dataSource!.OpenConnectionAsync();

            // Load sample-users.json
            var testDataPath = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", "test-data", "sample-users.json");
            var jsonContent = await System.IO.File.ReadAllTextAsync(testDataPath);
            var users = JsonSerializer.Deserialize<List<JsonElement>>(jsonContent);

            // Insert each user
            foreach (var user in users!)
            {
                var id = user.GetProperty("_id").GetString();
                var name = user.GetProperty("name").GetString();
                var age = user.GetProperty("age").GetInt32();
                var active = user.GetProperty("active").GetBoolean();

                await using var cmd = new NpgsqlCommand(
                    $"INSERT INTO {table} RECORDS {{_id: '{id}', name: '{name}', age: {age}, active: {active.ToString().ToLower()}}}", conn);
                await cmd.ExecuteNonQueryAsync();
            }

            // Query back and verify
            await using (var cmd = new NpgsqlCommand($"SELECT _id, name, age, active FROM {table} ORDER BY _id", conn))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                Assert.True(await reader.ReadAsync());
                Assert.Equal("alice", reader.GetString(0));
                Assert.Equal("Alice Smith", reader.GetString(1));
                Assert.Equal(30, reader.GetInt32(2));
                Assert.True(reader.GetBoolean(3));

                var count = 1;
                while (await reader.ReadAsync()) count++;
                Assert.Equal(3, count);
            }
        }

        // Transit-JSON Tests

        [Fact]
        public async Task TestTransitJsonFormat()
        {
            var table = GetCleanTable();
            await using var conn = await _dataSource!.OpenConnectionAsync();

            // Create transit-JSON
            var data = new Dictionary<string, object>
            {
                { "_id", "transit1" },
                { "name", "Transit User" },
                { "age", 42 },
                { "active", true }
            };
            var transitJson = BuildTransitJson(data);

            // Verify it contains transit markers
            Assert.Contains("~:", transitJson);

            // Insert using RECORDS syntax
            await using (var cmd = new NpgsqlCommand(
                $"INSERT INTO {table} RECORDS {{_id: 'transit1', name: 'Transit User', age: 42, active: true}}", conn))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            await using (var cmd = new NpgsqlCommand(
                $"SELECT _id, name, age, active FROM {table} WHERE _id = 'transit1'", conn))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                Assert.True(await reader.ReadAsync());
                Assert.Equal("transit1", reader.GetString(0));
                Assert.Equal("Transit User", reader.GetString(1));
                Assert.Equal(42, reader.GetInt32(2));
                Assert.True(reader.GetBoolean(3));
            }
        }

        [Fact]
        public async Task TestParseTransitJson()
        {
            var table = GetCleanTable();
            await using var conn = await _dataSource!.OpenConnectionAsync();

            // Load sample-users-transit.json
            var testDataPath = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", "test-data", "sample-users-transit.json");
            var lines = await System.IO.File.ReadAllLinesAsync(testDataPath);

            foreach (var line in lines.Where(l => !string.IsNullOrWhiteSpace(l)))
            {
                // Parse transit-JSON
                using var doc = JsonDocument.Parse(line);
                var root = doc.RootElement;

                // Transit format: ["^ ", "~:_id", "alice", "~:name", "Alice Smith", ...]
                var pairs = root.EnumerateArray().Skip(1).ToList();  // Skip "^ "
                var map = new Dictionary<string, JsonElement>();

                for (int i = 0; i < pairs.Count; i += 2)
                {
                    var key = pairs[i].GetString()!;
                    var value = pairs[i + 1];
                    map[key] = value;
                }

                var id = map["~:_id"].GetString();
                var name = map["~:name"].GetString();
                var age = map["~:age"].GetInt32();
                var active = map["~:active"].GetBoolean();

                await using var cmd = new NpgsqlCommand(
                    $"INSERT INTO {table} RECORDS {{_id: '{id}', name: '{name}', age: {age}, active: {active.ToString().ToLower()}}}", conn);
                await cmd.ExecuteNonQueryAsync();
            }

            // Query back and verify
            await using (var cmd = new NpgsqlCommand($"SELECT _id, name, age, active FROM {table} ORDER BY _id", conn))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                Assert.True(await reader.ReadAsync());
                Assert.Equal("alice", reader.GetString(0));
                Assert.Equal("Alice Smith", reader.GetString(1));
                Assert.Equal(30, reader.GetInt32(2));
                Assert.True(reader.GetBoolean(3));

                var count = 1;
                while (await reader.ReadAsync()) count++;
                Assert.Equal(3, count);
            }
        }

        [Fact]
        public async Task TestTransitJsonEncoding()
        {
            // Test transit encoding capabilities
            var data = new Dictionary<string, object>
            {
                { "string", "hello" },
                { "number", 42 },
                { "bool", true },
                { "array", new object[] { 1, 2, 3 } }
            };

            var transitJson = BuildTransitJson(data);

            // Verify encoding
            Assert.Contains("hello", transitJson);
            Assert.Contains("42", transitJson);
            Assert.Contains("true", transitJson);

            // Verify it can be parsed as JSON
            using var doc = JsonDocument.Parse(transitJson);
            Assert.NotNull(doc);
        }
    }
}
