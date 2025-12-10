using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using Npgsql.XtdbTransit;
using NpgsqlTypes;
using Xunit;

namespace XtdbTests
{
    public class XtdbTest : IAsyncLifetime
    {
        private static readonly string ConnectionString = GetConnectionString();
        private NpgsqlDataSource? _dataSource;

        private static string GetConnectionString()
        {
            var host = Environment.GetEnvironmentVariable("XTDB_HOST") ?? "xtdb";
            return $"Host={host};Port=5432;Database=xtdb;Username=xtdb;Password=;Server Compatibility Mode=NoTypeLoading";
        }

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

            // Insert using JSON type with single parameter per record
            // Must explicitly specify NpgsqlDbType.Json (OID 114) to avoid "expression is of type text" error
            foreach (var user in users!)
            {
                // Use GetRawText() to get the original JSON string without extra serialization
                var userJSON = user.GetRawText();

                await using var cmd = new NpgsqlCommand($"INSERT INTO {table} RECORDS @p1", conn);
                cmd.Parameters.AddWithValue("@p1", NpgsqlDbType.Json, userJSON);
                await cmd.ExecuteNonQueryAsync();
            }

            // Query back and verify with ALL fields including nested data
            await using (var cmd = new NpgsqlCommand($"SELECT _id, name, age, active, email, salary, tags, metadata FROM {table} ORDER BY _id", conn))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                Assert.True(await reader.ReadAsync());

                // Verify first record (alice) with all fields
                Assert.Equal("alice", reader.GetString(0));
                Assert.Equal("Alice Smith", reader.GetString(1));
                Assert.Equal(30, reader.GetInt32(2));
                Assert.True(reader.GetBoolean(3));
                Assert.Equal("alice@example.com", reader.GetString(4));
                Assert.Equal(125000.5, reader.GetDouble(5));

                // Verify nested array (tags) - comes as PostgreSQL array text[]
                var tags = reader.GetFieldValue<string[]>(6);
                Assert.NotNull(tags);
                Assert.Equal(2, tags!.Length);
                Assert.Contains("admin", tags);
                Assert.Contains("developer", tags);

                // Verify nested object (metadata) - comes as JSON string, needs parsing
                var metadataJson = reader.GetString(7);
                using var metadataDoc = JsonDocument.Parse(metadataJson);
                var metadata = metadataDoc.RootElement;

                Assert.Equal("Engineering", metadata.GetProperty("department").GetString());
                Assert.Equal(5, metadata.GetProperty("level").GetInt32());
                Assert.Equal("2020-01-15", metadata.GetProperty("joined").GetString());

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

        [Fact(Skip = "Transit OID 16384 not accessible via Npgsql - Npgsql validates DataTypeName before consulting custom resolvers. Blocked by https://github.com/xtdb/xtdb/issues/4421. Use JSON (OID 114) instead.")]
        public async Task TestParseTransitJson()
        {
            var table = GetCleanTable();

            // Create a transit-enabled data source using the plugin
            // Note: Connection string uses ServerCompatibilityMode=NoTypeLoading to avoid XTDB's SQL limitations
            var builder = new NpgsqlDataSourceBuilder(ConnectionString);
            Npgsql.XtdbTransit.NpgsqlXtdbTransitExtensions.UseTransit(builder);  // Enable transit support via plugin
            await using var transitDataSource = builder.Build();
            await using var conn = await transitDataSource.OpenConnectionAsync();

            // Load sample-users-transit.json
            var testDataPath = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", "test-data", "sample-users-transit.json");
            var lines = await System.IO.File.ReadAllLinesAsync(testDataPath);

            // Insert using transit OID (16384) with DataTypeName
            // XTDB registers 'transit' in pg_type, so Npgsql can discover it with type loading enabled
            foreach (var line in lines.Where(l => !string.IsNullOrWhiteSpace(l)))
            {
                await using var cmd = new NpgsqlCommand($"INSERT INTO {table} RECORDS @p1", conn);
                cmd.Parameters.Add(new NpgsqlParameter
                {
                    ParameterName = "@p1",
                    Value = line.Trim(),
                    DataTypeName = "transit"
                });
                await cmd.ExecuteNonQueryAsync();
            }

            // Query back and verify with ALL fields including nested data
            await using (var cmd = new NpgsqlCommand($"SELECT * FROM {table} ORDER BY _id", conn))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                Assert.True(await reader.ReadAsync());

                // Verify first record (alice) with all fields
                Assert.Equal("alice", reader.GetString(0));  // _id
                Assert.True(reader.GetBoolean(1));           // active
                Assert.Equal(30, reader.GetInt32(2));        // age
                Assert.Equal("alice@example.com", reader.GetString(3)); // email

                // Verify nested object (metadata) - comes back as JSON string
                var metadataJson = reader.GetString(4);
                using var metadataDoc = JsonDocument.Parse(metadataJson);
                var metadata = metadataDoc.RootElement;
                Assert.Equal("Engineering", metadata.GetProperty("department").GetString());
                Assert.Equal(5, metadata.GetProperty("level").GetInt32());

                // Verify joined date contains the expected date
                var joined = metadata.GetProperty("joined").GetString();
                Assert.Contains("2020-01-15", joined);

                Assert.Equal("Alice Smith", reader.GetString(5)); // name
                Assert.Equal(125000.5, reader.GetDouble(6));      // salary

                // Verify nested array (tags)
                var tags = reader.GetFieldValue<string[]>(7);
                Assert.Equal(2, tags.Length);
                Assert.Contains("admin", tags);
                Assert.Contains("developer", tags);

                var count = 1;
                while (await reader.ReadAsync()) count++;
                Assert.Equal(3, count);
            }

            Console.WriteLine("✅ Transit plugin successfully enabled OID 16384 support!");
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

        [Fact]
        public async Task TestJsonNestOneFullRecord()
        {
            var table = GetCleanTable();
            await using var conn = await _dataSource!.OpenConnectionAsync();

            // Load sample-users.json
            var testDataPath = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", "test-data", "sample-users.json");
            var jsonContent = await System.IO.File.ReadAllTextAsync(testDataPath);
            var users = JsonSerializer.Deserialize<List<JsonElement>>(jsonContent);

            // Insert using JSON type with single parameter per record
            foreach (var user in users!)
            {
                // Use GetRawText() to get the original JSON string without extra serialization
                var userJSON = user.GetRawText();

                await using var cmd = new NpgsqlCommand($"INSERT INTO {table} RECORDS @p1", conn);
                cmd.Parameters.AddWithValue("@p1", NpgsqlDbType.Json, userJSON);
                await cmd.ExecuteNonQueryAsync();
            }

            // Query using NEST_ONE to get entire record as a single nested object
            await using (var cmd = new NpgsqlCommand($"SELECT NEST_ONE(FROM {table} WHERE _id = 'alice') AS r", conn))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                Assert.True(await reader.ReadAsync());

                // The entire record comes back as a JSON string
                var recordJson = reader.GetString(0);
                Console.WriteLine($"\n✅ NEST_ONE returned entire record (JSON string)");
                Console.WriteLine($"   Raw record: {recordJson}");

                // Parse the JSON string
                using var doc = JsonDocument.Parse(recordJson);
                var record = doc.RootElement;

                // Verify all fields are accessible as native types
                Assert.Equal("alice", record.GetProperty("_id").GetString());
                Assert.Equal("Alice Smith", record.GetProperty("name").GetString());
                Assert.Equal(30, record.GetProperty("age").GetInt32());
                Assert.True(record.GetProperty("active").GetBoolean());
                Assert.Equal("alice@example.com", record.GetProperty("email").GetString());
                Assert.Equal(125000.5, record.GetProperty("salary").GetDouble());

                // Nested array should be accessible
                var tags = record.GetProperty("tags");
                Assert.Equal(JsonValueKind.Array, tags.ValueKind);
                var tagsList = tags.EnumerateArray().Select(e => e.GetString()).ToList();
                Assert.Equal(2, tagsList.Count);
                Assert.Contains("admin", tagsList);
                Assert.Contains("developer", tagsList);
                Console.WriteLine($"   ✅ Nested array (tags) properly typed: [{string.Join(", ", tagsList)}]");

                // Nested object should be accessible
                var metadata = record.GetProperty("metadata");
                Assert.Equal(JsonValueKind.Object, metadata.ValueKind);
                Assert.Equal("Engineering", metadata.GetProperty("department").GetString());
                Assert.Equal(5, metadata.GetProperty("level").GetInt32());

                // Verify joined date
                var joined = metadata.GetProperty("joined").GetString();
                Assert.Equal("2020-01-15", joined);
                Console.WriteLine($"   ✅ Nested object (metadata) properly typed with joined date: {joined}");

                Console.WriteLine("\n✅ NEST_ONE with JSON successfully decoded entire record!");
                Console.WriteLine("   All fields accessible as native C# types via JsonElement");
            }
        }

        [Fact]
        public void TestZzzFeatureReport()
        {
            // Report unsupported features for matrix generation. Runs last due to Zzz prefix.
            // C# Npgsql cannot access transit OID 16384 for COPY operations or parameterized queries (blocked by issue #4421)
            Console.WriteLine("XTDB_FEATURE_UNSUPPORTED: language=csharp feature=transit-json-copy reason=npgsql-cannot-access-transit-oid");
            Console.WriteLine("XTDB_FEATURE_UNSUPPORTED: language=csharp feature=transit-msgpack-copy reason=npgsql-cannot-access-transit-oid");
            Console.WriteLine("XTDB_FEATURE_UNSUPPORTED: language=csharp feature=transit-json-parameters reason=npgsql-cannot-access-transit-oid");
        }
    }
}
