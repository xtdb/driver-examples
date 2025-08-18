using System;
using System.Threading.Tasks;
using Npgsql;

class XtdbOdbcTest
{
    static async Task Main()
    {
        var connectionString = "Host=xtdb;Port=5432;Database=xtdb;Username=xtdb;Password=xtdb;";

        var dataSourceBuilder = new NpgsqlDataSourceBuilder(connectionString);
        
        // Required for XTDB support
        dataSourceBuilder.ConfigureTypeLoading(sb => {
            sb.EnableTypeLoading(false);
            sb.EnableTableCompositesLoading(false);
        });

        var dataSource = dataSourceBuilder.Build();
        var connection = await dataSource.OpenConnectionAsync();

        await using (var insertCommand = connection.CreateCommand())
        {
            insertCommand.Parameters.Add(new NpgsqlParameter()).Value = "baz";

            insertCommand.CommandText = "INSERT INTO foo (_id, bar) VALUES (1, ?)";
            await insertCommand.ExecuteNonQueryAsync();
        }
        
        await using (var queryCommand = connection.CreateCommand())
        {
            queryCommand.CommandText = """
                                       XTQL $$
                                         (from :foo [bar])
                                       $$
                                       """;
            var value = await queryCommand.ExecuteScalarAsync();
            
            Console.WriteLine($"foo:bar = {value}; ");
        }

        await using (var queryCommand = connection.CreateCommand())
        {
            queryCommand.CommandText = "SELECT xt.version() AS v, * FROM (XTQL $XT$ (from :foo [*]) $XT$) q";
            await using var reader = await queryCommand.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var name = reader.GetName(i);
                    var value = await reader.IsDBNullAsync(i) ? "NULL" : await reader.GetFieldValueAsync<object>(i);
                    Console.Write($"{name} = {value}; ");
                }
                Console.WriteLine();
            }
        }
    }
}
