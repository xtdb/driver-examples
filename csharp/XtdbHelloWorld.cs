using System;
using System.Data.Odbc;

class XtdbOdbcTest
{
    static void Main()
    {
        var connectionString = "Driver={PostgreSQL Unicode};Server=xtdb;Port=5432;Database=xtdb;Uid=xtdb;Pwd=xtdb;";
        using var connection = new OdbcConnection(connectionString);
        connection.Open();

        using (var insertCommand = connection.CreateCommand())
        {
            insertCommand.CommandText = "INSERT INTO foo (_id, bar) VALUES (1, 'baz')";
            insertCommand.ExecuteNonQuery();
        }

        using (var queryCommand = connection.CreateCommand())
        {
            queryCommand.CommandText = "SELECT xt.version() AS v, * FROM (XTQL $XT$ (from :foo [*]) $XT$) q";
            using var reader = queryCommand.ExecuteReader();
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var name = reader.GetName(i);
                    var value = reader.IsDBNull(i) ? "NULL" : reader.GetValue(i);
                    Console.Write($"{name} = {value}; ");
                }
                Console.WriteLine();
            }
        }
    }
}
