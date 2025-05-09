using System;
using System.Data.Odbc;

class XtdbOdbcTest
{
    static void Main()
    {
        var connectionString = "Driver={PostgreSQL Unicode};Server=xtdb;Port=5432;Database=xtdb;Uid=xtdb;Pwd=xtdb;";
        using var connection = new OdbcConnection(connectionString);
        connection.Open();

        using var command = connection.CreateCommand();

        command.CommandText = "SELECT xt.version() AS x";
        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            Console.WriteLine($"x = {reader["x"]}");
        }
    }
}
