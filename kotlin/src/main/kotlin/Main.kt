import java.sql.DriverManager

fun main() {
    val host = System.getenv("XTDB_HOST") ?: "xtdb"
    val url = "jdbc:xtdb://$host:5432/xtdb"

    // Connect to the XTDB database using JDBC
    DriverManager.getConnection(url).use { connection ->
        connection.createStatement().use { statement ->
            // Insert records using XTDB's RECORDS syntax
            statement.execute("INSERT INTO users RECORDS {_id: 'jms', name: 'James'}, {_id: 'joe', name: 'Joe'}")

            // Query the table and print results
            statement.executeQuery("SELECT * FROM users").use { rs ->
                println("Users:")

                while (rs.next()) {
                    println("  * ${rs.getString("_id")}: ${rs.getString("name")}")
                }
            }
        }
    }
}
