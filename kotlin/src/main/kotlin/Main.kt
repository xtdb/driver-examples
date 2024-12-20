import java.sql.DriverManager

fun main() {
    // Connect to the XTDB database using JDBC
    DriverManager.getConnection("jdbc:xtdb://xtdb:5432/xtdb").use { connection ->
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
