import java.sql.DriverManager;
import java.sql.SQLException;

public class XtdbHelloWorld {

    public static void main(String[] args) {
        try (var connection =
                     DriverManager.getConnection("jdbc:xtdb://xtdb:5432/xtdb", "xtdb", "xtdb");
             var statement = connection.createStatement()) {

            // Insert records using XTDB's RECORDS syntax
            statement.execute("INSERT INTO users RECORDS {_id: 'jms', name: 'James'}, {_id: 'joe', name: 'Joe'}");

            // Query the table and print results
            try (var resultSet = statement.executeQuery("SELECT * FROM users")) {
                System.out.println("Users:");

                while (resultSet.next()) {
                    System.out.printf("  * %s: %s%n", resultSet.getString("_id"), resultSet.getString("name"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
