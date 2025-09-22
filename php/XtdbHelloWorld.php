<?php

// Connect to XTDB using PostgreSQL driver
$connection_string = "host=xtdb port=5432 dbname=xtdb user=xtdb password=xtdb";
$connection = pg_connect($connection_string);

if (!$connection) {
    die("Connection failed: " . pg_last_error());
}

try {
    // Insert records using XTDB's RECORDS syntax
    $insert_query = "INSERT INTO users RECORDS {_id: 'jms', name: 'James'}, {_id: 'joe', name: 'Joe'}";
    $insert_result = pg_query($connection, $insert_query);

    if (!$insert_result) {
        throw new Exception("Insert failed: " . pg_last_error($connection));
    }

    // Query the table and print results
    $select_query = "SELECT * FROM users";
    $result = pg_query($connection, $select_query);

    if (!$result) {
        throw new Exception("Query failed: " . pg_last_error($connection));
    }

    echo "Users:\n";

    while ($row = pg_fetch_assoc($result)) {
        echo "  * " . $row['_id'] . ": " . $row['name'] . "\n";
    }

    // Free result
    pg_free_result($result);

} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
} finally {
    // Close connection
    pg_close($connection);
}

?>