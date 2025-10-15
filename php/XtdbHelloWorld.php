<?php

// Connect to XTDB using ext-pq (PECL pq) driver
// This gives us the ability to specify parameter OIDs for custom types
use pq\Connection;

try {
    // Connect to XTDB
    $connection = new Connection("host=xtdb port=5432 dbname=xtdb user=xtdb password=");

    // Insert records using XTDB's RECORDS syntax
    $insert_query = "INSERT INTO users RECORDS {_id: 'jms', name: 'James'}, {_id: 'joe', name: 'Joe'}";
    $connection->exec($insert_query);

    // Query the table and print results
    $result = $connection->exec("SELECT * FROM users");

    echo "Users:\n";

    while ($row = $result->fetchRow(\pq\Result::FETCH_ASSOC)) {
        echo "  * " . $row['_id'] . ": " . $row['name'] . "\n";
    }

} catch (\pq\Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}

?>
