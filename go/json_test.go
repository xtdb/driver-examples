package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func TestJSONInsertAndQuery(t *testing.T) {
	conn := getConn(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	// Insert using RECORDS syntax with literal JSON values
	_, err := conn.Exec(context.Background(),
		fmt.Sprintf(`INSERT INTO %s RECORDS
			{_id: 'json1', name: 'Alice', age: 30, active: true},
			{_id: 'json2', name: 'Bob', age: 25, active: false}`, table))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	rows, err := conn.Query(context.Background(),
		fmt.Sprintf("SELECT _id, name, age, active FROM %s ORDER BY _id", table))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, name string
		var age int
		var active bool
		if err := rows.Scan(&id, &name, &age, &active); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++

		if count == 1 {
			if id != "json1" || name != "Alice" || age != 30 || !active {
				t.Errorf("First row: expected (json1, Alice, 30, true), got (%s, %s, %d, %v)",
					id, name, age, active)
			}
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

func TestJSONLoadSampleData(t *testing.T) {
	conn := getConn(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	// Load sample-users.json
	data, err := os.ReadFile("../test-data/sample-users.json")
	if err != nil {
		t.Fatalf("Failed to read sample data: %v", err)
	}

	var users []map[string]interface{}
	if err := json.Unmarshal(data, &users); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	// Insert each user using RECORDS syntax
	for _, user := range users {
		id := user["_id"].(string)
		name := user["name"].(string)
		age := int(user["age"].(float64))
		active := user["active"].(bool)

		_, err := conn.Exec(context.Background(),
			fmt.Sprintf(`INSERT INTO %s RECORDS {_id: '%s', name: '%s', age: %d, active: %v}`,
				table, id, name, age, active))
		if err != nil {
			t.Fatalf("Insert failed for user %s: %v", id, err)
		}
	}

	// Query back and verify
	rows, err := conn.Query(context.Background(),
		fmt.Sprintf("SELECT _id, name, age, active FROM %s ORDER BY _id", table))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, name string
		var age int
		var active bool
		if err := rows.Scan(&id, &name, &age, &active); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++

		// Verify first user (alice)
		if count == 1 {
			if id != "alice" || name != "Alice Smith" || age != 30 || !active {
				t.Errorf("First user: expected (alice, Alice Smith, 30, true), got (%s, %s, %d, %v)",
					id, name, age, active)
			}
		}
	}

	if count != 3 {
		t.Errorf("Expected 3 users, got %d", count)
	}
}
