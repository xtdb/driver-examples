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
	content, err := os.ReadFile("../test-data/sample-users.json")
	if err != nil {
		t.Fatalf("Failed to read JSON file: %v", err)
	}

	var users []map[string]interface{}
	if err := json.Unmarshal(content, &users); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	sql := fmt.Sprintf("INSERT INTO %s RECORDS $1", table)

	// Insert using JSON OID (114) with single parameter per record
	// Use low-level ExecParams to specify OID explicitly
	pgconn := conn.PgConn()
	for _, user := range users {
		userJSON, err := json.Marshal(user)
		if err != nil {
			t.Fatalf("Failed to marshal user: %v", err)
		}

		// Use ExecParams with explicit OID 114 (JSON)
		result := pgconn.ExecParams(context.Background(), sql,
			[][]byte{userJSON},       // parameter values
			[]uint32{JSONOID},        // parameter OIDs - OID 114
			[]int16{0},               // parameter formats (0 = text)
			[]int16{0})               // result formats (0 = text)

		_, err = result.Close()
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Query back and verify - get ALL columns including nested data
	rows, err := conn.Query(context.Background(),
		fmt.Sprintf("SELECT * FROM %s ORDER BY _id", table))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	// Get column names
	fieldDescs := rows.FieldDescriptions()
	columnNames := make([]string, len(fieldDescs))
	for i, fd := range fieldDescs {
		columnNames[i] = string(fd.Name)
	}

	count := 0
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			t.Fatalf("Failed to get values: %v", err)
		}

		// Create a map of column name -> value
		rowMap := make(map[string]interface{})
		for i, colName := range columnNames {
			rowMap[colName] = values[i]
		}

		count++

		// Verify first record (alice)
		if count == 1 {
			if rowMap["_id"] != "alice" {
				t.Errorf("Expected _id='alice', got %v", rowMap["_id"])
			}
			if rowMap["name"] != "Alice Smith" {
				t.Errorf("Expected name='Alice Smith', got %v", rowMap["name"])
			}
			// Age might be int32, int64, or float64 depending on how pgx decodes it
			ageVal := rowMap["age"]
			var age int64
			switch v := ageVal.(type) {
			case int32:
				age = int64(v)
			case int64:
				age = v
			case float64:
				age = int64(v)
			default:
				t.Errorf("Expected age to be numeric, got %T: %v", ageVal, ageVal)
			}
			if age != 30 {
				t.Errorf("Expected age=30, got %d", age)
			}
			if active, ok := rowMap["active"].(bool); !ok || !active {
				t.Errorf("Expected active=true, got %v", rowMap["active"])
			}
			if rowMap["email"] != "alice@example.com" {
				t.Errorf("Expected email='alice@example.com', got %v", rowMap["email"])
			}

			// Verify salary (float field) - should be native float64
			if salary, ok := rowMap["salary"].(float64); !ok || salary != 125000.5 {
				t.Errorf("Expected salary=125000.5 (float64), got %v (type %T)", rowMap["salary"], rowMap["salary"])
			}

			// Verify nested array (tags) - should be native []interface{}
			if tags, ok := rowMap["tags"].([]interface{}); ok {
				t.Logf("✅ Tags properly typed as []interface{}: %v", tags)
				if len(tags) != 2 {
					t.Errorf("Expected 2 tags, got %d", len(tags))
				} else {
					if tags[0] != "admin" || tags[1] != "developer" {
						t.Errorf("Expected tags ['admin', 'developer'], got %v", tags)
					}
				}
			} else {
				t.Errorf("Expected tags to be []interface{}, got %T: %v", rowMap["tags"], rowMap["tags"])
			}

			// Verify nested object (metadata) - should be native map[string]interface{}
			if metadata, ok := rowMap["metadata"].(map[string]interface{}); ok {
				t.Logf("✅ Metadata properly typed as map[string]interface{}: %v", metadata)

				// Validate metadata fields
				if dept, ok := metadata["department"].(string); !ok || dept != "Engineering" {
					t.Errorf("Expected department='Engineering', got %v (type %T)", metadata["department"], metadata["department"])
				}

				// Level might be float64 or int from JSON parsing
				var level int64
				switch v := metadata["level"].(type) {
				case float64:
					level = int64(v)
				case int64:
					level = v
				case int32:
					level = int64(v)
				default:
					t.Errorf("Expected level to be numeric, got %T: %v", metadata["level"], metadata["level"])
				}
				if level != 5 {
					t.Errorf("Expected level=5, got %d", level)
				}

				// Joined date - should be a string
				if joined, ok := metadata["joined"].(string); !ok {
					t.Errorf("Expected joined to be string, got %T: %v", metadata["joined"], metadata["joined"])
				} else if joined != "2020-01-15" {
					t.Errorf("Expected joined='2020-01-15', got %v", joined)
				}
			} else {
				t.Errorf("Expected metadata to be map[string]interface{}, got %T: %v", rowMap["metadata"], rowMap["metadata"])
			}
		}
	}

	if count != 3 {
		t.Errorf("Expected 3 records, got %d", count)
	}

	t.Logf("✅ JSON OID approach working! Inserted and queried %d records with OID 114", count)
}
