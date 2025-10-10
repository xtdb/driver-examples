package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// MinimalTransitEncoder provides basic transit-JSON encoding
type MinimalTransitEncoder struct{}

// EncodeValue encodes a Go value to transit-JSON format
func (e *MinimalTransitEncoder) EncodeValue(value interface{}) string {
	switch v := value.(type) {
	case map[string]interface{}:
		return e.EncodeMap(v)
	case []interface{}:
		encoded := make([]string, len(v))
		for i, item := range v {
			encoded[i] = e.EncodeValue(item)
		}
		return "[" + strings.Join(encoded, ",") + "]"
	case string:
		data, _ := json.Marshal(v)
		return string(data)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case float64:
		return fmt.Sprintf("%v", v)
	case int:
		return fmt.Sprintf("%d", v)
	case time.Time:
		return fmt.Sprintf(`"~t%s"`, v.Format(time.RFC3339))
	case nil:
		return "null"
	default:
		data, _ := json.Marshal(fmt.Sprintf("%v", v))
		return string(data)
	}
}

// EncodeMap encodes a map to transit-JSON map format
func (e *MinimalTransitEncoder) EncodeMap(data map[string]interface{}) string {
	pairs := []string{}
	for key, value := range data {
		pairs = append(pairs, fmt.Sprintf(`"~:%s"`, key))
		pairs = append(pairs, e.EncodeValue(value))
	}
	return `["^ ",` + strings.Join(pairs, ",") + `]`
}

// DecodeTransitLine decodes a transit-JSON line to a map (simplified)
func (e *MinimalTransitEncoder) DecodeTransitLine(line string) (map[string]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal([]byte(line), &data); err != nil {
		return nil, err
	}

	if len(data) == 0 || data[0] != "^ " {
		return nil, fmt.Errorf("not a transit map")
	}

	result := make(map[string]interface{})
	for i := 1; i < len(data); i += 2 {
		if i+1 >= len(data) {
			break
		}

		key, ok := data[i].(string)
		if !ok {
			continue
		}

		// Remove ~: prefix
		if strings.HasPrefix(key, "~:") {
			key = key[2:]
		}

		value := data[i+1]
		// Handle ~t dates
		if str, ok := value.(string); ok && strings.HasPrefix(str, "~t") {
			value = str[2:]
		}

		result[key] = value
	}

	return result, nil
}

func TestTransitJSONFormat(t *testing.T) {
	conn := getConn(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	encoder := &MinimalTransitEncoder{}

	// Create transit-JSON
	data := map[string]interface{}{
		"_id":    "transit1",
		"name":   "Transit User",
		"age":    float64(42),
		"active": true,
	}
	transitJSON := encoder.EncodeMap(data)

	// Verify it has proper transit format markers
	if !strings.Contains(transitJSON, `["^ "`) {
		t.Errorf("Transit JSON should contain map marker")
	}
	if !strings.Contains(transitJSON, `"~:_id"`) {
		t.Errorf("Transit JSON should contain keyword markers")
	}

	// Insert using RECORDS curly brace syntax (pgx doesn't easily support OID 16384)
	_, err := conn.Exec(context.Background(),
		fmt.Sprintf(`INSERT INTO %s RECORDS {_id: 'transit1', name: 'Transit User', age: 42, active: true}`, table))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	rows, err := conn.Query(context.Background(),
		fmt.Sprintf("SELECT _id, name, age, active FROM %s", table))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected at least one row")
	}

	var id, name string
	var age int
	var active bool
	if err := rows.Scan(&id, &name, &age, &active); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if id != "transit1" || name != "Transit User" || age != 42 || !active {
		t.Errorf("Got (%s, %s, %d, %v), expected (transit1, Transit User, 42, true)",
			id, name, age, active)
	}
}

func TestTransitJSONParsing(t *testing.T) {
	conn := getConn(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	// Load and parse sample-users-transit.json
	file, err := os.Open("../test-data/sample-users-transit.json")
	if err != nil {
		t.Fatalf("Failed to open transit file: %v", err)
	}
	defer file.Close()

	encoder := &MinimalTransitEncoder{}
	scanner := bufio.NewScanner(file)

	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Decode transit-JSON
		userData, err := encoder.DecodeTransitLine(line)
		if err != nil {
			t.Fatalf("Failed to decode transit line: %v", err)
		}

		// Extract values with type assertions
		id, _ := userData["_id"].(string)
		name, _ := userData["name"].(string)
		age := int(userData["age"].(float64))
		active, _ := userData["active"].(bool)

		// Insert using RECORDS curly brace syntax
		_, err = conn.Exec(context.Background(),
			fmt.Sprintf(`INSERT INTO %s RECORDS {_id: '%s', name: '%s', age: %d, active: %v}`,
				table, id, name, age, active))
		if err != nil {
			t.Fatalf("Insert failed for user %s: %v", id, err)
		}
		count++
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("Scanner error: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected to parse 3 users, got %d", count)
	}

	// Query back and verify
	rows, err := conn.Query(context.Background(),
		fmt.Sprintf("SELECT _id, name, age, active FROM %s ORDER BY _id", table))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	verifyCount := 0
	for rows.Next() {
		var id, name string
		var age int
		var active bool
		if err := rows.Scan(&id, &name, &age, &active); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		verifyCount++

		// Check first user
		if verifyCount == 1 {
			if id != "alice" || name != "Alice Smith" || age != 30 || !active {
				t.Errorf("First user: got (%s, %s, %d, %v), expected (alice, Alice Smith, 30, true)",
					id, name, age, active)
			}
		}
	}

	if verifyCount != 3 {
		t.Errorf("Expected 3 users from query, got %d", verifyCount)
	}
}

func TestTransitJSONWithDate(t *testing.T) {
	conn := getConn(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	encoder := &MinimalTransitEncoder{}

	// Create data with date
	now := time.Now()
	data := map[string]interface{}{
		"_id":     "date_test",
		"name":    "Date Test",
		"created": now,
	}

	transitJSON := encoder.EncodeMap(data)

	// Verify it contains date marker
	if !strings.Contains(transitJSON, `"~t`) {
		t.Errorf("Transit JSON should contain date marker ~t")
	}

	// For insertion, use string format
	dateStr := now.Format("2006-01-02")
	_, err := conn.Exec(context.Background(),
		fmt.Sprintf(`INSERT INTO %s RECORDS {_id: 'date_test', name: 'Date Test', created_date: '%s'}`,
			table, dateStr))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	rows, err := conn.Query(context.Background(),
		fmt.Sprintf("SELECT _id, name FROM %s", table))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected at least one row")
	}

	var id, name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if id != "date_test" || name != "Date Test" {
		t.Errorf("Got (%s, %s), expected (date_test, Date Test)", id, name)
	}
}
