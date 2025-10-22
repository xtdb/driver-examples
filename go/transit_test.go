package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// DecodeTransitValue attempts to decode a transit-encoded value (copied from json_test.go)
func DecodeTransitValueTransit(val interface{}) interface{} {
	// Handle if val is already a decoded array or object (not a JSON string)
	if arr, ok := val.([]interface{}); ok {
		return decodeTransitArray(arr)
	}

	// Handle if val is a JSON string that needs parsing
	str, ok := val.(string)
	if !ok {
		return val
	}

	// Try to parse as JSON
	var data interface{}
	if err := json.Unmarshal([]byte(str), &data); err != nil {
		return val
	}

	// Check if it's a transit structure
	arr, ok := data.([]interface{})
	if !ok {
		return data
	}

	return decodeTransitArray(arr)
}

func decodeTransitArray(arr []interface{}) interface{} {
	if len(arr) == 0 {
		return arr
	}

	// Transit tagged value: [tag, value]
	if len(arr) == 2 {
		if tag, ok := arr[0].(string); ok && len(tag) > 0 && tag[0:2] == "~#" {
			// For nested tagged values, recursively decode
			return DecodeTransitValueTransit(arr[1])
		}
	}

	// Transit map: ["^ ", key1, val1, key2, val2, ...]
	if len(arr) > 0 {
		if firstElem, ok := arr[0].(string); ok && firstElem == "^ " {
			result := make(map[string]interface{})
			for i := 1; i < len(arr); i += 2 {
				if i+1 >= len(arr) {
					break
				}
				key := fmt.Sprintf("%v", arr[i])
				// Recursively decode the value (handles nested maps)
				value := DecodeTransitValueTransit(arr[i+1])

				result[key] = value
			}
			return result
		}
	}

	// Regular array - recursively decode elements
	result := make([]interface{}, len(arr))
	for i, elem := range arr {
		result[i] = DecodeTransitValueTransit(elem)
	}
	return result
}

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

func TestSimpleRecordsInsert(t *testing.T) {
	conn := getConnTransit(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	// Demonstrate using low-level PgConn.ExecParams to specify OID explicitly
	testJSON := `{"_id": "test1", "name": "Test User"}`
	sql := fmt.Sprintf("INSERT INTO %s RECORDS $1", table)

	// Use low-level ExecParams with explicit OID 114 (JSON)
	pgconn := conn.PgConn()
	result := pgconn.ExecParams(context.Background(), sql,
		[][]byte{[]byte(testJSON)}, // parameter values
		[]uint32{JSONOID},           // parameter OIDs - OID 114
		[]int16{0},                  // parameter formats (0 = text)
		[]int16{0})                  // result formats (0 = text)

	_, err := result.Close()
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Verify the insert worked by querying
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

	if id != "test1" || name != "Test User" {
		t.Errorf("Got (_id=%s, name=%s), expected (test1, Test User)", id, name)
	}
}

func TestTransitJSONFormat(t *testing.T) {
	conn := getConnTransit(t)
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
	conn := getConnTransit(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	// Load sample-users-transit.json
	content, err := os.ReadFile("../test-data/sample-users-transit.json")
	if err != nil {
		t.Fatalf("Failed to read transit file: %v", err)
	}

	lines := strings.Split(string(content), "\n")
	sql := fmt.Sprintf("INSERT INTO %s RECORDS $1", table)

	// Insert using transit OID (16384) with single parameter per record
	// Use low-level ExecParams to specify OID explicitly
	pgconn := conn.PgConn()
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Encode parameter as bytes
		buf := []byte(line)

		// Use ExecParams with explicit OID 16384 (transit-JSON)
		result := pgconn.ExecParams(context.Background(), sql,
			[][]byte{buf},            // parameter values
			[]uint32{TransitOID},     // parameter OIDs - OID 16384
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

			// Verify salary (float field) - May be transit-encoded, decode if needed
			salaryDecoded := DecodeTransitValueTransit(rowMap["salary"])
			if salary, ok := salaryDecoded.(float64); !ok || salary != 125000.5 {
				t.Errorf("Expected salary=125000.5 (float64), got %v (type %T)", salaryDecoded, salaryDecoded)
			}

			// Verify nested array (tags) - With transit output format, properly typed
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

			// Verify nested object (metadata) - May be transit-encoded, decode if needed
			metadataDecoded := DecodeTransitValueTransit(rowMap["metadata"])
			if metadata, ok := metadataDecoded.(map[string]interface{}); ok {
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

				// Joined date - should be present
				if metadata["joined"] == nil {
					t.Error("Expected joined field in metadata")
				} else {
					t.Logf("Joined date: %v (type: %T)", metadata["joined"], metadata["joined"])
				}
			} else {
				t.Errorf("Expected metadata to be map[string]interface{}, got %T: %v", rowMap["metadata"], rowMap["metadata"])
			}
		}
	}

	if count != 3 {
		t.Errorf("Expected 3 records, got %d", count)
	}

	t.Logf("✅ Transit-JSON OID approach working! Inserted and queried %d records with OID 16384", count)
}

/*
func TestTransitJSONParsingOriginal(t *testing.T) {
	// Original test that unmarshalls the transit data:
	conn := getConnTransit(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	// Load sample-users-transit.json
	file, err := os.Open("../test-data/sample-users-transit.json")
	if err != nil {
		t.Fatalf("Failed to open transit file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	encoder := &MinimalTransitEncoder{}

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
*/

func TestTransitJSONWithDate(t *testing.T) {
	conn := getConnTransit(t)
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

func TestTransitMsgpackCopyFrom(t *testing.T) {
	conn := getConnTransit(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	// Read the msgpack file as binary data
	msgpackData, err := os.ReadFile("../test-data/sample-users-transit.msgpack")
	if err != nil {
		t.Fatalf("Failed to read msgpack file: %v", err)
	}

	// Use COPY FROM STDIN with transit-msgpack format
	_, err = conn.PgConn().CopyFrom(
		context.Background(),
		strings.NewReader(string(msgpackData)),
		fmt.Sprintf("COPY %s FROM STDIN WITH (FORMAT 'transit-msgpack')", table),
	)
	if err != nil {
		t.Fatalf("COPY FROM failed: %v", err)
	}

	// Query back and verify - get ALL columns
	rows, err := conn.Query(context.Background(),
		fmt.Sprintf("SELECT * FROM %s ORDER BY _id", table))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			t.Fatalf("Failed to get values: %v", err)
		}
		count++

		if count == 1 {
			// Verify first record has expected fields
			fieldDescs := rows.FieldDescriptions()
			rowMap := make(map[string]interface{})
			for i, fd := range fieldDescs {
				rowMap[string(fd.Name)] = values[i]
			}

			if rowMap["name"] != "Alice Smith" {
				t.Errorf("Expected name='Alice Smith', got %v", rowMap["name"])
			}
		}
	}

	if count != 3 {
		t.Errorf("Expected 3 records, got %d", count)
	}

	t.Logf("✅ Successfully tested transit-msgpack with COPY FROM! Loaded %d records from msgpack binary format", count)
}

func TestTransitNestOneFullRecord(t *testing.T) {
	conn := getConnTransit(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	// Load sample-users-transit.json
	content, err := os.ReadFile("../test-data/sample-users-transit.json")
	if err != nil {
		t.Fatalf("Failed to read transit file: %v", err)
	}

	lines := strings.Split(string(content), "\n")
	sql := fmt.Sprintf("INSERT INTO %s RECORDS $1", table)

	// Insert using transit OID (16384)
	pgconn := conn.PgConn()
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		result := pgconn.ExecParams(context.Background(), sql,
			[][]byte{[]byte(line)},
			[]uint32{TransitOID},
			[]int16{0},
			[]int16{0})

		_, err = result.Close()
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Query using NEST_ONE to get entire record as a single nested object
	rows, err := conn.Query(context.Background(),
		fmt.Sprintf("SELECT NEST_ONE(FROM %s WHERE _id = 'alice') AS r", table))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected one result")
	}

	values, err := rows.Values()
	if err != nil {
		t.Fatalf("Failed to get values: %v", err)
	}

	// The entire record comes back as a transit-JSON string that needs to be decoded
	recordRaw := values[0]
	t.Logf("\n✅ NEST_ONE returned entire record: %T", recordRaw)
	t.Logf("   Raw record: %v", recordRaw)

	// Decode the transit-JSON string
	recordDecoded := DecodeTransitValueTransit(recordRaw)
	record, ok := recordDecoded.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected map[string]interface{} after decoding, got %T", recordDecoded)
	}

	t.Logf("   Decoded record: %T", record)

	// Verify all fields are accessible as native types
	if record["_id"] != "alice" {
		t.Errorf("Expected _id='alice', got %v", record["_id"])
	}
	if record["name"] != "Alice Smith" {
		t.Errorf("Expected name='Alice Smith', got %v", record["name"])
	}

	// Age might be different numeric types
	var age int64
	switch v := record["age"].(type) {
	case int32:
		age = int64(v)
	case int64:
		age = v
	case float64:
		age = int64(v)
	default:
		t.Errorf("Expected age to be numeric, got %T: %v", record["age"], record["age"])
	}
	if age != 30 {
		t.Errorf("Expected age=30, got %d", age)
	}

	if active, ok := record["active"].(bool); !ok || !active {
		t.Errorf("Expected active=true, got %v", record["active"])
	}

	// Nested array should be native []interface{}
	if tags, ok := record["tags"].([]interface{}); ok {
		t.Logf("   ✅ Nested array (tags) properly typed: %v", tags)
		if len(tags) != 2 || tags[0] != "admin" || tags[1] != "developer" {
			t.Errorf("Expected tags ['admin', 'developer'], got %v", tags)
		}
	} else {
		t.Errorf("Expected tags to be []interface{}, got %T: %v", record["tags"], record["tags"])
	}

	// Nested object should be native map[string]interface{}
	t.Logf("   Raw metadata value: %v (type: %T)", record["metadata"], record["metadata"])
	if metadata, ok := record["metadata"].(map[string]interface{}); ok {
		t.Logf("   ✅ Nested object (metadata) properly typed: %v", metadata)
		if metadata["department"] != "Engineering" {
			t.Errorf("Expected department='Engineering', got %v", metadata["department"])
		}

		// Verify joined date - after transit decoding, tagged values like ["~#time/zoned-date-time", "..."]
		// are decoded to just the value string
		joinedRaw := metadata["joined"]
		t.Logf("   Joined raw value: %v (type: %T)", joinedRaw, joinedRaw)

		if joinedStr, ok := joinedRaw.(string); ok {
			// The transit decoder extracts the value from ["~#time/zoned-date-time", "2020-01-15T00:00Z[UTC]"]
			// leaving us with just "2020-01-15T00:00Z[UTC]"
			// Remove the [UTC] timezone annotation
			dateStr := strings.Split(joinedStr, "[")[0]

			// Parse the ISO datetime string - handle both RFC3339 and simplified Z format
			var parsedDate time.Time
			var err error
			if strings.HasSuffix(dateStr, "Z") {
				// Try parsing with custom format for simplified Z notation
				parsedDate, err = time.Parse("2006-01-02T15:04:05Z", dateStr)
				if err != nil {
					// Try without seconds
					parsedDate, err = time.Parse("2006-01-02T15:04Z", dateStr)
				}
			} else {
				parsedDate, err = time.Parse(time.RFC3339, dateStr)
			}

			if err != nil {
				t.Errorf("Failed to parse date %s: %v", dateStr, err)
			} else {
				t.Logf("   ✅ Decoded joined date to time.Time: %v", parsedDate)
				// Verify it's the expected date
				if parsedDate.Year() != 2020 || parsedDate.Month() != 1 || parsedDate.Day() != 15 {
					t.Errorf("Expected date 2020-01-15, got %v", parsedDate)
				}
				t.Logf("   ✅ Transit tagged date successfully decoded and verified")
			}
		} else {
			t.Errorf("Expected joined to be string, got %T: %v", joinedRaw, joinedRaw)
		}
	} else {
		t.Errorf("Expected metadata to be map[string]interface{}, got %T: %v", record["metadata"], record["metadata"])
	}

	t.Logf("\n✅ NEST_ONE with transit fallback successfully decoded entire record!")
	t.Logf("   All fields accessible as native Go types")
}
