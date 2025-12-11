package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

const JSONOID = 114 // PostgreSQL JSON type OID

// DebeziumEvent represents a CDC event in Debezium format
type DebeziumEvent struct {
	Payload struct {
		Op     string                 `json:"op"`    // c=create, u=update, d=delete, r=read
		TsMs   int64                  `json:"ts_ms"` // Timestamp in milliseconds
		Source struct {
			DB    string `json:"db"`
			Table string `json:"table"`
		} `json:"source"`
		Before map[string]any `json:"before"`
		After  map[string]any `json:"after"`
	} `json:"payload"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	// Read CDC events file
	eventsFile := "cdc/events.json"
	if len(os.Args) > 1 {
		eventsFile = os.Args[1]
	}

	events, err := loadEvents(eventsFile)
	if err != nil {
		return fmt.Errorf("loading events: %w", err)
	}

	fmt.Printf("Loaded %d CDC events from %s\n", len(events), eventsFile)

	// Connect to XTDB
	host := os.Getenv("XTDB_HOST")
	if host == "" {
		host = "xtdb"
	}
	connStr := fmt.Sprintf("postgres://xtdb:xtdb@%s:5432/xtdb", host)

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("connecting to XTDB: %w", err)
	}
	defer conn.Close(ctx)

	fmt.Println("Connected to XTDB")

	// Process events
	stats := map[string]int{"inserts": 0, "updates": 0, "deletes": 0}
	tables := map[string]bool{}

	for i, event := range events {
		op := event.Payload.Op
		table := event.Payload.Source.Table
		tables[table] = true

		switch op {
		case "c", "r": // create or read (snapshot)
			if err := insertRecord(ctx, conn, event); err != nil {
				return fmt.Errorf("event %d: insert: %w", i, err)
			}
			stats["inserts"]++

		case "u": // update
			if err := insertRecord(ctx, conn, event); err != nil {
				return fmt.Errorf("event %d: update: %w", i, err)
			}
			stats["updates"]++

		case "d": // delete
			if err := deleteRecord(ctx, conn, event); err != nil {
				return fmt.Errorf("event %d: delete: %w", i, err)
			}
			stats["deletes"]++

		default:
			fmt.Printf("Warning: unknown operation %q in event %d\n", op, i)
		}
	}

	// Print summary
	fmt.Println("\n--- Ingestion Complete ---")
	fmt.Printf("Tables: %v\n", sortedKeys(tables))
	fmt.Printf("Inserts: %d\n", stats["inserts"])
	fmt.Printf("Updates: %d\n", stats["updates"])
	fmt.Printf("Deletes: %d\n", stats["deletes"])

	return nil
}

func loadEvents(filename string) ([]DebeziumEvent, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var events []DebeziumEvent
	if err := json.Unmarshal(data, &events); err != nil {
		return nil, err
	}

	return events, nil
}

func insertRecord(ctx context.Context, conn *pgx.Conn, event DebeziumEvent) error {
	table := event.Payload.Source.Table
	record := event.Payload.After
	if record == nil {
		return fmt.Errorf("insert/update event has nil 'after' field")
	}

	// Extract ID
	id, ok := record["id"]
	if !ok {
		return fmt.Errorf("record missing 'id' field")
	}

	// Convert ts_ms to timestamp for _valid_from
	validFrom := time.UnixMilli(event.Payload.TsMs).UTC()

	// Build record map for XTDB
	recordMap := map[string]any{
		"_id":         id,
		"_valid_from": validFrom.Format(time.RFC3339),
	}

	// Copy all fields except 'id' (we use _id)
	for k, v := range record {
		if k != "id" {
			recordMap[k] = v
		}
	}

	// Serialize to JSON
	recordJSON, err := json.Marshal(recordMap)
	if err != nil {
		return fmt.Errorf("marshaling record: %w", err)
	}

	// Use ExecParams with explicit JSON OID (114) to send the record
	sql := fmt.Sprintf("INSERT INTO %s RECORDS $1", table)
	pgconn := conn.PgConn()

	result := pgconn.ExecParams(ctx, sql,
		[][]byte{recordJSON}, // parameter values
		[]uint32{JSONOID},    // parameter OIDs - OID 114 for JSON
		[]int16{0},           // parameter formats (0 = text)
		[]int16{0})           // result formats (0 = text)

	_, err = result.Close()
	if err != nil {
		return fmt.Errorf("executing insert for %s: %w", table, err)
	}

	fmt.Printf("  [%s] INSERT id=%v (%d fields)\n", table, id, len(recordMap)-2)
	return nil
}

func deleteRecord(ctx context.Context, conn *pgx.Conn, event DebeziumEvent) error {
	table := event.Payload.Source.Table
	record := event.Payload.Before
	if record == nil {
		return fmt.Errorf("delete event has nil 'before' field")
	}

	id, ok := record["id"]
	if !ok {
		return fmt.Errorf("record missing 'id' field")
	}

	// Convert ts_ms to timestamp for _valid_from
	validFrom := time.UnixMilli(event.Payload.TsMs).UTC()

	// XTDB delete with valid time - use simple DELETE with the timestamp embedded
	sql := fmt.Sprintf("DELETE FROM %s FOR PORTION OF VALID_TIME FROM TIMESTAMP '%s' TO NULL WHERE _id = %v",
		table, validFrom.Format(time.RFC3339), formatID(id))

	pgconn := conn.PgConn()
	result := pgconn.ExecParams(ctx, sql,
		nil, nil, nil, nil)

	_, err := result.Close()
	if err != nil {
		return fmt.Errorf("executing delete for %s: %w", table, err)
	}

	fmt.Printf("  [%s] DELETE id=%v\n", table, id)
	return nil
}

func formatID(id any) string {
	switch v := id.(type) {
	case string:
		return fmt.Sprintf("'%s'", v)
	case float64:
		return fmt.Sprintf("%d", int64(v))
	default:
		return fmt.Sprintf("%v", v)
	}
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
