package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func getFlightSqlURI() string {
	host := os.Getenv("XTDB_HOST")
	if host == "" {
		host = "xtdb"
	}
	return fmt.Sprintf("grpc://%s:9833", host)
}

var adbcTableCounter int

func getAdbcCleanTable() string {
	adbcTableCounter++
	return fmt.Sprintf("test_adbc_%d_%d", time.Now().Unix(), adbcTableCounter)
}

// Helper to create an ADBC connection
func getAdbcConn(t *testing.T) (adbc.Database, adbc.Connection) {
	alloc := memory.NewGoAllocator()
	driver := flightsql.NewDriver(alloc)

	db, err := driver.NewDatabase(map[string]string{
		"uri": getFlightSqlURI(),
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	conn, err := db.Open(context.Background())
	if err != nil {
		db.Close()
		t.Fatalf("Failed to open connection: %v", err)
	}

	return db, conn
}

func cleanupAdbc(conn adbc.Connection, table string, ids ...int) {
	ctx := context.Background()
	for _, id := range ids {
		stmt, err := conn.NewStatement()
		if err != nil {
			continue
		}
		stmt.SetSqlQuery(fmt.Sprintf("ERASE FROM %s WHERE _id = %d", table, id))
		stmt.ExecuteUpdate(ctx)
		stmt.Close()
	}
}

// === Connection Tests ===

func TestAdbcConnection(t *testing.T) {
	db, conn := getAdbcConn(t)
	defer conn.Close()
	defer db.Close()

	if conn == nil {
		t.Fatal("Connection should be established")
	}
}

func TestAdbcSimpleQuery(t *testing.T) {
	db, conn := getAdbcConn(t)
	defer conn.Close()
	defer db.Close()

	ctx := context.Background()
	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("Failed to create statement: %v", err)
	}
	defer stmt.Close()

	stmt.SetSqlQuery("SELECT 1 AS x, 'hello' AS greeting")
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("Should have at least one batch")
	}

	record := reader.Record()
	if record.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", record.NumRows())
	}
	if record.NumCols() != 2 {
		t.Errorf("Expected 2 columns, got %d", record.NumCols())
	}
}

func TestAdbcQueryWithExpressions(t *testing.T) {
	db, conn := getAdbcConn(t)
	defer conn.Close()
	defer db.Close()

	ctx := context.Background()
	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("Failed to create statement: %v", err)
	}
	defer stmt.Close()

	stmt.SetSqlQuery("SELECT 2 + 2 AS sum, UPPER('hello') AS upper_greeting")
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("Should have at least one batch")
	}

	record := reader.Record()
	if record.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", record.NumRows())
	}
}

func TestAdbcSystemTables(t *testing.T) {
	db, conn := getAdbcConn(t)
	defer conn.Close()
	defer db.Close()

	ctx := context.Background()
	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("Failed to create statement: %v", err)
	}
	defer stmt.Close()

	stmt.SetSqlQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' LIMIT 10")
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer reader.Release()

	// Should execute without error
	if reader == nil {
		t.Fatal("Reader should not be nil")
	}
}

// === DML Tests ===

func TestAdbcInsertAndQuery(t *testing.T) {
	db, conn := getAdbcConn(t)
	defer conn.Close()
	defer db.Close()

	ctx := context.Background()
	table := getAdbcCleanTable()

	// INSERT using RECORDS syntax
	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("Failed to create statement: %v", err)
	}

	stmt.SetSqlQuery(fmt.Sprintf(
		"INSERT INTO %s RECORDS "+
			"{_id: 1, name: 'Widget', price: 19.99, category: 'gadgets'}, "+
			"{_id: 2, name: 'Gizmo', price: 29.99, category: 'gadgets'}, "+
			"{_id: 3, name: 'Thingamajig', price: 9.99, category: 'misc'}",
		table,
	))
	_, err = stmt.ExecuteUpdate(ctx)
	stmt.Close()
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}


	// Query the inserted data
	stmt2, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("Failed to create statement: %v", err)
	}
	defer stmt2.Close()

	stmt2.SetSqlQuery(fmt.Sprintf("SELECT * FROM %s ORDER BY _id", table))
	reader, _, err := stmt2.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("Should have at least one batch")
	}

	record := reader.Record()
	if record.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", record.NumRows())
	}

	// Cleanup
	cleanupAdbc(conn, table, 1, 2, 3)
}

func TestAdbcUpdate(t *testing.T) {
	db, conn := getAdbcConn(t)
	defer conn.Close()
	defer db.Close()

	ctx := context.Background()
	table := getAdbcCleanTable()

	// Insert initial data
	stmt, _ := conn.NewStatement()
	stmt.SetSqlQuery(fmt.Sprintf("INSERT INTO %s RECORDS {_id: 1, name: 'Widget', price: 19.99}", table))
	stmt.ExecuteUpdate(ctx)
	stmt.Close()

	// Update the price
	stmt2, _ := conn.NewStatement()
	stmt2.SetSqlQuery(fmt.Sprintf("UPDATE %s SET price = 24.99 WHERE _id = 1", table))
	stmt2.ExecuteUpdate(ctx)
	stmt2.Close()

	// Verify update
	stmt3, _ := conn.NewStatement()
	defer stmt3.Close()
	stmt3.SetSqlQuery(fmt.Sprintf("SELECT price FROM %s WHERE _id = 1", table))
	reader, _, err := stmt3.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("Should have at least one batch")
	}

	record := reader.Record()
	if record.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", record.NumRows())
	}

	// Cleanup
	cleanupAdbc(conn, table, 1)
}

func TestAdbcDelete(t *testing.T) {
	db, conn := getAdbcConn(t)
	defer conn.Close()
	defer db.Close()

	ctx := context.Background()
	table := getAdbcCleanTable()

	// Insert data
	stmt, _ := conn.NewStatement()
	stmt.SetSqlQuery(fmt.Sprintf("INSERT INTO %s RECORDS {_id: 1, name: 'ToDelete'}, {_id: 2, name: 'ToKeep'}", table))
	stmt.ExecuteUpdate(ctx)
	stmt.Close()

	// Delete one record
	stmt2, _ := conn.NewStatement()
	stmt2.SetSqlQuery(fmt.Sprintf("DELETE FROM %s WHERE _id = 1", table))
	stmt2.ExecuteUpdate(ctx)
	stmt2.Close()

	// Verify only one record remains
	stmt3, _ := conn.NewStatement()
	defer stmt3.Close()
	stmt3.SetSqlQuery(fmt.Sprintf("SELECT * FROM %s", table))
	reader, _, err := stmt3.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("Should have at least one batch")
	}

	record := reader.Record()
	if record.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", record.NumRows())
	}

	// Cleanup
	cleanupAdbc(conn, table, 1, 2)
}

func TestAdbcHistoricalQuery(t *testing.T) {
	db, conn := getAdbcConn(t)
	defer conn.Close()
	defer db.Close()

	ctx := context.Background()
	table := getAdbcCleanTable()

	// Insert initial data
	stmt, _ := conn.NewStatement()
	stmt.SetSqlQuery(fmt.Sprintf("INSERT INTO %s RECORDS {_id: 1, name: 'Widget', price: 19.99}", table))
	stmt.ExecuteUpdate(ctx)
	stmt.Close()

	// Update (creates new version)
	stmt2, _ := conn.NewStatement()
	stmt2.SetSqlQuery(fmt.Sprintf("UPDATE %s SET price = 24.99 WHERE _id = 1", table))
	stmt2.ExecuteUpdate(ctx)
	stmt2.Close()

	// Query historical data
	stmt3, _ := conn.NewStatement()
	defer stmt3.Close()
	stmt3.SetSqlQuery(fmt.Sprintf(
		"SELECT *, _valid_from, _valid_to FROM %s FOR ALL VALID_TIME ORDER BY _id, _valid_from",
		table,
	))
	reader, _, err := stmt3.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("Should have at least one batch")
	}

	record := reader.Record()
	// Should have 2 versions
	if record.NumRows() != 2 {
		t.Errorf("Expected 2 rows (historical versions), got %d", record.NumRows())
	}

	// Cleanup
	cleanupAdbc(conn, table, 1)
}

func TestAdbcErase(t *testing.T) {
	db, conn := getAdbcConn(t)
	defer conn.Close()
	defer db.Close()

	ctx := context.Background()
	table := getAdbcCleanTable()

	// Insert data
	stmt, _ := conn.NewStatement()
	stmt.SetSqlQuery(fmt.Sprintf("INSERT INTO %s RECORDS {_id: 1, name: 'ToErase'}, {_id: 2, name: 'ToKeep'}", table))
	stmt.ExecuteUpdate(ctx)
	stmt.Close()

	// Update to create history
	stmt2, _ := conn.NewStatement()
	stmt2.SetSqlQuery(fmt.Sprintf("UPDATE %s SET name = 'UpdatedErase' WHERE _id = 1", table))
	stmt2.ExecuteUpdate(ctx)
	stmt2.Close()

	// Erase record 1 completely
	stmt3, _ := conn.NewStatement()
	stmt3.SetSqlQuery(fmt.Sprintf("ERASE FROM %s WHERE _id = 1", table))
	stmt3.ExecuteUpdate(ctx)
	stmt3.Close()

	// Verify erased from all history
	stmt4, _ := conn.NewStatement()
	defer stmt4.Close()
	stmt4.SetSqlQuery(fmt.Sprintf("SELECT * FROM %s FOR ALL VALID_TIME ORDER BY _id", table))
	reader, _, err := stmt4.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("Should have at least one batch")
	}

	record := reader.Record()
	// Only record 2 should remain
	if record.NumRows() != 1 {
		t.Errorf("Expected 1 row, got %d", record.NumRows())
	}

	// Cleanup
	cleanupAdbc(conn, table, 2)
}
