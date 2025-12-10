package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

var tableCounter int

func getXtdbHost() string {
	host := os.Getenv("XTDB_HOST")
	if host == "" {
		host = "xtdb"
	}
	return host
}

// getConn creates a standard database connection (for JSON and basic tests)
func getConn(t *testing.T) *pgx.Conn {
	connStr := fmt.Sprintf("postgres://%s:5432/xtdb", getXtdbHost())
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		t.Fatalf("Unable to connect: %v", err)
	}
	return conn
}

// getConnTransit creates a database connection with transit fallback (for transit tests only)
func getConnTransit(t *testing.T) *pgx.Conn {
	connStr := fmt.Sprintf("postgres://%s:5432/xtdb?fallback_output_format=transit", getXtdbHost())
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		t.Fatalf("Unable to connect: %v", err)
	}
	return conn
}

func getCleanTable() string {
	tableCounter++
	return fmt.Sprintf("test_table_%d_%d", time.Now().Unix(), tableCounter)
}

func TestConnection(t *testing.T) {
	conn := getConn(t)
	defer conn.Close(context.Background())

	var result int
	err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected 1, got %d", result)
	}
}

func TestInsertAndQuery(t *testing.T) {
	conn := getConn(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	_, err := conn.Exec(context.Background(),
		fmt.Sprintf("INSERT INTO %s RECORDS {_id: 'test1', value: 'hello'}, {_id: 'test2', value: 'world'}", table))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	rows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT _id, value FROM %s ORDER BY _id", table))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, value string
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++

		if count == 1 && (id != "test1" || value != "hello") {
			t.Errorf("First row: expected (test1, hello), got (%s, %s)", id, value)
		}
		if count == 2 && (id != "test2" || value != "world") {
			t.Errorf("Second row: expected (test2, world), got (%s, %s)", id, value)
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

func TestWhereClause(t *testing.T) {
	conn := getConn(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	_, err := conn.Exec(context.Background(),
		fmt.Sprintf("INSERT INTO %s (_id, age) VALUES (1, 25), (2, 35), (3, 45)", table))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	rows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT _id FROM %s WHERE age > 30 ORDER BY _id", table))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}
