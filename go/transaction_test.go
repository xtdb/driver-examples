package main

import (
	"context"
	"fmt"
	"testing"
)

func TestTransactionCommit(t *testing.T) {
	conn := getConn(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	// Start transaction
	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert data in transaction
	_, err = tx.Exec(context.Background(),
		fmt.Sprintf("INSERT INTO %s RECORDS {_id: 'tx1', value: 'committed'}", table))
	if err != nil {
		tx.Rollback(context.Background())
		t.Fatalf("Insert failed: %v", err)
	}

	// Commit transaction
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify data is there
	rows, err := conn.Query(context.Background(),
		fmt.Sprintf("SELECT _id, value FROM %s WHERE _id = 'tx1'", table))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected row after commit, got none")
	}

	var id, value string
	if err := rows.Scan(&id, &value); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if id != "tx1" || value != "committed" {
		t.Errorf("Got (%s, %s), expected (tx1, committed)", id, value)
	}
}

func TestTransactionRollback(t *testing.T) {
	conn := getConn(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	// Start transaction
	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert data in transaction
	_, err = tx.Exec(context.Background(),
		fmt.Sprintf("INSERT INTO %s RECORDS {_id: 'tx_rollback', value: 'should not exist'}", table))
	if err != nil {
		tx.Rollback(context.Background())
		t.Fatalf("Insert failed: %v", err)
	}

	// Rollback transaction
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify data is NOT there
	rows, err := conn.Query(context.Background(),
		fmt.Sprintf("SELECT _id FROM %s WHERE _id = 'tx_rollback'", table))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Error("Expected no rows after rollback, but found data")
	}
}

func TestTransactionWithError(t *testing.T) {
	conn := getConn(t)
	defer conn.Close(context.Background())

	table := getCleanTable()

	// Start transaction
	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert valid data
	_, err = tx.Exec(context.Background(),
		fmt.Sprintf("INSERT INTO %s RECORDS {_id: 'tx_error_1', value: 'first'}", table))
	if err != nil {
		tx.Rollback(context.Background())
		t.Fatalf("First insert failed: %v", err)
	}

	// Try to insert invalid SQL (this should fail)
	_, err = tx.Exec(context.Background(),
		fmt.Sprintf("INSERT INTO %s RECORDS {invalid syntax here}", table))
	if err == nil {
		t.Error("Expected error for invalid SQL, got none")
	}

	// Rollback due to error
	tx.Rollback(context.Background())

	// Verify first insert was rolled back too
	rows, err := conn.Query(context.Background(),
		fmt.Sprintf("SELECT _id FROM %s WHERE _id = 'tx_error_1'", table))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Error("Expected no rows after rollback, but found data from first insert")
	}
}
