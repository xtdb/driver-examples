package main

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Bulk ingest (FlightSQL CommandStatementIngest) and prepared-statement
// tests. Corresponds to Python tests/test_adbc_ingest_prepare.py.
//
// Known state (XTDB nightly 75472e4):
//   * adbc_ingest is NOT_IMPLEMENTED — adbc-bugs.md #3b. Tests use t.Skip.
//   * Prepare works for SELECT + parameterized queries; unparseable SQL
//     (e.g. DROP TABLE) comes back as INTERNAL — adbc-bugs.md #3.

func newIngestData(t *testing.T) arrow.Record {
	t.Helper()
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues(
		[]string{"alpha", "beta", "gamma"}, nil,
	)
	return b.NewRecord()
}

func runIngest(t *testing.T, mode string) {
	t.Helper()
	db, conn := getAdbcConn(t)
	defer conn.Close()
	defer db.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	table := getAdbcCleanTable()
	ingestErr := func(err error) bool {
		return strings.Contains(err.Error(), "Unimplemented") ||
			strings.Contains(err.Error(), "ExecuteIngest") ||
			strings.Contains(err.Error(), "Unknown statement option")
	}
	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, table); err != nil {
		if ingestErr(err) {
			t.Skipf("adbc-bugs.md #3b (ingest unsupported): %v", err)
		}
		t.Fatalf("SetOption target: %v", err)
	}
	if err := stmt.SetOption(adbc.OptionKeyIngestMode, mode); err != nil {
		if ingestErr(err) {
			t.Skipf("adbc-bugs.md #3b (ingest unsupported): %v", err)
		}
		t.Fatalf("SetOption mode: %v", err)
	}

	rec := newIngestData(t)
	defer rec.Release()

	if err := stmt.Bind(context.Background(), rec); err != nil {
		if strings.Contains(err.Error(), "Unimplemented") ||
			strings.Contains(err.Error(), "ExecuteIngest") {
			t.Skipf("adbc-bugs.md #3b (ExecuteIngest): %v", err)
		}
		t.Fatalf("Bind: %v", err)
	}

	if _, err := stmt.ExecuteUpdate(context.Background()); err != nil {
		if strings.Contains(err.Error(), "Unimplemented") ||
			strings.Contains(err.Error(), "ExecuteIngest") {
			t.Skipf("adbc-bugs.md #3b (ExecuteIngest): %v", err)
		}
		t.Fatalf("ExecuteUpdate: %v", err)
	}

	defer cleanupAdbc(conn, table, 1, 2, 3)
}

func TestAdbcIngestCreate(t *testing.T) {
	runIngest(t, adbc.OptionValueIngestModeCreate)
}

func TestAdbcIngestAppend(t *testing.T) {
	runIngest(t, adbc.OptionValueIngestModeAppend)
}

func TestAdbcIngestReplace(t *testing.T) {
	runIngest(t, adbc.OptionValueIngestModeReplace)
}

func TestAdbcIngestCreateAppend(t *testing.T) {
	runIngest(t, adbc.OptionValueIngestModeCreateAppend)
}

func TestAdbcPrepareSelect(t *testing.T) {
	db, conn := getAdbcConn(t)
	defer conn.Close()
	defer db.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("SELECT 1 AS x"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	if err := stmt.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("expected at least one record batch")
	}
	rec := reader.Record()
	if rec.NumRows() != 1 || rec.NumCols() != 1 {
		t.Errorf("expected 1x1 record, got %dx%d", rec.NumRows(), rec.NumCols())
	}
}

func TestAdbcPrepareParameterized(t *testing.T) {
	db, conn := getAdbcConn(t)
	defer conn.Close()
	defer db.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("SELECT ? + 1 AS r"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	if err := stmt.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "p0", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{41}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	if err := stmt.Bind(context.Background(), rec); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("expected at least one record batch")
	}
	got := reader.Record().Column(0).(*array.Int64).Value(0)
	if got != 42 {
		t.Errorf("expected 42, got %d", got)
	}
}

func TestAdbcPrepareUnparseable(t *testing.T) {
	// Documents the current behavior: DROP TABLE isn't in XTDB's grammar,
	// and FlightSQL Prepare returns INTERNAL instead of INVALID_ARGUMENT.
	// This test PASSES by detecting that wrong shape; flip once the
	// server classifies parser errors correctly (adbc-bugs.md #3).
	db, conn := getAdbcConn(t)
	defer conn.Close()
	defer db.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("DROP TABLE IF EXISTS never_existed"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	err = stmt.Prepare(context.Background())
	if err == nil {
		t.Fatal("expected Prepare to fail for DROP TABLE (XTDB has no DDL)")
	}
	if !strings.Contains(err.Error(), "Internal") {
		t.Logf("Prepare now returns a non-Internal error — flip this test!")
		t.Errorf("expected Internal-shaped error, got: %v", err)
	}
}
