package main

// XTDB PostgreSQL wire protocol OIDs
const (
	TransitOID = 16384 // transit-JSON type OID
	JSONOID    = 114   // JSON type OID
)

// Note: Go pgx driver requires using the low-level PgConn.ExecParams API
// to specify parameter OIDs explicitly. See json_test.go and transit_test.go
// for working examples.
//
// The high-level Exec() method tries to use statement preparation with DESCRIBE,
// which doesn't work with XTDB's INSERT...RECORDS syntax. Using ExecParams
// bypasses statement preparation and sends parameters with explicit OIDs.
