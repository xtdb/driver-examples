package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
)

func main() {
	conn, err := pgx.Connect(context.Background(), "postgres://xtdb:5432/xtdb")
	if err != nil {
		log.Fatalf("Unable to connect: %v\n", err)
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(),
		"INSERT INTO go_users RECORDS {_id: 'alice', name: 'Alice'}, {_id: 'bob', name: 'Bob'}")
	if err != nil {
		log.Fatalf("Insert failed: %v\n", err)
	}

	rows, err := conn.Query(context.Background(), "SELECT _id, name FROM go_users")
	if err != nil {
		log.Fatalf("Query failed: %v\n", err)
	}
	defer rows.Close()

	fmt.Println("Users:")
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatalf("Scan failed: %v\n", err)
		}
		fmt.Printf("  * %s: %s\n", id, name)
	}

	fmt.Println("\n✓ XTDB connection successful")
}
