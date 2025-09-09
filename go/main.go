package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
)

type Trade struct {
	ID       int                    `json:"_id"`
	Name     string                 `json:"name"`
	Quantity int                    `json:"quantity"`
	Info     map[string]interface{} `json:"info"`
}

func main() {
	// Connect to XTDB using PostgreSQL protocol
	connStr := "postgres://xtdb:5432/xtdb"
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer conn.Close(context.Background())

	fmt.Println("Connected to XTDB successfully!")

	// Note: XTDB automatically creates tables on first insert, no CREATE TABLE needed

	// Prepare sample trades data
	trades := []Trade{
		{
			ID:       1,
			Name:     "Trade1",
			Quantity: 1001,
			Info: map[string]interface{}{
				"some_nested": []interface{}{
					"json",
					42,
					map[string]interface{}{
						"data": []string{"hello"},
					},
				},
			},
		},
		{
			ID:       2,
			Name:     "Trade2",
			Quantity: 15,
			Info:     map[string]interface{}{"value": 2},
		},
		{
			ID:       3,
			Name:     "Trade3",
			Quantity: 200,
			Info:     map[string]interface{}{"value": 3},
		},
	}

	// Insert trades using XTDB RECORDS syntax
	for _, trade := range trades {
		infoJSON, err := json.Marshal(trade.Info)
		if err != nil {
			log.Printf("Error marshaling info for trade %d: %v\n", trade.ID, err)
			continue
		}

		insertQuery := fmt.Sprintf(`INSERT INTO trades RECORDS {_id: %d, name: '%s', quantity: %d, info: '%s'}`,
			trade.ID, trade.Name, trade.Quantity, string(infoJSON))
		
		_, err = conn.Exec(context.Background(), insertQuery)
		if err != nil {
			log.Printf("Error inserting trade %d: %v\n", trade.ID, err)
		}
	}
	fmt.Println("Trades inserted successfully")

	// Query trades with quantity > 100
	fmt.Println("\nTrades with quantity > 100:")
	rows, err := conn.Query(context.Background(), "SELECT _id, name, quantity, info FROM trades WHERE quantity > 100")
	if err != nil {
		log.Printf("Error querying trades: %v\n", err)
	} else {
		defer rows.Close()

		for rows.Next() {
			var id int
			var name string
			var quantity int
			var infoJSON []byte

			err := rows.Scan(&id, &name, &quantity, &infoJSON)
			if err != nil {
				log.Printf("Error scanning row: %v\n", err)
				continue
			}

			var info map[string]interface{}
			if err := json.Unmarshal(infoJSON, &info); err != nil {
				log.Printf("Error unmarshaling info: %v\n", err)
			}

			fmt.Printf("  * ID: %d, Name: %s, Quantity: %d, Info: %v\n", id, name, quantity, info)
		}
	}

	// Demonstrate XTDB's RECORDS syntax (similar to Java example)
	fmt.Println("\nUsing XTDB RECORDS syntax:")
	_, err = conn.Exec(context.Background(), `
		INSERT INTO users RECORDS 
			{_id: 'jms', name: 'James'}, 
			{_id: 'joe', name: 'Joe'}
	`)
	if err != nil {
		log.Printf("Note: RECORDS syntax may not be supported via PostgreSQL protocol: %v\n", err)
	} else {
		// Query users table
		rows, err := conn.Query(context.Background(), "SELECT _id, name FROM users")
		if err != nil {
			log.Printf("Error querying users: %v\n", err)
		} else {
			defer rows.Close()
			fmt.Println("Users:")
			for rows.Next() {
				var id, name string
				if err := rows.Scan(&id, &name); err != nil {
					log.Printf("Error scanning user row: %v\n", err)
					continue
				}
				fmt.Printf("  * %s: %s\n", id, name)
			}
		}
	}

	// Demonstrate transaction support
	fmt.Println("\nDemonstrating transaction support:")
	tx, err := conn.Begin(context.Background())
	if err != nil {
		log.Printf("Error beginning transaction: %v\n", err)
	} else {
		// Insert within transaction using XTDB RECORDS syntax
		_, err = tx.Exec(context.Background(), 
			`INSERT INTO trades RECORDS {_id: 4, name: 'Trade4', quantity: 500, info: '{"transaction": true}'}`)
		
		if err != nil {
			log.Printf("Error in transaction: %v\n", err)
			tx.Rollback(context.Background())
		} else {
			// Commit transaction
			if err := tx.Commit(context.Background()); err != nil {
				log.Printf("Error committing transaction: %v\n", err)
			} else {
				fmt.Println("Transaction committed successfully")
			}
		}
	}

	// Final count
	var count int
	err = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM trades").Scan(&count)
	if err != nil {
		log.Printf("Error counting trades: %v\n", err)
	} else {
		fmt.Printf("\nTotal trades in database: %d\n", count)
	}

	fmt.Println("\nXTDB Go example completed successfully!")
}
