// Copyright 2026 XTDB contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func main() {
	drv := flightsql.NewDriver(memory.NewGoAllocator())

	db, err := drv.NewDatabase(map[string]string{
		"uri": "grpc://localhost:9833",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(
		"SELECT version() AS server, CURRENT_TIMESTAMP AS current_ts",
	); err != nil {
		log.Fatal(err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Release()

	for reader.Next() {
		fmt.Println(reader.Record())
	}
	if err := reader.Err(); err != nil {
		log.Fatal(err)
	}
}
