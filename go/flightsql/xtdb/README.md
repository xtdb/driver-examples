<!--
Copyright 2026 XTDB contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Connecting Go and XTDB with ADBC

This example connects to [XTDB](https://xtdb.com/) over Arrow Flight SQL using
the pure-Go ADBC driver — no C shared library required.

### Prerequisites

1. [Install Go](https://go.dev/doc/install) 1.23+
1. [Install Docker](https://docs.docker.com/get-docker/)

### Set up XTDB

```sh
docker run --rm -p 9833:9833 -p 5432:5432 ghcr.io/xtdb/xtdb:edge
```

The container is ready when it logs `Node started`.

### Run

```sh
go mod init xtdb-adbc-quickstart   # if not already a module
go get github.com/apache/arrow-adbc/go/adbc/driver/flightsql
go get github.com/apache/arrow/go/v17/arrow/memory
go run main.go
```

Expected output (abbreviated):

```
record:
  schema:
  fields: 2
    - server: type=utf8
    - current_ts: type=timestamp[us, tz=Z]
  rows: 1
  col[0][server]: ["PostgreSQL 16"]
  col[1][current_ts]: [2026-04-15 10:51:07.123456]
```

> **Note:** `version()` reports `PostgreSQL 16` because XTDB advertises
> Postgres wire compatibility; the response still comes from XTDB.

## Learn more

- [XTDB docs](https://docs.xtdb.com/)
- [ADBC Flight SQL (Go)](https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc/driver/flightsql)
