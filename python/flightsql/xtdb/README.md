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

# Connecting Python and XTDB with ADBC

## Instructions

This example uses [XTDB](https://xtdb.com/), an immutable SQL database with
bitemporal semantics that speaks Arrow Flight SQL on port 9833 alongside its
Postgres wire protocol on 5432.

> [!TIP]
> If you already have an XTDB instance running, skip the setup section below.

### Prerequisites

1. [Install uv](https://docs.astral.sh/uv/getting-started/installation/)
1. [Install dbc](https://docs.columnar.tech/dbc/getting_started/installation/)
1. [Install Docker](https://docs.docker.com/get-docker/) (to run XTDB locally)

### Set up XTDB

Start an XTDB node with in-memory storage:

```sh
docker run --rm -p 9833:9833 -p 5432:5432 ghcr.io/xtdb/xtdb:edge
```

The container is ready when it logs `Node started`. Flight SQL is available at
`grpc://localhost:9833`; Postgres wire is on `:5432`.

### Connect to XTDB

1. Install the Flight SQL ADBC driver:

   ```sh
   dbc install flightsql
   ```

1. Customize the Python script `main.py` as needed:
   - `uri` is the gRPC URI of your XTDB instance. Use `grpc://host:9833` for
     plaintext, `grpc+tls://host:9833` if you terminate TLS at XTDB or a load
     balancer in front of it.
   - XTDB does not require authentication by default, so `db_kwargs` only
     needs `uri`. If you run XTDB behind a reverse proxy that enforces auth,
     add `"adbc.flight.sql.authorization_header": "Bearer " + token`.

1. Run the Python script:

   ```sh
   uv run main.py
   ```

   Expected output (abbreviated):

   ```
   pyarrow.Table
   server: string not null
   current_ts: timestamp[us, tz=Z] not null
   ----
   server: [["PostgreSQL 16"]]
   current_ts: [[2026-04-15 10:51:07.123456Z]]
   ```

   > **Note:** `version()` reports `PostgreSQL 16` because XTDB advertises
   > Postgres wire compatibility; it's XTDB serving this response.

## Alternative: use `adbc-driver-flightsql` from pip

If you'd rather not install `dbc`, the Python wheel ships with the shared
library bundled and exposes its own DB-API entry point:

```sh
pip install adbc-driver-flightsql pyarrow
```

Then swap the import in `main.py`:

```python
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect("grpc://localhost:9833") as con, con.cursor() as cursor:
    cursor.execute("SELECT version() AS server, CURRENT_TIMESTAMP AS current_ts")
    print(cursor.fetch_arrow_table())
```

## Learn more

- [XTDB docs](https://docs.xtdb.com/)
- [ADBC Flight SQL driver](https://arrow.apache.org/adbc/current/driver/flight_sql.html)
- Language variants alongside this example:
  [Go](../../../go/flightsql/xtdb), [Java, Kotlin, C#, …](../../../)
