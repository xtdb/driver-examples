# Copyright 2026 XTDB contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# /// script
# requires-python = ">=3.10"
# dependencies = ["adbc-driver-flightsql>=1.11.0", "pyarrow>=20.0.0"]
# ///

import adbc_driver_flightsql.dbapi as flight_sql

with (
    flight_sql.connect("grpc://localhost:9833") as con,
    con.cursor() as cursor,
):
    cursor.execute("SELECT version() AS server, CURRENT_TIMESTAMP AS current_ts")
    table = cursor.fetch_arrow_table()

print(table)

# For submission to columnar-tech/adbc-quickstarts, use the driver-manager
# variant instead (requires `dbc install flightsql`):
#
#   from adbc_driver_manager import dbapi
#   with (
#       dbapi.connect(
#           driver="flightsql",
#           db_kwargs={"uri": "grpc://localhost:9833"},
#       ) as con,
#       con.cursor() as cursor,
#   ):
#       ...
