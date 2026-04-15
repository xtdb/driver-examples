"""Driver quirks for XTDB's Flight SQL ADBC driver.

Consumed by the adbc-drivers-validation test suite. See
https://github.com/adbc-drivers/validation
"""

import re
from pathlib import Path

from adbc_drivers_validation import model, quirks


class XtdbQuirks(model.DriverQuirks):
    name = "xtdb"
    # Loaded via ADBC driver manager; path is resolved in conftest.py.
    driver = "adbc_driver_flightsql"
    driver_name = "ADBC Flight SQL Driver - Go"
    vendor_name = "XTDB"
    # Nightly builds report commit shas; match anything non-empty that isn't
    # the Flight SQL placeholder.
    vendor_version = re.compile(r".+")
    short_version = "edge"

    features = model.DriverFeatures(
        # Server-side metadata endpoints that currently work on XTDB nightly
        # (see adbc-bugs.md). Note: connection_get_table_schema and the
        # column-depth variant of get_objects hit the Arrow-IPC encoding bug.
        connection_get_table_schema=False,
        connection_transactions=False,
        get_objects=True,
        get_objects_constraints_check=False,
        get_objects_constraints_foreign=False,
        get_objects_constraints_primary=False,
        get_objects_constraints_unique=False,
        statement_bind=True,
        statement_bulk_ingest=False,
        statement_bulk_ingest_catalog=False,
        statement_bulk_ingest_schema=False,
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=False,
        statement_get_parameter_schema=False,
        statement_prepare=False,
        statement_rows_affected=False,
        # XTDB's FlightSQL server does not yet expose GetCurrentCatalog /
        # GetCurrentDbSchema over the wire — see adbc-bugs.md #3.
        current_catalog=None,
        current_schema=None,
        supported_xdbc_fields=[],
        quirk_foundry=False,
    )

    setup = model.DriverSetup(
        database={"uri": model.FromEnv("XTDB_FLIGHT_SQL_URI")},
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path, ...]:
        # Directory where XTDB-specific overrides live (may be empty today).
        return (Path(__file__).parent / "queries",)

    def bind_parameter(self, index: int) -> str:
        return "?"

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        msg = str(error).lower()
        if "table" in msg and ("not found" in msg or "does not exist" in msg):
            return table_name is None or table_name.lower() in msg
        return False

    def quote_one_identifier(self, identifier: str) -> str:
        # XTDB SQL uses Postgres-style double-quoted identifiers.
        return '"' + identifier.replace('"', '""') + '"'

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement, dialect="postgres")


QUIRKS = [XtdbQuirks()]
