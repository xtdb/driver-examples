"""Validation-suite conftest for XTDB.

Wires the adbc-drivers-validation fixtures to XTDB's FlightSQL driver.
Run with:  pytest validation/tests/
Requires:  XTDB_FLIGHT_SQL_URI=grpc://localhost:9833 (or similar)
"""

import os
import pathlib

import adbc_driver_flightsql
import pytest

# Re-export the common fixtures/hooks from the validation package so pytest
# picks them up in this directory.
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    manual_test,
    noci,
    pytest_addoption,
    pytest_collection_modifyitems,
)

from validation.xtdb import XtdbQuirks


@pytest.fixture(scope="session", autouse=True)
def _default_uri() -> None:
    # Provide a sensible default so local runs don't require pre-setting env.
    os.environ.setdefault(
        "XTDB_FLIGHT_SQL_URI",
        f"grpc://{os.environ.get('XTDB_HOST', 'xtdb')}:9833",
    )


@pytest.fixture(scope="session")
def driver(request: pytest.FixtureRequest) -> XtdbQuirks:
    # test_*.py modules each call generate_tests(QUIRKS, metafunc) to
    # parametrize `driver` indirectly. This fixture turns the string param
    # back into the quirks instance.
    param = request.param
    assert param.startswith("xtdb:"), param
    return XtdbQuirks()


@pytest.fixture(scope="session")
def driver_path(driver: XtdbQuirks) -> str:
    # Resolve the adbc-driver-flightsql shared library that ships inside the
    # Python package. ADBC driver manager will dlopen this.
    pkg_dir = pathlib.Path(adbc_driver_flightsql.__file__).parent
    so = pkg_dir / "libadbc_driver_flightsql.so"
    if not so.exists():
        pytest.skip(f"libadbc_driver_flightsql not found at {so}")
    return str(so)
