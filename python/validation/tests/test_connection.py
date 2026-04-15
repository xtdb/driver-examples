from adbc_drivers_validation.tests.connection import (  # noqa: F401
    TestConnection,
    generate_tests,
)

from validation import xtdb


def pytest_generate_tests(metafunc) -> None:
    return generate_tests(xtdb.QUIRKS, metafunc)
