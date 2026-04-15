from adbc_drivers_validation.tests.query import (  # noqa: F401
    TestQuery,
    generate_tests,
)

from validation import xtdb


def pytest_generate_tests(metafunc) -> None:
    return generate_tests(xtdb.QUIRKS, metafunc)
