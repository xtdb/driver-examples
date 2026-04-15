from adbc_drivers_validation.tests.ingest import (  # noqa: F401
    TestIngest,
    generate_tests,
)

from validation import xtdb


def pytest_generate_tests(metafunc) -> None:
    return generate_tests(xtdb.QUIRKS, metafunc)
