import pytest
from datetime import date


@pytest.fixture
def sample_config() -> dict:
    """
    Shared test configuration fixture.

    Returns a minimal valid config dict that mirrors what
    load_config() returns in production. Used across all test files
    to avoid repeating the same setup code.
    """
    return {
        "api_key": "test_api_key",
        "base_currency": "GBP",
        "base_url": "https://api.freecurrencyapi.com/v1/historical",
        "target_currencies": ["USD", "EUR"],
        "output_dir": "tests/output/",
    }


@pytest.fixture
def sample_rates() -> dict:
    """
    Shared sample rates fixture.

    Returns a minimal valid rates dict that mirrors what
    get_rates_for_date_range() returns in production.
    """
    return {
        "2010-12-01": {"USD": 1.5619, "EUR": 1.1891},
        "2010-12-02": {"USD": 1.5592, "EUR": 1.1802},
        "2010-12-03": {"USD": 1.5705, "EUR": 1.1751},
    }


@pytest.fixture
def start_date() -> date:
    """Start date used across tests."""
    return date(2010, 12, 1)


@pytest.fixture
def end_date() -> date:
    """End date used across tests."""
    return date(2010, 12, 3)
