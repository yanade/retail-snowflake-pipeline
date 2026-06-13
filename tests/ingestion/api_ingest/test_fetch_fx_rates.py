import json
import pytest
import requests
from datetime import date
from unittest.mock import patch, MagicMock
from pathlib import Path

from ingestion.api_ingest.fetch_fx_rates import (
    load_config,
    fetch_fx_rates,
    save_rates_to_json,
)


# ── load_config() tests ──────────────────────────────────────────────────────

def test_load_config_success(monkeypatch):
    """
    load_config() returns a complete config dict when all
    required environment variables are set.
    """
    monkeypatch.setenv("EXCHANGE_RATE_API_KEY", "test_key")
    monkeypatch.setenv("BASE_CURRENCY", "GBP")
    monkeypatch.setenv("EXCHANGE_RATE_BASE_URL", "https://api.freecurrencyapi.com/v1/historical")
    monkeypatch.setenv("OUTPUT_PATH", "tests/output/")
    monkeypatch.setenv("TARGET_CURRENCIES", "USD,EUR")

    config = load_config()

    assert config["api_key"] == "test_key"
    assert config["base_currency"] == "GBP"
    assert config["target_currencies"] == ["USD", "EUR"]
    assert config["output_dir"] == "tests/output/"


def test_load_config_missing_api_key(monkeypatch):
    """
    load_config() raises ValueError when EXCHANGE_RATE_API_KEY is missing.
    """
    monkeypatch.delenv("EXCHANGE_RATE_API_KEY", raising=False)
    monkeypatch.setenv("BASE_CURRENCY", "GBP")
    monkeypatch.setenv("EXCHANGE_RATE_BASE_URL", "https://api.freecurrencyapi.com/v1/historical")
    monkeypatch.setenv("OUTPUT_PATH", "tests/output/")
    monkeypatch.setenv("TARGET_CURRENCIES", "USD,EUR")

    # prevent load_dotenv() from reading real .env and restoring the key
    with patch("ingestion.api_ingest.fetch_fx_rates.load_dotenv"):
        with pytest.raises(ValueError, match="EXCHANGE_RATE_API_KEY"):
            load_config()


def test_load_config_missing_target_currencies(monkeypatch):
    """
    load_config() raises ValueError when TARGET_CURRENCIES is empty.
    """
    monkeypatch.setenv("EXCHANGE_RATE_API_KEY", "test_key")
    monkeypatch.setenv("BASE_CURRENCY", "GBP")
    monkeypatch.setenv("EXCHANGE_RATE_BASE_URL", "https://api.freecurrencyapi.com/v1/historical")
    monkeypatch.setenv("OUTPUT_PATH", "tests/output/")
    monkeypatch.setenv("TARGET_CURRENCIES", "")

    with pytest.raises(ValueError, match="TARGET_CURRENCIES"):
        load_config()


# ── fetch_fx_rates() tests ───────────────────────────────────────────────────

def test_fetch_fx_rates_success(sample_config):
    """
    fetch_fx_rates() returns correct rates dict when API responds successfully.
    """
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": {
            "2010-12-01": {"USD": 1.5619, "EUR": 1.1891}
        }
    }
    mock_response.raise_for_status = MagicMock()

    with patch("ingestion.api_ingest.fetch_fx_rates.requests.get", return_value=mock_response):
        rates = fetch_fx_rates(sample_config, date(2010, 12, 1))

    assert rates == {"USD": 1.5619, "EUR": 1.1891}


def test_fetch_fx_rates_http_error(sample_config):
    """
    fetch_fx_rates() raises HTTPError when API returns a 4xx response.
    """
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("403 Forbidden")

    with patch("ingestion.api_ingest.fetch_fx_rates.requests.get", return_value=mock_response):
        with pytest.raises(requests.HTTPError):
            fetch_fx_rates(sample_config, date(2010, 12, 1))


def test_fetch_fx_rates_missing_currency(sample_config):
    """
    fetch_fx_rates() raises KeyError when a target currency
    is missing from the API response.
    """
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": {
            "2010-12-01": {"USD": 1.5619}  # EUR is missing
        }
    }
    mock_response.raise_for_status = MagicMock()

    with patch("ingestion.api_ingest.fetch_fx_rates.requests.get", return_value=mock_response):
        with pytest.raises(KeyError, match="EUR"):
            fetch_fx_rates(sample_config, date(2010, 12, 1))


# ── save_rates_to_json() tests ───────────────────────────────────────────────

def test_save_rates_to_json_creates_file(sample_config, sample_rates, start_date, end_date, tmp_path):
    """
    save_rates_to_json() creates a JSON file with correct metadata and rates.
    """
    save_rates_to_json(
        rates=sample_rates,
        output_dir=str(tmp_path),
        base_currency=sample_config["base_currency"],
        target_currencies=sample_config["target_currencies"],
        start_date=start_date,
        end_date=end_date,
    )

    # find the created file
    files = list(tmp_path.glob("fx_rates_*.json"))
    assert len(files) == 1

    # read and verify content
    content = json.loads(files[0].read_text())
    assert content["base_currency"] == "GBP"
    assert content["target_currencies"] == ["USD", "EUR"]
    assert content["rates"] == sample_rates


def test_save_rates_to_json_unique_filenames(sample_config, sample_rates, start_date, end_date, tmp_path):
    """
    save_rates_to_json() creates a new unique file on every run —
    no overwriting.
    """
    save_rates_to_json(
        rates=sample_rates,
        output_dir=str(tmp_path),
        base_currency=sample_config["base_currency"],
        target_currencies=sample_config["target_currencies"],
        start_date=start_date,
        end_date=end_date,
    )
    save_rates_to_json(
        rates=sample_rates,
        output_dir=str(tmp_path),
        base_currency=sample_config["base_currency"],
        target_currencies=sample_config["target_currencies"],
        start_date=start_date,
        end_date=end_date,
    )

    files = list(tmp_path.glob("fx_rates_*.json"))
    assert len(files) == 2  # two separate files created
