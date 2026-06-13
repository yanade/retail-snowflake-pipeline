import os
import logging
from utils.logger import setup_logging
from dotenv import load_dotenv
import requests
from datetime import date, timedelta, datetime, timezone
import json
from pathlib import Path


logger = setup_logging()


def load_config() -> dict:
    """Load configuration from environment variables.
    Returns:
        dict with keys:
            - api_key (str): ExchangeRate-API authentication key
            - base_currency (str): currency to convert FROM, e.g. 'GBP'
            - target_currencies (list[str]): currencies to convert TO,
              e.g. ['USD', 'EUR', 'JPY']

    Raises:
        ValueError: if any required environment variable is missing or invalid"""
    
    load_dotenv()  # Load .env file if it exists
    api_key = os.getenv("EXCHANGE_RATE_API_KEY")
    base_currency = os.getenv("BASE_CURRENCY")
    base_url = os.getenv("EXCHANGE_RATE_BASE_URL")
    output_dir = os.getenv("OUTPUT_PATH")

    # Validate all simple required variables in one place
    required_vars = {
        "EXCHANGE_RATE_API_KEY": api_key,
        "BASE_CURRENCY": base_currency,
        "EXCHANGE_RATE_BASE_URL": base_url,
        "OUTPUT_PATH": output_dir,
    }
    for var_name, var_value in required_vars.items():
        if not var_value:
            raise ValueError( f"{var_name} is not set. Add it to your .env file.")

    target_currencies = [
        cur.strip()
        for cur in os.getenv("TARGET_CURRENCIES", "").split(",") if cur.strip()
        ]

    # Separate check — different validation logic (list, not string)
    if not target_currencies:
        raise ValueError(
            "TARGET_CURRENCIES is not set or empty. Add it to your .env file."
        )

    config = {
        "api_key": api_key,
        "base_currency": base_currency,
        "base_url": base_url,
        "target_currencies": target_currencies,
        "output_dir": output_dir,
    }

    logger.info(
        "Config loaded — base: %s, targets: %s",
        base_currency,
        target_currencies
    )

    return config




def fetch_fx_rates(config: dict, target_date: date) -> dict:
    """Fetch FX rates from ExchangeRate-API for a specific date.
        Calls the historical rates endpoint and extracts rates only for
        the target currencies defined in config.

    Args:
        config (dict): configuration dictionary with keys:
            - api_key (str): API authentication key
            - base_currency (str): currency to convert FROM
            - target_currencies (list[str]): currencies to convert TO
        target_date (date): the date for which to fetch rates

    Returns:
        dict: mapping of target currency to its exchange rate against the base currency

    Raises:
        requests.HTTPError: if the API returns a non-200 status code
        ValueError: if the API response result is not 'success'
        KeyError: if a target currency is missing from the response
        requests.Timeout: if the API call exceeds the timeout limit"""
    
    api_key = config["api_key"]
    base_currency = config["base_currency"]
    target_currencies = config["target_currencies"]

    # Build the URL
    base_url = config["base_url"]
    url = (
        f"{base_url}?apikey={api_key}"
        f"&base_currency={base_currency}"
        f"&currencies={','.join(target_currencies)}"
        f"&date={target_date.isoformat()}"
    )

    logger.info("Fetching FX rates for %s on %s", base_currency, target_date)


    response = requests.get(url, timeout=10)  # 10 second timeout
    response.raise_for_status()  # Raise HTTPError for bad responses
    data = response.json()

    # Validate the API's own result field before processing rates
    if "data" not in data:
        raise ValueError(
            f"Unexpected API response for {target_date}: {data}"
        )

    # Extract rates for the requested date
    date_key = target_date.isoformat()
    all_rates = data["data"].get(date_key, {})

    if not all_rates:
        raise ValueError(
            f"No rates found in API response for date {target_date}."
        )

    # Extract only the currencies we need
    rates = {}
    for currency in target_currencies:
        if currency not in all_rates:
            raise KeyError(
                f"Currency '{currency}' not found in API response "
                f"for date {target_date}. Check TARGET_CURRENCIES in .env"
            )
        rates[currency] = all_rates[currency]

    logger.info(
        "Fetched rates for %s: %s",
        target_date, rates
    )

    return rates


def get_rates_for_date_range(
        config: dict,
        start_date: date,
        end_date: date
) -> dict:
    
    """
    Fetch exchange rates for every date in a given range.

    Calls fetch_fx_rates() for each date. If a single date fails due to
    a timeout or missing data, it is skipped and logged — the loop
    continues. If an unrecoverable HTTP error occurs, the function
    raises immediately.

    Args:
        config (dict): config dict returned by load_config()
        start_date (date): first date to fetch, inclusive
        end_date (date): last date to fetch, inclusive

    Returns:
        dict mapping date strings (YYYY-MM-DD) to rate dicts, e.g.
        {"2011-01-15": {"USD": 1.5823, "EUR": 1.1742}, ...}

    Raises:
        requests.HTTPError: if the API returns an unrecoverable error
        ValueError: if start_date is after end_date
    """
    if start_date > end_date:
        raise ValueError(
            f"start_date {start_date} must not be after end_date {end_date}"
        )

    all_rates = {}
    current_date = start_date

    while current_date <= end_date:
        try:
            rates = fetch_fx_rates(config, current_date)
            # Store with ISO date string as key for easy JSON serialisation
            all_rates[current_date.isoformat()] = rates
        
        except requests.Timeout:
            # Skip this date and keep going — timeout is recoverable
            logger.warning(
                "Timeout fetching rates for %s. Skipping this date.",
                current_date
            )

        except KeyError as e:
            # Currency missing from response — skip and log
            logger.warning(
                "Missing currency for %s: %s — skipping", current_date, e
            )

        except requests.HTTPError as e:
            # HTTP error is unrecoverable — stop everything
            logger.error(
                "HTTP error fetching rates for %s: %s", current_date, e
            )
            raise

        # Move to the next day regardless of success or skip
        current_date += timedelta(days=1)

    logger.info(
        "Completed fetching rates for range %s to %s. Total successful days: %d",
        start_date,
        end_date,
        len(all_rates)
    )
    return all_rates


def save_rates_to_json(
        rates: dict,
        output_dir: str,
        base_currency: str,
        target_currencies: list[str],
        start_date: date,
        end_date: date
) -> None:

    """
    Save the fetched exchange rates dictionary to a JSON file.

    Filename is built automatically from base currency, date range, and
    current timestamp — a new file is created on every run, nothing is
    overwritten.

    Args:
        rates (dict): exchange rates dict returned by get_rates_for_date_range()
            e.g. {"2011-01-15": {"USD": 1.5823, "EUR": 1.1742}, ...}
        output_dir (str): folder to write the file into,
            e.g. "ingestion/api_ingest/output/"
        base_currency (str): currency converted FROM, e.g. "GBP"
            used in the filename so content is clear without opening the file
        start_date (date): first date in the rates dict — used in filename
        target_currencies (list[str]): currencies converted TO, e.g. ["USD", "EUR"]
        included in output file metadata
        end_date (date): last date in the rates dict — used in filename

    Returns:
        None

    Raises:
        OSError: if the file cannot be written due to permissions
    """

    # Build timestamp string e.g. 20260611_143022
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

    # Build filename e.g. fx_rates_GBP_2011-01-15_2011-01-17_20260611_143022.json
    filename = (
        f"fx_rates"
        f"_{base_currency}"
        f"_{start_date.isoformat()}"
        f"_{end_date.isoformat()}"
        f"_{run_timestamp}.json"
    )

    # Join folder path and filename into a single Path object
    output_file_path = Path(output_dir) / filename

    # Create parent directories if they don't exist
    output_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Include metadata alongside rates for clarity
    output_payload = {
        "base_currency": base_currency,
        "target_currencies": target_currencies,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "rates": rates
    }

    # Write the rates dict to the specified JSON file
    with open(output_file_path, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, indent=2)

    logger.info("Saved %d date entries to %s", len(rates), output_file_path)


