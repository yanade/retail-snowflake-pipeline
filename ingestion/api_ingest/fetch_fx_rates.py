import argparse
import os
import logging
import time
from utils.logger import setup_logging
from utils.db import get_database_url
from dotenv import load_dotenv
import requests
import json
from datetime import date, timedelta, datetime, timezone
from decimal import Decimal
from pathlib import Path


logger = setup_logging()


DEFAULT_LOOKBACK_DAYS = 30  # same window as generate_data.py's default --days 30

# The free tier is rate limited per minute: pace requests, retry with backoff on 429
REQUEST_DELAY_SECONDS = 1.0
RATE_LIMIT_MAX_RETRIES = 3
RATE_LIMIT_BACKOFF_SECONDS = 20
HTTP_TOO_MANY_REQUESTS = 429
REQUEST_TIMEOUT_SECONDS = 10


def load_config() -> dict:
    """
    Load and validate the FX settings from the environment.

    Returns:
        Dict with api_key, base_currency, base_url, target_currencies, output_dir.

    Raises:
        ValueError: If a required variable is missing.
    """

    load_dotenv()  # .env, if present
    api_key = os.getenv("EXCHANGE_RATE_API_KEY")
    base_currency = os.getenv("BASE_CURRENCY")
    base_url = os.getenv("EXCHANGE_RATE_BASE_URL")
    output_dir = os.getenv("OUTPUT_PATH")

    # The required string settings
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

    # A list, so checked separately
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




def request_rates(base_url: str, params: dict, target_date: date) -> requests.Response:
    """
    GET the FX API, retrying on HTTP 429.

    Args:
        base_url: Endpoint URL without query parameters.
        params: Query parameters, including the API key.
        target_date: Date being fetched, for log messages only.

    Returns:
        The successful response.

    Raises:
        requests.HTTPError: On an error status or a persistent rate limit, without the key-bearing URL.
        requests.Timeout: If a request exceeds REQUEST_TIMEOUT_SECONDS.
    """

    for attempt in range(1, RATE_LIMIT_MAX_RETRIES + 1):
        response = requests.get(base_url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)

        if response.status_code != HTTP_TOO_MANY_REQUESTS:
            break

        if attempt == RATE_LIMIT_MAX_RETRIES:
            raise requests.HTTPError(
                f"Rate limited by the FX API on {target_date} after "
                f"{RATE_LIMIT_MAX_RETRIES} attempts. Try again later or "
                "fetch a shorter date range."
            )

        # Retry-After wins when the server sends it
        wait_seconds = int(
            response.headers.get(
                "Retry-After", RATE_LIMIT_BACKOFF_SECONDS * attempt
            )
        )
        logger.warning(
            "Rate limited on %s. Waiting %d seconds before retry %d of %d.",
            target_date,
            wait_seconds,
            attempt + 1,
            RATE_LIMIT_MAX_RETRIES,
        )
        time.sleep(wait_seconds)

    try:
        response.raise_for_status()
    except requests.HTTPError:
        # The original message holds the URL with the API key; from None keeps it out of the traceback
        raise requests.HTTPError(
            f"FX API returned {response.status_code} for {target_date}."
        ) from None

    return response


def fetch_fx_rates(config: dict, target_date: date) -> dict:
    """
    Fetch the target currencies' rates for one date.

    Args:
        config: Output of load_config().
        target_date: Date to fetch.

    Returns:
        Mapping of target currency to its rate against the base currency.

    Raises:
        requests.HTTPError: On an error status.
        ValueError: If the response has no rates for the date.
        KeyError: If a target currency is missing from the response.
        requests.Timeout: If the call times out.
    """

    api_key = config["api_key"]
    base_currency = config["base_currency"]
    target_currencies = config["target_currencies"]

    # params, not a hand-built URL, so the key is never in a string this module formats
    base_url = config["base_url"]
    params = {
        "apikey": api_key,
        "base_currency": base_currency,
        "currencies": ",".join(target_currencies),
        "date": target_date.isoformat(),
    }

    logger.info("Fetching FX rates for %s on %s", base_currency, target_date)

    response = request_rates(base_url, params, target_date)
    data = response.json()

    # No data field means the API returned an error payload
    if "data" not in data:
        raise ValueError(
            f"Unexpected API response for {target_date}: {data}"
        )

    date_key = target_date.isoformat()
    all_rates = data["data"].get(date_key, {})

    if not all_rates:
        raise ValueError(
            f"No rates found in API response for date {target_date}."
        )

    # Keep only the requested currencies
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
    Fetch rates for every date in an inclusive range.

    A timeout or missing currency skips that date; an HTTP error stops the run.

    Args:
        config: Output of load_config().
        start_date: First date, inclusive.
        end_date: Last date, inclusive.

    Returns:
        Mapping of ISO date to {currency: rate}.

    Raises:
        requests.HTTPError: On an unrecoverable API error.
        ValueError: If start_date is after end_date.
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
            all_rates[current_date.isoformat()] = rates  # ISO string key, so the dict serialises to JSON

        except requests.Timeout:
            # Recoverable: skip this date
            logger.warning(
                "Timeout fetching rates for %s. Skipping this date.",
                current_date
            )

        except KeyError as e:
            # Missing currency: skip this date
            logger.warning(
                "Missing currency for %s: %s — skipping", current_date, e
            )

        except requests.HTTPError as e:
            # Unrecoverable: stop the run
            logger.error(
                "HTTP error fetching rates for %s: %s", current_date, e
            )
            raise

        current_date += timedelta(days=1)

        # Pace requests under the per-minute limit; no wait after the last date
        if current_date <= end_date:
            time.sleep(REQUEST_DELAY_SECONDS)

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
    Write the rates to a new timestamped JSON file; nothing is overwritten.

    Args:
        rates: Output of get_rates_for_date_range().
        output_dir: Folder to write into.
        base_currency: Base currency, used in the filename.
        target_currencies: Target currencies, stored as metadata.
        start_date: First date, used in the filename.
        end_date: Last date, used in the filename.

    Raises:
        OSError: If the file cannot be written.
    """

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")  # e.g. 20260611_143022_123456

    # e.g. fx_rates_GBP_2011-01-15_2011-01-17_<timestamp>.json
    filename = (
        f"fx_rates"
        f"_{base_currency}"
        f"_{start_date.isoformat()}"
        f"_{end_date.isoformat()}"
        f"_{run_timestamp}.json"
    )

    output_file_path = Path(output_dir) / filename
    output_file_path.parent.mkdir(parents=True, exist_ok=True)

    output_payload = {
        "base_currency": base_currency,
        "target_currencies": target_currencies,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "rates": rates
    }

    with open(output_file_path, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, indent=2)

    logger.info("Saved %d date entries to %s", len(rates), output_file_path)


def upsert_rates_to_postgres(
    rates: dict,
    database_url: str,
    base_currency: str,
    source_system: str = "freecurrencyapi",
) -> int:
    """
    Upsert rates into retail_oltp.exchange_rates, updating a row only when it changed.

    Args:
        rates: Mapping of ISO date to {currency: rate}.
        database_url: PostgreSQL connection string.
        base_currency: Currency all rates convert from.
        source_system: Value stored in source_system.

    Returns:
        Rows sent, or 0 without connecting when rates is empty.
    """

    rows = [
        {
            "rate_date": date.fromisoformat(date_str),
            "base_currency_code": base_currency,
            "target_currency_code": currency_code,
            "exchange_rate": Decimal(str(rate)),
            "source_system": source_system,
        }
        for date_str, currency_rates in rates.items()
        for currency_code, rate in currency_rates.items()
    ]

    if not rows:
        logger.info("No rates to upsert — skipping PostgreSQL write.")
        return 0

    import psycopg  # lazy: only --write-postgres needs it

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                insert into retail_oltp.exchange_rates
                    (
                        rate_date,
                        base_currency_code,
                        target_currency_code,
                        exchange_rate,
                        source_system
                    )
                values
                    (
                        %(rate_date)s,
                        %(base_currency_code)s,
                        %(target_currency_code)s,
                        %(exchange_rate)s,
                        %(source_system)s
                    )
                on conflict (rate_date, base_currency_code, target_currency_code) do update
                set
                    exchange_rate = excluded.exchange_rate,
                    source_system = excluded.source_system
                where (exchange_rates.exchange_rate, exchange_rates.source_system)
                    is distinct from (excluded.exchange_rate, excluded.source_system)
                """,
                rows,
            )

    logger.info("Sent %d exchange rate rows; unchanged ones are left untouched.", len(rows))
    return len(rows)


def parse_args() -> argparse.Namespace:
    """
    Parse the CLI arguments.

    Returns:
        Namespace with start, end and write_postgres.
    """

    parser = argparse.ArgumentParser(
        description="Fetch historical FX rates and optionally load them into PostgreSQL."
    )

    default_end = datetime.now(timezone.utc).date()
    default_start = default_end - timedelta(days=DEFAULT_LOOKBACK_DAYS - 1)  # inclusive range: one less than the window

    parser.add_argument(
        "--start",
        type=date.fromisoformat,  # a bad date fails at parse time
        default=default_start,
        help="First date to fetch, inclusive (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=default_end,
        help="Last date to fetch, inclusive (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--write-postgres",
        action="store_true",
        default=False,
        help=(
            "Also upsert fetched rates into retail_oltp.exchange_rates. "
            "Requires DATABASE_URL. Without this flag the script only writes JSON."
        ),
    )

    return parser.parse_args()


def main() -> None:
    """
    Fetch a date range, save it as JSON, and optionally upsert it into PostgreSQL.

    Raises:
        RuntimeError: If no date in the range could be fetched.
    """

    args = parse_args()
    config = load_config()

    rates = get_rates_for_date_range(config, args.start, args.end)

    # Every date failed: a key or quota problem, so stop before writing anything
    if not rates:
        raise RuntimeError(
            f"No rates fetched for {args.start} to {args.end}. "
            "Check EXCHANGE_RATE_API_KEY and your API quota."
        )

    save_rates_to_json(
        rates=rates,
        output_dir=config["output_dir"],
        base_currency=config["base_currency"],
        target_currencies=config["target_currencies"],
        start_date=args.start,
        end_date=args.end,
    )

    if args.write_postgres:
        row_count = upsert_rates_to_postgres(
            rates=rates,
            database_url=get_database_url(),
            base_currency=config["base_currency"],
        )
        logger.info("Wrote %d exchange rate rows to PostgreSQL.", row_count)
    else:
        logger.info("JSON only. Pass --write-postgres to load into the database.")


if __name__ == "__main__":
    main()
