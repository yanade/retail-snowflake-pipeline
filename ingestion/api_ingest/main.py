import logging
from utils.logger import setup_logging
from utils.db import get_database_url
import os
import sys
from urllib.parse import urlparse
from ingestion.api_ingest.cli import parse_arguments
from ingestion.api_ingest.fetch_fx_rates import (
    load_config,
    get_rates_for_date_range,
    save_rates_to_json,
    upsert_rates_to_postgres,
)


logger = setup_logging()


def main() -> None:
    """
    Main entry point for the FX rates fetch script.

    Orchestrates argument parsing, config loading, API fetching
    and saving results to a JSON file.

    Usage:
        python main.py --start 2010-12-01 --end 2010-12-31

    Exit codes:
        0 — success
        1 — failure (config error, API error, file write error)
    """
    
    # step 1: Parse and validate command line arguments
    args = parse_arguments()

    # step 2: Load configuration .env. Fail laudly if missing or invalid
    try:
        config = load_config()
    except ValueError as e:
        logger.error("Configuration error: %s", e)
        sys.exit(1)

    # step 3: Fetch FX rates for the specified date range
    try:
        rates = get_rates_for_date_range(
            config,
            args.start,
            args.end
        )
    except Exception as e:
        logger.error("Error fetching FX rates: %s", e)
        sys.exit(1)

    # step 4: Save the fetched rates to a uniquely name JSON file
    try:
        save_rates_to_json(
            rates=rates,
            output_dir=config["output_dir"],
            base_currency=config["base_currency"],
            target_currencies=config["target_currencies"],
            start_date=args.start,
            end_date=args.end
        )
    except OSError as e:
        logger.error("Failed to save rates to file: %s", e)
        sys.exit(1)

    # step 5: optionally upsert the same rates into PostgreSQL
    if args.write_postgres:
        database_url = get_database_url()
        parsed = urlparse(database_url)
        logger.info(
            "Connecting to host=%s database=%s schema=retail_oltp",
            parsed.hostname,
            parsed.path.lstrip("/"),
        )
        try:
            row_count = upsert_rates_to_postgres(
                rates=rates,
                database_url=database_url,
                base_currency=config["base_currency"],
            )
        except Exception as e:
            logger.error("Failed to write rates to PostgreSQL: %s", e)
            sys.exit(1)

        logger.info("Upserted %d rows into retail_oltp.exchange_rates", row_count)

    logger.info(
        "Script completed successfully — %d dates fetched",
        len(rates)
    )
    sys.exit(0)

# Only runs when executed directly — not when imported as a module
if __name__ == "__main__":
    main()
