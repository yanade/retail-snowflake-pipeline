import argparse
from datetime import date

# ------------------------------------
# CLI argument parsing
# ------------------------------------


def parse_arguments():
    """
    Parse and validate command line arguments for the FX rates fetch script.

    Parses expected arguments from the command line,
    and converts date strings to date objects.

    Returns:
        argparse.Namespace with attributes:
            - start (date): start date for fetching rates, inclusive
            - end (date): end date for fetching rates, inclusive

    Raises:
        SystemExit: if required arguments are missing or dates are invalid
        ValueError: if date strings are not in YYYY-MM-DD format

    Usage:
        python main.py --start 2010-12-01 --end 2010-12-31
"""

    parser = argparse.ArgumentParser(
        description="Fetch FX rates for a given date range."
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date in YYYY-MM-DD format (2010-12-01)."
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End date in YYYY-MM-DD format (2010-12-31)."
    )

    args = parser.parse_args()

    # Convert string dates to date objects. Fail loudly if format is wrong

    try:
        args.start = date.fromisoformat(args.start)
        args.end = date.fromisoformat(args.end)
    except ValueError:
        parser.error(
            "Invalid date format — use YYYY-MM-DD, "
            "e.g. --start 2010-12-01 --end 2010-12-31"
        )

    # Validate date range makes logical sense
    if args.start > args.end:
        parser.error(
            f"--start {args.start} must not be after --end {args.end}"
        )

    return args