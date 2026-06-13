import pytest
from datetime import date
from unittest.mock import patch

from ingestion.api_ingest.cli import parse_arguments


# ── parse_arguments() tests ──────────────────────────────────────────────────

def test_valid_dates():
    """
    parse_arguments() returns correct date objects when
    valid --start and --end are provided.
    """
    with patch("sys.argv", ["main.py", "--start", "2010-12-01", "--end", "2010-12-03"]):
        args = parse_arguments()

    assert args.start == date(2010, 12, 1)
    assert args.end == date(2010, 12, 3)


def test_invalid_start_date_format():
    """
    parse_arguments() exits with error when --start is not in YYYY-MM-DD format.
    """
    with patch("sys.argv", ["main.py", "--start", "01-12-2010", "--end", "2010-12-03"]):
        with pytest.raises(SystemExit):
            parse_arguments()


def test_invalid_end_date_format():
    """
    parse_arguments() exits with error when --end is not in YYYY-MM-DD format.
    """
    with patch("sys.argv", ["main.py", "--start", "2010-12-01", "--end", "03/12/2010"]):
        with pytest.raises(SystemExit):
            parse_arguments()


def test_start_after_end():
    """
    parse_arguments() exits with error when --start is after --end.
    """
    with patch("sys.argv", ["main.py", "--start", "2010-12-31", "--end", "2010-12-01"]):
        with pytest.raises(SystemExit):
            parse_arguments()


def test_missing_start():
    """
    parse_arguments() exits with error when --start is missing.
    """
    with patch("sys.argv", ["main.py", "--end", "2010-12-03"]):
        with pytest.raises(SystemExit):
            parse_arguments()


def test_missing_end():
    """
    parse_arguments() exits with error when --end is missing.
    """
    with patch("sys.argv", ["main.py", "--start", "2010-12-01"]):
        with pytest.raises(SystemExit):
            parse_arguments()


def test_same_start_and_end():
    """
    parse_arguments() accepts same date for --start and --end —
    valid single day fetch.
    """
    with patch("sys.argv", ["main.py", "--start", "2010-12-01", "--end", "2010-12-01"]):
        args = parse_arguments()

    assert args.start == args.end == date(2010, 12, 1)
