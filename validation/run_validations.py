"""Validate one load window with DVT and exit 1 on any mismatch (ADR-020)."""

import argparse
import json
import os
import sys
import tempfile
import uuid
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

from utils.logger import setup_logging
from validation.dvt_suite import (
    CONN_HOME_VARIABLE,
    COUNT_TABLES,
    RECONCILIATIONS,
    add_connections,
    count_command,
    failed_rows,
    missing_tables,
    parse_window_end,
    reconciliation_command,
    run_dvt,
)

RESULTS_DIR = Path(__file__).resolve().parent / "results"  # gitignored, one file per run

logger = setup_logging()


def run_suite(window_end: datetime, run_id: str, env: Mapping[str, str]) -> list[dict]:
    """
    Run every check for one load window, each row labelled with its check.

    Args:
        window_end: Aware UTC datetime from parse_window_end().
        run_id: Id shared by every validation of this run.
        env: Environment with DATABASE_URL and the SNOWFLAKE_* variables.

    Returns:
        Every validation row, with a 'check' field added.
    """
    with tempfile.TemporaryDirectory() as conn_home:  # connection files hold a password; gone after the run
        add_connections(env, conn_home)
        dvt_env = {**env, CONN_HOME_VARIABLE: conn_home}

        count_rows = run_dvt(count_command(window_end, run_id), dvt_env)
        missing = missing_tables(count_rows, COUNT_TABLES)
        if missing:
            raise RuntimeError(f"DVT returned no result for: {', '.join(missing)}")
        rows = [{**row, "check": row["source_table_name"]} for row in count_rows]

        for check in RECONCILIATIONS:
            check_rows = run_dvt(reconciliation_command(check, window_end, run_id), dvt_env)
            rows += [{**row, "check": check.table} for row in check_rows]  # DVT leaves the table name empty here
    return rows


def describe(row: dict) -> str:
    """One result row as a log line: check, aggregate, group, both values, status."""
    group = f" {row['group_by_columns']}" if row.get("group_by_columns") else ""
    return (
        f"{row.get('check')} {row.get('validation_name')}{group}: "
        f"source {row.get('source_agg_value')}, target {row.get('target_agg_value')}, {row.get('validation_status')}"
    )


def write_results(rows: list[dict], run_id: str, results_dir: Path) -> Path:
    """
    Save every row of the run as JSON for the audit table and the dashboard.

    Args:
        rows: Rows from run_suite().
        run_id: Names the file.
        results_dir: Folder for result files.

    Returns:
        The file written.
    """
    results_dir.mkdir(parents=True, exist_ok=True)
    path = results_dir / f"{run_id}.json"
    path.write_text(json.dumps(rows, indent=2))
    return path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Command line: --window-end is required, --run-id defaults to a new UUID."""
    parser = argparse.ArgumentParser(description="Validate one load window with DVT.")
    parser.add_argument("--window-end", required=True,
                        help="UTC end of the ADF load window, e.g. '2026-10-03 15:25:24'")
    parser.add_argument("--run-id", default=str(uuid.uuid4()),
                        help="Id shared by every validation; Airflow passes its run id")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None, results_dir: Path = RESULTS_DIR) -> int:
    """
    Validate one window and return the exit code: 0 all matched, 1 any mismatch.

    Args:
        argv: Command line arguments; None reads sys.argv.
        results_dir: Folder for the result file.

    Returns:
        The process exit code. A crash raises instead, which also exits non-zero.
    """
    args = parse_args(argv)
    window_end = parse_window_end(args.window_end)
    logger.info("run %s, window end %s", args.run_id, window_end.isoformat())

    rows = run_suite(window_end, args.run_id, os.environ)
    path = write_results(rows, args.run_id, results_dir)

    failures = failed_rows(rows)
    for row in failures:
        logger.error(describe(row))
    logger.info("%d validations, %d failed, results in %s", len(rows), len(failures), path)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
