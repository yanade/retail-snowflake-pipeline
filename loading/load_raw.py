"""Load every served export in the window into raw, then check each one (ADR-019, ADR-020)."""

import argparse
import logging
from collections.abc import Sequence

from snowflake.connector import DictCursor

from loading.manifest_reader import LOAD_WINDOW_DAYS, ManifestExport, read_manifest
from loading.raw_loader import (
    ExportCheck,
    connect_snowflake,
    copy_table,
    fetch_loaded_counts,
    fetch_raw_columns,
    find_schema_drift,
    reconcile,
)
from utils.logger import setup_logging

logger = setup_logging()
for noisy in ("databricks.sql", "snowflake.connector"):
    logging.getLogger(noisy).setLevel(logging.WARNING)  # drivers log every HTTP call at INFO
logging.getLogger("botocore").setLevel(logging.ERROR)  # the Snowflake driver probes AWS credentials we don't use


def group_files_by_table(exports: Sequence[ManifestExport]) -> dict[str, list[str]]:
    """
    Files of all exports, per table, in manifest order.

    Args:
        exports: Exports as read_manifest() returns them.

    Returns:
        Table name to the files of all its exports.
    """
    files_by_table: dict[str, list[str]] = {}
    for export in exports:
        files_by_table.setdefault(export.table_name, []).extend(export.files)
    return files_by_table


def load_raw(window_days: int = LOAD_WINDOW_DAYS, dry_run: bool = False) -> list[ExportCheck]:
    """
    COPY every export in the window into raw and check each is there exactly once.

    Args:
        window_days: How far back to read the manifest, in days.
        dry_run: Only report what would be loaded; Snowflake is not touched.

    Returns:
        One ExportCheck per export; empty for a dry run or an empty window.

    Raises:
        RuntimeError: If any raw table differs from the column contract, before anything is loaded,
            or if any export is not in raw exactly once.
    """
    exports = read_manifest(window_days)
    files_by_table = group_files_by_table(exports)
    logger.info("%d exports, %d tables in the last %d days", len(exports), len(files_by_table), window_days)
    if not exports:
        return []  # nothing to load, so no warehouse wakes up
    if dry_run:
        for table, files in files_by_table.items():
            logger.info("Would load %s: %d files", table, len(files))
        return []

    loaded_counts: dict[str, int] = {}
    with connect_snowflake() as conn, conn.cursor(DictCursor) as cursor:
        drift = find_schema_drift(fetch_raw_columns(cursor))
        if drift:
            raise RuntimeError(f"Raw tables differ from the column contract, nothing loaded: {drift}")
        for table, files in files_by_table.items():
            loads = copy_table(cursor, table, files)
            new = [load for load in loads if load.status == "LOADED"]
            logger.info("%s: %d files loaded, %d rows, %d skipped",
                        table, len(new), sum(load.rows_loaded for load in new), len(loads) - len(new))
            loaded_counts |= fetch_loaded_counts(cursor, table, files)

    checks = reconcile(exports, loaded_counts)
    for check in checks:
        logger.info("%s run %s: expected %d, in raw %d, %s",
                    check.table_name, check.run_id, check.expected, check.actual, "OK" if check.ok else "FAIL")
    failed = [check for check in checks if not check.ok]
    if failed:
        raise RuntimeError(f"{len(failed)} of {len(checks)} exports not in raw exactly once: {failed}")
    return checks


def parse_args() -> argparse.Namespace:
    """
    Parse the CLI arguments.

    Returns:
        Namespace with window_days and dry_run.
    """
    parser = argparse.ArgumentParser(description="Load served exports from the manifest into Snowflake raw.")
    parser.add_argument("--window-days", type=int, default=LOAD_WINDOW_DAYS,
                        help=f"Days of manifest to load (default {LOAD_WINDOW_DAYS}, ADR-020).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be loaded without touching Snowflake.")
    args = parser.parse_args()
    if args.window_days < 1:
        parser.error(f"--window-days must be at least 1, got {args.window_days}")
    return args


def main() -> None:
    """Run the load from the command line."""
    args = parse_args()
    load_raw(window_days=args.window_days, dry_run=args.dry_run)


if __name__ == "__main__":
    main()