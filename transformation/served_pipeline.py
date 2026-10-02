"""Export one curated table to served and record it in the manifest (ADR-020)."""

from datetime import date, datetime

from pyspark.sql import SparkSession

from transformation.served_export import curated_state, read_changes, read_snapshot, write_served
from transformation.served_manifest import (
    SNAPSHOT_MODE,
    ManifestRow,
    append_manifest_row,
    plan_export,
    read_last_export,
)


def export_table(
    spark: SparkSession,
    curated_root: str,
    served_root: str,
    table: str,
    run_id: str,
    run_date: date,
    full_reload: bool,
    written_at: datetime,
    export_id: str,
) -> dict | None:
    """
    Export what changed in one curated table since its last export.

    Args:
        spark: Active SparkSession.
        curated_root: Root of the curated zone.
        served_root: Root of the served zone.
        table: Source table name.
        run_id: Airflow run id.
        run_date: UTC run date.
        full_reload: Export the whole table.
        written_at: UTC manifest timestamp.
        export_id: Id of this export attempt.

    Returns:
        Summary for the audit record, or None if nothing changed.
    """
    version, table_id = curated_state(spark, curated_root, table)  # pin first
    last_end_version, last_table_id = read_last_export(spark, curated_root, table)
    plan = plan_export(table, version, table_id, last_end_version, last_table_id, full_reload)
    if plan is None:
        return None

    if plan.export_mode == SNAPSHOT_MODE:
        df = read_snapshot(spark, curated_root, table, plan.end_version)
    else:
        df = read_changes(spark, curated_root, table, plan.start_version, plan.end_version)

    files, row_count = write_served(spark, df, served_root, table, run_date, export_id)

    append_manifest_row(spark, curated_root, ManifestRow(
        run_id=run_id,
        table_name=table,
        table_id=table_id,
        export_mode=plan.export_mode,
        start_version=plan.start_version,
        end_version=plan.end_version,
        files=files,
        row_count=row_count,
        written_at=written_at,
    ))  # commit point

    return {
        "export_mode": plan.export_mode,
        "start_version": plan.start_version,
        "end_version": plan.end_version,
        "row_count": row_count,
        "file_count": len(files),
    }