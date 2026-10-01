"""Plan each served export from the table's manifest history (ADR-020)."""

from dataclasses import dataclass

SNAPSHOT_MODE = "snapshot"
CDF_MODE = "cdf"


@dataclass(frozen=True)
class ExportPlan:
    """What one export reads: a whole table, or a range of versions."""

    export_mode: str
    start_version: int | None
    end_version: int


def plan_export(
    table: str,
    current_version: int,
    current_table_id: str,
    last_end_version: int | None,
    last_table_id: str | None,
    full_reload: bool,
) -> ExportPlan | None:
    """
    Choose the export mode and version range for one curated table.

    Args:
        table: Curated table name, for error messages.
        current_version: Pinned Delta version.
        current_table_id: Delta table id from DESCRIBE DETAIL.
        last_end_version: max(end_version) from the manifest, None if no rows.
        last_table_id: table_id of the latest manifest row.
        full_reload: Export the whole table regardless of history.

    Returns:
        The plan, or None when nothing was committed since the last export.

    Raises:
        ValueError: No history, rebuilt table, or manifest ahead of the table.
    """
    if full_reload:  # first: it is the remedy for every check below
        return ExportPlan(SNAPSHOT_MODE, None, current_version)
    if last_end_version is None:
        raise ValueError(f"{table}: no export history; run with full_reload=true")
    if last_table_id != current_table_id:
        raise ValueError(f"{table}: table was rebuilt; run with full_reload=true")
    if current_version < last_end_version:
        raise ValueError(f"{table}: manifest is ahead of the table; run with full_reload=true")
    if current_version == last_end_version:
        return None

    return ExportPlan(CDF_MODE, last_end_version + 1, current_version)
