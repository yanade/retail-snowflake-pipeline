"""Rows for pipeline_audit: the status vocabulary and the rules every row must meet (ADR-026)."""

from dataclasses import dataclass
from datetime import datetime

SUCCESS, FAILED, SKIPPED = "SUCCESS", "FAILED", "SKIPPED"  # did the task run, Airflow's words
DVT_MATCH, DVT_MISMATCH, DVT_SKIPPED = "MATCH", "MISMATCH", "SKIPPED"  # did source and target agree
STATUSES = (SUCCESS, FAILED, SKIPPED)
DVT_STATUSES = (DVT_MATCH, DVT_MISMATCH, DVT_SKIPPED)


@dataclass(frozen=True)
class TaskOutcome:
    """What one task produced, before Airflow adds which run and when."""

    status: str
    dvt_status: str | None = None
    rows_ingested: int | None = None  # None means not measured, 0 means measured and none
    rows_failed: int | None = None
    error_message: str | None = None
    details: dict | None = None

    def __post_init__(self) -> None:
        """Reject combinations the audit contract does not allow; Snowflake has no CHECK constraints."""
        if self.status not in STATUSES:
            raise ValueError(f"status must be one of {STATUSES}, got {self.status!r}")
        if self.dvt_status not in (None, *DVT_STATUSES):
            raise ValueError(f"dvt_status must be one of {DVT_STATUSES} or None, got {self.dvt_status!r}")
        if self.status == FAILED and not self.error_message:
            raise ValueError("a FAILED outcome needs an error_message")
        if self.dvt_status == DVT_MATCH and self.status != SUCCESS:
            raise ValueError("dvt_status MATCH needs status SUCCESS")
        if self.dvt_status == DVT_MISMATCH and self.status != FAILED:
            raise ValueError("dvt_status MISMATCH needs status FAILED")  # the runner exits 1, so Airflow fails the task
        for name in ("rows_ingested", "rows_failed"):
            if (getattr(self, name) or 0) < 0:
                raise ValueError(f"{name} cannot be negative, got {getattr(self, name)}")


@dataclass(frozen=True)
class AuditRecord:
    """One pipeline_audit row: which task of which run, and what it produced."""

    dag_name: str
    run_id: str
    task_name: str
    outcome: TaskOutcome
    try_number: int | None = None
    started_at: datetime | None = None  # UTC
    finished_at: datetime | None = None  # UTC

    def __post_init__(self) -> None:
        """Reject an empty key part; NOT NULL in Snowflake still accepts ''."""
        for name in ("dag_name", "run_id", "task_name"):
            if not getattr(self, name):
                raise ValueError(f"{name} must not be empty")
