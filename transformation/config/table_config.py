"""
Table-level configuration for the raw-to-curated transformation.

This module defines metadata and data-quality rules that are specific
to each source table. The transformation logic itself lives elsewhere.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TableConfig:
    """
    Configuration for one source table
    Attributes:
        source_table: Name of the source table in PostgreSQL and its
            corresponding raw-data directory.
        primary_key: Source column(s) that uniquely identify a record.
            A list is used to support both single and composite keys.
        watermark_column: Column used to identify new or changed records.
        not_null: Columns that must not contain NULL values.
        non_zero: Numeric columns that must not contain zero values.
        allowed_values: Allowed values for categorical columns.
    """

    source_table: str
    primary_key: list[str]

    watermark_column: str = "updated_at"

    not_null: list[str] = field(default_factory=list)
    non_zero: list[str] = field(default_factory=list)

    allowed_values: dict[str, list[str]] = field(default_factory=dict)

    TableConfig(
        source_table = "payments"
        primary_key = ["payment_id]"]
        
    )