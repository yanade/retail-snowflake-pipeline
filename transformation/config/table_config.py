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


_CONFIGS: tuple[TableConfig, ...] = (
    TableConfig(
        source_table="payments",
        primary_key=["payment_id"],
        allowed_values={
            "payment_status": [
                "PENDING",
                "AUTHORIZED",
                "CAPTURED",
                "FAILED",
                "REFUNDED",
                "PARTIALLY_REFUNDED",
            ],
        },
    ),

    TableConfig(
        source_table="order_items",
        primary_key=["order_item_id"],
        not_null=["product_id", "unit_price"],
        non_zero=["quantity"],
    ),

    TableConfig(
        source_table="currencies",
        primary_key=["currency_code"],
    ),

    TableConfig(
        source_table="product_categories",
        primary_key=["category_id"],
    ),

    TableConfig(
        source_table="suppliers",
        primary_key=["supplier_id"],
    ),

    TableConfig(
        source_table="products",
        primary_key=["product_id"],
    ),

    TableConfig(
        source_table="customers",
        primary_key=["customer_id"],
    ),

    TableConfig(
        source_table="customer_addresses",
        primary_key=["address_id"],
    ),

    TableConfig(
        source_table="stores",
        primary_key=["store_id"],
    ),

    TableConfig(
        source_table="employees",
        primary_key=["employee_id"],
    ),

    TableConfig(
        source_table="exchange_rates",
        primary_key=["exchange_rate_id"],
    ),

    TableConfig(
        source_table="orders",
        primary_key=["order_id"],
        allowed_values={
            "order_status": [
                "PENDING",
                "PAID",
                "SHIPPED",
                "COMPLETED",
                "CANCELLED",
                "RETURNED",
            ],
        },
    ),
)


TABLE_CONFIGS: dict[str, TableConfig] = {c.source_table: c for c in _CONFIGS}