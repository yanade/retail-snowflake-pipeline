"""
Apply per-table data quality rules and split rows into valid and rejected.

Every rule a row breaks is recorded in _dq_errors, not just the first, so a
dead-letter row shows everything wrong with it. Reason codes are stable
strings: the reprocess DAG and the dashboard match on them.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from transformation.config.table_config import TABLE_CONFIGS, TableConfig
from transformation.raw_reader import CORRUPT_RECORD_COLUMN
from transformation.type_casting import CAST_ERRORS_COLUMN

DQ_ERRORS_COLUMN = "_dq_errors"          # every reason code a row fails; empty means valid
MALFORMED_JSON_REASON = "malformed_json"
INVALID_TYPE_PREFIX = "invalid_type_"


def _reason_if(condition: Column, reason: str) -> Column:
    """
    Return a reason code when a condition holds.

    Args:
        condition: Boolean Column that is true when the row breaks the rule.
        reason: Reason code to record.

    Returns:
        The reason as a string literal when the condition is true, else NULL.
    """
    return F.when(condition, F.lit(reason))


def _rule_checks(config: TableConfig) -> list[Column]:
    """
    Build one check per configured rule for a table.

    Args:
        config: The table's TableConfig.

    Returns:
        One Column per rule: its reason code if the row breaks it, else NULL.
    """
    checks = [_reason_if(F.col(c).isNull(), f"null_{c}") for c in config.primary_key]  # MERGE cannot match a NULL key
    checks += [_reason_if(F.col(c).isNull(), f"null_{c}") for c in config.not_null]
    checks += [_reason_if(F.col(c) == 0, f"zero_{c}") for c in config.non_zero]
    checks += [
        # isin() on NULL returns NULL, not false; coalesce makes NULL a failure
        _reason_if(~F.coalesce(F.col(c).isin(values), F.lit(False)), f"invalid_{c}")
        for c, values in config.allowed_values.items()
    ]
    return checks


def _cast_failure_reasons() -> Column:
    """
    Turn the column names in _cast_errors into reason codes.

    Returns:
        An array Column such as ["invalid_type_customer_id"].
    """
    return F.transform(
        F.col(CAST_ERRORS_COLUMN),
        lambda column_name: F.concat(F.lit(INVALID_TYPE_PREFIX), column_name),
    )


def apply_dq_rules(df: DataFrame, table: str) -> DataFrame:
    """
    Record every data quality rule each row breaks.

    Args:
        df: Output of cast_to_target(), including _corrupt_record and _cast_errors.
        table: Source table name, a key of TABLE_CONFIGS.

    Returns:
        The same DataFrame with _dq_errors added: an array of reason codes,
        empty when the row passes every rule.

    Raises:
        ValueError: If the table has no config.
    """
    if table not in TABLE_CONFIGS:
        raise ValueError(
            f"Unknown source table '{table}'. Expected one of: {sorted(TABLE_CONFIGS)}"
        )

    all_reasons = F.concat(
        _cast_failure_reasons(),
        F.array(*_rule_checks(TABLE_CONFIGS[table])),
    )

    return df.withColumn(
        DQ_ERRORS_COLUMN,
        F.when(
            F.col(CORRUPT_RECORD_COLUMN).isNotNull(),
            F.array(F.lit(MALFORMED_JSON_REASON)),  # other checks are meaningless on a line that didn't parse
        ).otherwise(F.array_distinct(F.array_compact(all_reasons))),  # drop NULLs, and duplicates from overlapping rules
    )


def split_valid_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split rows by whether they broke any data quality rule.

    Args:
        df: Output of apply_dq_rules().

    Returns:
        (valid, rejected): rows with no reasons, and rows with at least one.
    """
    is_valid = F.size(DQ_ERRORS_COLUMN) == 0
    return df.filter(is_valid), df.filter(~is_valid)