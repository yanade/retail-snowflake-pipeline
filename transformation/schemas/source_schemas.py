"""
Explicit source schemas for every retail_oltp table read from the raw zone.

These declare the TARGET types. The read schema is derived from them by
replacing every type with StringType, so an unparseable value survives as
text instead of becoming NULL during the read itself. See ADR-015.
"""


from pyspark.sql.types import(
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SOURCE_SCHEMAS: dict[str, StructType] = {
    "payments": StructType(
        [
            StructField("payment_id", LongType(), True),
            StructField("order_id", LongType(), True),
            StructField("payment_reference", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("payment_status", StringType(), True),
            StructField("payment_amount", DecimalType(12, 2), True),
            StructField("currency_code", StringType(), True),
            StructField("payment_date", TimestampType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    ),
    
    "orders": StructType(
        [
            StructField("order_id", LongType(), True),
            StructField("order_number", StringType(), True),
            StructField("customer_id", LongType(), True),
            StructField("store_id", LongType(), True),
            StructField("employee_id", LongType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_date", TimestampType(), True),
            StructField("currency_code", StringType(), True),
            StructField("subtotal_amount", DecimalType(12, 2), True),
            StructField("tax_amount", DecimalType(12, 2), True),
            StructField("shipping_amount", DecimalType(12, 2), True),
            StructField("discount_amount", DecimalType(12, 2), True),
            StructField("total_amount", DecimalType(12, 2), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    ),

        "currencies": StructType(
        [
            StructField("currency_code", StringType(), True),
            StructField("currency_name", StringType(), True),
            StructField("currency_symbol", StringType(), True),
            StructField("decimal_places", IntegerType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    ),
    "product_categories": StructType(
        [
            StructField("category_id", LongType(), True),
            StructField("parent_category_id", LongType(), True),
            StructField("category_code", StringType(), True),
            StructField("category_name", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    ),
    "suppliers": StructType(
        [
            StructField("supplier_id", LongType(), True),
            StructField("supplier_code", StringType(), True),
            StructField("supplier_name", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("contact_email", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    ),
    "products": StructType(
        [
            StructField("product_id", LongType(), True),
            StructField("sku", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category_id", LongType(), True),
            StructField("supplier_id", LongType(), True),
            StructField("brand", StringType(), True),
            StructField("standard_unit_price", DecimalType(12, 2), True),
            StructField("default_currency_code", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    ),
    "customers": StructType(
        [
            StructField("customer_id", LongType(), True),
            StructField("customer_number", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("customer_status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    ),
    "customer_addresses": StructType(
        [
            StructField("address_id", LongType(), True),
            StructField("customer_id", LongType(), True),
            StructField("address_type", StringType(), True),
            StructField("address_line_1", StringType(), True),
            StructField("address_line_2", StringType(), True),
            StructField("city", StringType(), True),
            StructField("region", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("is_default", BooleanType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    ),
    "stores": StructType(
        [
            StructField("store_id", LongType(), True),
            StructField("store_code", StringType(), True),
            StructField("store_name", StringType(), True),
            StructField("store_type", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("city", StringType(), True),
            # date, not timestamptz. DateType has no time component.
            StructField("opened_date", DateType(), True),
            StructField("closed_date", DateType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    ),
    "employees": StructType(
        [
            StructField("employee_id", LongType(), True),
            StructField("employee_number", StringType(), True),
            StructField("store_id", LongType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("job_title", StringType(), True),
            StructField("email", StringType(), True),
            # date, not timestamptz. DateType has no time component.
            StructField("hire_date", DateType(), True),
            StructField("termination_date", DateType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    ),
    "exchange_rates": StructType(
        [
            StructField("exchange_rate_id", LongType(), True),
            # DateType. Joining this to orders.order_date needs to_date() and a
            # pinned session time zone, or the FX lookup picks the wrong day.
            StructField("rate_date", DateType(), True),
            StructField("base_currency_code", StringType(), True),
            StructField("target_currency_code", StringType(), True),
            # numeric(18, 8), not (12, 2). A rate is a coefficient, not money.
            StructField("exchange_rate", DecimalType(18, 8), True),
            StructField("source_system", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    ),
    "order_items": StructType(
        [
            StructField("order_item_id", LongType(), True),
            StructField("order_id", LongType(), True),
            StructField("line_number", IntegerType(), True),
            StructField("product_id", LongType(), True),
            StructField("source_product_sku", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DecimalType(12, 2), True),
            StructField("discount_amount", DecimalType(12, 2), True),
            StructField("tax_amount", DecimalType(12, 2), True),
            StructField("line_total_amount", DecimalType(12, 2), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    ),

}

def to_read_schema(target_schema: StructType) -> StructType:
    """
    Derive the raw-zone read schema from a target schema.

    Every field keeps its name and position but becomes a nullable string, so
    the JSON reader never converts, and never silently NULLs, a value it cannot
    parse. Conversion to the target type happens later, with try_cast.

    Args:
        target_schema: A schema from SOURCE_SCHEMAS.

    Returns:
        A new StructType with the same field names, all as nullable strings.
    """
    return StructType([StructField(f.name, StringType(), True) for f in target_schema.fields])

def get_source_schema(table: str) -> StructType:
    """
    Look up a table's target schema, failing loudly on an unknown name.

    Args:
        table: Source table name.

    Returns:
        A copy of the table's StructType from SOURCE_SCHEMAS.

    Raises:
        ValueError: If the table has no declared schema.
    """
    if table not in SOURCE_SCHEMAS:
        raise ValueError(
            f"Unknown source table '{table}'. Expected one of: {sorted(SOURCE_SCHEMAS)}"
        )

    # A copy, because StructType.add() mutates in place: handing out the shared
    # object would let one caller corrupt the registry for every other caller.
    return StructType(list(SOURCE_SCHEMAS[table].fields))