from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("signup_date", StringType(), True), # Load as string first, cast in Silver
    StructField("customer_segment", StringType(), True)
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("brand", StringType(), True),
    StructField("is_active", BooleanType(), True)
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_timestamp", StringType(), True), # Load as string first
    StructField("order_status", StringType(), True),
    StructField("order_total", DoubleType(), True),
    StructField("sales_channel", StringType(), True)
])

ORDERS_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_total", DoubleType(), True),
    StructField("sales_channel", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("schema_version", StringType(), True)
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("line_total", DoubleType(), True)
])

CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("source", StringType(), True)
])

SUPPORT_TICKETS_SCHEMA = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("issue_type", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("resolution_status", StringType(), True),
    StructField("resolved_at", StringType(), True),
    StructField("priority", StringType(), True)
])

SCHEMAS = {
    "customers": CUSTOMERS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "orders_event": ORDERS_EVENT_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "clickstream": CLICKSTREAM_SCHEMA,
    "support_tickets": SUPPORT_TICKETS_SCHEMA
}
