from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType


sales_event_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_email", StringType(), True),
        StructField("product_id", StringType(), False),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("order_amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("sales_channel", StringType(), True),
        StructField("store_region", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("is_test_event", BooleanType(), True),
        StructField("source_system", StringType(), True),
    ]
)

