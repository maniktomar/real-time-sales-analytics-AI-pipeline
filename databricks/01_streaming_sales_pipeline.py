# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Sales Streaming Pipeline
# MAGIC Attach this notebook to a Databricks cluster with the Kafka and Snowflake Spark connector libraries installed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Widgets

# COMMAND ----------

dbutils.widgets.text("kafka_bootstrap_servers", "localhost:9092")
dbutils.widgets.text("sales_topic", "sales-transactions")
dbutils.widgets.text("checkpoint_base", "dbfs:/checkpoints/realtime-sales")
dbutils.widgets.text("snowflake_database", "REALTIME_ANALYTICS")
dbutils.widgets.text("snowflake_schema", "CURATED")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType

sales_event_schema = StructType([
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
])

# COMMAND ----------

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", dbutils.widgets.get("kafka_bootstrap_servers"))
    .option("subscribe", dbutils.widgets.get("sales_topic"))
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

events = raw.select(F.from_json(F.col("value").cast("string"), sales_event_schema).alias("event")).select("event.*")

clean = (
    events.withColumn("event_timestamp", F.to_timestamp("event_time"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("customer_email", F.lower(F.trim("customer_email")))
    .withColumn("computed_order_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("quality_status", F.when(
        (F.col("quantity") > 0)
        & (F.col("order_amount") > 0)
        & (F.col("order_amount") <= 10000)
        & (F.abs(F.col("order_amount") - F.col("computed_order_amount")) <= 0.05),
        "valid",
    ).otherwise("invalid"))
    .withColumn("anomaly_reason", F.when(F.col("quantity") >= 20, "unusually_high_quantity")
        .when(F.col("order_amount") >= 5000, "high_value_order")
        .when(F.col("quality_status") == "invalid", "quality_rule_failed"))
    .withColumn("is_anomaly", F.col("anomaly_reason").isNotNull())
)

metrics_1m = (
    clean.filter("quality_status = 'valid'")
    .withWatermark("event_timestamp", "10 minutes")
    .groupBy(F.window("event_timestamp", "1 minute"), "sales_channel", "category", "currency")
    .agg(
        F.count("*").alias("order_count"),
        F.round(F.sum("order_amount"), 2).alias("gross_sales"),
        F.round(F.avg("order_amount"), 2).alias("avg_order_value"),
        F.approx_count_distinct("customer_id").alias("unique_customers"),
        F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "sales_channel",
        "category",
        "currency",
        "order_count",
        "gross_sales",
        "avg_order_value",
        "unique_customers",
        "anomaly_count",
    )
)

# COMMAND ----------

sf_options = {
    "sfURL": dbutils.secrets.get("snowflake", "url"),
    "sfUser": dbutils.secrets.get("snowflake", "user"),
    "sfPassword": dbutils.secrets.get("snowflake", "password"),
    "sfDatabase": dbutils.widgets.get("snowflake_database"),
    "sfSchema": dbutils.widgets.get("snowflake_schema"),
    "sfWarehouse": dbutils.secrets.get("snowflake", "warehouse"),
    "sfRole": dbutils.secrets.get("snowflake", "role"),
}

def write_to_snowflake(table_name):
    def writer(batch_df, batch_id):
        if not batch_df.isEmpty():
            batch_df.withColumn("batch_id", F.lit(batch_id)).write.format("snowflake").options(**sf_options).option("dbtable", table_name).mode("append").save()
    return writer

# COMMAND ----------

clean.filter("quality_status = 'valid'").writeStream.foreachBatch(write_to_snowflake("FACT_SALES_EVENTS")).option("checkpointLocation", dbutils.widgets.get("checkpoint_base") + "/fact_sales_events").trigger(processingTime="15 seconds").start()

clean.filter("quality_status = 'invalid'").writeStream.foreachBatch(write_to_snowflake("ERROR_SALES_EVENTS")).option("checkpointLocation", dbutils.widgets.get("checkpoint_base") + "/error_sales_events").trigger(processingTime="15 seconds").start()

metrics_1m.writeStream.foreachBatch(write_to_snowflake("AGG_SALES_METRICS_1M")).option("checkpointLocation", dbutils.widgets.get("checkpoint_base") + "/agg_sales_metrics_1m").outputMode("update").trigger(processingTime="15 seconds").start()
