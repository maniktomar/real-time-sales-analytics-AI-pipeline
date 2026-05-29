from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_sales_events(events: DataFrame, max_order_amount: float = 10000.0) -> DataFrame:
    return (
        events.withColumn("event_timestamp", F.to_timestamp("event_time"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("customer_email", F.lower(F.trim("customer_email")))
        .withColumn("category", F.lower(F.trim("category")))
        .withColumn("sales_channel", F.lower(F.trim("sales_channel")))
        .withColumn("computed_order_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("is_valid_quantity", F.col("quantity") > 0)
        .withColumn("is_valid_amount", (F.col("order_amount") > 0) & (F.col("order_amount") <= max_order_amount))
        .withColumn("is_amount_consistent", F.abs(F.col("order_amount") - F.col("computed_order_amount")) <= 0.05)
        .withColumn(
            "quality_status",
            F.when(
                F.col("is_valid_quantity") & F.col("is_valid_amount") & F.col("is_amount_consistent"),
                F.lit("valid"),
            ).otherwise(F.lit("invalid")),
        )
    )


def add_anomaly_flags(events: DataFrame) -> DataFrame:
    return events.withColumn(
        "anomaly_reason",
        F.when(F.col("quantity") <= 0, F.lit("non_positive_quantity"))
        .when(F.col("quantity") >= 20, F.lit("unusually_high_quantity"))
        .when(F.col("order_amount") >= 5000, F.lit("high_value_order"))
        .when(~F.col("is_amount_consistent"), F.lit("amount_mismatch"))
        .otherwise(F.lit(None)),
    ).withColumn("is_anomaly", F.col("anomaly_reason").isNotNull())


def aggregate_sales_metrics(valid_events: DataFrame, watermark_delay: str) -> DataFrame:
    return (
        valid_events.withWatermark("event_timestamp", watermark_delay)
        .groupBy(
            F.window("event_timestamp", "1 minute"),
            "sales_channel",
            "category",
            "currency",
        )
        .agg(
            F.count("*").alias("order_count"),
            F.sum("order_amount").alias("gross_sales"),
            F.avg("order_amount").alias("avg_order_value"),
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
            F.round("gross_sales", 2).alias("gross_sales"),
            F.round("avg_order_value", 2).alias("avg_order_value"),
            "unique_customers",
            "anomaly_count",
        )
    )
