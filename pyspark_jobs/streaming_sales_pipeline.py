from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQueryException
from pyspark.sql import functions as F

from pyspark_jobs.schemas import sales_event_schema
from pyspark_jobs.transformations import add_anomaly_flags, aggregate_sales_metrics, clean_sales_events
from utils.config import load_config


FACT_SALES_COLUMNS = [
    "event_id",
    "event_time",
    "event_timestamp",
    "ingestion_timestamp",
    "order_id",
    "customer_id",
    "customer_email",
    "product_id",
    "category",
    "quantity",
    "unit_price",
    "order_amount",
    "computed_order_amount",
    "currency",
    "payment_method",
    "sales_channel",
    "store_region",
    "ip_address",
    "is_test_event",
    "source_system",
    "is_valid_quantity",
    "is_valid_amount",
    "is_amount_consistent",
    "quality_status",
    "anomaly_reason",
    "is_anomaly",
    "topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "batch_id",
]

AGG_SALES_COLUMNS = [
    "window_start",
    "window_end",
    "sales_channel",
    "category",
    "currency",
    "order_count",
    "gross_sales",
    "avg_order_value",
    "unique_customers",
    "anomaly_count",
    "batch_id",
]


def create_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark: SparkSession, bootstrap_servers: str, topic: str) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return raw.select(
        F.from_json(F.col("value").cast("string"), sales_event_schema).alias("event"),
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select("event.*", "topic", "kafka_partition", "kafka_offset", "kafka_timestamp")


def snowflake_options(config: dict) -> dict[str, str]:
    sf = config["snowflake"]
    return {
        "sfURL": sf["url"],
        "sfUser": sf["user"],
        "sfPassword": sf["password"],
        "sfDatabase": sf["database"],
        "sfSchema": sf["schema"],
        "sfWarehouse": sf["warehouse"],
        "sfRole": sf["role"],
    }


def write_batch_to_snowflake(table_name: str, sf_options: dict[str, str]):
    def _writer(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        columns = AGG_SALES_COLUMNS if table_name == "AGG_SALES_METRICS_1M" else FACT_SALES_COLUMNS
        (
            batch_df.withColumn("batch_id", F.lit(batch_id))
            .select(*columns)
            .write.format("snowflake")
            .options(**sf_options)
            .option("dbtable", table_name)
            .mode("append")
            .save()
        )

    return _writer


def start_pipeline(sink: str = "console") -> None:
    config = load_config()
    spark = create_spark(config["spark"]["app_name"])
    events = read_kafka_stream(
        spark,
        config["kafka"]["bootstrap_servers"],
        config["kafka"]["sales_topic"],
    )
    enriched = add_anomaly_flags(clean_sales_events(events, float(config["quality_rules"]["max_order_amount"])))
    valid = enriched.filter(F.col("quality_status") == "valid")
    invalid = enriched.filter(F.col("quality_status") == "invalid")
    metrics = aggregate_sales_metrics(valid, config["spark"]["watermark_delay"])

    checkpoint_base = config["spark"]["checkpoint_base"]
    trigger = config["spark"]["trigger_processing_time"]
    queries = []

    if sink == "snowflake":
        sf_options = snowflake_options(config)
        queries.extend(
            [
                valid.writeStream.foreachBatch(write_batch_to_snowflake("FACT_SALES_EVENTS", sf_options))
                .option("checkpointLocation", f"{checkpoint_base}/fact_sales_events")
                .trigger(processingTime=trigger)
                .start(),
                invalid.writeStream.foreachBatch(write_batch_to_snowflake("ERROR_SALES_EVENTS", sf_options))
                .option("checkpointLocation", f"{checkpoint_base}/error_sales_events")
                .trigger(processingTime=trigger)
                .start(),
                metrics.writeStream.foreachBatch(write_batch_to_snowflake("AGG_SALES_METRICS_1M", sf_options))
                .option("checkpointLocation", f"{checkpoint_base}/agg_sales_metrics_1m")
                .outputMode("update")
                .trigger(processingTime=trigger)
                .start(),
            ]
        )
    else:
        queries.append(
            metrics.writeStream.format("console")
            .outputMode("update")
            .option("truncate", "false")
            .trigger(processingTime=trigger)
            .start()
        )

    print(f"Started {len(queries)} streaming query/query group. Waiting for data...", flush=True)
    try:
        while True:
            active_queries = [query for query in queries if query.isActive]
            if not active_queries:
                for query in queries:
                    print(f"Query stopped: name={query.name}, id={query.id}, status={query.status}", flush=True)
                    if query.exception() is not None:
                        print("Streaming query failed. Check the exception below:", flush=True)
                        print(query.exception(), flush=True)
                        raise query.exception()
                print("No active streaming queries remain.", flush=True)
                break

            for query in active_queries:
                print(f"Query active: id={query.id}, status={query.status.get('message', 'running')}", flush=True)
            time.sleep(15)
    except StreamingQueryException as exc:
        print("Streaming query failed. Check the exception below:", flush=True)
        print(exc, flush=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the real-time sales streaming pipeline.")
    parser.add_argument("--sink", choices=["console", "snowflake"], default="console")
    args = parser.parse_args()
    start_pipeline(args.sink)
