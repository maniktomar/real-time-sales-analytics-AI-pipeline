# Architecture

```mermaid
flowchart LR
  A["Python producer\nsimulated sales events"] --> B["Kafka topic\nsales-transactions"]
  B --> C["PySpark Structured Streaming\nlocal or Databricks"]
  C --> D["Validation and cleaning"]
  D --> E["Curated events\nSnowflake FACT_SALES_EVENTS"]
  D --> F["Rejected events\nSnowflake ERROR_SALES_EVENTS"]
  D --> G["1-minute aggregates\nSnowflake AGG_SALES_METRICS_1M"]
  E --> H["Snowflake MARTS views"]
  F --> H
  G --> H
  H --> I["Power BI dashboard\nKPIs, trends, anomalies"]
  H --> J["Optional FastAPI metrics endpoint"]
```

## Data Flow

1. The producer emits JSON sales transactions to Kafka.
2. Spark reads Kafka in micro-batches, parses the JSON payload, and applies quality checks.
3. Valid events, invalid events, and aggregate metrics are written to Snowflake through `foreachBatch`.
4. Power BI connects to Snowflake mart views for operational reporting.

## Operational Notes

- Kafka partitions can be increased as event volume grows.
- Spark checkpoint locations must be durable in production, for example DBFS or cloud object storage.
- Snowflake warehouses should use auto-suspend and separate ingestion/reporting workloads for cost control.

