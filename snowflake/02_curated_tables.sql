USE DATABASE REALTIME_ANALYTICS;
USE SCHEMA CURATED;

CREATE TABLE IF NOT EXISTS FACT_SALES_EVENTS (
  event_id STRING,
  event_time STRING,
  event_timestamp TIMESTAMP_NTZ,
  ingestion_timestamp TIMESTAMP_NTZ,
  order_id STRING,
  customer_id STRING,
  customer_email STRING,
  product_id STRING,
  category STRING,
  quantity NUMBER,
  unit_price FLOAT,
  order_amount FLOAT,
  computed_order_amount FLOAT,
  currency STRING,
  payment_method STRING,
  sales_channel STRING,
  store_region STRING,
  ip_address STRING,
  is_test_event BOOLEAN,
  source_system STRING,
  is_valid_quantity BOOLEAN,
  is_valid_amount BOOLEAN,
  is_amount_consistent BOOLEAN,
  quality_status STRING,
  anomaly_reason STRING,
  is_anomaly BOOLEAN,
  topic STRING,
  kafka_partition NUMBER,
  kafka_offset NUMBER,
  kafka_timestamp TIMESTAMP_NTZ,
  batch_id NUMBER
);

CREATE TABLE IF NOT EXISTS ERROR_SALES_EVENTS LIKE FACT_SALES_EVENTS;

CREATE TABLE IF NOT EXISTS AGG_SALES_METRICS_1M (
  window_start TIMESTAMP_NTZ,
  window_end TIMESTAMP_NTZ,
  sales_channel STRING,
  category STRING,
  currency STRING,
  order_count NUMBER,
  gross_sales FLOAT,
  avg_order_value FLOAT,
  unique_customers NUMBER,
  anomaly_count NUMBER,
  batch_id NUMBER
);

CREATE TABLE IF NOT EXISTS AI_ANOMALY_INSIGHTS (
  insight_id STRING,
  generated_at TIMESTAMP_NTZ,
  event_timestamp TIMESTAMP_NTZ,
  order_id STRING,
  customer_id STRING,
  product_id STRING,
  category STRING,
  quantity NUMBER,
  order_amount FLOAT,
  currency STRING,
  sales_channel STRING,
  anomaly_reason STRING,
  risk_score NUMBER,
  risk_level STRING,
  anomaly_explanation STRING,
  recommended_action STRING,
  model_provider STRING
);
