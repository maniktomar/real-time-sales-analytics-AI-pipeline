USE DATABASE REALTIME_ANALYTICS;
USE SCHEMA MARTS;

CREATE OR REPLACE VIEW VW_REALTIME_KPIS AS
SELECT
  MAX(window_end) AS latest_window,
  SUM(order_count) AS orders_last_loaded_period,
  SUM(gross_sales) AS sales_last_loaded_period,
  AVG(avg_order_value) AS avg_order_value,
  SUM(anomaly_count) AS anomalies_last_loaded_period
FROM CURATED.AGG_SALES_METRICS_1M
WHERE window_end >= DATEADD(hour, -1, CURRENT_TIMESTAMP());

CREATE OR REPLACE VIEW VW_SALES_TRENDS AS
SELECT
  window_start,
  sales_channel,
  category,
  currency,
  order_count,
  gross_sales,
  avg_order_value,
  unique_customers,
  anomaly_count
FROM CURATED.AGG_SALES_METRICS_1M;

CREATE OR REPLACE VIEW VW_ANOMALY_EVENTS AS
SELECT
  event_timestamp,
  order_id,
  customer_id,
  product_id,
  category,
  quantity,
  order_amount,
  currency,
  sales_channel,
  anomaly_reason
FROM CURATED.FACT_SALES_EVENTS
WHERE is_anomaly = TRUE
UNION ALL
SELECT
  event_timestamp,
  order_id,
  customer_id,
  product_id,
  category,
  quantity,
  order_amount,
  currency,
  sales_channel,
  anomaly_reason
FROM CURATED.ERROR_SALES_EVENTS;

CREATE OR REPLACE VIEW VW_AI_ANOMALY_INSIGHTS AS
SELECT
  generated_at,
  event_timestamp,
  order_id,
  customer_id,
  product_id,
  category,
  quantity,
  order_amount,
  currency,
  sales_channel,
  anomaly_reason,
  risk_score,
  risk_level,
  anomaly_explanation,
  recommended_action,
  model_provider
FROM CURATED.AI_ANOMALY_INSIGHTS;

CREATE OR REPLACE VIEW VW_AI_BUSINESS_SUMMARY AS
WITH sales AS (
  SELECT
    COUNT(*) AS total_orders,
    ROUND(SUM(order_amount), 2) AS total_sales,
    ROUND(AVG(order_amount), 2) AS avg_order_value,
    COUNT_IF(is_anomaly) AS anomaly_count
  FROM CURATED.FACT_SALES_EVENTS
  WHERE event_timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
),
top_category AS (
  SELECT category, ROUND(SUM(gross_sales), 2) AS category_sales
  FROM CURATED.AGG_SALES_METRICS_1M
  WHERE window_end >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
  GROUP BY category
  QUALIFY ROW_NUMBER() OVER (ORDER BY category_sales DESC) = 1
),
top_channel AS (
  SELECT sales_channel, ROUND(SUM(gross_sales), 2) AS channel_sales
  FROM CURATED.AGG_SALES_METRICS_1M
  WHERE window_end >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
  GROUP BY sales_channel
  QUALIFY ROW_NUMBER() OVER (ORDER BY channel_sales DESC) = 1
)
SELECT
  CURRENT_TIMESTAMP() AS generated_at,
  'In the last hour, the platform processed ' || COALESCE(total_orders, 0) ||
  ' orders with total sales of ' || COALESCE(total_sales, 0) ||
  '. Average order value was ' || COALESCE(avg_order_value, 0) ||
  '. The top category was ' || COALESCE(top_category.category, 'n/a') ||
  ' and the leading channel was ' || COALESCE(top_channel.sales_channel, 'n/a') ||
  '. Detected anomaly count was ' || COALESCE(anomaly_count, 0) || '.' AS business_summary,
  total_orders,
  total_sales,
  avg_order_value,
  anomaly_count,
  top_category.category AS top_category,
  top_channel.sales_channel AS top_channel
FROM sales
LEFT JOIN top_category ON TRUE
LEFT JOIN top_channel ON TRUE;
