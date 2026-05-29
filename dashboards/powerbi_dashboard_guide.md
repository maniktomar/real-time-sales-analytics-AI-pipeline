# Power BI Dashboard Guide

Connect Power BI Desktop to Snowflake using `Get Data > Snowflake`.

Recommended tables or views:

- `MARTS.VW_REALTIME_KPIS`
- `MARTS.VW_SALES_TRENDS`
- `MARTS.VW_ANOMALY_EVENTS`

Suggested report pages:

1. Executive KPI Monitor
   - Cards: gross sales, order count, average order value, anomaly count.
   - Line chart: gross sales by `window_start`.
   - Slicer: currency, sales channel, category.

2. Sales Trends
   - Area chart: gross sales over time by category.
   - Bar chart: order count by sales channel.
   - Matrix: category, currency, order count, gross sales, average order value.

3. Operational Quality
   - Table: anomaly events with order id, reason, channel, amount.
   - Column chart: anomaly count by reason.
   - KPI: invalid event rate.

4. AI Insights
   - Card: average risk score.
   - Bar chart: count of orders by risk level.
   - Table: anomaly explanation and recommended action.
   - Text/table visual: hourly business summary from `MARTS.VW_AI_BUSINESS_SUMMARY`.

Refresh options:

- Import mode is simplest for portfolio demos.
- DirectQuery is better for near real-time reporting against Snowflake.
- In the Power BI Service, configure scheduled refresh or automatic page refresh where supported.
