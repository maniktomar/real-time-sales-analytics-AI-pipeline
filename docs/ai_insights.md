# AI Insights Upgrade

This project includes an optional AI-style insights layer that turns anomaly rows into business-readable explanations, risk scores, and recommended actions.

## What It Adds

- `CURATED.AI_ANOMALY_INSIGHTS` Snowflake table.
- `MARTS.VW_AI_ANOMALY_INSIGHTS` Power BI view.
- `MARTS.VW_AI_BUSINESS_SUMMARY` plain-English executive summary view.
- `ai_insights/anomaly_insights.py` Python generator.

The first version uses a deterministic rules-based AI baseline so it is easy to run, explain, and demo without a paid LLM dependency. The module is intentionally isolated so a future LLM provider can replace only the explanation function.

## Run It

After the streaming pipeline has written anomaly rows to Snowflake:

```powershell
cd "C:\Users\manik\OneDrive\Documents\Playground"
.\.venv\Scripts\Activate.ps1
python -m ai_insights.anomaly_insights --limit 100
```

Then verify in Snowflake:

```sql
SELECT COUNT(*)
FROM REALTIME_ANALYTICS.CURATED.AI_ANOMALY_INSIGHTS;

SELECT *
FROM REALTIME_ANALYTICS.MARTS.VW_AI_ANOMALY_INSIGHTS
ORDER BY generated_at DESC
LIMIT 20;

SELECT *
FROM REALTIME_ANALYTICS.MARTS.VW_AI_BUSINESS_SUMMARY;
```

## Power BI Page

Add a fourth page named `AI Insights`.

Recommended visuals:

- Card: average `RISK_SCORE`.
- Bar chart: count of `ORDER_ID` by `RISK_LEVEL`.
- Table: `ORDER_ID`, `RISK_LEVEL`, `ANOMALY_REASON`, `ANOMALY_EXPLANATION`, `RECOMMENDED_ACTION`.
- Multi-row card or table: `VW_AI_BUSINESS_SUMMARY.BUSINESS_SUMMARY`.

