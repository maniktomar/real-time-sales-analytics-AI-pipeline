from __future__ import annotations

import argparse
import hashlib
from datetime import UTC, datetime
from typing import Any

import snowflake.connector
from snowflake.connector import DictCursor

from utils.config import load_config
from utils.logging_utils import get_logger


logger = get_logger("ai_insights.anomaly_insights", "logs/ai_insights.log")


def connect_to_snowflake():
    config = load_config()["snowflake"]
    return snowflake.connector.connect(
        account=config["account"],
        user=config["user"],
        password=config["password"],
        role=config["role"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"],
    )


def fetch_unexplained_anomalies(limit: int) -> list[dict[str, Any]]:
    sql = """
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
    FROM REALTIME_ANALYTICS.MARTS.VW_ANOMALY_EVENTS a
    WHERE NOT EXISTS (
      SELECT 1
      FROM REALTIME_ANALYTICS.CURATED.AI_ANOMALY_INSIGHTS i
      WHERE i.order_id = a.order_id
    )
    ORDER BY event_timestamp DESC
    LIMIT %(limit)s
    """
    with connect_to_snowflake() as conn:
        cursor = conn.cursor(DictCursor)
        try:
            cursor.execute(sql, {"limit": limit})
            return list(cursor.fetchall())
        finally:
            cursor.close()


def score_anomaly(row: dict[str, Any]) -> tuple[int, str]:
    reason = str(row.get("ANOMALY_REASON") or "").lower()
    amount = float(row.get("ORDER_AMOUNT") or 0)
    quantity = int(row.get("QUANTITY") or 0)

    score = 35
    if "non_positive_quantity" in reason:
        score += 35
    if "high_quantity" in reason or quantity >= 20:
        score += 25
    if "high_value" in reason or amount >= 5000:
        score += 30
    if "mismatch" in reason or "quality" in reason:
        score += 20

    score = min(score, 100)
    if score >= 80:
        return score, "HIGH"
    if score >= 55:
        return score, "MEDIUM"
    return score, "LOW"


def explain_anomaly(row: dict[str, Any], risk_level: str) -> tuple[str, str]:
    reason = row.get("ANOMALY_REASON") or "unknown anomaly"
    order_id = row.get("ORDER_ID")
    quantity = row.get("QUANTITY")
    amount = row.get("ORDER_AMOUNT")
    channel = row.get("SALES_CHANNEL")
    category = row.get("CATEGORY")

    explanation = (
        f"Order {order_id} was flagged for {reason}. "
        f"The transaction came through the {channel} channel in the {category} category, "
        f"with quantity {quantity} and order amount {amount}. "
        f"The event is classified as {risk_level} risk based on transaction size and data quality signals."
    )

    if risk_level == "HIGH":
        action = "Review immediately, verify customer/order details, and confirm this is not fraud or test data."
    elif risk_level == "MEDIUM":
        action = "Review in the next operations cycle and compare against recent customer/category behavior."
    else:
        action = "Monitor trend volume; no immediate action required unless similar events increase."

    return explanation, action


def build_insight(row: dict[str, Any]) -> dict[str, Any]:
    risk_score, risk_level = score_anomaly(row)
    explanation, action = explain_anomaly(row, risk_level)
    generated_at = datetime.now(UTC).replace(tzinfo=None)
    insight_key = f"{row.get('ORDER_ID')}|{row.get('ANOMALY_REASON')}"
    insight_id = hashlib.sha256(insight_key.encode("utf-8")).hexdigest()[:32]

    return {
        "insight_id": insight_id,
        "generated_at": generated_at,
        "event_timestamp": row.get("EVENT_TIMESTAMP"),
        "order_id": row.get("ORDER_ID"),
        "customer_id": row.get("CUSTOMER_ID"),
        "product_id": row.get("PRODUCT_ID"),
        "category": row.get("CATEGORY"),
        "quantity": row.get("QUANTITY"),
        "order_amount": row.get("ORDER_AMOUNT"),
        "currency": row.get("CURRENCY"),
        "sales_channel": row.get("SALES_CHANNEL"),
        "anomaly_reason": row.get("ANOMALY_REASON"),
        "risk_score": risk_score,
        "risk_level": risk_level,
        "anomaly_explanation": explanation,
        "recommended_action": action,
        "model_provider": "rules_based_ai_baseline",
    }


def write_insights(insights: list[dict[str, Any]]) -> int:
    if not insights:
        return 0

    sql = """
    INSERT INTO REALTIME_ANALYTICS.CURATED.AI_ANOMALY_INSIGHTS (
      insight_id,
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
    )
    SELECT
      %(insight_id)s,
      %(generated_at)s,
      %(event_timestamp)s,
      %(order_id)s,
      %(customer_id)s,
      %(product_id)s,
      %(category)s,
      %(quantity)s,
      %(order_amount)s,
      %(currency)s,
      %(sales_channel)s,
      %(anomaly_reason)s,
      %(risk_score)s,
      %(risk_level)s,
      %(anomaly_explanation)s,
      %(recommended_action)s,
      %(model_provider)s
    WHERE NOT EXISTS (
      SELECT 1
      FROM REALTIME_ANALYTICS.CURATED.AI_ANOMALY_INSIGHTS
      WHERE insight_id = %(insight_id)s
    )
    """
    with connect_to_snowflake() as conn:
        cursor = conn.cursor()
        try:
            inserted = 0
            for insight in insights:
                cursor.execute(sql, insight)
                inserted += cursor.rowcount
            return inserted
        finally:
            cursor.close()


def run(limit: int) -> None:
    rows = fetch_unexplained_anomalies(limit)
    insights = [build_insight(row) for row in rows]
    written = write_insights(insights)
    logger.info(
        "Generated AI anomaly insights",
        extra={"extra_fields": {"fetched": len(rows), "written": written}},
    )
    print(f"Generated {len(insights)} insights; inserted {written} new rows.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate AI-style anomaly explanations from Snowflake events.")
    parser.add_argument("--limit", type=int, default=100, help="Maximum unexplained anomaly rows to process.")
    args = parser.parse_args()
    run(args.limit)
