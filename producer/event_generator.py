from __future__ import annotations

import random
import uuid
from datetime import UTC, datetime
from typing import Any

from faker import Faker


fake = Faker()

PRODUCTS = [
    {"product_id": "SKU-1001", "category": "electronics", "base_price": 499.0},
    {"product_id": "SKU-1002", "category": "home", "base_price": 89.0},
    {"product_id": "SKU-1003", "category": "sports", "base_price": 129.0},
    {"product_id": "SKU-1004", "category": "fashion", "base_price": 59.0},
    {"product_id": "SKU-1005", "category": "beauty", "base_price": 39.0},
]
CHANNELS = ["web", "mobile", "store", "partner"]
CURRENCIES = ["USD", "GBP", "EUR"]
PAYMENT_METHODS = ["card", "paypal", "bank_transfer", "gift_card"]


def generate_sales_event(anomaly_rate: float = 0.03) -> dict[str, Any]:
    """Generate one realistic transactional event with occasional anomalies."""
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    unit_price = round(product["base_price"] * random.uniform(0.75, 1.25), 2)
    is_anomaly = random.random() < anomaly_rate

    if is_anomaly:
        quantity = random.choice([0, -1, 25, 50])
        unit_price = round(unit_price * random.choice([0.01, 8, 20]), 2)

    order_amount = round(quantity * unit_price, 2)
    event_time = datetime.now(UTC).isoformat()

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": event_time,
        "order_id": f"ORD-{uuid.uuid4().hex[:12].upper()}",
        "customer_id": f"CUST-{random.randint(10000, 99999)}",
        "customer_email": fake.email(),
        "product_id": product["product_id"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": unit_price,
        "order_amount": order_amount,
        "currency": random.choice(CURRENCIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "sales_channel": random.choice(CHANNELS),
        "store_region": fake.state_abbr(),
        "ip_address": fake.ipv4_public(),
        "is_test_event": False,
        "source_system": "simulated-pos",
    }

