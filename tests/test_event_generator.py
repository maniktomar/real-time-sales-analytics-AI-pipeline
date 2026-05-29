from producer.event_generator import generate_sales_event


def test_generate_sales_event_has_required_fields():
    event = generate_sales_event(anomaly_rate=0)
    required = {
        "event_id",
        "event_time",
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "order_amount",
        "currency",
        "sales_channel",
    }
    assert required.issubset(event)
    assert event["quantity"] > 0
    assert event["order_amount"] > 0

