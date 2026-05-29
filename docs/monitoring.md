# Monitoring and Logging

## Local Development

- Producer logs are written to `logs/producer.log`.
- Debug consumer logs are written to `logs/consumer.log`.
- Kafka UI is available at `http://localhost:8080` when Docker Compose is running.
- Spark checkpoint directories under `checkpoints/` track streaming progress.

## Production Recommendations

- Send application logs to a centralized platform such as Datadog, CloudWatch, Azure Monitor, or Splunk.
- Track Kafka consumer lag by topic, partition, and consumer group.
- Alert on Spark query termination, repeated failed micro-batches, and checkpoint storage failures.
- Monitor Snowflake warehouse credits, query duration, failed loads, and table growth.
- Add data quality alerts for invalid event rate and anomaly count thresholds.

## Useful Metrics

- Events produced per second.
- Kafka topic bytes in/out.
- Structured Streaming input rows per second.
- Structured Streaming processed rows per second.
- Snowflake rows inserted per micro-batch.
- Power BI refresh latency.

