# Project Showcase

## Real-Time Sales Analytics Pipeline with AI Insights

This project demonstrates a production-style streaming analytics platform for simulated sales transactions. It combines Kafka, PySpark Structured Streaming, Snowflake, Power BI, and an AI-style anomaly explanation layer.

## Business Problem

Modern commerce teams need to monitor sales performance, operational quality, and suspicious transaction patterns as events arrive. Batch dashboards can miss fast-moving anomalies and make it harder for business users to understand what requires attention.

## Solution

The platform ingests transaction events in real time, validates and transforms them with Spark, stores curated facts and aggregates in Snowflake, and presents operational KPIs in Power BI. An AI insights layer converts anomaly records into risk scores, explanations, and recommended actions.

## Architecture Highlights

- Event simulation with a Python Kafka producer.
- Kafka topic for streaming transaction ingestion.
- PySpark Structured Streaming for parsing, cleaning, validation, anomaly tagging, and aggregation.
- Snowflake curated tables and BI-friendly mart views.
- Power BI dashboard pages for KPIs, trends, anomalies, and AI insights.
- AI-style anomaly explanations and business summaries.
- Dockerized local Kafka stack.
- CI test workflow with GitHub Actions.

## What Makes It Stand Out

- Real-time streaming design rather than static CSV analysis.
- End-to-end journey from event generation to BI dashboard.
- Snowflake schema design with fact, error, aggregate, and AI insight tables.
- AI-assisted recommendations that are understandable to non-technical stakeholders.
- Beginner-friendly scripts and documentation while preserving professional architecture.

## Interview Talking Points

- How Kafka decouples producers from streaming consumers.
- Why Spark Structured Streaming checkpoints matter.
- How invalid events are separated from curated facts.
- How Snowflake mart views simplify Power BI reporting.
- How the AI insight layer can be swapped from rules-based explanations to an LLM provider.
- How this would scale with Schema Registry, Delta Lake, orchestration, monitoring, and IaC.

## Resume Bullet

Developed a real-time sales analytics platform using Kafka, PySpark Structured Streaming, Snowflake, Power BI, and AI-generated anomaly insights to ingest, transform, monitor, and explain streaming transaction data.

