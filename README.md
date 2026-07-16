# Real-Time Quick-Commerce Operations Lakehouse (Multi-Cloud)

> Blinkit's ops floor: orders stream in every second, dark stores stock out in minutes, and
> the dashboard must show it now.

Kafka (Redpanda) · Spark Structured Streaming · Databricks · Delta Lake · GCP Dataproc · BigQuery · Scala · Airflow · Great Expectations

**Status:** under construction — built milestone by milestone, one PR each.

| # | Milestone | Status |
|---|-----------|--------|
| M1 | Kafka + producer | ✅ |
| M2 | Streaming bronze (exactly-once) | ✅ |
| M3 | Streaming silver (watermark, schema evolution, dead-letter) | ⬜ |
| M4 | Streaming gold + stockout detector | ⬜ |
| M5 | Batch backfill in PySpark AND Scala | ⬜ |
| M6 | GCP path (Dataproc + BigQuery) | ⬜ |
| M7 | Great Expectations + Airflow + runbooks | ⬜ |
| M8 | ML handoff + final README | ⬜ |


