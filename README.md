# Weather-Based Electricity Price Analysis Pipeline ⚡🌡️

A robust, idempotent data engineering pipeline built in Databricks implementing the **Medallion Architecture (Bronze ➔ Silver ➔ Gold)**. This project analyzes the correlation between weather conditions (temperature, wind speed, humidity) and hourly electricity prices for price zone NO2 (Sandnes/Stavanger/Kristiansand region).

---

## Architecture Overview

The pipeline follows a modern lakehouse pattern using Delta Lake on Databricks:

1. **Bronze Layer (`bronze_power_prices`, `bronze_weather`):**
   - Ingests raw JSON data directly from open APIs (`hvakosterstrømmen.no` for day-ahead electricity prices and `MET Locationforecast API` for weather).
   - Enriches raw payloads with metadata (`ingestion_timestamp`, `source`) and persists them as immutable Delta tables.
   - **Compliance:** Properly formats the `User-Agent` header according to MET's Terms of Service requirements.

2. **Silver Layer (`silver_weather_power_joined`):**
   - Standardizes timestamps and truncates them to a uniform hourly grain (`hour_slot`).
   - Cleans and casts data types (e.g., converting price keys and meteorological fields).
   - **Data Quality & Null Handling:** Implements deterministic row cleaning (`.na.drop()`) to filter out missing or misaligned observations, ensuring downstream joins remain structurally sound.

3. **Gold Layer (`gold_weather_price_summary`):**
   - Aggregates the joined dataset by temperature brackets (rounded degrees Celsius).
   - Computes average electricity prices (`avg_price_nok`), average wind speeds, and observation counts to surface actionable correlations.

---

## Key Technical Design Choices & Trade-offs

- **Idempotency:** All Delta writes use `.mode("overwrite")` (or partition-safe equivalent logic in production pipelines), ensuring that re-running the notebook or task multiple times produces identical, deterministic table states without duplicate entries.
- **Error Handling & Resilience:** API requests incorporate status code validation to catch upstream service outages gracefully before triggering downstream Spark transformations.
- **Simplicity vs. Scale:** Designed for Databricks Free Edition constraints—focusing on clean code architecture, robust PySpark patterns, and native Delta lake features over heavy distributed streaming infrastructure.

---

## Repository Structure

```text
├── notebook/
│   └── weather_power_pipeline.py     # Main end-to-end Databricks notebook
├── README.md                         # Project documentation
```

---

## Orchestration & Scheduling
- In production, this notebook would be converted into a Databricks Workflow, scheduled via cron at a desired interval; configured with automatic retries and task dependencies

---

## Monitoring & Alerting
- Implementing Databricks SQL alerts or webhooks to a desired service (Slack/Teams/Etc.); triggered on failed API responses (!=200), if execution fails or if data quality checks result in zero rows passing into Silver

---

## Unity Catalog
- Goverened using Unity Catalog's three-level namespace (catalog.schema.table), providing automatic column-level data lineage tracking and fine-grained access policies per workspace group

## AI Usage
- Fully leveraged AI (Gemini) as an active collaborator and tutor to scaffold PySpark syntax, handle API schema parsing details; structuring the pipeline rapidly, while maintaining strict oversight and full comprehension of every architectural choice and trade-off.

