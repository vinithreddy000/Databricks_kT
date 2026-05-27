# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 102: Logging, Metrics & Alerting
# MAGIC ## Module 19: Monitoring & Troubleshooting
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Logging** records what your code is doing. **Metrics** measure how well it’s performing. **Alerting** notifies you when something goes wrong. Together, these three form the **observability stack** that keeps production pipelines reliable.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC - **Logging** = A ship's logbook ("At 10:05, we entered rough waters. Cargo secure.")
# MAGIC - **Metrics** = The ship's dashboard (speed, fuel level, engine temperature)
# MAGIC - **Alerting** = The alarm system ("Engine temperature critical! Notify captain.")
# MAGIC
# MAGIC ### Databricks Observability Stack:
# MAGIC | Tool | Purpose | Where |
# MAGIC |------|---------|-------|
# MAGIC | Python `logging` | Structured application logs | Driver logs |
# MAGIC | `print()` / `display()` | Quick notebook output | Cell output |
# MAGIC | Spark event logs | Spark-level metrics | Spark UI, cluster logs |
# MAGIC | System tables | Audit, billing, lineage | `system.access.*` |
# MAGIC | Databricks SQL Alerts | Threshold-based notifications | SQL Alerts page |
# MAGIC | Job notifications | Job success/failure alerts | Job config |
# MAGIC | Ganglia metrics | Cluster hardware metrics | Cluster → Metrics tab |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Observability in Databricks:
# MAGIC
# MAGIC   Application Level          Platform Level           Infrastructure Level
# MAGIC   ─────────────────         ──────────────          ────────────────────
# MAGIC   Python logging             System tables             Cluster metrics
# MAGIC   Custom metrics table       Audit logs                CPU/Memory/Disk
# MAGIC   Row counts, durations      Billing usage             Network I/O
# MAGIC   Data quality checks        Lineage tracking          Ganglia dashboard
# MAGIC
# MAGIC Logging Pattern for ETL:
# MAGIC
# MAGIC   import logging
# MAGIC   logger = logging.getLogger("etl_pipeline")
# MAGIC   logger.setLevel(logging.INFO)
# MAGIC
# MAGIC   logger.info(f"Starting ingest for date={date}")
# MAGIC   logger.info(f"Processed {count} rows in {duration}s")
# MAGIC   logger.warning(f"Null rate {null_pct:.1%} exceeds threshold")
# MAGIC   logger.error(f"FAILED: {error_message}")
# MAGIC
# MAGIC   Logs appear in: Driver Logs → stdout/log4j-active.log
# MAGIC
# MAGIC Alerting Flow:
# MAGIC
# MAGIC   1. Define metric (e.g., row count from a query).
# MAGIC   2. Set threshold (e.g., row_count < 1000).
# MAGIC   3. Schedule check (e.g., every hour).
# MAGIC   4. Alert action (e.g., email team@company.com).
# MAGIC
# MAGIC   Databricks SQL Alerts:
# MAGIC     - Query runs on schedule.
# MAGIC     - If result meets condition → send notification.
# MAGIC     - Supports: email, Slack (webhook), PagerDuty.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-5: Examples
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-5 — LOGGING, METRICS & ALERTING EXAMPLES
# ═══════════════════════════════════════════════════════════════════

import logging  # Python's built-in logging module.
from datetime import datetime  # For timestamps.
import time  # For timing.
from pyspark.sql.functions import col, count, lit, current_timestamp  # Spark functions.

print("="*70)
print("SECTIONS 3-5: Logging, Metrics & Alerting")
print("="*70)

# ─── EXAMPLE 1: Python logging (structured logs) ───
print("\n" + "-"*60)
print("EXAMPLE 1: Python logging module")
print("-"*60)

# Configure logger.
logger = logging.getLogger("etl_pipeline")  # Named logger.
logger.setLevel(logging.INFO)  # Set minimum level.

# Add handler (console output in notebooks).
if not logger.handlers:  # Avoid duplicate handlers on re-run.
    handler = logging.StreamHandler()  # Console output.
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')  # Format.
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Use structured logging in ETL.
logger.info("Pipeline started")  # INFO: normal operation.
logger.info(f"Processing date: 2024-05-27")
logger.info(f"Source: catalog.schema.raw_orders")

# Simulate processing.
row_count = 15000  # Simulated.
duration = 3.2  # Simulated.
logger.info(f"Processed {row_count:,} rows in {duration:.1f}s")

# Warning for data quality.
null_pct = 0.08  # 8% nulls.
if null_pct > 0.05:  # Threshold.
    logger.warning(f"Null rate {null_pct:.1%} exceeds 5% threshold!")  # WARNING.

logger.info("Pipeline completed successfully")
print("\n✓ Logs appear in Driver Logs (Cluster → Driver Logs tab).")
print("  Levels: DEBUG < INFO < WARNING < ERROR < CRITICAL.")

# ─── EXAMPLE 2: Custom metrics table (ETL observability) ───
print("\n" + "-"*60)
print("EXAMPLE 2: ETL metrics table (audit trail)")
print("-"*60)

# Create a metrics log entry.
metrics_entry = spark.createDataFrame([(
    "daily_orders_etl",          # pipeline_name.
    "ingest",                    # stage_name.
    "2024-05-27",                # processing_date.
    "success",                   # status.
    15000,                       # rows_processed.
    3.2,                         # duration_seconds.
    0.08,                        # null_rate.
    datetime.now().isoformat()   # logged_at.
)], ["pipeline", "stage", "date", "status", "rows", "duration_sec", "null_rate", "logged_at"])

print("\nMetrics entry:")
display(metrics_entry)  # display() for output.

print("""
\nProduction pattern:
  1. Create a Delta table: catalog.ops.etl_metrics.
  2. Each ETL task appends a row after completion.
  3. Dashboard queries this table for:
     - Failed pipelines in last 24h.
     - Average duration trends.
     - Row count anomalies.
  4. SQL Alert triggers if:
     - rows_processed < minimum_expected.
     - status = 'failed'.
     - duration_sec > SLA_threshold.
""")

# ─── EXAMPLE 3: Databricks SQL Alerts ───
print("\n" + "-"*60)
print("EXAMPLE 3: SQL Alerts (threshold-based notifications)")
print("-"*60)

print("""
Databricks SQL Alerts (UI-based):

  Step 1: Create a SQL query that returns a metric.
    SELECT COUNT(*) as failed_count
    FROM catalog.ops.etl_metrics
    WHERE status = 'failed'
      AND logged_at >= now() - INTERVAL 1 HOUR;

  Step 2: Create an Alert (SQL → Alerts → Create Alert).
    - Query: Select the query above.
    - Trigger condition: "Value is above 0" (any failure = alert).
    - Schedule: Every 15 minutes.
    - Notification: Email to oncall@company.com.

  Step 3: Alert fires when condition is met.
    - Email sent with query result.
    - Can also send to Slack via webhook destination.

Alert examples:
  - Row count drops below threshold.
  - Pipeline hasn't run in expected window.
  - Data freshness (last update > 2 hours ago).
  - Error rate exceeds threshold.
  - Cost (billing.usage) exceeds daily budget.
""")

# ─── EXAMPLE 4: Timing and profiling code ───
print("\n" + "-"*60)
print("EXAMPLE 4: Timing and profiling")
print("-"*60)

import time  # Timer.

def timed_operation(name, func):
    """Wrapper to time any operation and log results."""
    start = time.time()  # Start.
    result = func()  # Execute.
    duration = time.time() - start  # Measure.
    logger.info(f"{name}: {duration:.2f}s")  # Log timing.
    return result, duration

# Time a Spark operation.
result, dur = timed_operation("GroupBy aggregation", 
    lambda: spark.range(5000000).groupBy((col("id") % 100).alias("grp")).count().count()
)
print(f"\n  Aggregation result: {result} groups in {dur:.2f}s")

# Time a write operation.
result, dur = timed_operation("Write to noop",
    lambda: spark.range(1000000).write.format("noop").mode("overwrite").save()
)
print(f"  Write completed in {dur:.2f}s")

print("\n✓ Always time critical operations in production ETL.")
print("  Log durations to metrics table for trend analysis.")

# COMMAND ----------

# DBTITLE 1,Section 6: Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Using print() instead of proper logging
# MAGIC ```python
# MAGIC # BAD: print() disappears after cell clears. No timestamps, no levels.
# MAGIC print("Starting ETL...")  # Lost in notebook output.
# MAGIC
# MAGIC # GOOD: Use logging module (persists in driver logs, has levels + timestamps).
# MAGIC import logging
# MAGIC logger = logging.getLogger("my_etl")
# MAGIC logger.info("Starting ETL...")  # Structured, searchable, timestamped.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: No alerting on failures (silent data outages)
# MAGIC ```python
# MAGIC # BAD: Pipeline fails silently. Dashboard shows stale data for days.
# MAGIC # Nobody notices until the CEO asks why the report is wrong.
# MAGIC
# MAGIC # GOOD: Configure alerts at multiple levels.
# MAGIC # 1. Job-level: email on failure (job config).
# MAGIC # 2. Data freshness: SQL Alert if last_updated > 2 hours.
# MAGIC # 3. Quality: Alert if null_rate > threshold.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Not logging row counts (can't detect data loss)
# MAGIC ```python
# MAGIC # BAD: No idea if pipeline processed 0 rows or 1 million.
# MAGIC df.write.saveAsTable("output")  # How many rows? Did it work?
# MAGIC
# MAGIC # GOOD: Always count and log.
# MAGIC row_count = df.count()
# MAGIC logger.info(f"Writing {row_count:,} rows to output table")
# MAGIC assert row_count > 0, "Zero rows processed!"
# MAGIC df.write.saveAsTable("output")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Logging sensitive data
# MAGIC ```python
# MAGIC # BAD: Logging credentials or PII.
# MAGIC logger.info(f"Connecting with password={password}")  # Password in logs!
# MAGIC logger.info(f"Processing customer email={email}")     # PII in logs!
# MAGIC
# MAGIC # GOOD: Log only metadata, never values.
# MAGIC logger.info(f"Connecting to database (scope=jdbc-scope)")
# MAGIC logger.info(f"Processing {count} customer records")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: No trend analysis (only looking at latest run)
# MAGIC ```python
# MAGIC # BAD: Only checking if current run succeeded.
# MAGIC # Doesn't catch: gradual slowdown, increasing null rates, shrinking row counts.
# MAGIC
# MAGIC # GOOD: Store metrics historically and alert on TRENDS.
# MAGIC # - Duration increased 50% over last week.
# MAGIC # - Row count dropped 30% vs same day last week.
# MAGIC # - Null rate trending upward.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7: Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

import logging  # For homework.

print("="*70)
print("HOMEWORK — Logging, Metrics & Alerting")
print("="*70)

print("\n--- Level 1: Basic logging ---")
logger = logging.getLogger("homework")
logger.setLevel(logging.INFO)
logger.info("Homework started")  # Logs to driver logs.
print("  Check: Cluster → Driver Logs → log4j-active.log")

print("\n--- Level 2: Log with context ---")
logger.info(f"Processing date=2024-05-27, source=raw_orders")
print("  Always include: date, source, target in log messages.")

print("\n--- Level 3: Time an operation ---")
import time
start = time.time()
spark.range(1000000).count()  # Operation to time.
print(f"  Duration: {time.time()-start:.2f}s")

print("\n--- Level 4: Log row counts ---")
print("  count = df.count()")
print("  logger.info(f'Processed {count:,} rows')")
print("  assert count > 0, 'Zero rows!'")

print("\n--- Level 5: Metrics table pattern ---")
print("  Store: pipeline, stage, date, status, rows, duration, quality_score.")
print("  Query for dashboards and alerts.")

print("\n--- Level 6: SQL Alerts ---")
print("  SQL query + condition + schedule + notification.")
print("  Example: Alert if no successful runs in last 2 hours.")

print("\n--- Level 7: Job notifications ---")
print("  Job config → email_notifications → on_failure.")
print("  Also: webhook_notifications for Slack/Teams.")

print("\n--- Level 8: System tables for monitoring ---")
print("  system.access.audit: access events.")
print("  system.billing.usage: cost by workspace/cluster/job.")
print("  system.compute.clusters: cluster inventory.")

print("\n--- Level 9: Ganglia metrics ---")
print("  Cluster → Metrics tab: CPU, Memory, Network, HDFS.")
print("  High CPU + low shuffle = good. High shuffle = optimization needed.")

print("\n--- Level 10: Teach observability ---")
print("""
"Databricks observability:
  Logging: Python logging module → driver logs (timestamped, leveled).
  Metrics: Custom Delta table with pipeline stats (rows, duration, quality).
  Alerting: SQL Alerts (query + threshold + notification).
  Platform: system tables (audit, billing, lineage).
  Infra: Ganglia (CPU, memory), Spark UI (tasks, shuffles).
  Best practices: Log row counts, time operations, alert on failures,
  store metrics historically, never log sensitive data."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 102")
print("="*70)