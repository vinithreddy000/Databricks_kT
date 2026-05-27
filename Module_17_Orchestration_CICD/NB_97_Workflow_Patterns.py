# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 97: Workflow Patterns & Best Practices
# MAGIC ## Module 17: Orchestration & CI/CD
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC This notebook covers **production-grade workflow patterns** for ETL orchestration — idempotency, error handling, logging, monitoring, and the Medallion architecture (Bronze/Silver/Gold) as implemented through Lakeflow Jobs.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Building a reliable ETL pipeline is like building a **commercial kitchen**:
# MAGIC - **Idempotency** = Accidentally pressing the "start" button twice doesn't produce double food.
# MAGIC - **Error handling** = If one dish burns, the kitchen doesn't shut down — it logs the failure and continues.
# MAGIC - **Monitoring** = Cameras and timers on every station to catch problems early.
# MAGIC - **Medallion pattern** = Raw ingredients (Bronze) → Prepped ingredients (Silver) → Plated dishes (Gold).
# MAGIC
# MAGIC ### Key Patterns:
# MAGIC | Pattern | Description |
# MAGIC |---------|------------|
# MAGIC | Idempotent writes | Re-running produces the same result (no duplicates) |
# MAGIC | MERGE (upsert) | Insert new, update existing, delete removed |
# MAGIC | Checkpoint/restart | Resume from failure point (not from scratch) |
# MAGIC | Circuit breaker | Stop downstream tasks if data quality fails |
# MAGIC | Backfill | Process historical dates without affecting current |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Medallion Architecture with Jobs:
# MAGIC
# MAGIC   ┌────────────┐    ┌────────────┐    ┌────────────┐
# MAGIC   │   BRONZE   │    │   SILVER   │    │    GOLD    │
# MAGIC   │   (Raw)    │ →  │ (Cleaned)  │ →  │ (Business) │
# MAGIC   │            │    │            │    │            │
# MAGIC   │ Append-only│    │ Deduped    │    │ Aggregated │
# MAGIC   │ Schema evo │    │ Type-cast  │    │ Joined     │
# MAGIC   │ No deletes │    │ Validated  │    │ Dashboard- │
# MAGIC   │            │    │            │    │ ready      │
# MAGIC   └────────────┘    └────────────┘    └────────────┘
# MAGIC   Task 1: Ingest   Task 2: Transform   Task 3: Aggregate
# MAGIC
# MAGIC Idempotency Patterns:
# MAGIC
# MAGIC   1. MERGE (upsert): Insert new rows, update existing.
# MAGIC      MERGE INTO target USING source
# MAGIC      ON target.id = source.id
# MAGIC      WHEN MATCHED THEN UPDATE ...
# MAGIC      WHEN NOT MATCHED THEN INSERT ...
# MAGIC
# MAGIC   2. Overwrite partition: Replace only the date partition being processed.
# MAGIC      df.write.mode("overwrite")
# MAGIC        .option("replaceWhere", f"date = '{processing_date}'")
# MAGIC        .saveAsTable("gold.daily_metrics")
# MAGIC
# MAGIC   3. Delta MERGE with conditions:
# MAGIC      - Safe for re-runs (won't create duplicates).
# MAGIC      - Handles late-arriving data.
# MAGIC      - Supports SCD Type 2 (slowly changing dimensions).
# MAGIC
# MAGIC Error Handling Strategy:
# MAGIC   Task 1: Try/except → log error → set task value "status=failed".
# MAGIC   Task 2: Check task value → if failed, skip or alert.
# MAGIC   Job-level: on_failure email + retry 2x.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: Beginner Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, current_timestamp, lit  # Spark functions.
from delta.tables import DeltaTable  # Delta MERGE.

print("="*70)
print("SECTION 3 — BEGINNER: Workflow Patterns")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Idempotent write with MERGE (upsert)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Idempotent MERGE (safe to re-run)")
print("-"*60)

# Create target table.
spark.sql("DROP TABLE IF EXISTS default.etl_target")  # Clean slate.
spark.createDataFrame([
    (1, "Alice", 100.0, "2024-01-01"),
    (2, "Bob", 200.0, "2024-01-01"),
    (3, "Charlie", 150.0, "2024-01-01")
], ["id", "name", "amount", "updated_at"]).write.format("delta").mode("overwrite").saveAsTable("default.etl_target")

# Incoming batch (new + updated records).
new_batch = spark.createDataFrame([
    (2, "Bob", 250.0, "2024-01-02"),     # UPDATE: Bob's amount changed.
    (4, "Diana", 300.0, "2024-01-02")    # INSERT: New record.
], ["id", "name", "amount", "updated_at"])

# MERGE: Upsert (idempotent — safe to run multiple times!).
target = DeltaTable.forName(spark, "default.etl_target")  # Reference target table.
target.alias("t").merge(
    new_batch.alias("s"),  # Source data.
    "t.id = s.id"          # Match condition.
).whenMatchedUpdateAll(    # If match: update all columns.
).whenNotMatchedInsertAll( # If no match: insert new row.
).execute()

print("\nAfter MERGE (run this multiple times — same result!):")
display(spark.table("default.etl_target").orderBy("id"))  # display() for output.

print("\n✓ MERGE is idempotent: re-running doesn't create duplicates.")
print("  Bob was updated (100→250), Diana was inserted, Alice/Charlie unchanged.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Overwrite partition (idempotent date processing)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Overwrite partition (replaceWhere)")
print("-"*60)

processing_date = "2024-01-15"  # Date being processed (from widget/parameter).

# Simulate daily data.
daily_data = spark.createDataFrame([
    ("2024-01-15", "ProductA", 100),
    ("2024-01-15", "ProductB", 200)
], ["date", "product", "revenue"])

# Write with replaceWhere (only overwrites matching partition).
daily_data.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"date = '{processing_date}'") \
    .saveAsTable("default.daily_metrics")

print(f"\n✓ Overwrote ONLY the '{processing_date}' partition.")
print("  Other dates untouched. Safe to re-run for same date.")
print("  This is the idempotent pattern for date-partitioned ETL.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Error handling in ETL notebooks
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Production error handling pattern")
print("-"*60)

import json  # For exit message.
from datetime import datetime  # For timestamps.

def run_etl_with_error_handling():
    """Production ETL pattern with logging and error handling."""
    status = {"start_time": datetime.now().isoformat(), "status": "unknown"}  # Init.
    
    try:
        # ─── Step 1: Validate inputs.
        print("  Step 1: Validating inputs...")
        # In production: check widgets, verify source tables exist.
        
        # ─── Step 2: Process data.
        print("  Step 2: Processing data...")
        df = spark.range(1000)  # Simulated ETL work.
        row_count = df.count()  # Count processed rows.
        
        # ─── Step 3: Quality check.
        print("  Step 3: Quality check...")
        if row_count == 0:  # No data = likely a problem.
            raise ValueError("Zero rows processed — source may be empty!")
        
        # ─── Success path.
        status.update({"status": "success", "rows": row_count})  # Update status.
        print(f"  ✓ SUCCESS: Processed {row_count} rows.")
        
    except Exception as e:
        # ─── Failure path.
        status.update({"status": "failed", "error": str(e)})  # Capture error.
        print(f"  ✗ FAILED: {e}")
        # In production: dbutils.notebook.exit(json.dumps(status))
        # The Job will see the failure and trigger retries/alerts.
        
    finally:
        status["end_time"] = datetime.now().isoformat()  # Always log end time.
        print(f"  Status: {json.dumps(status, indent=2)}")
    
    return status

# Run the pattern.
result = run_etl_with_error_handling()
print("\n✓ Pattern: try/except + status logging + notebook.exit for Jobs.")

# Cleanup demo tables.
spark.sql("DROP TABLE IF EXISTS default.etl_target")
spark.sql("DROP TABLE IF EXISTS default.daily_metrics")

# COMMAND ----------

# DBTITLE 1,Sections 4-5: Intermediate and Advanced
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4-5 — INTERMEDIATE & ADVANCED
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, current_timestamp, lit, count  # Imports.
from datetime import datetime, timedelta  # Date handling.
import json  # JSON.

print("="*70)
print("SECTIONS 4-5: Advanced Workflow Patterns")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Backfill pattern (process date range)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Backfill pattern (process historical dates)")
print("-"*60)

def generate_date_range(start_str, end_str):
    """Generate list of dates between start and end (inclusive)."""
    start = datetime.strptime(start_str, "%Y-%m-%d")  # Parse start.
    end = datetime.strptime(end_str, "%Y-%m-%d")      # Parse end.
    dates = []  # Collect dates.
    current = start  # Iterator.
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))  # Add formatted date.
        current += timedelta(days=1)  # Next day.
    return dates

# Example: backfill January 2024.
backfill_dates = generate_date_range("2024-01-01", "2024-01-05")
print(f"\nBackfill dates: {backfill_dates}")

print("""
\nBackfill execution pattern (via Databricks SDK):

  from databricks.sdk import WorkspaceClient
  w = WorkspaceClient()

  for date in backfill_dates:
      run = w.jobs.run_now(
          job_id=12345,
          notebook_params={"processing_date": date}  # Override date parameter.
      )
      print(f"Triggered run for {date}: run_id={run.run_id}")

Each run processes ONE date (idempotent with replaceWhere).
Safe to re-run any individual date without affecting others.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: ETL monitoring table (audit trail)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: ETL audit/monitoring log table")
print("-"*60)

# Create an audit log entry.
audit_entry = spark.createDataFrame([(
    "daily_sales_etl",         # pipeline_name.
    "2024-05-27",              # processing_date.
    "ingest_bronze",           # task_name.
    "success",                 # status.
    1500,                      # rows_processed.
    12.5,                      # duration_seconds.
    datetime.now().isoformat() # timestamp.
)], ["pipeline", "date", "task", "status", "rows", "duration_sec", "logged_at"])

print("\nAudit log entry:")
display(audit_entry)  # display() for output.

print("""
\nMonitoring pattern:
  1. Each task logs to: catalog.ops.etl_audit_log (Delta table).
  2. Dashboard queries the log for:
     - Failed tasks in last 24h.
     - Tasks with 0 rows (data quality issue).
     - Tasks exceeding SLA (duration > threshold).
  3. Alerts trigger if:
     - row_count < expected_minimum.
     - status = 'failed' AND retry_count >= max_retries.
     - duration > SLA threshold.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Quality gate (circuit breaker pattern)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Quality gate (stop pipeline if data is bad)")
print("-"*60)

def quality_gate(df, table_name, min_rows=100, max_null_pct=0.05):
    """Check data quality before writing. Raises exception if fails."""
    total_rows = df.count()  # Count rows.
    print(f"  Checking {table_name}: {total_rows} rows")
    
    # Check 1: Minimum row count.
    if total_rows < min_rows:  # Too few rows = likely incomplete source.
        raise ValueError(f"QUALITY GATE FAILED: {table_name} has {total_rows} rows (min: {min_rows})")
    
    # Check 2: Null percentage in key columns.
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()  # Count nulls.
        null_pct = null_count / total_rows  # Null percentage.
        if null_pct > max_null_pct:  # Too many nulls.
            raise ValueError(f"QUALITY GATE FAILED: {col_name} has {null_pct:.1%} nulls (max: {max_null_pct:.1%})")
    
    print(f"  ✓ PASSED: {table_name} quality checks.")
    return True

# Test with good data.
good_data = spark.range(500).selectExpr("id", "id * 2 as value")  # 500 rows, no nulls.
quality_gate(good_data, "silver.orders", min_rows=100)  # Passes!

# Test with bad data (would fail in production).
print("\n  If quality fails:")
print("    dbutils.jobs.taskValues.set(key='quality_passed', value=False)")
print("    dbutils.notebook.exit(json.dumps({'status': 'quality_failed'}))")
print("    Downstream tasks check this value and skip if failed.")
print("\n✓ Quality gates prevent bad data from reaching Gold layer.")
print("  Combined with Job's If/Else task for conditional branching.")

# COMMAND ----------

# DBTITLE 1,Section 6: Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Non-idempotent writes (duplicates on re-run)
# MAGIC ```python
# MAGIC # BAD: Append mode creates duplicates if job reruns!
# MAGIC df.write.mode("append").saveAsTable("gold.orders")  # Duplicate rows!
# MAGIC
# MAGIC # GOOD: Use MERGE for upsert (idempotent).
# MAGIC target.merge(source, "t.id = s.id") \
# MAGIC     .whenMatchedUpdateAll() \
# MAGIC     .whenNotMatchedInsertAll() \
# MAGIC     .execute()
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: No quality checks before writing to Gold
# MAGIC ```python
# MAGIC # BAD: Write garbage directly to Gold layer.
# MAGIC df_transformed.write.saveAsTable("gold.metrics")  # What if data is wrong?
# MAGIC
# MAGIC # GOOD: Validate before writing.
# MAGIC assert df_transformed.count() > 0, "No data to write!"
# MAGIC assert df_transformed.filter(col("revenue") < 0).count() == 0, "Negative revenue!"
# MAGIC df_transformed.write.saveAsTable("gold.metrics")  # Safe.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Hard-coded dates (can't backfill)
# MAGIC ```python
# MAGIC # BAD: Always processes "today". Can't reprocess historical dates.
# MAGIC from datetime import date
# MAGIC df = spark.table("source").filter(f"date = '{date.today()}'")
# MAGIC
# MAGIC # GOOD: Parameterized date (from widget/Job parameter).
# MAGIC processing_date = dbutils.widgets.get("date")  # "2024-01-15" or today.
# MAGIC df = spark.table("source").filter(f"date = '{processing_date}'")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Swallowing exceptions (silent failures)
# MAGIC ```python
# MAGIC # BAD: Catching ALL exceptions and continuing. Job shows "success" but data is wrong.
# MAGIC try:
# MAGIC     risky_operation()
# MAGIC except:
# MAGIC     pass  # Silent failure! Nobody knows.
# MAGIC
# MAGIC # GOOD: Log, re-raise, or exit with failure.
# MAGIC try:
# MAGIC     risky_operation()
# MAGIC except Exception as e:
# MAGIC     print(f"ERROR: {e}")
# MAGIC     dbutils.notebook.exit(json.dumps({"status": "failed", "error": str(e)}))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: No audit trail (can't debug past runs)
# MAGIC ```python
# MAGIC # BAD: No record of what was processed.
# MAGIC df.write.mode("overwrite").saveAsTable("gold.table")
# MAGIC
# MAGIC # GOOD: Add metadata columns for debugging.
# MAGIC df.withColumn("_etl_timestamp", current_timestamp()) \
# MAGIC   .withColumn("_etl_batch_id", lit(run_id)) \
# MAGIC   .write.mode("overwrite").saveAsTable("gold.table")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7: Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("HOMEWORK — Workflow Patterns")
print("="*70)

print("\n--- Level 1: MERGE for idempotent upsert ---")
print("  DeltaTable.forName(spark, 'target').alias('t').merge(source.alias('s'), 'condition')")
print("  .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()")

print("\n--- Level 2: replaceWhere for partition overwrite ---")
print("  df.write.option('replaceWhere', \"date = '2024-01-15'\").saveAsTable(...)")

print("\n--- Level 3: Error handling template ---")
print("  try: process() except: dbutils.notebook.exit(json.dumps({'error': str(e)}))")

print("\n--- Level 4: Add audit columns ---")
print("  .withColumn('_etl_timestamp', current_timestamp())")
print("  .withColumn('_etl_source', lit('daily_batch'))")

print("\n--- Level 5: Quality gate ---")
print("  assert df.count() > 0; assert df.filter('amount < 0').count() == 0")

print("\n--- Level 6: Backfill pattern ---")
print("  Process range of dates: for date in date_range: job.run_now(params={'date': date})")

print("\n--- Level 7: Circuit breaker ---")
print("  If error_rate > threshold: set task_value('halt', True); skip downstream.")

print("\n--- Level 8: Medallion in Jobs ---")
print("  Bronze task → Silver task → Gold task (DAG with depends_on).")

print("\n--- Level 9: Monitoring ---")
print("  Log row counts, durations to a metrics table.")
print("  Alert if count drops below expected threshold.")

print("\n--- Level 10: Teach workflow patterns ---")
print("""
"Production ETL patterns:
  Idempotency: MERGE or replaceWhere (safe to re-run).
  Error handling: try/except + exit with status JSON.
  Quality gates: assertions before writing to Gold.
  Audit trail: _etl_timestamp, _etl_batch_id columns.
  Backfill: parameterize dates, re-run for historical.
  Medallion: Bronze (raw) → Silver (clean) → Gold (business).
  Always: log, validate, parameterize, make idempotent."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 97")
print("="*70)