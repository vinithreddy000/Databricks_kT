# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 80: Streaming Best Practices & Production Patterns
# MAGIC ## Module 12: Streaming
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC This notebook covers everything you need to run **production-grade streaming pipelines** in Databricks: monitoring, error handling, scaling, Delta integration, and the medallion architecture (Bronze → Silver → Gold) for streaming.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Building a streaming pipeline is like running a **24/7 factory assembly line**:
# MAGIC - You need **monitoring dashboards** (is the line running? Any jams?)
# MAGIC - You need **error handling** (what happens when a part is defective?)
# MAGIC - You need **scaling** (can we handle Black Friday volume?)
# MAGIC - You need **maintenance windows** (how to upgrade without stopping production)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Production Streaming Architecture (Medallion):
# MAGIC
# MAGIC   RAW FILES        BRONZE              SILVER              GOLD
# MAGIC   (landing zone)   (raw ingestion)     (cleaned/deduped)   (aggregated/BI)
# MAGIC   ────────────     ───────────────  ─────────────────  ──────────────────
# MAGIC   CSV/JSON/Parquet  Auto Loader         Stream from Delta   Stream from Delta
# MAGIC   dropped in ADLS   → Delta table       → Clean + Dedup     → Aggregate
# MAGIC                     + metadata cols     → Delta table       → Delta table
# MAGIC
# MAGIC   Each arrow is a streaming pipeline with its own checkpoint.
# MAGIC
# MAGIC Monitoring Checklist:
# MAGIC   ═════════════════
# MAGIC   1. Input rate (rows/sec): Is data arriving?
# MAGIC   2. Processing rate (rows/sec): Keeping up with input?
# MAGIC   3. Batch duration: How long each micro-batch takes?
# MAGIC   4. State rows: Growing unboundedly? (Memory concern)
# MAGIC   5. Trigger interval: How often batches run?
# MAGIC   6. Exceptions: Any failures?
# MAGIC
# MAGIC   Access via:
# MAGIC     query.lastProgress      → JSON with all metrics
# MAGIC     query.status            → Current state
# MAGIC     Spark UI → Structured Streaming tab
# MAGIC     Databricks SQL → query the streaming_metrics system table
# MAGIC
# MAGIC Error Handling Strategy:
# MAGIC   ═══════════════════════
# MAGIC   1. Corrupt records → Rescue column (_rescued_data) or badRecordsPath
# MAGIC   2. Schema changes  → Schema evolution (mergeSchema/addNewColumns)
# MAGIC   3. Transient failures → Spark retries automatically (checkpoints)
# MAGIC   4. Persistent failures → Dead letter queue (write bad data to separate table)
# MAGIC   5. OOM → Increase driver/executor memory, reduce state with tighter watermarks
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-5: Production Patterns
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-5 — PRODUCTION PATTERNS
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, current_timestamp, input_file_name, lit  # Imports.
import time, json  # Utilities.

print("="*70)
print("SECTIONS 3-5: Production Streaming Patterns")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Monitoring streaming query progress
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Monitoring with query.lastProgress")
print("-"*60)

# Start a test stream.
q = spark.readStream.format("rate").option("rowsPerSecond", 10).load() \
    .writeStream.format("memory").queryName("monitor_demo") \
    .outputMode("append").start()

time.sleep(5)  # Let a few batches run.

# Get progress metrics.
progress = q.lastProgress  # Returns dict with detailed metrics.
if progress:
    print("\nLast batch progress:")
    print(f"  Batch ID: {progress.get('batchId')}")
    print(f"  Input rows: {progress.get('numInputRows')}")
    print(f"  Input rows/sec: {progress.get('inputRowsPerSecond')}")
    print(f"  Process rows/sec: {progress.get('processedRowsPerSecond')}")
    print(f"  Batch duration: {progress.get('batchDuration')}")
    
    # Check if keeping up.
    input_rate = progress.get('inputRowsPerSecond', 0)
    process_rate = progress.get('processedRowsPerSecond', 0)
    if process_rate and input_rate:
        if process_rate >= input_rate:
            print("  \n  ✓ HEALTHY: Processing rate >= input rate (keeping up).")
        else:
            print("  \n  ⚠️  FALLING BEHIND: Processing slower than input!")

print(f"\nQuery status: {q.status}")
q.stop()

print("")
print("Key metrics to monitor:")
print("  inputRowsPerSecond > processedRowsPerSecond = FALLING BEHIND")
print("  Fix: Add more cores, optimize transformations, or increase trigger interval.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: foreachBatch MERGE (upsert pattern)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: foreachBatch MERGE (upsert into Delta)")
print("-"*60)

print("""
The #1 production pattern for streaming to Delta:

  from delta.tables import DeltaTable

  def upsert_to_delta(batch_df, batch_id):
      '''MERGE streaming batch into target Delta table.'''
      # Create temp view from micro-batch.
      batch_df.createOrReplaceTempView("stream_batch")
      
      # MERGE: update existing rows, insert new ones.
      spark.sql(\"\"\"
          MERGE INTO catalog.schema.target t
          USING stream_batch s
          ON t.id = s.id
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *
      \"\"\")

  # Start the upsert stream.
  stream.writeStream
      .foreachBatch(upsert_to_delta)
      .outputMode("append")
      .option("checkpointLocation", "/checkpoints/upsert_stream")
      .trigger(processingTime="30 seconds")
      .start()

Why MERGE + foreachBatch:
  - Handles late duplicates (MATCHED → UPDATE).
  - Handles new events (NOT MATCHED → INSERT).
  - Exactly-once with Delta + checkpoint.
  - The standard pattern for CDC-style streaming.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Complete medallion streaming pipeline
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Medallion architecture for streaming")
print("-"*60)

print("""
┌────────────────────────────────────────────────────────────────────┐
│  STREAMING MEDALLION ARCHITECTURE                              │
└────────────────────────────────────────────────────────────────────┘

Layer 1 — BRONZE (Raw Ingestion):
  Source: Auto Loader (cloudFiles)
  Transform: Add metadata columns only
  Sink: Delta table (append)
  Trigger: availableNow (scheduled) or processingTime (continuous)
  Goal: Raw data landed quickly, no transformation.

Layer 2 — SILVER (Cleaned & Enriched):
  Source: readStream from Bronze Delta
  Transform: Validate, clean, deduplicate, join lookups
  Sink: Delta table (append or foreachBatch + MERGE)
  Goal: Clean, deduplicated, type-correct data.

Layer 3 — GOLD (Business Aggregates):
  Source: readStream from Silver Delta
  Transform: Windowed aggregations, business metrics
  Sink: Delta table (append with watermark)
  Goal: Ready for dashboards and reporting.

Key principles:
  1. Each layer is its own streaming query with its own checkpoint.
  2. Delta-to-Delta streaming: source emits only new committed data.
  3. Exactly-once: checkpoint + Delta = no duplicates.
  4. Independent scaling: each layer can be on different clusters.
  5. Reprocessing: delete checkpoint to reprocess from Bronze.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Production streaming checklist
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Production Streaming Checklist")
print("-"*60)

print("""
✅ PRODUCTION STREAMING CHECKLIST:

  ☐ Checkpoint on cloud storage (ADLS/S3), not DBFS local.
  ☐ Unique checkpoint per query.
  ☐ Trigger mode chosen: availableNow (ETL) or processingTime (real-time).
  ☐ Output mode appropriate: append for ETL, update for dashboards.
  ☐ Watermark set for stateful operations.
  ☐ Error handling: rescue column or dead letter queue.
  ☐ Monitoring: alerts on input vs processing rate.
  ☐ Auto Optimize on output Delta tables (optimizeWrite + autoCompact).
  ☐ Cluster: use Jobs cluster (auto-terminates) for scheduled streams.
  ☐ Testing: test with rate source and memory sink before production.

  SCALING TIPS:
  • Add shuffle partitions if processing is slow (but AQE helps).
  • Increase maxFilesPerTrigger for Auto Loader to control batch size.
  • Use Photon for faster Delta writes.
  • Use Delta Lake's Auto Compaction to prevent small files.
  • Consider Trigger.AvailableNow in scheduled jobs (cost-effective).

  COMMON FAILURES & FIXES:
  • OutOfMemory: Reduce state (tighter watermark), increase executor memory.
  • Checkpoint corruption: Delete checkpoint and reprocess (data in Delta).
  • Schema mismatch: Use schemaEvolution or rescue column.
  • Slow batches: Check Spark UI for skew, increase parallelism.
""")

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

import time

print("="*70)
print("HOMEWORK — Streaming Best Practices")
print("="*70)

# Level 1: Check query progress.
print("\n--- Level 1: Monitor a stream ---")
q = spark.readStream.format("rate").option("rowsPerSecond", 10).load() \
    .writeStream.format("memory").queryName("hw80_monitor").outputMode("append").start()
time.sleep(3)
p = q.lastProgress
if p:
    print(f"Input rate: {p.get('inputRowsPerSecond', 'N/A')} rows/sec")
    print(f"Process rate: {p.get('processedRowsPerSecond', 'N/A')} rows/sec")
q.stop()
# WHY: lastProgress shows if your stream is keeping up.

# Level 2: Stop all streams safely.
print("\n--- Level 2: Stop all streams ---")
q2 = spark.readStream.format("rate").option("rowsPerSecond", 5).load() \
    .writeStream.format("memory").queryName("hw80_l2").outputMode("append").start()
time.sleep(1)
print(f"Active before: {len(spark.streams.active)}")
for s in spark.streams.active:
    s.stop()
print(f"Active after: {len(spark.streams.active)}")
# WHY: Always clean up streams in notebooks to free cluster resources.

# Level 3-10: Conceptual.
print("\n--- Level 3: Medallion layers ---")
print("Bronze: raw ingestion (Auto Loader → Delta).")
print("Silver: cleaned, deduped, joined (Stream from Bronze → Delta).")
print("Gold: aggregated metrics (Stream from Silver → Delta).")

print("\n--- Level 4: foreachBatch MERGE ---")
print("Best pattern for upserts: foreachBatch + MERGE INTO.")
print("Handles duplicates + new records in one operation.")

print("\n--- Level 5: Exactly-once guarantee ---")
print("Requires: Checkpoint (tracks progress) + Idempotent sink (Delta).")
print("On failure: restart from checkpoint = no duplicates.")

print("\n--- Level 6: availableNow for cost savings ---")
print("Schedule with Jobs (hourly). availableNow processes backlog, then cluster terminates.")
print("Much cheaper than running 24/7 for batch-like requirements.")

print("\n--- Level 7: Monitoring alerts ---")
print("Alert when: processedRowsPerSecond < inputRowsPerSecond (falling behind).")
print("Alert when: state rows growing unboundedly (missing watermark).")
print("Alert when: query.exception() is not None (stream crashed).")

print("\n--- Level 8: Handling schema evolution ---")
print("Auto Loader: schemaEvolutionMode=addNewColumns.")
print("Delta writer: .option('mergeSchema', 'true').")

print("\n--- Level 9: Scaling strategies ---")
print("1. More cores = more parallelism in micro-batches.")
print("2. maxFilesPerTrigger = control batch size.")
print("3. Photon = faster Delta writes (2-5x).")
print("4. Separate clusters per medallion layer.")

print("\n--- Level 10: Teach streaming best practices ---")
print("""
"Production streaming checklist:
  1. Checkpoint on cloud storage (unique per query).
  2. availableNow for ETL, processingTime for real-time.
  3. Watermark for all stateful operations (memory bounded).
  4. Monitor: input rate vs process rate.
  5. foreachBatch + MERGE for upserts.
  6. Medallion: Bronze (raw) → Silver (clean) → Gold (aggregate).
  7. Auto Optimize on output tables.
  8. Test with rate source before production."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 80")
print("✓ MODULE 12 (STREAMING) COMPLETE! All 6 notebooks (75-80) done.")
print("="*70)