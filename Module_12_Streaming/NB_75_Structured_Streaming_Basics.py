# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview and How It Works
# MAGIC %md
# MAGIC # Notebook 75: Structured Streaming — Fundamentals
# MAGIC ## Module 12: Streaming
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 55 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Structured Streaming** is Spark's engine for processing data that arrives continuously — events, logs, sensor readings, transactions — using the exact same DataFrame/SQL API you already know. Instead of processing a fixed file, you process an **infinite table** that grows as new data arrives.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine a sushi conveyor belt restaurant:
# MAGIC - **Batch processing** = You wait until the restaurant closes, collect ALL plates, then count and bill everything at once.
# MAGIC - **Streaming** = A camera watches the belt continuously. Every time a plate passes, it's instantly counted and billed. You always know your running total.
# MAGIC
# MAGIC Structured Streaming treats the data stream as a table that new rows are continuously appended to. Your query runs on this "unbounded table" and outputs results to a "result table."
# MAGIC
# MAGIC ### Key Concepts:
# MAGIC | Concept | Meaning |
# MAGIC |---------|--------|
# MAGIC | Source | Where data comes from (Kafka, files, Delta, socket) |
# MAGIC | Sink | Where results go (Delta, console, memory, Kafka) |
# MAGIC | Trigger | How often to process (micro-batch, continuous, availableNow) |
# MAGIC | Checkpoint | Saves progress so streaming can resume after failure |
# MAGIC | Watermark | How long to wait for late data before closing a window |
# MAGIC | Output Mode | append (new rows only), complete (full result), update (changed rows) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Structured Streaming Execution Model:
# MAGIC
# MAGIC   DATA SOURCE (unbounded)           YOUR QUERY             RESULT TABLE (sink)
# MAGIC   ─────────────────────────           ──────────             ───────────────────
# MAGIC   t=0: [row1, row2, row3]    →     df.groupBy()   →    [grp_A: 2, grp_B: 1]
# MAGIC   t=1: [row4, row5]          →     .count()        →    [grp_A: 3, grp_B: 2]
# MAGIC   t=2: [row6, row7, row8]    →                     →    [grp_A: 5, grp_B: 3]
# MAGIC   ...continues forever...
# MAGIC
# MAGIC Micro-Batch Execution (default):
# MAGIC   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
# MAGIC   │  Batch 0     │    │  Batch 1     │    │  Batch 2     │
# MAGIC   │ rows 1-100   │ →  │ rows 101-250 │ →  │ rows 251-400 │ → ...
# MAGIC   │ (trigger)    │    │ (trigger)    │    │ (trigger)    │
# MAGIC   └──────────────┘    └──────────────┘    └──────────────┘
# MAGIC        │                    │                    │
# MAGIC        └────checkpoint─────┴────checkpoint─────┘
# MAGIC        (progress saved after each batch for fault tolerance)
# MAGIC
# MAGIC The Code Pattern (always the same):
# MAGIC
# MAGIC   # 1. READ: Define the streaming source.
# MAGIC   stream_df = spark.readStream
# MAGIC       .format("delta")           # or "kafka", "cloudFiles", "rate"
# MAGIC       .load("/path/to/source")
# MAGIC
# MAGIC   # 2. TRANSFORM: Use regular DataFrame operations.
# MAGIC   result = stream_df
# MAGIC       .filter(col("value") > 0)
# MAGIC       .groupBy("key")
# MAGIC       .count()
# MAGIC
# MAGIC   # 3. WRITE: Start the streaming query.
# MAGIC   query = result.writeStream
# MAGIC       .format("delta")           # Sink format.
# MAGIC       .outputMode("append")      # append | complete | update
# MAGIC       .option("checkpointLocation", "/path/to/checkpoint")
# MAGIC       .trigger(availableNow=True)  # or processingTime="10 seconds"
# MAGIC       .start("/path/to/output")
# MAGIC
# MAGIC   # 4. WAIT (optional in notebooks):
# MAGIC   query.awaitTermination()
# MAGIC
# MAGIC Output Modes:
# MAGIC   append:   Only NEW rows added since last trigger. (Most common for ETL.)
# MAGIC   complete: ENTIRE result table rewritten. (For aggregations.)
# MAGIC   update:   Only CHANGED rows emitted. (Efficient for aggregations.)
# MAGIC
# MAGIC Trigger Options:
# MAGIC   processingTime="10 seconds"  → Run every 10 seconds.
# MAGIC   availableNow=True            → Process all available, then stop. (Batch-like.)
# MAGIC   once=True                    → Process one micro-batch, then stop. (Deprecated.)
# MAGIC   continuous="1 second"        → Low-latency continuous (experimental).
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, current_timestamp, expr  # Imports.

print("="*70)
print("SECTION 3 — BEGINNER EXAMPLES: Structured Streaming Basics")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: The Rate source (built-in test data generator)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Rate source (generates test streaming data)")
print("-"*60)

# The 'rate' source generates rows with (timestamp, value) at a given rate.
# Perfect for learning/testing without needing real data.
rate_stream = (
    spark.readStream                   # readStream = streaming source.
    .format("rate")                    # Built-in rate generator.
    .option("rowsPerSecond", 10)       # Generate 10 rows per second.
    .load()                            # Start reading.
)

# Check: is this a streaming DataFrame?
print(f"\nIs streaming: {rate_stream.isStreaming}")  # True.
print(f"Schema: {rate_stream.schema}")
print("  Columns: timestamp (when generated), value (auto-incrementing long)")

# Write to memory sink for quick inspection.
query1 = (
    rate_stream
    .writeStream                       # writeStream = streaming output.
    .format("memory")                  # Memory sink (for testing only!).
    .queryName("rate_test")            # Table name to query.
    .outputMode("append")              # Append new rows.
    .start()                           # Start the stream.
)

import time  # For waiting.
time.sleep(5)  # Let it run 5 seconds to generate data.

# Query the in-memory table.
print("\nData generated (last 5 rows):")
display(spark.sql("SELECT * FROM rate_test ORDER BY timestamp DESC LIMIT 5"))

# ALWAYS stop streaming queries when done!
query1.stop()  # Stop the stream.
print("\n✓ Stream stopped. In production, streams run continuously.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Streaming from Delta (file-based streaming)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Stream from Delta table (most common in Databricks)")
print("-"*60)

from pyspark.sql.functions import rand, lit  # Additional imports.

# First, create a Delta source table with some data.
source_path = "/tmp/delta_kt/streaming_source"
spark.range(100).select(
    col("id"),
    (rand() * 1000).alias("amount"),
    lit("2025-01-01").alias("date")
).write.format("delta").mode("overwrite").save(source_path)

# Now read it as a STREAM (will pick up new data automatically).
stream_df = (
    spark.readStream                   # Streaming read.
    .format("delta")                   # Delta format.
    .load(source_path)                 # Path to Delta table.
)

print(f"Is streaming: {stream_df.isStreaming}")  # True.
print(f"Schema: {stream_df.schema.simpleString()}")

# Write stream to Delta output with checkpoint.
output_path = "/tmp/delta_kt/streaming_output"
checkpoint_path = "/tmp/delta_kt/streaming_checkpoint"

query2 = (
    stream_df
    .filter(col("amount") > 500)       # Transform: filter high amounts.
    .writeStream
    .format("delta")                   # Write to Delta.
    .outputMode("append")              # Append only new rows.
    .option("checkpointLocation", checkpoint_path)  # REQUIRED for fault tolerance.
    .trigger(availableNow=True)        # Process all available data, then stop.
    .start(output_path)                # Output path.
)

query2.awaitTermination()  # Wait for it to finish (since trigger=availableNow).

# Check results.
result_df = spark.read.format("delta").load(output_path)
print(f"\nRows written to output: {result_df.count()}")
print("\n✓ Stream processed all available data and stopped (availableNow mode).")
print("  In production: use processingTime='10 seconds' for continuous processing.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Checking stream status and listing active streams
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Managing streaming queries")
print("-"*60)

# Start a test stream.
test_stream = (
    spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    .writeStream.format("memory").queryName("mgmt_test").outputMode("append").start()
)

time.sleep(2)  # Let it run briefly.

# List all active streaming queries.
print("\nActive streaming queries:")
for q in spark.streams.active:  # spark.streams.active = list of running queries.
    print(f"  Name: {q.name}, ID: {q.id}, Status: {q.status}")

# Get detailed status of a specific query.
print(f"\nQuery status: {test_stream.status}")
print(f"Is active: {test_stream.isActive}")
print(f"Last progress: {test_stream.lastProgress}")

# Stop the test.
test_stream.stop()
print("\n✓ All streams stopped.")
print("")
print("Key management methods:")
print("  spark.streams.active         → list all running streams")
print("  query.status                 → current state of query")
print("  query.lastProgress           → metrics from last batch")
print("  query.stop()                 → stop the stream")
print("  query.awaitTermination()     → block until stream stops")
print("  query.exception()            → get any error that stopped it")

# COMMAND ----------

# DBTITLE 1,Section 4-5: Intermediate and Advanced
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4-5 — INTERMEDIATE & ADVANCED
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, window, sum as spark_sum, count, current_timestamp  # Imports.
import time  # Timing.

print("="*70)
print("SECTIONS 4-5: Intermediate & Advanced Streaming")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Trigger modes compared
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Trigger modes")
print("-"*60)

print("""
Trigger Mode Comparison:

┌─────────────────────────┬──────────────────────────────────────────────────┐
│ Trigger                   │ Behavior                                         │
├─────────────────────────┼──────────────────────────────────────────────────┤
│ (none / default)          │ Process as fast as possible, one batch at a time  │
│ processingTime="10 sec"   │ Run every 10 seconds (even if no new data)        │
│ availableNow=True         │ Process ALL backlog, then stop. Best for ETL.     │
│ once=True (deprecated)    │ One micro-batch then stop. Use availableNow.      │
└─────────────────────────┴──────────────────────────────────────────────────┘

When to use each:
  - Continuous pipeline (near real-time): processingTime="10 seconds"
  - Scheduled batch-like ETL (e.g., hourly job): availableNow=True
  - Lowest latency (experimental): continuous="1 second"
""")

# Demo: availableNow vs processingTime.
source_path = "/tmp/delta_kt/streaming_source"  # Reuse from Example 2.
cp_path = "/tmp/delta_kt/stream_trigger_demo_cp"
out_path = "/tmp/delta_kt/stream_trigger_demo_out"

# availableNow: processes everything and stops.
query = (
    spark.readStream.format("delta").load(source_path)
    .writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", cp_path)
    .trigger(availableNow=True)  # Process all, then stop.
    .start(out_path)
)
query.awaitTermination()
print(f"availableNow: processed {spark.read.format('delta').load(out_path).count()} rows and stopped.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: Output modes (append vs complete vs update)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: Output modes explained")
print("-"*60)

print("""
Output Modes:

  APPEND (default, most common for ETL):
    - Only NEW rows since last trigger are written to sink.
    - Cannot be used with aggregations (unless with watermark).
    - Use for: ETL pipelines, log processing, simple transforms.

  COMPLETE:
    - ENTIRE result table is rewritten every trigger.
    - Required for aggregations without watermark.
    - Use for: dashboards, running totals, small result sets.

  UPDATE:
    - Only CHANGED rows are written (new or updated).
    - More efficient than complete for aggregations.
    - Use for: aggregations where you only need deltas.

Which mode supports which operations:

  ┌───────────────────────┬────────┬──────────┬────────┐
  │ Operation             │ Append │ Complete │ Update │
  ├───────────────────────┼────────┼──────────┼────────┤
  │ Simple transform      │ ✓      │ ✓        │ ✓      │
  │ Aggregation           │ ✗*     │ ✓        │ ✓      │
  │ Agg + watermark       │ ✓      │ ✓        │ ✓      │
  │ De-duplication        │ ✓      │ ✗        │ ✗      │
  └───────────────────────┴────────┴──────────┴────────┘
  * Append works with aggregation only when watermark is set.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Checkpoint deep dive
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Why checkpoints matter")
print("-"*60)

print("""
Checkpoints store:
  1. What data has been processed (offsets/file list)
  2. The state of aggregations (running counts, sums)
  3. Metadata about the query

Why they're critical:
  - If your stream CRASHES, it restarts FROM the checkpoint.
  - Without checkpoint: all progress lost, reprocesses everything.
  - With checkpoint: resumes exactly where it left off.

Rules:
  1. EVERY production stream MUST have a checkpoint.
  2. Checkpoint location must be UNIQUE per stream.
  3. Don't share checkpoints between different queries.
  4. Store checkpoints on cloud storage (ADLS/S3), not local.
  5. Don't delete checkpoints unless you want to reprocess everything.

Checkpoint directory structure:
  /checkpoint/
    /offsets/      ← Which data has been consumed
    /commits/      ← Which batches completed successfully
    /state/        ← Aggregation state (for stateful queries)
    /metadata      ← Query metadata (ID, etc.)
""")

print("✓ Always set checkpointLocation in production streams!")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Forgetting checkpointLocation
# MAGIC ```python
# MAGIC # BAD: No checkpoint. If stream crashes, all progress is lost.
# MAGIC stream.writeStream.format("delta").start("/output")  # Will ERROR or lose data!
# MAGIC
# MAGIC # GOOD: Always include checkpoint.
# MAGIC stream.writeStream.format("delta")
# MAGIC     .option("checkpointLocation", "/checkpoints/my_stream")
# MAGIC     .start("/output")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Using complete mode with huge result tables
# MAGIC ```python
# MAGIC # BAD: Complete mode rewrites ENTIRE result every batch.
# MAGIC # If result has 100M rows, every micro-batch writes 100M rows!
# MAGIC stream.groupBy("key").count().writeStream.outputMode("complete")  # Slow for big results.
# MAGIC
# MAGIC # GOOD: Use update mode for large aggregation results.
# MAGIC stream.groupBy("key").count().writeStream.outputMode("update")  # Only emits changes.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Not stopping test streams in notebooks
# MAGIC ```python
# MAGIC # BAD: Streams run forever, consuming cluster resources.
# MAGIC query = stream.writeStream.format("memory").start()  # Runs forever!
# MAGIC # ...you forget about it...
# MAGIC
# MAGIC # GOOD: Always stop when done testing.
# MAGIC query = stream.writeStream.format("memory").start()
# MAGIC # ...inspect results...
# MAGIC query.stop()  # Free resources!
# MAGIC
# MAGIC # Or stop ALL streams:
# MAGIC for q in spark.streams.active:
# MAGIC     q.stop()
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Using .collect() or .show() on a streaming DataFrame
# MAGIC ```python
# MAGIC # BAD: Can't call batch actions on streaming DataFrames.
# MAGIC stream_df.show()     # ERROR!
# MAGIC stream_df.count()    # ERROR!
# MAGIC stream_df.collect()  # ERROR!
# MAGIC
# MAGIC # GOOD: Use writeStream to output data, then read the output.
# MAGIC stream_df.writeStream.format("memory").queryName("t").start()
# MAGIC spark.sql("SELECT * FROM t").show()  # Query the memory table.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Sharing checkpoint between different queries
# MAGIC ```python
# MAGIC # BAD: Two streams sharing one checkpoint = corruption!
# MAGIC q1 = stream_a.writeStream.option("checkpointLocation", "/cp/shared").start()
# MAGIC q2 = stream_b.writeStream.option("checkpointLocation", "/cp/shared").start()  # CORRUPT!
# MAGIC
# MAGIC # GOOD: Each stream gets its own unique checkpoint.
# MAGIC q1 = stream_a.writeStream.option("checkpointLocation", "/cp/stream_a").start()
# MAGIC q2 = stream_b.writeStream.option("checkpointLocation", "/cp/stream_b").start()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, lit, current_timestamp  # Imports.
import time  # Timing.

print("="*70)
print("HOMEWORK — Structured Streaming Basics")
print("="*70)

# Level 1: Create a rate stream and verify it's streaming.
print("\n--- Level 1: Create a rate stream ---")
rate_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
print(f"Is streaming: {rate_df.isStreaming}")  # True.
print(f"Schema: {rate_df.schema.simpleString()}")
# WHY: readStream creates a streaming DataFrame. isStreaming confirms it.

# Level 2: Write to memory sink and query.
print("\n--- Level 2: Write to memory, query results ---")
q = rate_df.writeStream.format("memory").queryName("hw_rate").outputMode("append").start()
time.sleep(3)  # Generate some data.
print(f"Rows generated: {spark.sql('SELECT count(*) FROM hw_rate').collect()[0][0]}")
q.stop()
# WHY: Memory sink is easiest for testing. queryName becomes a SQL table.

# Level 3: Stream from Delta with availableNow.
print("\n--- Level 3: Delta stream with availableNow ---")
src = "/tmp/delta_kt/streaming_source"
cp = "/tmp/delta_kt/hw75_l3_cp"
out = "/tmp/delta_kt/hw75_l3_out"
q3 = spark.readStream.format("delta").load(src) \
    .writeStream.format("delta").outputMode("append") \
    .option("checkpointLocation", cp).trigger(availableNow=True).start(out)
q3.awaitTermination()
print(f"Output rows: {spark.read.format('delta').load(out).count()}")
# WHY: availableNow processes all backlog then stops (ideal for scheduled ETL).

# Level 4: List active streams.
print("\n--- Level 4: List active streams ---")
q4 = spark.readStream.format("rate").option("rowsPerSecond",1).load() \
    .writeStream.format("memory").queryName("hw_l4").outputMode("append").start()
print(f"Active streams: {len(spark.streams.active)}")
for s in spark.streams.active:
    print(f"  {s.name}: isActive={s.isActive}")
q4.stop()
# WHY: spark.streams.active shows all running queries in this session.

# Level 5: Add a filter transformation.
print("\n--- Level 5: Transform in a stream ---")
src = "/tmp/delta_kt/streaming_source"
cp5 = "/tmp/delta_kt/hw75_l5_cp"
out5 = "/tmp/delta_kt/hw75_l5_out"
q5 = spark.readStream.format("delta").load(src) \
    .filter(col("amount") > 800) \
    .writeStream.format("delta").outputMode("append") \
    .option("checkpointLocation", cp5).trigger(availableNow=True).start(out5)
q5.awaitTermination()
print(f"Filtered output rows: {spark.read.format('delta').load(out5).count()}")
# WHY: Streaming supports all DataFrame transforms (filter, select, join, etc.).

# Levels 6-10: Conceptual.
print("\n--- Level 6: Output modes ---")
print("append: only new rows. complete: full result. update: changed rows.")

print("\n--- Level 7: When to use each trigger ---")
print("Real-time: processingTime. Scheduled ETL: availableNow.")

print("\n--- Level 8: Checkpoint contents ---")
print("offsets/ (what was consumed), commits/ (completed), state/ (aggregations).")

print("\n--- Level 9: Fault tolerance ---")
print("Stream crashes → restart from checkpoint = exactly-once processing.")
print("Requires: idempotent sink (Delta!) + checkpoint + replayable source.")

print("\n--- Level 10: Teach streaming to a colleague ---")
print("""
"Structured Streaming = process infinite data with the DataFrame API.
  readStream → transform (same as batch!) → writeStream.
  Key pieces: source, transform, sink, trigger, checkpoint.
  Trigger.availableNow for ETL. processingTime for real-time.
  ALWAYS set checkpointLocation for production.
  Delta + streaming = exactly-once, fault-tolerant pipelines."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 75")
print("="*70)