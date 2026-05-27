# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 77: Streaming Aggregations and Windows
# MAGIC ## Module 12: Streaming
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Streaming aggregations** let you compute running totals, counts, averages, and time-windowed metrics on data that never stops arriving. Combined with **watermarks**, you can handle late-arriving data gracefully while keeping memory bounded.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine you run a highway toll booth:
# MAGIC - **Simple aggregation (no window)** = Count ALL cars that ever passed. Counter grows forever.
# MAGIC - **Tumbling window** = Count cars per hour. At the end of each hour, report the count and start fresh. Windows don't overlap.
# MAGIC - **Sliding window** = Count cars in the last 30 minutes, updating every 5 minutes. Windows overlap.
# MAGIC - **Watermark** = "If a car's timestamp is more than 15 minutes late, we won't update old windows for it." This lets you discard old state and free memory.
# MAGIC
# MAGIC ### Key Concepts:
# MAGIC | Concept | Meaning |
# MAGIC |---------|--------|
# MAGIC | Tumbling window | Fixed-size, non-overlapping time buckets (e.g., every 5 minutes) |
# MAGIC | Sliding window | Overlapping windows (e.g., 10-min window, sliding every 2 min) |
# MAGIC | Watermark | Threshold for how late data can arrive before being dropped |
# MAGIC | Event time | Timestamp IN the data (when event occurred) |
# MAGIC | Processing time | When Spark processes the event (wall clock) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Tumbling Windows (non-overlapping):
# MAGIC
# MAGIC   Time:  |--0:00--|--0:05--|--0:10--|--0:15--|--0:20--|
# MAGIC   Window: [W1     ] [W2     ] [W3     ] [W4     ]
# MAGIC   
# MAGIC   Each event falls into exactly ONE window.
# MAGIC   Window closes after its end time + watermark delay.
# MAGIC
# MAGIC Sliding Windows (overlapping):
# MAGIC
# MAGIC   Time:  |--0:00--|--0:05--|--0:10--|--0:15--|
# MAGIC   W1:    [========== 10 min ==========]
# MAGIC   W2:         [========== 10 min ==========]
# MAGIC   W3:              [========== 10 min ==========]
# MAGIC   
# MAGIC   Window size: 10 minutes. Slide interval: 5 minutes.
# MAGIC   Each event may fall into MULTIPLE windows.
# MAGIC
# MAGIC Watermarks (handling late data):
# MAGIC
# MAGIC   Watermark = "10 minutes"
# MAGIC   Current max event time seen: 12:30
# MAGIC   
# MAGIC   Watermark boundary = 12:30 - 10 min = 12:20
# MAGIC   
# MAGIC   Event at 12:25 → ACCEPTED (after watermark boundary)
# MAGIC   Event at 12:15 → DROPPED (before watermark boundary, too late)
# MAGIC   
# MAGIC   Windows ending before 12:20 are CLOSED and state is freed.
# MAGIC   This prevents unbounded state growth!
# MAGIC
# MAGIC Code Pattern:
# MAGIC
# MAGIC   stream_df
# MAGIC     .withWatermark("event_time", "10 minutes")   # Allow 10 min late.
# MAGIC     .groupBy(
# MAGIC         window(col("event_time"), "5 minutes")   # 5-min tumbling windows.
# MAGIC     )
# MAGIC     .agg(count("*").alias("event_count"))
# MAGIC     .writeStream
# MAGIC     .outputMode("append")   # Append: emit window result AFTER watermark closes it.
# MAGIC     .start()
# MAGIC
# MAGIC Output Mode with Windows:
# MAGIC   append:   Emit window result ONLY after watermark closes window (delayed but final).
# MAGIC   update:   Emit window result on EVERY batch (shows intermediate counts).
# MAGIC   complete: Emit ALL windows every batch (memory-expensive, rarely used).
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3-5: Examples
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-5 — EXAMPLES (Beginner to Advanced)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import (
    col, window, count, sum as spark_sum, avg,
    current_timestamp, expr, to_timestamp, lit
)  # Imports.
import time  # For waiting.

print("="*70)
print("SECTIONS 3-5: Streaming Aggregations & Windows")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Simple streaming count (no window)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Running count by key (no window)")
print("-"*60)

# Generate streaming data with a key column.
rate_stream = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 20)  # 20 rows/sec.
    .load()
    .withColumn("key", (col("value") % 5).cast("string"))  # 5 keys: 0-4.
)

# Simple aggregation: count per key (running total).
agg_stream = rate_stream.groupBy("key").agg(count("*").alias("total_events"))

# Write with complete mode (rewrites all results each batch).
query1 = (
    agg_stream.writeStream
    .format("memory")
    .queryName("simple_count")
    .outputMode("complete")  # Complete: emit ALL group results each batch.
    .start()
)

time.sleep(5)  # Let it accumulate data.

# Check running totals.
print("\nRunning counts by key:")
display(spark.sql("SELECT * FROM simple_count ORDER BY key"))

query1.stop()
print("\n✓ Running count shows cumulative totals. Complete mode emits all groups.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Tumbling window aggregation
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Tumbling window (fixed non-overlapping buckets)")
print("-"*60)

# Rate source provides a 'timestamp' column (event time).
rate_stream2 = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("category", (col("value") % 3).cast("string"))  # 3 categories.
)

# Tumbling window: 10-second buckets.
windowed = (
    rate_stream2
    .withWatermark("timestamp", "10 seconds")  # Allow 10 sec late data.
    .groupBy(
        window(col("timestamp"), "10 seconds"),  # 10-sec tumbling window.
        col("category")
    )
    .agg(count("*").alias("event_count"))
)

# Write with update mode (emit changed windows each batch).
query2 = (
    windowed.writeStream
    .format("memory")
    .queryName("tumbling_windows")
    .outputMode("update")  # Update: emit only changed rows.
    .start()
)

time.sleep(15)  # Let multiple windows fill.

print("\nTumbling window results:")
display(spark.sql("""
    SELECT window.start, window.end, category, event_count 
    FROM tumbling_windows 
    ORDER BY window.start DESC, category
    LIMIT 10
"""))

query2.stop()
print("\n✓ Each row = one 10-second window + category. Non-overlapping buckets.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Sliding window aggregation
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Sliding window (overlapping buckets)")
print("-"*60)

rate_stream3 = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 10).load()
)

# Sliding window: 20-second window, sliding every 5 seconds.
# Each event appears in up to 4 windows (20/5 = 4).
sliding = (
    rate_stream3
    .withWatermark("timestamp", "15 seconds")
    .groupBy(
        window(col("timestamp"), "20 seconds", "5 seconds")  # (size, slide).
    )
    .agg(count("*").alias("events_in_window"))
)

query3 = (
    sliding.writeStream
    .format("memory")
    .queryName("sliding_windows")
    .outputMode("update")
    .start()
)

time.sleep(20)  # Let windows overlap.

print("\nSliding window results (20-sec window, 5-sec slide):")
display(spark.sql("""
    SELECT window.start, window.end, events_in_window
    FROM sliding_windows
    ORDER BY window.start DESC
    LIMIT 8
"""))

query3.stop()
print("\n✓ Overlapping windows: each event counted in multiple windows.")
print("  Window(20s, 5s) = each event in up to 4 windows.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Watermark + Append mode (production pattern)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Watermark + Append (final window results)")
print("-"*60)

print("""
Production Pattern:

  stream
    .withWatermark("event_time", "10 minutes")   # Wait 10 min for late data.
    .groupBy(window("event_time", "5 minutes"))  # 5-min tumbling windows.
    .agg(count("*"), sum("amount"))
    .writeStream
    .outputMode("append")    # Append: emit window ONLY after watermark closes it.
    .format("delta")
    .start("/output/windowed_metrics")

Why Append + Watermark is best for production:
  1. Each window result emitted ONCE (final, immutable).
  2. No duplicate writes (idempotent).
  3. Downstream can safely read append-only output.
  4. Memory bounded: closed windows' state is freed.

When window emits in Append mode:
  Watermark = 10 min, Window = 5 min (12:00-12:05)
  Max event time seen reaches 12:15.
  Watermark boundary = 12:15 - 10 min = 12:05.
  Window [12:00-12:05] closes because 12:05 <= watermark boundary.
  → Window result emitted to output.
  → State for that window freed from memory.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: Multiple aggregations in one window
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: Multiple metrics per window")
print("-"*60)

from pyspark.sql.functions import min as spark_min, max as spark_max  # More aggs.

rate_stream5 = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 15).load()
    .withColumn("amount", (col("value") % 100 + 1).cast("double"))  # Simulated amount.
    .withColumn("region", (col("value") % 3).cast("string"))  # 3 regions.
)

# Multiple aggregations per window + grouping key.
multi_agg = (
    rate_stream5
    .withWatermark("timestamp", "10 seconds")
    .groupBy(
        window(col("timestamp"), "10 seconds"),  # 10-sec window.
        col("region")
    )
    .agg(
        count("*").alias("num_events"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        spark_min("amount").alias("min_amount"),
        spark_max("amount").alias("max_amount")
    )
)

query5 = multi_agg.writeStream.format("memory").queryName("multi_agg").outputMode("update").start()
time.sleep(12)

print("\nMultiple metrics per window + region:")
display(spark.sql("""
    SELECT window.start, region, num_events, 
           round(total_amount, 2) as total, round(avg_amount, 2) as avg_amt
    FROM multi_agg
    ORDER BY window.start DESC, region
    LIMIT 9
"""))

query5.stop()
print("\n✓ Multiple aggregations computed in a single pass over the window.")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Aggregation without watermark in append mode
# MAGIC ```python
# MAGIC # BAD: Append mode with aggregation but no watermark = ERROR.
# MAGIC stream.groupBy("key").count()
# MAGIC     .writeStream.outputMode("append").start()  # AnalysisException!
# MAGIC
# MAGIC # GOOD: Add watermark OR use complete/update mode.
# MAGIC stream.withWatermark("event_time", "10 minutes")
# MAGIC     .groupBy(window("event_time", "5 minutes")).count()
# MAGIC     .writeStream.outputMode("append").start()  # Works!
# MAGIC ```
# MAGIC **Why**: Without watermark, Spark can never finalize a window (data might arrive forever).
# MAGIC
# MAGIC ### Mistake 2: Setting watermark too short (dropping valid data)
# MAGIC ```python
# MAGIC # BAD: 1-second watermark when data can arrive 5 minutes late.
# MAGIC .withWatermark("event_time", "1 second")  # Late data silently dropped!
# MAGIC
# MAGIC # GOOD: Set watermark based on actual max lateness.
# MAGIC .withWatermark("event_time", "15 minutes")  # Allows 15 min late.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Using processing time instead of event time for windows
# MAGIC ```python
# MAGIC # BAD: current_timestamp() is processing time (when Spark sees it).
# MAGIC stream.withColumn("time", current_timestamp())
# MAGIC     .groupBy(window("time", "5 minutes")).count()  # Not event time!
# MAGIC
# MAGIC # GOOD: Use the actual event timestamp from your data.
# MAGIC stream.withWatermark("event_time", "10 minutes")
# MAGIC     .groupBy(window("event_time", "5 minutes")).count()  # Correct!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Forgetting that complete mode keeps ALL state forever
# MAGIC ```python
# MAGIC # BAD: Complete mode never frees state. Memory grows forever.
# MAGIC stream.groupBy(window("ts", "1 minute")).count()
# MAGIC     .writeStream.outputMode("complete").start()  # Unbounded memory!
# MAGIC
# MAGIC # GOOD: Use append + watermark for bounded state.
# MAGIC stream.withWatermark("ts", "10 minutes")
# MAGIC     .groupBy(window("ts", "1 minute")).count()
# MAGIC     .writeStream.outputMode("append").start()  # Memory bounded.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Window column is a struct (not a simple timestamp)
# MAGIC ```python
# MAGIC # The window() function creates a STRUCT with .start and .end fields.
# MAGIC # BAD: Treating window as a timestamp.
# MAGIC df.select("window")  # Shows struct<start:timestamp, end:timestamp>
# MAGIC
# MAGIC # GOOD: Extract start/end from the struct.
# MAGIC df.select(col("window.start"), col("window.end"), "count")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, window, count, avg, current_timestamp  # Imports.
import time

print("="*70)
print("HOMEWORK — Streaming Aggregations & Windows")
print("="*70)

# Level 1: Simple streaming count.
print("\n--- Level 1: Streaming count ---")
df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
agg = df.withColumn("k", (col("value")%3).cast("string")).groupBy("k").count()
q = agg.writeStream.format("memory").queryName("hw77_l1").outputMode("complete").start()
time.sleep(3)
print(f"Groups: {spark.sql('SELECT * FROM hw77_l1').count()}")
q.stop()
# WHY: groupBy on streaming requires complete or update mode.

# Level 2: Tumbling window.
print("\n--- Level 2: Tumbling window ---")
df2 = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
win = df2.withWatermark("timestamp", "5 seconds") \
    .groupBy(window("timestamp", "5 seconds")).count()
q2 = win.writeStream.format("memory").queryName("hw77_l2").outputMode("update").start()
time.sleep(8)
display(spark.sql("SELECT window.start, window.end, count FROM hw77_l2 ORDER BY window.start DESC LIMIT 3"))
q2.stop()
# WHY: window(col, "5 seconds") creates 5-sec non-overlapping buckets.

# Level 3: Sliding window.
print("\n--- Level 3: Sliding window ---")
df3 = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
slide = df3.withWatermark("timestamp", "10 seconds") \
    .groupBy(window("timestamp", "10 seconds", "3 seconds")).count()
q3 = slide.writeStream.format("memory").queryName("hw77_l3").outputMode("update").start()
time.sleep(10)
print(f"Windows: {spark.sql('SELECT count(*) FROM hw77_l3').collect()[0][0]}")
q3.stop()
# WHY: window(col, "10 sec", "3 sec") = 10-sec window sliding every 3 sec.

# Level 4: Watermark explanation.
print("\n--- Level 4: Watermark purpose ---")
print("Watermark = max lateness allowed. Enables:")
print("  1. Append mode with aggregations (window closes after watermark).")
print("  2. Memory management (old state freed after window closes).")
print("  3. Late data handling (events before watermark are dropped).")

# Level 5-10: Conceptual.
print("\n--- Level 5: Tumbling vs Sliding ---")
print("Tumbling: non-overlapping, event in exactly 1 window.")
print("Sliding: overlapping, event in multiple windows.")

print("\n--- Level 6: Output mode choice ---")
print("Append + watermark: best for production (bounded, final results).")
print("Update: good for dashboards (see intermediate counts).")
print("Complete: rarely used (rewrites everything, unbounded memory).")

print("\n--- Level 7: Window struct ---")
print("window() returns struct<start, end>. Access: col('window.start').")

print("\n--- Level 8: Multiple aggs per window ---")
print(".agg(count('*'), sum('amt'), avg('amt')) in single groupBy.")

print("\n--- Level 9: Bounded state ---")
print("Watermark + Append = Spark frees state for closed windows.")
print("Without watermark: state grows forever (OOM on long-running streams).")

print("\n--- Level 10: Teach windows ---")
print("""
"Streaming windows: bucket events by time.
  Tumbling(5 min): non-overlapping 5-min buckets.
  Sliding(10 min, 2 min): 10-min windows, sliding every 2 min.
  Watermark: how late data can arrive (enables state cleanup).
  Production: watermark + append mode = bounded memory + final results."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 77")
print("="*70)