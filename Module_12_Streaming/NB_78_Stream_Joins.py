# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 78: Stream Joins (Stream-Static & Stream-Stream)
# MAGIC ## Module 12: Streaming
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Streaming joins let you **enrich or combine** streaming data with other data sources:
# MAGIC 1. **Stream-Static join** = Join streaming events with a static lookup table (e.g., enrich orders with customer info)
# MAGIC 2. **Stream-Stream join** = Join two live streams together (e.g., match ad impressions with ad clicks)
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC **Stream-Static**: A conveyor belt carries packages (stream). A worker looks up each package's destination in a fixed address book (static table) and sticks a label on it.
# MAGIC
# MAGIC **Stream-Stream**: Two conveyor belts merge. Belt A carries online orders. Belt B carries payment confirmations. A worker matches each order to its payment within a 30-minute window.
# MAGIC
# MAGIC ### Join Types Supported:
# MAGIC | Join Type | Stream-Static | Stream-Stream |
# MAGIC |-----------|:---:|:---:|
# MAGIC | Inner | ✓ | ✓ (with watermark) |
# MAGIC | Left Outer | ✓ | ✓ (with watermark) |
# MAGIC | Right Outer | ✗ | ✓ (with watermark) |
# MAGIC | Full Outer | ✗ | ✗ |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Stream-Static Join:
# MAGIC
# MAGIC   [Streaming DataFrame]     [Static DataFrame (batch)]
# MAGIC          │                           │
# MAGIC          └────── JOIN ON key ──────┘
# MAGIC                     │
# MAGIC          [Enriched Streaming DataFrame]
# MAGIC                     │
# MAGIC               writeStream → output
# MAGIC
# MAGIC   Notes:
# MAGIC     - Static side re-read each micro-batch (gets latest data).
# MAGIC     - No watermark needed (static data isn't time-based).
# MAGIC     - ANY join type works (inner, left, right).
# MAGIC     - Static side should be small-medium (gets broadcast if < threshold).
# MAGIC
# MAGIC Stream-Stream Join:
# MAGIC
# MAGIC   [Stream A]        [Stream B]
# MAGIC       │                  │
# MAGIC       └─── JOIN ON key ──┘
# MAGIC               │
# MAGIC       [Matched pairs stream]
# MAGIC               │
# MAGIC         writeStream → output
# MAGIC
# MAGIC   Challenge: Stream A event at t=10 might need to match
# MAGIC   Stream B event at t=15 (arrives 5 seconds later).
# MAGIC   Solution: Spark BUFFERS both sides in state, using watermarks
# MAGIC   to know when to stop waiting.
# MAGIC
# MAGIC   Watermark requirements:
# MAGIC     - BOTH streams must have watermarks defined.
# MAGIC     - Join condition must include a time constraint:
# MAGIC       e.g., "AND a.time BETWEEN b.time AND b.time + interval 10 minutes"
# MAGIC     - Without time constraint: state grows forever!
# MAGIC
# MAGIC Code Patterns:
# MAGIC
# MAGIC   # Stream-Static:
# MAGIC   static_df = spark.read.format("delta").load("/lookups/customers")
# MAGIC   enriched = stream_df.join(static_df, "customer_id", "left")
# MAGIC
# MAGIC   # Stream-Stream:
# MAGIC   stream_a.withWatermark("ts_a", "10 minutes")
# MAGIC   stream_b.withWatermark("ts_b", "10 minutes")
# MAGIC   joined = stream_a.join(
# MAGIC       stream_b,
# MAGIC       expr("a.key = b.key AND a.ts BETWEEN b.ts AND b.ts + interval 10 minutes")
# MAGIC   )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3-5: Examples
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-5 — EXAMPLES (Beginner to Advanced)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, expr, rand, lit, current_timestamp  # Imports.
import time  # Timing.

print("="*70)
print("SECTIONS 3-5: Stream Joins")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Stream-Static join (enrich stream with lookup)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Stream-Static join (enrich events with lookup)")
print("-"*60)

# Create a STATIC lookup table (e.g., department info).
static_lookup = spark.createDataFrame([
    (0, "Engineering", "Building A"),
    (1, "Marketing", "Building B"),
    (2, "Sales", "Building C"),
    (3, "HR", "Building D"),
    (4, "Finance", "Building E")
], ["dept_id", "dept_name", "location"])

print(f"Static lookup ({static_lookup.count()} rows):")
static_lookup.show()

# Create a STREAMING source with dept_id.
event_stream = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 10).load()
    .withColumn("dept_id", (col("value") % 5).cast("int"))  # FK to lookup.
    .withColumn("event_type", lit("login"))
)

# JOIN: Stream (left) with Static (right).
enriched_stream = event_stream.join(
    static_lookup,  # Static DataFrame.
    "dept_id",      # Join key.
    "left"          # Left join: keep all stream rows even if no match.
)

# Write enriched stream.
query1 = (
    enriched_stream.writeStream
    .format("memory")
    .queryName("stream_static")
    .outputMode("append")
    .start()
)

time.sleep(3)

print("\nEnriched stream (events + department info):")
display(spark.sql("""
    SELECT timestamp, dept_id, dept_name, location, event_type 
    FROM stream_static 
    ORDER BY timestamp DESC LIMIT 5
"""))

query1.stop()
print("\n✓ Each streaming event enriched with department name and location.")
print("  Static table is re-read each batch (picks up updates if any).")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Stream-Static join with Delta table
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Stream-Static with Delta lookup table")
print("-"*60)

# Write static data to Delta (production pattern).
lookup_path = "/tmp/delta_kt/stream_join_lookup"
static_lookup.write.format("delta").mode("overwrite").save(lookup_path)

# Read static from Delta.
static_from_delta = spark.read.format("delta").load(lookup_path)  # Batch read!

# Join streaming with Delta static.
stream2 = spark.readStream.format("rate").option("rowsPerSecond", 5).load() \
    .withColumn("dept_id", (col("value") % 5).cast("int"))

enriched2 = stream2.join(static_from_delta, "dept_id", "inner")

query2 = enriched2.writeStream.format("memory").queryName("delta_static") \
    .outputMode("append").start()
time.sleep(3)
print(f"Enriched rows: {spark.sql('SELECT count(*) FROM delta_static').collect()[0][0]}")
query2.stop()

print("\n✓ Production pattern: read lookup from Delta, join with stream.")
print("  Delta lookup is re-read each batch (sees latest updates).")
print("  If lookup is large, it gets broadcast automatically.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Stream-Stream join concept
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Stream-Stream join (matching two live streams)")
print("-"*60)

print("""
Stream-Stream Join Pattern (e.g., match impressions to clicks):

  # Stream A: Ad impressions.
  impressions = spark.readStream.format("kafka")...
      .withWatermark("impression_time", "10 minutes")

  # Stream B: Ad clicks.
  clicks = spark.readStream.format("kafka")...
      .withWatermark("click_time", "10 minutes")

  # Join: match click to impression within 5-minute window.
  matched = impressions.join(
      clicks,
      expr(\"\"\"
          impressions.ad_id = clicks.ad_id
          AND clicks.click_time BETWEEN impressions.impression_time
              AND impressions.impression_time + interval 5 minutes
      \"\"\"")
  )

  # Write matched pairs.
  matched.writeStream.format("delta")
      .outputMode("append")
      .option("checkpointLocation", "/cp/matched_clicks")
      .start("/output/matched_clicks")

Key requirements for Stream-Stream joins:
  1. BOTH streams must have watermarks.
  2. Join condition MUST include a time constraint (time range).
  3. Without time constraint: Spark buffers ALL data forever (OOM).
  4. Watermark + time constraint = Spark knows when to discard old state.

Supported join types:
  Inner: Emit only when match found (within time window).
  Left Outer: Emit left row with NULL right if no match by watermark.
  Right Outer: Emit right row with NULL left if no match by watermark.
""")

# Demo with two rate streams.
print("\nDemo: Two rate streams joined on key within time range:")
stream_a = spark.readStream.format("rate").option("rowsPerSecond", 5).load() \
    .withColumn("key", (col("value") % 10).cast("int")) \
    .withColumnRenamed("timestamp", "ts_a") \
    .withColumnRenamed("value", "val_a") \
    .withWatermark("ts_a", "10 seconds")  # Watermark on stream A.

stream_b = spark.readStream.format("rate").option("rowsPerSecond", 5).load() \
    .withColumn("key", (col("value") % 10).cast("int")) \
    .withColumnRenamed("timestamp", "ts_b") \
    .withColumnRenamed("value", "val_b") \
    .withWatermark("ts_b", "10 seconds")  # Watermark on stream B.

# Join on key + time constraint.
joined = stream_a.join(
    stream_b,
    expr("stream_a.key = stream_b.key AND ts_b BETWEEN ts_a AND ts_a + interval 5 seconds"),
    "inner"
)

q3 = joined.writeStream.format("memory").queryName("stream_stream") \
    .outputMode("append").start()
time.sleep(8)

matched = spark.sql("SELECT count(*) FROM stream_stream").collect()[0][0]
print(f"\nMatched pairs: {matched}")
q3.stop()
print("✓ Stream-stream join matched events within 5-second window.")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Stream-Stream join without time constraint
# MAGIC ```python
# MAGIC # BAD: No time range = Spark buffers EVERYTHING forever (OOM).
# MAGIC stream_a.join(stream_b, "key")  # State grows infinitely!
# MAGIC
# MAGIC # GOOD: Always include time constraint.
# MAGIC stream_a.join(stream_b, expr("a.key = b.key AND b.ts BETWEEN a.ts AND a.ts + interval 10 min"))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Forgetting watermark on both streams
# MAGIC ```python
# MAGIC # BAD: Watermark on only one side.
# MAGIC stream_a.withWatermark("ts", "5 min").join(stream_b, ...)  # stream_b has no watermark!
# MAGIC
# MAGIC # GOOD: Both sides need watermarks.
# MAGIC stream_a.withWatermark("ts_a", "5 min")
# MAGIC stream_b.withWatermark("ts_b", "5 min")
# MAGIC stream_a.join(stream_b, ...)
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Joining stream with a very large static table
# MAGIC ```python
# MAGIC # BAD: 100GB static table = re-read every micro-batch = slow.
# MAGIC stream.join(huge_static_table, "key")
# MAGIC
# MAGIC # GOOD: Filter static table first, or use Delta + caching.
# MAGIC small_lookup = spark.read.format("delta").load("/lookup").filter("active=true")
# MAGIC stream.join(small_lookup, "key")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Using right/full outer join on stream-static
# MAGIC ```python
# MAGIC # BAD: Right outer with stream on left = not supported.
# MAGIC stream.join(static, "key", "right")  # ERROR!
# MAGIC
# MAGIC # GOOD: Use inner or left outer for stream-static.
# MAGIC stream.join(static, "key", "left")  # Stream on left, static on right.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not understanding state growth in stream-stream joins
# MAGIC ```
# MAGIC State stores BOTH sides of the join until watermark expires.
# MAGIC If watermark is 1 hour and you get 1M events/sec:
# MAGIC   State = 2 streams × 1M/sec × 3600 sec = 7.2B rows in state!
# MAGIC
# MAGIC Fix: Set tighter watermarks and time constraints.
# MAGIC   Watermark = actual max lateness (don't over-provision).
# MAGIC   Time constraint = actual join window (don't be too generous).
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, lit, expr  # Imports.
import time

print("="*70)
print("HOMEWORK — Stream Joins")
print("="*70)

# Level 1: Stream-static join.
print("\n--- Level 1: Stream-static join ---")
lookup = spark.createDataFrame([(0,"A"),(1,"B"),(2,"C")], ["key","label"])
stream = spark.readStream.format("rate").option("rowsPerSecond",5).load() \
    .withColumn("key", (col("value")%3).cast("int"))
enriched = stream.join(lookup, "key", "left")
q = enriched.writeStream.format("memory").queryName("hw78_l1").outputMode("append").start()
time.sleep(3)
print(f"Enriched rows: {spark.sql('SELECT count(*) FROM hw78_l1').collect()[0][0]}")
q.stop()
# WHY: Stream joins with static lookup to add context.

# Level 2: Check that all events got enriched.
print("\n--- Level 2: Verify no nulls (inner join) ---")
stream2 = spark.readStream.format("rate").option("rowsPerSecond",5).load() \
    .withColumn("key", (col("value")%3).cast("int"))
enriched2 = stream2.join(lookup, "key", "inner")  # Inner = only matches.
q2 = enriched2.writeStream.format("memory").queryName("hw78_l2").outputMode("append").start()
time.sleep(3)
null_count = spark.sql("SELECT count(*) FROM hw78_l2 WHERE label IS NULL").collect()[0][0]
print(f"Null labels (should be 0 for inner): {null_count}")
q2.stop()
# WHY: Inner join guarantees no null matches.

# Level 3-10: Conceptual.
print("\n--- Level 3: Stream-static vs Stream-stream ---")
print("Stream-static: one side is batch (re-read each batch). Simple.")
print("Stream-stream: both sides are streams. Requires watermarks + time constraint.")

print("\n--- Level 4: Why time constraint is mandatory ---")
print("Without time constraint, Spark buffers all data forever (OOM).")
print("Time constraint tells Spark when to discard old unmatched events.")

print("\n--- Level 5: Supported join types ---")
print("Stream-Static: inner, left, right (stream on left/right respectively).")
print("Stream-Stream: inner, left outer, right outer (all need watermark).")

print("\n--- Level 6: State management ---")
print("Stream-stream state = both sides buffered until watermark expires.")
print("Tight watermark + narrow time range = less state = less memory.")

print("\n--- Level 7: Delta as static source ---")
print("spark.read.format('delta').load('/lookup') = static read.")
print("Re-read every batch. Sees latest version of lookup table.")

print("\n--- Level 8: Left outer in stream-stream ---")
print("Left row emitted with NULLs if no match found before watermark expires.")

print("\n--- Level 9: Performance tips ---")
print("1. Keep static side small (broadcast). 2. Tight watermarks.")
print("3. Narrow time ranges. 4. Monitor state size in Spark UI.")

print("\n--- Level 10: Teach stream joins ---")
print("""
"Stream-Static: enrich events with a lookup table.
   Just join stream DF with batch DF. Simple, no watermarks needed.
 Stream-Stream: match events from two live streams.
   Requires: watermarks on BOTH sides + time constraint in join condition.
   Without time constraint = unbounded state = OOM."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 78")
print("="*70)