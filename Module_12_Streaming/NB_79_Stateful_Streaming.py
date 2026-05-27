# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 79: Stateful Streaming & De-duplication
# MAGIC ## Module 12: Streaming
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Stateful streaming** means Spark remembers information across micro-batches. The two most common stateful operations are:
# MAGIC 1. **De-duplication** — Drop duplicate events that arrive in different batches
# MAGIC 2. **Custom state** — Track sessions, running calculations, or complex patterns using `applyInPandasWithState` (or `flatMapGroupsWithState` in Scala)
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC **De-duplication**: A ticket scanner at a concert remembers every ticket ID scanned. If someone tries to scan the same ticket twice (even hours apart), it's rejected. The scanner's memory = Spark's state store.
# MAGIC
# MAGIC **Custom state (sessions)**: A hotel front desk tracks guest check-in/check-out. When a guest checks in, a session starts. Events during their stay are grouped. When they check out (or after 24h of no activity), the session closes and the final bill is computed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC De-duplication with Watermark:
# MAGIC
# MAGIC   Batch 0: [event_id=1, event_id=2, event_id=3]
# MAGIC   Batch 1: [event_id=2, event_id=4, event_id=5]   ← event_id=2 is DUPLICATE
# MAGIC   Batch 2: [event_id=1, event_id=6]               ← event_id=1 is DUPLICATE
# MAGIC
# MAGIC   With dropDuplicatesWithinWatermark("event_id"):
# MAGIC     Output: [1, 2, 3, 4, 5, 6]  (each ID emitted only once)
# MAGIC
# MAGIC   Watermark controls HOW LONG Spark remembers IDs:
# MAGIC     Watermark = 10 min → remembers IDs for 10 minutes.
# MAGIC     After 10 min: same ID arriving again would NOT be caught.
# MAGIC     Trade-off: longer watermark = more memory, better dedup.
# MAGIC
# MAGIC State Store:
# MAGIC   Spark stores state in a checkpoint-backed state store:
# MAGIC   ┌──────────────────────────────────────────────┐
# MAGIC   │  STATE STORE (per partition)                  │
# MAGIC   │  Key: group key (event_id, session_id, etc.)  │
# MAGIC   │  Value: accumulated state (counts, timestamps) │
# MAGIC   │  Backed by: checkpoint directory               │
# MAGIC   │  Cleanup: watermark triggers state expiry      │
# MAGIC   └──────────────────────────────────────────────┘
# MAGIC
# MAGIC De-duplication Methods:
# MAGIC   1. dropDuplicates(["id"])                      → Exact dedup (remembers ALL IDs forever)
# MAGIC   2. dropDuplicatesWithinWatermark(["id"])       → Dedup within watermark window (bounded)
# MAGIC
# MAGIC Custom Stateful Processing:
# MAGIC   applyInPandasWithState:  Python/Pandas UDF with custom state.
# MAGIC   flatMapGroupsWithState:  Scala/Java custom state (most flexible).
# MAGIC   Use cases: session windows, pattern detection, running models.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: Examples and Homework
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7 — EXAMPLES AND HOMEWORK
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, expr, rand, lit, count  # Imports.
import time  # Timing.

print("="*70)
print("SECTIONS 3-5: Stateful Streaming & De-duplication")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: De-duplication with dropDuplicates
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Streaming de-duplication")
print("-"*60)

# Generate data with deliberate duplicates.
# value % 20 creates IDs 0-19, but rate generates multiple per second.
dup_stream = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 20).load()
    .withColumn("event_id", (col("value") % 20).cast("int"))  # Only 20 unique IDs!
)

# De-duplicate by event_id (keeps first occurrence only).
deduped = dup_stream.dropDuplicates(["event_id"])  # Exact dedup.

q1 = deduped.writeStream.format("memory").queryName("dedup_demo") \
    .outputMode("append").start()
time.sleep(3)

total_unique = spark.sql("SELECT count(*) FROM dedup_demo").collect()[0][0]
print(f"\nUnique events emitted: {total_unique} (should be ~20, not 60+)")
q1.stop()

print("\n✓ dropDuplicates keeps only first occurrence of each event_id.")
print("  WARNING: Without watermark, state grows forever (remembers all IDs).")
print("  For production: use dropDuplicatesWithinWatermark instead.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Bounded dedup with watermark
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: dropDuplicatesWithinWatermark (bounded memory)")
print("-"*60)

print("""
Production de-duplication pattern:

  stream
    .withWatermark("event_time", "1 hour")        # Remember IDs for 1 hour.
    .dropDuplicatesWithinWatermark(["event_id"])  # Dedup within watermark.
    .writeStream
    .outputMode("append")
    .start()

How it works:
  1. First time event_id=X is seen: emitted to output.
  2. Same event_id=X arrives within 1 hour: DROPPED (duplicate).
  3. After 1 hour: state for event_id=X is freed.
  4. If event_id=X arrives again after 1 hour: treated as NEW (not caught).

Trade-off:
  Longer watermark = catches more duplicates but uses more memory.
  Shorter watermark = less memory but may miss late duplicates.
  Choose based on your actual duplication window (max delay between retries).
""")

# Demo.
print("Demo: dropDuplicatesWithinWatermark")
dup_stream2 = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 15).load()
    .withColumn("event_id", (col("value") % 10).cast("int"))  # 10 unique IDs.
)

deduped2 = (
    dup_stream2
    .withWatermark("timestamp", "10 seconds")  # Remember for 10 sec.
    .dropDuplicatesWithinWatermark(["event_id"])  # Bounded dedup.
)

q2 = deduped2.writeStream.format("memory").queryName("bounded_dedup") \
    .outputMode("append").start()
time.sleep(5)
unique = spark.sql("SELECT count(*) FROM bounded_dedup").collect()[0][0]
print(f"Unique events (should be ~10): {unique}")
q2.stop()
print("✓ Bounded dedup: state is cleaned up after watermark expires.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: foreach/foreachBatch (custom sinks)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: foreachBatch (custom processing per micro-batch)")
print("-"*60)

# foreachBatch gives you the micro-batch as a regular DataFrame.
# You can do ANYTHING with it: MERGE, write to multiple sinks, call APIs.

def process_batch(batch_df, batch_id):
    """Custom processing for each micro-batch."""
    row_count = batch_df.count()  # Regular DataFrame operations.
    print(f"  Batch {batch_id}: {row_count} rows processed")
    # In production: MERGE INTO delta table, write to API, etc.

rate_for_batch = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

q3 = (
    rate_for_batch.writeStream
    .foreachBatch(process_batch)  # Custom function per batch.
    .outputMode("append")
    .start()
)

time.sleep(5)
q3.stop()

print("\n✓ foreachBatch: full DataFrame API available per micro-batch.")
print("  Use for: MERGE/upsert, multi-sink writes, API calls, custom logic.")
print("  The batch_df is a regular (non-streaming) DataFrame.")

# ─────────────────────────────────────────────────────────────────
# SECTION 6 — Common Mistakes
# ─────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("SECTION 6 — COMMON MISTAKES")
print("="*70)
print("""
1. dropDuplicates without watermark: State grows forever (OOM).
   Fix: Use dropDuplicatesWithinWatermark + watermark.

2. foreachBatch without idempotent writes: Duplicates on retry.
   Fix: Use MERGE INTO or check if batch already processed.

3. Not monitoring state store size in Spark UI.
   Fix: Check SQL tab → State rows / bytes. Alert if growing unboundedly.

4. Using dropDuplicates with output mode 'complete': Not supported.
   Fix: Use append mode for de-duplication.

5. Setting watermark too short for dedup: misses late duplicates.
   Fix: Set watermark = max time between original and duplicate arrival.
""")

# ─────────────────────────────────────────────────────────────────
# SECTION 7 — HOMEWORK
# ─────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("SECTION 7 — HOMEWORK")
print("="*70)

# Level 1-3: Deduplication practice.
print("\n--- Level 1: dropDuplicates ---")
print("stream.dropDuplicates(['id']) → exact dedup, unbounded state.")

print("\n--- Level 2: dropDuplicatesWithinWatermark ---")
print("stream.withWatermark('ts','1 hour').dropDuplicatesWithinWatermark(['id'])")
print("→ bounded state, dedup within 1 hour window.")

print("\n--- Level 3: foreachBatch pattern ---")
print("""
def upsert_batch(batch_df, batch_id):
    batch_df.createOrReplaceTempView("updates")
    spark.sql(\"\"\"
        MERGE INTO target t
        USING updates u ON t.id = u.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    \"\"\")

stream.writeStream.foreachBatch(upsert_batch).start()
""")

print("\n--- Level 5: State store monitoring ---")
print("Spark UI → Structured Streaming tab → State Rows / State Memory.")
print("Alert if state rows grow without bound.")

print("\n--- Level 10: Teach stateful streaming ---")
print("""
"Stateful streaming = Spark remembers data across batches.
  De-duplication: dropDuplicatesWithinWatermark (bounded memory).
  Custom logic: foreachBatch (full DataFrame API per batch, MERGE/upsert).
  State cleanup: watermarks expire old state automatically.
  Monitor: check state size in Spark UI to prevent OOM."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 79")
print("="*70)