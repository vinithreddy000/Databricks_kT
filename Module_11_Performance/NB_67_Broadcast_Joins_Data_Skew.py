# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Notebook 67: Broadcast Joins and Handling Data Skew
# MAGIC ## Module 11: Performance Optimization
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Two of the biggest performance problems in Spark are:
# MAGIC 1. **Large table joins** — joining two big tables shuffles billions of rows over the network
# MAGIC 2. **Data skew** — one key has way more data than others, creating a single slow task
# MAGIC
# MAGIC **Broadcast join** solves #1 by sending the small table to every executor (no shuffle needed).
# MAGIC **Salting/AQE** solves #2 by splitting the overloaded partition into smaller ones.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC **Broadcast Join**: Instead of 100 workers walking to a central library to look up a reference book, you **photocopy the book and give one copy to each worker**. Now everyone can look things up locally without leaving their desk.
# MAGIC
# MAGIC **Data Skew**: Imagine 100 workers processing mail. Worker #1 gets assigned all mail for "Amazon" (10,000 packages) while everyone else gets 100 packages each. Worker #1 takes 100x longer while others sit idle. **Salting** = split Amazon's mail across 10 workers with labels: "Amazon_1", "Amazon_2", ..., "Amazon_10".
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Broadcast Join:
# MAGIC                                                              
# MAGIC   Normal Join (SortMergeJoin):     Broadcast Join:
# MAGIC   ──────────────────────────     ────────────────────────────
# MAGIC   [Large 100GB] ──shuffle──┐       [Large 100GB] stays put.
# MAGIC                            ├ join  [Small 10MB] copied to ALL executors.
# MAGIC   [Small 10MB]  ──shuffle──┘       Each executor joins locally. No shuffle!
# MAGIC   
# MAGIC   Cost: 110GB over network          Cost: 10MB × N executors (trivial)
# MAGIC
# MAGIC Data Skew:
# MAGIC   After groupBy("customer_id"):
# MAGIC   
# MAGIC     Partition 1: [Customer A: 10M rows]  ← Takes 10 minutes alone!
# MAGIC     Partition 2: [Customer B: 100 rows]  ← Done in 1 second.
# MAGIC     Partition 3: [Customer C: 200 rows]  ← Done in 1 second.
# MAGIC     ...
# MAGIC     Stage completes when ALL tasks finish → total = 10 minutes (waiting on Partition 1)
# MAGIC
# MAGIC   After SALTING (split hot key):
# MAGIC     Partition 1a: [Customer A_salt0: 1M rows]  ← 1 minute.
# MAGIC     Partition 1b: [Customer A_salt1: 1M rows]  ← 1 minute.
# MAGIC     ...
# MAGIC     Partition 1j: [Customer A_salt9: 1M rows]  ← 1 minute.
# MAGIC     Stage completes in ~1 minute (all balanced!)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Broadcast and Skew Demo
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, lit, broadcast, concat, count  # Imports.
from pyspark.sql.functions import spark_partition_id, floor, expr  # More imports.

print("="*70)
print("SECTION 3 — BEGINNER EXAMPLES")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Normal join (with shuffle) vs Broadcast join (no shuffle)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Broadcast join eliminates shuffle")
print("-"*60)

# Large fact table: 1 million orders.
large = spark.range(1000000).select(
    col("id").alias("order_id"),
    (rand() * 100).cast("int").alias("dept_id"),  # FK to departments.
    (rand() * 500).alias("amount")                # Order amount.
)

# Small dimension table: 100 departments.
small = spark.range(100).select(
    col("id").alias("dept_id"),                    # PK.
    concat(lit("Dept_"), col("id")).alias("dept_name")  # Department name.
)

# WITHOUT broadcast: Both tables get shuffled (expensive).
print("\nWithout broadcast (SortMergeJoin):")
result_normal = large.join(small, "dept_id")
result_normal.explain()  # Shows 2 Exchange nodes (shuffle both sides).

# WITH broadcast: Small table copied to all executors (free join).
print("\nWith broadcast (BroadcastHashJoin):")
result_broadcast = large.join(broadcast(small), "dept_id")
result_broadcast.explain()  # Shows 0 Exchange nodes for the join!

print("")
print("✓ BroadcastHashJoin = No shuffle! Small table sent to all executors.")
print(f"  Auto-broadcast threshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')} bytes")
print("  Tables smaller than this are auto-broadcast without you doing anything.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Detecting data skew
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Detecting data skew")
print("-"*60)

# Create HEAVILY skewed data: customer_id=0 has 90% of all orders.
skewed_orders = spark.range(1000000).select(
    col("id").alias("order_id"),
    # 90% of rows go to customer 0, rest distributed across 1-99.
    (col("id") < 900000).cast("int").alias("customer_id"),
    (rand() * 500).alias("amount")
)

# Check the distribution — this is how you DETECT skew.
print("\nKey distribution (TOP 5 by count):")
display(
    skewed_orders.groupBy("customer_id")
    .agg(count("*").alias("row_count"))
    .orderBy(col("row_count").desc())
    .limit(5)
)

print("")
print("⚠️  customer_id=1 has 900,000 rows while customer_id=0 has 100,000.")
print("  After groupBy('customer_id'), one task processes 9x more data!")
print("  That single slow task holds up the entire stage.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: AQE skew join (automatic fix in Spark 3.2+)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: AQE automatic skew handling")
print("-"*60)

# Check AQE skew configs.
print("AQE Skew Join Configuration:")
print(f"  spark.sql.adaptive.enabled = {spark.conf.get('spark.sql.adaptive.enabled', 'true')}")
print(f"  spark.sql.adaptive.skewJoin.enabled = {spark.conf.get('spark.sql.adaptive.skewJoin.enabled', 'true')}")
print(f"  spark.sql.adaptive.skewJoin.skewedPartitionFactor = {spark.conf.get('spark.sql.adaptive.skewJoin.skewedPartitionFactor', '5')}")
print(f"  spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = {spark.conf.get('spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes', '256MB')}")
print("")
print("How AQE skew join works:")
print("  1. After shuffle, AQE detects partition sizes.")
print("  2. If one partition is > 5x the median AND > 256MB, it's 'skewed'.")
print("  3. AQE automatically splits that partition into smaller sub-partitions.")
print("  4. The other side of the join is replicated to match.")
print("  5. No code changes needed! Just enable AQE (default in DBR 12+).")
print("")
print("👉 In Databricks, AQE is ON by default. Skew joins 'just work' for most cases.")
print("   If you still see skew, check if AQE is being bypassed (e.g., by REPARTITION hints).")

# COMMAND ----------

# DBTITLE 1,Sections 4-5: Salting Pattern
# ═══════════════════════════════════════════════════════════════════
# SECTION 4-5: INTERMEDIATE & ADVANCED: Salting Technique
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, lit, concat, count, sum as spark_sum  # Imports.

print("="*70)
print("SECTIONS 4-5: Manual Salting for Extreme Skew")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Full salting pattern for aggregation
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Salting technique step-by-step")
print("-"*60)

# Skewed data: customer_id=1 has 900K rows.
skewed = spark.range(1000000).select(
    col("id"),
    (col("id") < 900000).cast("int").alias("customer_id"),  # 1=900K rows, 0=100K rows.
    (rand() * 100).alias("amount")
)

NUM_SALTS = 10  # Split hot key into 10 pieces.

# Step 1: Add random salt.
print("\nStep 1: Add random salt (0-9)")
salted = skewed.withColumn("salt", (rand() * NUM_SALTS).cast("int"))
salted.filter("customer_id = 1").select("customer_id", "salt", "amount").show(5)

# Step 2: Aggregate by (customer_id, salt) — distributes evenly.
print("Step 2: Partial aggregate by (customer_id, salt)")
partial = salted.groupBy("customer_id", "salt").agg(
    spark_sum("amount").alias("partial_sum"),
    count("*").alias("partial_count")
)
partial.filter("customer_id = 1").show(5)

# Step 3: Combine partials back.
print("Step 3: Final aggregate by customer_id (combine partials)")
final = partial.groupBy("customer_id").agg(
    spark_sum("partial_sum").alias("total_amount"),
    spark_sum("partial_count").alias("total_orders")
)
display(final.orderBy(col("total_orders").desc()))

print("")
print("Without salting: 1 task processes 900K rows (bottleneck).")
print("With 10 salts:   10 tasks each process ~90K rows (10x faster).")
print("Trade-off: One extra groupBy at the end, but overall much faster.")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Broadcasting a table that's too large
# MAGIC ```python
# MAGIC # BAD: Broadcasting a 5GB table crashes executors.
# MAGIC result = large.join(broadcast(five_gb_table), "key")  # OOM!
# MAGIC
# MAGIC # GOOD: Only broadcast tables < ~200MB.
# MAGIC result = large.join(broadcast(small_lookup), "key")  # Fine.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Not checking for skew before running expensive joins
# MAGIC ```python
# MAGIC # ALWAYS check key distribution before a production join:
# MAGIC df.groupBy("join_key").count().orderBy(col("count").desc()).show(10)
# MAGIC # If top key has 100x more rows than median = SKEW = needs fixing.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Using too many or too few salt values
# MAGIC ```python
# MAGIC # Too few (2 salts): Hot key only split in half (still slow).
# MAGIC # Too many (1000 salts): Creates 1000 tiny groups (overhead).
# MAGIC # Good rule: salts = hot_key_rows / target_rows_per_task
# MAGIC #   Example: 10M hot key rows / 1M target = 10 salts.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Salting for a join but forgetting to replicate the other side
# MAGIC ```python
# MAGIC # For salted JOINS (not just aggregation), you must:
# MAGIC # 1. Salt the large (skewed) table: add random salt 0-9.
# MAGIC # 2. EXPLODE the small table: replicate each row 10 times with salts 0-9.
# MAGIC # 3. Join on (key, salt) — both sides now have matching salts.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Ignoring AQE and manually salting everything
# MAGIC ```python
# MAGIC # AQE handles moderate skew (5-10x) automatically.
# MAGIC # Only add manual salting for EXTREME skew (100x+).
# MAGIC # Check Spark UI first: if tasks are roughly balanced, AQE is working!
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, lit, broadcast, count, sum as spark_sum, concat  # Imports.

print("="*70)
print("HOMEWORK — Broadcast Joins and Data Skew")
print("="*70)

# Level 1: Use broadcast().
print("\n--- Level 1: Basic broadcast join ---")
facts = spark.range(100000).select(col("id"), (col("id")%10).alias("key"), rand().alias("val"))
dims = spark.range(10).select(col("id").alias("key"), lit("info").alias("dim_val"))
result = facts.join(broadcast(dims), "key")
result.explain()  # BroadcastHashJoin.
print(f"Result: {result.count():,} rows. No shuffle!")
# WHY: broadcast() sends small table to all executors, eliminating join shuffle.

# Level 2: Detect skew.
print("\n--- Level 2: Detect skewed key ---")
skewed = spark.range(50000).select(col("id"), (col("id")<45000).cast("int").alias("key"))
print("Key distribution:")
skewed.groupBy("key").count().show()
print("key=1 has 45K rows, key=0 has 5K = 9x skew!")
# WHY: Imbalanced keys cause one task to take much longer than others.

# Level 3: Apply 5-way salt.
print("\n--- Level 3: Salt with 5 buckets ---")
salted = skewed.withColumn("salt", (rand()*5).cast("int"))
partial = salted.groupBy("key", "salt").agg(count("*").alias("cnt"))
final = partial.groupBy("key").agg(spark_sum("cnt").alias("total"))
final.show()
print("Salting split key=1 across 5 parallel tasks.")
# WHY: Instead of 1 task with 45K rows, we get 5 tasks with ~9K rows each.

# Level 4: Check auto-broadcast threshold.
print("\n--- Level 4: Auto-broadcast threshold ---")
threshold = int(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
print(f"Threshold: {threshold} bytes = {threshold/1024/1024:.0f} MB")
print("Tables smaller than this are auto-broadcast without explicit hint.")
# WHY: You may not even need to call broadcast() if table is small enough.

# Level 5: Salted join (both sides).
print("\n--- Level 5: Salted join pattern ---")
print("""
For a salted JOIN (not just aggregation):
  1. Left side (skewed): add random salt column (0 to N-1)
  2. Right side (small): explode with array(0..N-1) to replicate N times
  3. Join on (original_key, salt)
""")
# WHY: Both sides need matching salt values to join correctly.

# Levels 6-10: Conceptual.
print("--- Level 6: When does AQE skew join activate? ---")
print("When partition > 5x median AND > 256MB after shuffle.")

print("\n--- Level 7: Design a pipeline with both broadcast and salting ---")
print("Use broadcast for dimension joins, salting for skewed fact aggregations.")

print("\n--- Level 10: Teach broadcast + skew to a colleague ---")
print("""
"Broadcast join: photocopy the small table to every worker (no shuffle).
 Data skew: one worker gets 90% of the mail (others idle).
 Fix: AQE auto-handles moderate skew. For extreme skew, use salting
 (split hot key into N pieces, aggregate partially, then combine)."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 67")
print("="*70)