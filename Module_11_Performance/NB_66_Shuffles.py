# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Notebook 66: Shuffles — Understanding and Minimizing
# MAGIC ## Module 11: Performance Optimization
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC A **shuffle** is when Spark redistributes ALL your data across ALL executors over the network. It's the **single most expensive operation** in Spark — involving disk writes, network transfer, disk reads, and deserialization. Every `groupBy`, `join`, `orderBy`, `distinct`, and `repartition` causes a shuffle.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine 100 people seated at random tables at a banquet. The organizer announces: **"Everyone born in January sit at Table 1, February at Table 2..."**
# MAGIC
# MAGIC Every single person must:
# MAGIC 1. Stand up from their current seat (read from disk)
# MAGIC 2. Walk across the room (network transfer)
# MAGIC 3. Find their new table (hash/sort to correct partition)
# MAGIC 4. Sit down (write to new location)
# MAGIC
# MAGIC That's a shuffle. If you have 10 billion rows and 200 executors, ALL 10 billion rows get sent over the network. This is why shuffles dominate query runtime.
# MAGIC
# MAGIC ### Operations that ALWAYS shuffle:
# MAGIC | Operation | Why it shuffles |
# MAGIC |-----------|----------------|
# MAGIC | `groupBy()` | Need all rows with same key on same partition to aggregate |
# MAGIC | `join()` (sort-merge) | Need matching keys co-located |
# MAGIC | `orderBy()` / `sort()` | Need global ordering across all data |
# MAGIC | `distinct()` | Need all duplicates co-located to deduplicate |
# MAGIC | `repartition()` | Explicitly redistributes data |
# MAGIC
# MAGIC ### Operations that NEVER shuffle (narrow):
# MAGIC - `filter()`, `select()`, `withColumn()`, `map()`, `flatMap()`
# MAGIC - `coalesce()` (decrease only — just merges adjacent partitions)
# MAGIC - `union()` (just concatenates, no redistribution)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC What Happens During a Shuffle:
# MAGIC
# MAGIC   BEFORE SHUFFLE (Stage N):              AFTER SHUFFLE (Stage N+1):
# MAGIC   Executor 1: [A,B,C,D,E]               Executor 1: [A,A,A,A] (all A's)
# MAGIC   Executor 2: [A,C,B,E,D]               Executor 2: [B,B,B,B] (all B's)
# MAGIC   Executor 3: [B,A,D,C,E]               Executor 3: [C,C,C,C] (all C's)
# MAGIC   Executor 4: [D,E,A,B,C]       →       Executor 4: [D,D,D,D] (all D's)
# MAGIC                                          Executor 5: [E,E,E,E] (all E's)
# MAGIC
# MAGIC   Step by step:
# MAGIC   1. MAP SIDE (before shuffle):
# MAGIC      - Each executor writes its data to local disk, sorted by target partition.
# MAGIC      - Data is divided into "shuffle files" (one per output partition).
# MAGIC
# MAGIC   2. NETWORK TRANSFER:
# MAGIC      - Each executor pulls its partition's data from ALL other executors.
# MAGIC      - This is the expensive part: network I/O for every row.
# MAGIC
# MAGIC   3. REDUCE SIDE (after shuffle):
# MAGIC      - Each executor reads its portion from disk.
# MAGIC      - Data is now grouped by key and ready for aggregation/join.
# MAGIC
# MAGIC   Cost formula:
# MAGIC     Shuffle cost ≈ (data size) × (serialization) + (network transfer) + (disk I/O × 2)
# MAGIC     For 100GB of data: can take 5-30 minutes depending on cluster.
# MAGIC
# MAGIC   Key metrics in Spark UI:
# MAGIC     Shuffle Write: data written to disk before transfer (Stage N output)
# MAGIC     Shuffle Read:  data read after transfer (Stage N+1 input)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Shuffle Demo
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, count, sum as spark_sum, avg  # Imports.

print("="*70)
print("SECTION 3 — BEGINNER EXAMPLES: Identifying Shuffles")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Narrow transformation (NO shuffle) — just filter + add column
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Narrow transformations = NO shuffle")
print("-"*60)

# Create test data: 1 million rows.
df = spark.range(1000000).select(
    col("id"),                                  # Row ID.
    (rand() * 100).cast("int").alias("key"),    # Random key 0-99.
    (rand() * 1000).alias("value")             # Random value 0-1000.
)

# These operations are ALL narrow (no shuffle):
# filter, select, withColumn, alias — each row processed independently.
narrow_result = (
    df
    .filter(col("value") > 500)                # Keep rows where value > 500.
    .withColumn("doubled", col("value") * 2)   # Add a new column.
    .select("id", "key", "doubled")            # Pick columns.
)

# PROVE IT: explain() shows no "Exchange" node.
print("\nQuery plan for narrow operations:")
narrow_result.explain()  # Look: NO 'Exchange' in the output!

print("\n✓ No 'Exchange' in plan = No shuffle = All operations stay on same partition.")
print("  Narrow operations are FREE — they don't move data between executors.")

# Expected: Plan shows Filter → Project, no Exchange.

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Wide transformation (CAUSES shuffle) — groupBy
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Wide transformation (groupBy) = SHUFFLE")
print("-"*60)

# groupBy MUST shuffle: all rows with same key must be on same partition.
wide_result = df.groupBy("key").agg(
    count("*").alias("cnt"),          # Count rows per key.
    spark_sum("value").alias("total")  # Sum values per key.
)

# PROVE IT: explain() shows "Exchange hashpartitioning" = shuffle!
print("\nQuery plan for groupBy:")
wide_result.explain()

print("\n✗ 'Exchange hashpartitioning(key)' = SHUFFLE!")
print("  All 1 million rows were redistributed across the network by 'key'.")
print("  This is the expensive part of your query.")

# Expected: Plan shows Exchange hashpartitioning(key, 200) = shuffle.

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: orderBy — another common shuffle trigger
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: orderBy() = SHUFFLE (global sort needs all data)")
print("-"*60)

# orderBy requires global ordering = all data must be compared.
sorted_result = df.orderBy(col("value").desc())  # Sort descending by value.

print("\nQuery plan for orderBy:")
sorted_result.explain()

print("\n✗ 'Exchange rangepartitioning(value DESC)' = SHUFFLE for sorting!")
print("  Sorting requires a special 'range' shuffle to ensure global order.")
print("")
print("  Tip: If you only need top-N, use .limit(N) instead of .orderBy().")
print("  Spark optimizes limit + orderBy to avoid full sort when possible.")

# Expected: Plan shows Exchange rangepartitioning.

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 4 — INTERMEDIATE EXAMPLES: Strategies to Minimize Shuffles
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, count, sum as spark_sum, avg, broadcast  # Imports.
import time  # For timing comparisons.

print("="*70)
print("SECTION 4 — INTERMEDIATE: Strategies to Minimize Shuffles")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Broadcast join eliminates shuffle entirely
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Broadcast join = ZERO shuffle for the join")
print("-"*60)

# Large table: 1 million rows (fact table).
large_df = spark.range(1000000).select(
    col("id").alias("order_id"),
    (rand() * 100).cast("int").alias("product_id"),  # FK to products.
    (rand() * 500).alias("amount")
)

# Small table: 100 rows (dimension/lookup table).
small_df = spark.range(100).select(
    col("id").alias("product_id"),
    (rand() * 10).alias("category_id")
)

# WITHOUT broadcast: Spark shuffles BOTH tables by product_id.
print("\nPlan WITHOUT broadcast (SortMergeJoin with shuffle):")
large_df.join(small_df, "product_id").explain()
print("  ↑ Notice 2 'Exchange' nodes = BOTH tables shuffled!")

# WITH broadcast: Small table sent to all executors. NO shuffle at all.
print("\nPlan WITH broadcast (BroadcastHashJoin, NO shuffle):")
large_df.join(broadcast(small_df), "product_id").explain()
print("  ↑ Notice: ZERO 'Exchange' nodes! BroadcastHashJoin = no shuffle.")

print("")
print("When to use broadcast:")
print("  • One table is small (< 100MB, ideally < 10MB)")
print("  • Joining a large fact table with a small dimension table")
print("  • Spark auto-broadcasts tables < 10MB (spark.sql.autoBroadcastJoinThreshold)")
print(f"  • Current threshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')} bytes")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: Filter BEFORE shuffle to reduce data moved
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: Filter early = less data to shuffle")
print("-"*60)

df = spark.range(2000000).select(
    col("id"),
    (rand() * 50).cast("int").alias("category"),  # 50 categories.
    (rand() * 100).alias("metric")                # Random metric.
)

# APPROACH A: groupBy on ALL data, THEN filter results.
print("\nApproach A: groupBy ALL data, then filter (more data shuffled):")
start_a = time.time()
result_a = df.groupBy("category").agg(avg("metric").alias("avg_m")).filter(col("avg_m") > 50)
result_a.collect()  # Trigger.
time_a = time.time() - start_a
print(f"  Time: {time_a:.2f}s | Shuffled: 2M rows")

# APPROACH B: filter FIRST, then groupBy (less data shuffled).
print("\nApproach B: filter first, THEN groupBy (less data shuffled):")
start_b = time.time()
result_b = df.filter(col("metric") > 50).groupBy("category").agg(avg("metric").alias("avg_m"))
result_b.collect()  # Trigger.
time_b = time.time() - start_b
print(f"  Time: {time_b:.2f}s | Shuffled: ~1M rows (filtered half first)")

print(f"\n  → Approach B shuffles ~50% less data by filtering BEFORE the groupBy.")
print("  Rule: Push filters as early as possible in your pipeline.")
print("  (Note: Catalyst does this automatically for simple filters, but not always.)")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Reduce shuffle with partial aggregation (how Spark helps)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Partial aggregation (Spark's built-in optimization)")
print("-"*60)

# When you do groupBy().sum(), Spark doesn't shuffle ALL rows.
# It first does a PARTIAL sum on each partition (before shuffle),
# then shuffles just the partial results (much less data!).

print("\nExplain plan for groupBy().sum():")
df.groupBy("category").agg(spark_sum("metric")).explain()

print("")
print("Notice TWO HashAggregate nodes in the plan:")
print("  1. HashAggregate (partial): sums within each partition BEFORE shuffle")
print("  2. Exchange (shuffle): sends partial sums over network")
print("  3. HashAggregate (final): combines partial sums AFTER shuffle")
print("")
print("This means:")
print("  Instead of shuffling 2M raw rows, Spark shuffles only ~50 partial sums!")
print("  This is reduceByKey vs groupByKey optimization, done automatically.")

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 5 — ADVANCED EXAMPLES: Production Shuffle Optimization
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, count, sum as spark_sum, avg, broadcast, expr  # Imports.
import time  # Timing.

print("="*70)
print("SECTION 5 — ADVANCED EXAMPLES (Production-Style)")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 7: Counting shuffles in a complex pipeline
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 7: How many shuffles does this pipeline have?")
print("-"*60)

# Create sample data.
orders = spark.range(500000).select(
    col("id").alias("order_id"),
    (rand() * 200).cast("int").alias("customer_id"),
    (rand() * 50).cast("int").alias("product_id"),
    (rand() * 500 + 10).alias("amount"),
    expr("CASE WHEN rand()<0.5 THEN 'online' ELSE 'store' END").alias("channel")
)

# Complex pipeline:
result = (
    orders
    .filter(col("amount") > 50)           # Narrow (no shuffle).
    .groupBy("channel", "product_id")     # SHUFFLE #1: groupBy.
    .agg(
        count("*").alias("order_count"),
        spark_sum("amount").alias("revenue")
    )
    .filter(col("order_count") > 5)       # Narrow (no shuffle).
    .orderBy(col("revenue").desc())        # SHUFFLE #2: sort.
    .limit(50)                             # Narrow (no shuffle).
)

print("\nQuery plan:")
result.explain()
result.show(5)  # Trigger execution.

print("")
print("Shuffle count: 2")
print("  Shuffle #1: Exchange hashpartitioning(channel, product_id) for groupBy")
print("  Shuffle #2: Exchange rangepartitioning(revenue DESC) for orderBy")
print("")
print("Optimization opportunity:")
print("  If you only need top 50, AQE/limit optimization may avoid full sort shuffle.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 8: Pre-partitioning to eliminate join shuffle
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 8: Pre-partition tables by join key")
print("-"*60)

print("""
Scenario: You join orders with customers EVERY DAY in your pipeline.
Instead of shuffling both tables daily, pre-partition them:

  # Write orders partitioned by customer_id:
  orders.repartition(100, "customer_id")
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save("/data/orders_by_customer")

  # Write customers partitioned the same way:
  customers.repartition(100, "customer_id")
    .write.format("delta").mode("overwrite")
    .save("/data/customers_by_customer")

  # Now the join is SHUFFLE-FREE (data already co-located):
  orders_df = spark.read.format("delta").load("/data/orders_by_customer")
  cust_df = spark.read.format("delta").load("/data/customers_by_customer")
  result = orders_df.join(cust_df, "customer_id")  # No Exchange in plan!

This is called 'bucketed join' or 'co-partitioned join'.
Trade-off: Write is slower (one-time), but every read+join is faster.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 9: Measuring actual shuffle sizes from Spark UI metrics
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 9: Spark UI metrics for shuffle analysis")
print("-"*60)

print("""
After running a query, go to Spark UI → Stages tab:

  ┌───────────┬─────────────┬──────────────┬──────────────┐
  │ Stage     │ Input       │ Shuffle Read │ Shuffle Write│
  ├───────────┼─────────────┼──────────────┼──────────────┤
  │ Stage 0   │ 500MB       │ -           │ 200MB        │  ← Read files, write shuffle
  │ Stage 1   │ -           │ 200MB       │ 50MB         │  ← Read shuffle, aggregate
  │ Stage 2   │ -           │ 50MB        │ -            │  ← Read shuffle, sort
  └───────────┴─────────────┴──────────────┴──────────────┘

Key metrics to check:
  • Shuffle Write (Stage 0): 200MB shuffled to next stage.
  • Shuffle Read (Stage 1): Received 200MB from previous stage.
  • If Shuffle Write/Read is in GB+ territory → consider optimization.
  • Task Duration distribution: uneven = data skew.
  • Spill (Memory): data didn't fit in memory during shuffle → add memory or partitions.

Optimization targets:
  1. Reduce Shuffle Write: filter data before shuffle, use partial aggregation.
  2. Reduce Shuffle Read: broadcast small tables, co-partition for joins.
  3. Balance tasks: fix data skew with salting or AQE.
""")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Unnecessary orderBy() in the middle of a pipeline
# MAGIC ```python
# MAGIC # BAD: Sorting mid-pipeline triggers an expensive shuffle, then gets destroyed by next groupBy.
# MAGIC df.orderBy("date").groupBy("category").count()  # The sort is WASTED!
# MAGIC
# MAGIC # GOOD: Only sort at the very end, right before output.
# MAGIC df.groupBy("category").count().orderBy("count")  # Sort only the final 50 rows.
# MAGIC ```
# MAGIC **Why**: orderBy creates a full shuffle for global sort. If a subsequent groupBy re-shuffles, the sort was pointless.
# MAGIC
# MAGIC ### Mistake 2: Using distinct() when dropDuplicates() with key is enough
# MAGIC ```python
# MAGIC # BAD: distinct() shuffles ALL columns to deduplicate.
# MAGIC df.select("id", "name", "email", "addr").distinct()  # Shuffles 4 columns.
# MAGIC
# MAGIC # GOOD: If dedup is by key, use dropDuplicates with subset.
# MAGIC df.dropDuplicates(["id"])  # Shuffles only by 'id', much less data.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Joining two large tables without broadcast or pre-partitioning
# MAGIC ```python
# MAGIC # BAD: Both 100GB tables get shuffled = 200GB over the network.
# MAGIC large_a.join(large_b, "key")  # SortMergeJoin, shuffles both.
# MAGIC
# MAGIC # GOOD options:
# MAGIC # 1. If one side is small: broadcast(small_table)
# MAGIC # 2. If joined repeatedly: pre-partition both by 'key'
# MAGIC # 3. Enable AQE: it may auto-broadcast if one side shrinks after filter.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Calling repartition() right before write (use coalesce instead)
# MAGIC ```python
# MAGIC # BAD: Full shuffle just to reduce files before writing.
# MAGIC df.repartition(10).write.parquet("/output")  # Unnecessary shuffle!
# MAGIC
# MAGIC # GOOD: coalesce reduces without shuffle.
# MAGIC df.coalesce(10).write.parquet("/output")  # No shuffle!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not checking explain() before running expensive queries
# MAGIC ```python
# MAGIC # ALWAYS check the plan before running on full production data:
# MAGIC my_query.explain()  # Free! Shows you all shuffles without running anything.
# MAGIC # Count 'Exchange' nodes = number of shuffles.
# MAGIC # If there are 5+ shuffles, investigate if some can be eliminated.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, count, avg, broadcast  # Imports.

print("="*70)
print("HOMEWORK — Shuffles: Understanding and Minimizing")
print("="*70)

# Level 1: Identify which has a shuffle.
print("\n--- Level 1: Which triggers a shuffle? ---")
df = spark.range(10000).select(col("id"), (col("id") % 5).alias("grp"), rand().alias("val"))
print("A: df.filter(col('val') > 0.5)")
df.filter(col("val") > 0.5).explain()  # No Exchange.
print("\nB: df.groupBy('grp').count()")
df.groupBy("grp").count().explain()  # Has Exchange.
print("Answer: B has a shuffle (Exchange). A does not.")
# WHY: groupBy needs to co-locate all rows with same key.

# Level 2: Use broadcast to eliminate join shuffle.
print("\n--- Level 2: Broadcast join ---")
lookup = spark.range(10).select(col("id").alias("grp"), (rand()*10).alias("score"))
result = df.join(broadcast(lookup), "grp")  # No shuffle!
result.explain()
print("No Exchange for the join = broadcast worked!")
# WHY: Small table sent to all executors, no redistribution needed.

# Level 3: Count Exchange nodes in explain.
print("\n--- Level 3: Count shuffles ---")
complex_q = df.groupBy("grp").agg(avg("val")).orderBy("grp")
complex_q.explain()
print("Answer: 2 shuffles (groupBy + orderBy = 2 Exchange nodes)")
# WHY: Each wide transformation creates one Exchange.

# Level 4: Prove filter-before-groupBy reduces shuffle.
print("\n--- Level 4: Filter before groupBy ---")
# Before filter: 10000 rows shuffled.
before = df.groupBy("grp").count()
# After filter: ~5000 rows shuffled.
after = df.filter(col("val") > 0.5).groupBy("grp").count()
print(f"Without filter: {df.count()} rows enter shuffle")
print(f"With filter: {df.filter(col('val') > 0.5).count()} rows enter shuffle")
# WHY: Less data in = less data shuffled = faster.

# Level 5: Replace orderBy + groupBy with just groupBy.
print("\n--- Level 5: Remove unnecessary sort ---")
# BAD: sort then group (sort is wasted).
bad = df.orderBy("val").groupBy("grp").count()
# GOOD: just group (no wasted sort shuffle).
good = df.groupBy("grp").count()
print("BAD has extra Exchange:")
bad.explain()
print("\nGOOD has fewer Exchanges:")
good.explain()
# WHY: The groupBy destroys any ordering, making the sort pointless.

# Level 6-10: Design challenges.
print("\n--- Level 6: Design a pipeline with exactly 1 shuffle ---")
result6 = df.groupBy("grp").agg(count("*"))  # Just 1 groupBy = 1 shuffle.
result6.explain()
print("1 Exchange = 1 shuffle. Confirmed!")

print("\n--- Level 7: Eliminate a join shuffle with broadcast ---")
print("Solution: Use broadcast() on any table < 100MB.")

print("\n--- Level 8: What if both tables are 50GB? ---")
print("Solution: Pre-partition both by join key, or use AQE skew join.")

print("\n--- Level 9: Calculate shuffle data size ---")
print("""Formula: 
  shuffle_bytes = num_rows × avg_row_size_bytes
  Example: 10M rows × 200 bytes/row = 2GB shuffle
  At 1Gbps network: ~16 seconds just for transfer.""")

print("\n--- Level 10: Teach shuffles to a colleague ---")
print("""
"A shuffle is when ALL your data moves across the network.
It happens on groupBy, join, orderBy, distinct.
It's the #1 performance killer in Spark.

Minimize shuffles by:
  1. Filter data before shuffles
  2. Broadcast small tables in joins
  3. Use AQE (auto-optimizes at runtime)
  4. Check explain() for Exchange nodes before running"
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 66")
print("="*70)