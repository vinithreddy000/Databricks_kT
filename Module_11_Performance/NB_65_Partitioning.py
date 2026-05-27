# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Notebook 65: Partitioning — The Foundation of Performance
# MAGIC ## Module 11: Performance Optimization
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC A **partition** is one chunk of your data that sits on one computer (executor). Spark splits your data into many partitions and processes them **in parallel**. The number of partitions directly controls:
# MAGIC - How many tasks run at once (parallelism)
# MAGIC - How much data each task processes (memory usage)
# MAGIC - How many output files get written
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Think of **slicing a pizza** for a dinner party:
# MAGIC - **4 people eating? Cut into 4 slices.** Each person gets a reasonable portion (efficient).
# MAGIC - **Cut into 200 micro-slices?** Each person has to pick up 50 tiny pieces (wasteful overhead from handling so many pieces).
# MAGIC - **Don't cut at all (1 big piece)?** Only 1 person can eat at a time while others wait (no parallelism).
# MAGIC
# MAGIC **The sweet spot**: Each partition should be **128MB–256MB** in size. Like Goldilocks — not too big, not too small, just right.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Partition Lifecycle:
# MAGIC
# MAGIC   INPUT PARTITIONS                    SHUFFLE PARTITIONS              OUTPUT PARTITIONS
# MAGIC   (determined by source)              (determined by config)           (determined by you)
# MAGIC
# MAGIC   ┌────────┐ File 1 (128MB)        After groupBy/join:             Before write:
# MAGIC   ┌────────┐ File 2 (128MB)        spark.sql.shuffle.partitions    df.coalesce(n) or
# MAGIC   ┌────────┐ File 3 (128MB)        = 200 (default)                  df.repartition(n)
# MAGIC   ┌────────┐ File 4 (128MB)
# MAGIC
# MAGIC   4 input partitions                  200 shuffle partitions          Controlled output
# MAGIC   = 4 parallel tasks                  = 200 tasks (many empty!)       = N output files
# MAGIC
# MAGIC
# MAGIC Key Operations:
# MAGIC   ┌──────────────────┬───────────────────┬──────────────────────────────┐
# MAGIC   │ Operation          │ Shuffle?            │ When to use                    │
# MAGIC   ├──────────────────┼───────────────────┼──────────────────────────────┤
# MAGIC   │ repartition(n)     │ YES (full shuffle)  │ Increase OR decrease partitions │
# MAGIC   │ repartition(n,col) │ YES (by column)     │ Co-locate data by key           │
# MAGIC   │ coalesce(n)        │ NO (narrow)         │ ONLY decrease partitions        │
# MAGIC   │ repartitionByRange │ YES (range-based)   │ Even distribution for sorting   │
# MAGIC   └──────────────────┴───────────────────┴──────────────────────────────┘
# MAGIC
# MAGIC   repartition(n) vs coalesce(n):
# MAGIC     repartition: Can increase OR decrease. Causes FULL SHUFFLE (expensive).
# MAGIC     coalesce:    Can ONLY decrease. NO shuffle (cheap). Just merges adjacent partitions.
# MAGIC
# MAGIC     Rule: Need fewer partitions? Use coalesce(). Need more or need to redistribute? Use repartition().
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Partitioning Deep Dive
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, spark_partition_id, count  # Import all functions.

print("="*70)
print("SECTION 3 — BEGINNER EXAMPLES: Understanding Partitions")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Checking how many partitions a DataFrame has
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: How many partitions does my data have?")
print("-"*60)

# Method: .rdd.getNumPartitions() tells you the partition count.
df_small = spark.range(100)       # 100 rows.
df_medium = spark.range(1000000)  # 1 million rows.
df_large = spark.range(10000000)  # 10 million rows.

print(f"100 rows:      {df_small.rdd.getNumPartitions()} partitions")
print(f"1,000,000 rows: {df_medium.rdd.getNumPartitions()} partitions")
print(f"10,000,000 rows: {df_large.rdd.getNumPartitions()} partitions")
print("")
print("The partition count depends on:")
print("  • spark.range(): partitions = number of cores")
print("  • Reading files: partitions = number of file blocks (1 block ~ 128MB)")
print("  • After shuffle: partitions = spark.sql.shuffle.partitions")

# Expected output: partition count varies by cluster size.

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Seeing rows per partition (are they balanced?)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Distribution of rows across partitions")
print("-"*60)

# spark_partition_id() tells you which partition each row belongs to.
df = spark.range(1000000)  # 1M rows.

# Count how many rows are in each partition.
partition_distribution = (
    df.withColumn("partition_id", spark_partition_id())  # Add partition ID column.
    .groupBy("partition_id")  # Group by partition.
    .agg(count("*").alias("row_count"))  # Count rows per partition.
    .orderBy("partition_id")  # Sort by partition ID.
)

print("Rows per partition (first 8):")
partition_distribution.show(8, truncate=False)

# Check if balanced.
stats = partition_distribution.agg(
    {"row_count": "min", "row_count": "max"}  # Min and max rows.
).collect()[0]
print(f"Min rows in a partition: {stats[0]:,}")
print(f"\nIdeal: All partitions have roughly equal rows (balanced).")
print("If one partition has 10x more rows = SKEW = performance problem!")

# Expected output: roughly equal rows per partition.

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: repartition() vs coalesce() — the most important difference
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: repartition() vs coalesce()")
print("-"*60)

df = spark.range(10000000)  # 10M rows.
original_parts = df.rdd.getNumPartitions()  # Original partition count.
print(f"Original partitions: {original_parts}")

# REPARTITION: Full shuffle. Can increase OR decrease.
df_repart_up = df.repartition(16)  # Increase to 16 (causes full shuffle).
df_repart_down = df.repartition(2)  # Decrease to 2 (still causes full shuffle!).
print(f"\nrepartition(16): {df_repart_up.rdd.getNumPartitions()} partitions (shuffle!)")
print(f"repartition(2):  {df_repart_down.rdd.getNumPartitions()} partitions (shuffle!)")

# COALESCE: No shuffle. Can ONLY decrease.
df_coal = df.coalesce(2)  # Decrease to 2 (NO shuffle — just merges adjacent).
print(f"\ncoalesce(2):     {df_coal.rdd.getNumPartitions()} partitions (NO shuffle!)")

# Trying to increase with coalesce doesn't work.
df_coal_up = df.coalesce(100)  # This won't actually increase beyond original.
print(f"coalesce(100):   {df_coal_up.rdd.getNumPartitions()} partitions (can't increase!)")

print("")
print("═"*50)
print("RULE: Need FEWER partitions? → Use coalesce() (free, no shuffle)")
print("      Need MORE partitions?  → Use repartition() (expensive shuffle)")
print("      Need to REDISTRIBUTE?  → Use repartition(n, col) (shuffle by key)")
print("═"*50)

# Expected output shows partition counts changing.

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 4 — INTERMEDIATE EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, spark_partition_id, count, expr  # Imports.

print("="*70)
print("SECTION 4 — INTERMEDIATE EXAMPLES")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Repartitioning by column (co-locate related data)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: repartition(n, col) — co-locate by key")
print("-"*60)

# Create data with a 'region' column (4 unique values).
df = spark.range(100000).select(
    col("id"),
    expr("CASE WHEN id%4=0 THEN 'North' WHEN id%4=1 THEN 'South' "
         "WHEN id%4=2 THEN 'East' ELSE 'West' END").alias("region"),
    (rand() * 1000).alias("revenue")
)

# Repartition by 'region': all rows with same region go to same partition.
df_by_region = df.repartition(4, "region")  # 4 partitions, grouped by region.

# Verify: show which partition each region lands in.
print("Partition assignment by region:")
df_by_region.withColumn("pid", spark_partition_id()) \
    .groupBy("region", "pid").count() \
    .orderBy("region") \
    .show(truncate=False)

print("Notice: Each region is entirely in ONE partition!")
print("This is useful BEFORE a join on 'region' — avoids shuffle at join time.")
print("")
print("Use case: If you join two tables on 'customer_id', repartition both")
print("by customer_id first. The join becomes a local operation (no shuffle).")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: spark.sql.shuffle.partitions — the most impactful config
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: spark.sql.shuffle.partitions")
print("-"*60)

# Show current value.
current = spark.conf.get("spark.sql.shuffle.partitions")
print(f"Current value: {current}")
print("This controls how many partitions exist AFTER a shuffle (groupBy, join).")
print("")

# Demo: too many partitions for small data.
small_data = spark.range(1000).select(col("id"), (col("id") % 5).alias("grp"))

# With default 200 partitions: most are empty.
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Default.
result_200 = small_data.groupBy("grp").count()
print(f"With 200 shuffle partitions: {result_200.rdd.getNumPartitions()} output partitions")
print("  For 1000 rows and 5 groups, 195 partitions are EMPTY! Waste of overhead.")

# With 8 partitions: appropriate for small data.
spark.conf.set("spark.sql.shuffle.partitions", "8")  # Better for small data.
result_8 = small_data.groupBy("grp").count()
print(f"\nWith 8 shuffle partitions: {result_8.rdd.getNumPartitions()} output partitions")
print("  Much better! No wasted empty partitions.")

# Reset to default.
spark.conf.set("spark.sql.shuffle.partitions", "200")
print(f"\nReset to: {spark.conf.get('spark.sql.shuffle.partitions')}")
print("")
print("Guideline:")
print("  Data < 1GB:    set to 8-50")
print("  Data 1-100GB:  set to 200 (default is fine)")
print("  Data > 100GB:  set to 500-4000")
print("  Or just use AQE (auto-coalesces empty partitions)!")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Partition pruning (skipping irrelevant partitions)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Partition pruning with disk-partitioned tables")
print("-"*60)

# Write a partitioned Delta table.
path = "/tmp/delta_kt/partition_pruning_demo"
data = spark.range(100000).select(
    col("id").alias("order_id"),
    expr("CASE WHEN rand()<0.25 THEN 2023 WHEN rand()<0.5 THEN 2024 ELSE 2025 END").alias("year"),
    (rand() * 500).alias("amount")
)
data.write.format("delta").mode("overwrite").partitionBy("year").save(path)  # Partition by year.

# Query with partition filter: only reads year=2024 folder.
print("Query: WHERE year = 2024")
result = spark.read.format("delta").load(path).filter("year = 2024")
result.explain(True)  # Look for PartitionFilters in the plan.
print(f"\nResult: {result.count():,} rows (only year=2024 data was read!)")
print("")
print("Partition Pruning: When you filter on the partition column,")
print("Spark skips entire folders it doesn't need to read.")
print("If you have 10 years of data and filter to 1 year, you read 1/10th!")

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 5 — ADVANCED EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, spark_partition_id, count, expr, round as spark_round  # Imports.
import time  # For timing.

print("="*70)
print("SECTION 5 — ADVANCED EXAMPLES (Production-Style)")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 7: Measuring the cost of repartition vs coalesce
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 7: Performance: repartition() vs coalesce()")
print("-"*60)

df = spark.range(5000000).select(col("id"), (rand()*100).alias("value"))  # 5M rows.

# Measure repartition (causes shuffle).
start = time.time()
df.repartition(4).write.format("delta").mode("overwrite").save("/tmp/delta_kt/repart_perf")
time_repart = time.time() - start

# Measure coalesce (no shuffle).
start = time.time()
df.coalesce(4).write.format("delta").mode("overwrite").save("/tmp/delta_kt/coal_perf")
time_coal = time.time() - start

print(f"  repartition(4) + write: {time_repart:.2f} seconds")
print(f"  coalesce(4) + write:    {time_coal:.2f} seconds")
print(f"  Difference: coalesce is ~{time_repart/max(time_coal,0.01):.1f}x faster")
print("")
print("WHY: repartition shuffles ALL data across the network.")
print("     coalesce just combines adjacent partitions locally.")
print("     Always use coalesce() when you only need to REDUCE partitions.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 8: Controlling output file count with coalesce before write
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 8: Controlling output files with coalesce")
print("-"*60)

path_many = "/tmp/delta_kt/output_many_files"
path_one = "/tmp/delta_kt/output_one_file"

df = spark.range(100000).select(col("id"), rand().alias("value"))

# Without coalesce: many files (one per partition).
df.write.format("delta").mode("overwrite").save(path_many)
files_many = len(spark.read.format("delta").load(path_many).inputFiles())

# With coalesce(1): exactly ONE output file.
df.coalesce(1).write.format("delta").mode("overwrite").save(path_one)
files_one = len(spark.read.format("delta").load(path_one).inputFiles())

print(f"Without coalesce: {files_many} output files")
print(f"With coalesce(1): {files_one} output file")
print("")
print("Use cases for controlling file count:")
print("  • coalesce(1): When downstream system needs a single file (rare)")
print("  • coalesce(n): Before write, to reduce small files")
print("  • Rule: target ~128MB-256MB per file for Delta tables")
print("  • Better: use OPTIMIZE after writing instead of coalesce")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 9: Detecting and fixing partition skew
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 9: Detecting partition skew")
print("-"*60)

# Create SKEWED data: 90% of data has key=0.
skewed = spark.range(1000000).select(
    col("id"),
    (col("id") < 900000).cast("int").alias("skew_key")  # key=1 for 900K, key=0 for 100K.
)

# After groupBy, one partition is massive.
spark.conf.set("spark.sql.shuffle.partitions", "10")
result = skewed.repartition(10, "skew_key")  # Redistribute by key.

# Check distribution.
print("Partition distribution (SKEWED):")
result.withColumn("pid", spark_partition_id()) \
    .groupBy("pid").count() \
    .orderBy(col("count").desc()) \
    .show(5)

print("Problem: One partition has 900K rows, others have much less!")
print("This means one task takes 9x longer than the rest.")
print("")
print("Fixes for skew:")
print("  1. Enable AQE skew join: spark.sql.adaptive.skewJoin.enabled = true")
print("  2. Salting: add random suffix to hot key, then aggregate twice")
print("  3. Broadcast the smaller table (if one side is small)")
print("  4. Filter out the hot key and process it separately")

spark.conf.set("spark.sql.shuffle.partitions", "200")  # Reset.

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Using repartition() when coalesce() would work
# MAGIC ```python
# MAGIC # BAD: Full network shuffle just to reduce partitions before write.
# MAGIC df.repartition(4).write.parquet("/output")  # Expensive shuffle!
# MAGIC
# MAGIC # GOOD: coalesce reduces partitions without shuffle.
# MAGIC df.coalesce(4).write.parquet("/output")  # No shuffle, just merges.
# MAGIC ```
# MAGIC **Why it matters**: repartition shuffles ALL data across the network. For a 100GB table, that's 100GB of unnecessary network I/O.
# MAGIC
# MAGIC ### Mistake 2: Leaving shuffle.partitions at 200 for tiny data
# MAGIC ```python
# MAGIC # BAD: 200 partitions for 1000 rows = 199 empty tasks (overhead).
# MAGIC small_df.groupBy("x").count().show()  # Creates 200 tasks, most do nothing.
# MAGIC
# MAGIC # GOOD: Set appropriate value or rely on AQE.
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", "8")
# MAGIC # Or better: AQE auto-coalesces (enabled by default).
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Too many disk partitions (over-partitioning)
# MAGIC ```python
# MAGIC # BAD: partitionBy on high-cardinality column = millions of tiny folders.
# MAGIC df.write.partitionBy("customer_id").parquet("/data")  # 1M folders with 1 file each!
# MAGIC
# MAGIC # GOOD: Partition by low-cardinality columns only (year, month, region).
# MAGIC df.write.partitionBy("year", "month").parquet("/data")  # ~36 folders (3 years).
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Not checking partition balance after repartition
# MAGIC ```python
# MAGIC # After repartition by column, always verify distribution:
# MAGIC df.repartition(8, "join_key")
# MAGIC   .withColumn("pid", spark_partition_id())
# MAGIC   .groupBy("pid").count()
# MAGIC   .show()  # If one partition has 10x more rows = problem!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Using coalesce(1) on large data
# MAGIC ```python
# MAGIC # BAD: All data funneled through ONE task = single-threaded bottleneck.
# MAGIC huge_df.coalesce(1).write.parquet("/out")  # One core does all the work!
# MAGIC
# MAGIC # GOOD: Let Spark use multiple partitions, then OPTIMIZE the Delta table.
# MAGIC huge_df.write.format("delta").save("/out")
# MAGIC spark.sql("OPTIMIZE delta.`/out`")  # Compacts to optimal file sizes.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, spark_partition_id, count  # Imports.

print("="*70)
print("HOMEWORK — Partitioning")
print("="*70)

# Level 1 (Just read and run).
print("\n--- Level 1: Check partitions of spark.range ---")
df = spark.range(100000)
print(f"Partitions: {df.rdd.getNumPartitions()}")  # Just run it.
# WHY: getNumPartitions() shows how many parallel tasks will process this data.

# Level 2 (Tiny change).
print("\n--- Level 2: Repartition to exactly 4 ---")
df4 = df.repartition(4)
print(f"After repartition(4): {df4.rdd.getNumPartitions()}")  # Should be 4.
# WHY: repartition forces a specific partition count via full shuffle.

# Level 3 (Combine two things).
print("\n--- Level 3: Coalesce then check ---")
df2 = df.coalesce(2)
print(f"After coalesce(2): {df2.rdd.getNumPartitions()}")  # Should be 2.
# WHY: coalesce reduces without shuffle (cheaper than repartition).

# Level 4 (New scenario).
print("\n--- Level 4: See rows per partition ---")
df.withColumn("pid", spark_partition_id()).groupBy("pid").count().show(5)
# WHY: Balanced partitions = balanced task execution = no straggler tasks.

# Level 5 (Intermediate project).
print("\n--- Level 5: Repartition by column and verify ---")
df_grp = spark.range(10000).select(col("id"), (col("id") % 3).alias("grp"))
df_rp = df_grp.repartition(3, "grp")  # 3 partitions by group.
df_rp.withColumn("pid", spark_partition_id()).groupBy("grp", "pid").count().show()
# WHY: Same group values land on same partition (useful before joins).

# Level 6 (Design first).
print("\n--- Level 6: Calculate optimal partitions ---")
print("""Your data is 50GB. Target partition size is 200MB.
  Optimal partitions = 50000MB / 200MB = 250.
  spark.conf.set("spark.sql.shuffle.partitions", "250")
""")
# WHY: Too few = memory pressure. Too many = scheduling overhead.

# Level 7 (Optimize it).
print("--- Level 7: Write with optimal file count ---")
df_write = spark.range(100000).select(col("id"), rand().alias("v"))
df_write.coalesce(4).write.format("delta").mode("overwrite").save("/tmp/delta_kt/hw65_l7")
files = len(spark.read.format("delta").load("/tmp/delta_kt/hw65_l7").inputFiles())
print(f"Output files: {files} (controlled with coalesce)")
# WHY: Fewer large files = faster reads downstream.

# Level 8 (Edge cases).
print("\n--- Level 8: What if coalesce(1) on 1 billion rows? ---")
print("Answer: ALL data funneled through 1 task = single-threaded, extremely slow.")
print("Fix: Use OPTIMIZE on the Delta table instead of coalesce(1).")
# WHY: coalesce(1) eliminates all parallelism.

# Level 9 (Production-grade).
print("\n--- Level 9: Dynamic partition calculation ---")
def optimal_partitions(size_gb, target_mb=200):
    """Calculate optimal shuffle partitions based on data size."""
    return max(8, int(size_gb * 1024 / target_mb))  # At least 8.

print(f"1GB data:   {optimal_partitions(1)} partitions")
print(f"50GB data:  {optimal_partitions(50)} partitions")
print(f"500GB data: {optimal_partitions(500)} partitions")
# WHY: This formula ensures each partition is roughly target_mb in size.

# Level 10 (Teach it).
print("\n--- Level 10: Explain to a colleague ---")
print("""
"Partitions are how Spark splits your data for parallel processing.
More partitions = more parallelism, but too many creates overhead.

Key rules:
  • Target 128-256MB per partition
  • coalesce(n) to decrease (free, no shuffle)
  • repartition(n) to increase (expensive shuffle)
  • spark.sql.shuffle.partitions controls post-shuffle parallelism
  • AQE auto-handles this in modern Databricks"
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 65")
print("="*70)