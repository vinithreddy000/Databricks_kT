# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 101: Spark UI & Debugging
# MAGIC ## Module 19: Monitoring & Troubleshooting
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC The **Spark UI** is your window into what Spark is actually doing. It shows jobs, stages, tasks, shuffle sizes, memory usage, and execution timelines. When a query is slow or fails, the Spark UI tells you WHY — whether it's data skew, insufficient memory, too many shuffles, or a single slow task.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC The Spark UI is like a **hospital monitoring system**:
# MAGIC - **Jobs tab** = Patient list (which operations are running?)
# MAGIC - **Stages tab** = Vital signs per organ (how is each stage performing?)
# MAGIC - **Tasks tab** = Cell-level analysis (which individual tasks are struggling?)
# MAGIC - **SQL tab** = The doctor's notes (execution plan with metrics)
# MAGIC - **Storage tab** = Blood bank inventory (cached data)
# MAGIC - **Executors tab** = Staff roster (how busy is each worker?)
# MAGIC
# MAGIC ### Key UI Tabs:
# MAGIC | Tab | Shows | Use For |
# MAGIC |-----|-------|--------|
# MAGIC | Jobs | All Spark jobs triggered | See overall progress, find failures |
# MAGIC | Stages | Breakdown of each stage | Find shuffles, skew, slow stages |
# MAGIC | Tasks | Individual task metrics | Find stragglers, data skew |
# MAGIC | SQL/DataFrame | Query plans with metrics | Understand execution strategy |
# MAGIC | Storage | Cached RDDs/DataFrames | Check cache utilization |
# MAGIC | Executors | Worker node stats | Memory pressure, GC issues |
# MAGIC | Environment | Spark config values | Verify configuration |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Spark Execution Hierarchy:
# MAGIC
# MAGIC   Action (e.g., .count(), .show(), .write())
# MAGIC     └── Job (one per action)
# MAGIC          └── Stage (split at shuffle boundaries)
# MAGIC               └── Task (one per partition, runs on executor)
# MAGIC
# MAGIC   Example: df.groupBy("key").count().show()
# MAGIC     Job 1:
# MAGIC       Stage 0: Read data (200 tasks = 200 partitions).
# MAGIC       Stage 1: Shuffle + aggregate (200 tasks after exchange).
# MAGIC       Stage 2: Collect to driver for .show().
# MAGIC
# MAGIC Spark UI Navigation (Databricks):
# MAGIC
# MAGIC   Cluster → Spark UI tab
# MAGIC   OR: Click the job link in cell output after execution.
# MAGIC
# MAGIC   Key metrics to look for:
# MAGIC   ┌─────────────────────┬────────────────────────────────────────┐
# MAGIC   │ Metric              │ What it means                          │
# MAGIC   ├─────────────────────┼────────────────────────────────────────┤
# MAGIC   │ Shuffle Read/Write  │ Data moved between stages (minimize!)  │
# MAGIC   │ Duration            │ How long each stage/task took           │
# MAGIC   │ GC Time             │ Time in garbage collection (>10% = bad)│
# MAGIC   │ Input/Output Size   │ Data read/written per stage             │
# MAGIC   │ Spill (Memory/Disk) │ Data that didn't fit in memory          │
# MAGIC   │ Skew (max vs median)│ Uneven task distribution                │
# MAGIC   └─────────────────────┴────────────────────────────────────────┘
# MAGIC
# MAGIC Red Flags in Spark UI:
# MAGIC   1. One task takes 100x longer than others → DATA SKEW.
# MAGIC   2. Huge shuffle write (GB+) → Too many shuffles / bad join strategy.
# MAGIC   3. Spill to disk > 0 → Not enough memory per task.
# MAGIC   4. GC time > 10% of task time → Memory pressure.
# MAGIC   5. Many failed tasks with retries → OOM or node failures.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: Beginner Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, expr, spark_partition_id  # Imports.
import time  # For timing.

print("="*70)
print("SECTION 3 — BEGINNER: Spark UI & Debugging")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Triggering a job and finding it in Spark UI
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Generate a Spark job and inspect in UI")
print("-"*60)

# Create a DataFrame with 1M rows.
df = spark.range(1000000).withColumn("value", rand() * 1000)  # 1M rows.

# This triggers a JOB (action = .count()).
start = time.time()  # Start timer.
row_count = df.groupBy((col("id") % 10).alias("group")).count().count()  # Triggers shuffle + count.
elapsed = time.time() - start  # End timer.

print(f"\n  Result: {row_count} groups")
print(f"  Duration: {elapsed:.2f}s")
print("")
print("  → Go to Spark UI (cluster → Spark UI tab) to see:")
print("    Jobs tab: You'll see a job for this cell.")
print("    Stages: Stage 0 (range + groupBy), Stage 1 (shuffle + count).")
print("    Tasks: 200 tasks per stage (default shuffle partitions).")
print("    SQL tab: Full DAG with metrics (rows processed, shuffle bytes).")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Reading the SQL/DataFrame execution plan
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: EXPLAIN — read the execution plan")
print("-"*60)

# A query with a join.
df_orders = spark.range(10000).selectExpr("id as order_id", "id % 100 as customer_id", "rand() * 500 as amount")
df_customers = spark.range(100).selectExpr("id as customer_id", "concat('Customer_', id) as name")

joined = df_orders.join(df_customers, "customer_id")  # Join.
agg = joined.groupBy("name").sum("amount")  # Aggregate.

# EXPLAIN shows the plan WITHOUT executing.
print("\nPhysical plan:")
agg.explain(mode="formatted")  # Shows operators: Scan, Exchange, HashAggregate.

print("\n✓ Look for:")
print("  'Exchange' = shuffle (data movement between nodes).")
print("  'BroadcastHashJoin' = small table broadcast (fast, no shuffle).")
print("  'SortMergeJoin' = both tables large (expensive shuffle).")
print("  'Filter' pushed into 'Scan' = predicate pushdown (good!).")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Detecting data skew from partition sizes
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Detecting data skew (partition analysis)")
print("-"*60)

# Create skewed data (key=0 has 90% of data).
skewed = spark.range(1000000).withColumn(
    "key", expr("CASE WHEN rand() < 0.9 THEN 0 ELSE int(rand() * 100) END")  # 90% key=0!
)

# Check partition distribution after shuffle.
skewed_grouped = skewed.groupBy("key").count()  # This will create skew.
print("\nKey distribution (skewed):")
display(skewed_grouped.orderBy(col("count").desc()).limit(10))  # display() for output.

# Partition size analysis.
print("\nPartition size analysis:")
partition_sizes = skewed.withColumn("partition_id", spark_partition_id()) \
    .groupBy("partition_id").count() \
    .orderBy(col("count").desc())
display(partition_sizes.limit(5))  # display() shows largest partitions.

print("\n✓ In Spark UI: if one task takes 100x longer than median → SKEW.")
print("  Fix: salting, AQE skew join optimization, repartition.")

# COMMAND ----------

# DBTITLE 1,Sections 4-5: Intermediate and Advanced
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4-5 — INTERMEDIATE & ADVANCED
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, count, avg, max as spark_max, min as spark_min  # Imports.

print("="*70)
print("SECTIONS 4-5: Advanced Debugging Techniques")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Memory debugging (spill, GC, OOM)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Diagnosing memory issues")
print("-"*60)

print("""
Memory Architecture per Executor:

  Total Executor Memory (e.g., 28GB for Standard_E4ds_v5)
  ├── Reserved Memory (300MB)
  ├── User Memory (40%): Python objects, UDFs.
  └── Spark Memory (60%):
      ├── Execution (shuffles, joins, sorts).
      └── Storage (cached DataFrames).

Signs of memory issues (visible in Spark UI):

  1. SPILL TO DISK (Stages tab → click stage → 'Spill' column):
     - Data didn't fit in memory, written to disk temporarily.
     - Fix: Increase executor memory OR reduce partition count.
     - spark.conf.set("spark.sql.shuffle.partitions", "400")  # More partitions = less per task.

  2. HIGH GC TIME (Executors tab → 'GC Time' column):
     - >10% of total time = memory pressure.
     - Fix: Increase memory, reduce data per executor, avoid .collect().

  3. OOM (Out of Memory) errors:
     - Task killed because it exceeded memory limit.
     - Fix: Increase executor memory, reduce parallelism, repartition.
     - spark.conf.set("spark.executor.memory", "8g")

Debugging steps:
  1. Spark UI → Stages → Click failed/slow stage.
  2. Check 'Shuffle Spill (Memory)' and 'Shuffle Spill (Disk)'.
  3. Check 'GC Time' vs 'Duration' per task.
  4. Check 'Input Size' per task (skew if max >> median).
""")

# Check current memory configuration.
print("Current memory config:")
for key in ["spark.executor.memory", "spark.driver.memory", "spark.sql.shuffle.partitions"]:
    val = spark.conf.get(key, "not set")
    print(f"  {key} = {val}")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: Using Spark listeners and metrics programmatically
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: Programmatic Spark metrics")
print("-"*60)

# Access Spark context metrics.
sc = spark.sparkContext  # Get SparkContext.
print(f"\n  Application ID: {sc.applicationId}")
print(f"  Application name: {sc.appName}")
print(f"  Default parallelism: {sc.defaultParallelism}")

# Get status tracker (live job/stage info).
status = sc.statusTracker()  # StatusTracker object.
active_jobs = status.getActiveJobIds()  # Currently running jobs.
print(f"  Active jobs: {list(active_jobs)}")

# Run a query and check metrics from SQL execution.
df_test = spark.range(1000000).groupBy((col("id") % 50).alias("grp")).count()
df_test.write.format("noop").mode("overwrite").save()  # Force execution.

print("\n  After execution, check Spark UI → SQL tab for:")
print("    - Number of output rows per operator.")
print("    - Shuffle data size.")
print("    - Time spent in each operator.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Debugging slow joins
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Debugging join performance")
print("-"*60)

# Create tables of different sizes.
large_df = spark.range(5000000).selectExpr("id", "id % 1000 as key", "rand() as val")  # 5M rows.
small_df = spark.range(1000).selectExpr("id as key", "concat('name_', id) as name")   # 1K rows.

# Join and check plan.
joined = large_df.join(small_df, "key")
print("\nJoin plan (should use BroadcastHashJoin for small table):")
joined.explain(mode="simple")  # Check if broadcast is used.

# Force broadcast if Spark doesn't auto-detect.
from pyspark.sql.functions import broadcast  # Broadcast hint.
joined_broadcast = large_df.join(broadcast(small_df), "key")  # Explicit broadcast.
print("\nWith broadcast hint:")
joined_broadcast.explain(mode="simple")

print("\n✓ BroadcastHashJoin: small table copied to all executors (no shuffle!).")
print("  SortMergeJoin: both tables shuffled (expensive for large tables).")
print("  Auto-broadcast threshold: spark.sql.autoBroadcastJoinThreshold = 10MB.")
print(f"  Current: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")

# COMMAND ----------

# DBTITLE 1,Section 6: Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Not checking Spark UI before tuning blindly
# MAGIC ```python
# MAGIC # BAD: Randomly increasing memory/partitions without evidence.
# MAGIC spark.conf.set("spark.executor.memory", "64g")  # Overkill? Wasteful?
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", "2000")  # Maybe too many?
# MAGIC
# MAGIC # GOOD: Check Spark UI first, then tune based on evidence.
# MAGIC # 1. Spark UI → Stages → Check spill, GC time, task duration.
# MAGIC # 2. If spill > 0: increase memory OR increase partitions.
# MAGIC # 3. If tasks are fast but there are 10,000 of them: reduce partitions.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Ignoring data skew (one task takes forever)
# MAGIC ```python
# MAGIC # BAD: Wondering why a job with 200 tasks takes 30 min.
# MAGIC # (199 tasks finish in 10s, 1 task takes 30 min = SKEW!)
# MAGIC
# MAGIC # GOOD: Check Spark UI → Stages → Task duration distribution.
# MAGIC # If max >> median, you have skew. Fix with:
# MAGIC # - AQE skew join: spark.sql.adaptive.skewJoin.enabled = true (default)
# MAGIC # - Salting: add random prefix to skewed key.
# MAGIC # - Filter out the hot key and process separately.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Using .collect() on large DataFrames
# MAGIC ```python
# MAGIC # BAD: Collecting millions of rows to driver.
# MAGIC all_data = df.collect()  # OOM if df has millions of rows!
# MAGIC
# MAGIC # GOOD: Collect only aggregated/small results.
# MAGIC summary = df.groupBy("key").count().collect()  # Small result.
# MAGIC # Or use .limit() before collect.
# MAGIC sample = df.limit(100).collect()  # Safe.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Not using .explain() before running expensive queries
# MAGIC ```python
# MAGIC # BAD: Running a complex query and waiting 2 hours to discover it's wrong.
# MAGIC result = complex_join_query.write.saveAsTable("output")  # 2 hours wasted!
# MAGIC
# MAGIC # GOOD: Check plan first (free, instant).
# MAGIC complex_join_query.explain(mode="formatted")  # See the plan.
# MAGIC # Look for: unexpected shuffles, missing predicate pushdown, wrong join type.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Forgetting to check executor/driver logs
# MAGIC ```
# MAGIC # When tasks fail with cryptic errors, the answer is often in:
# MAGIC # 1. Spark UI → Executors → stderr (click the link).
# MAGIC # 2. Driver logs (Cluster → Driver Logs tab).
# MAGIC # 3. Event log (for post-mortem after cluster terminates).
# MAGIC
# MAGIC # Common findings:
# MAGIC #   java.lang.OutOfMemoryError → increase memory.
# MAGIC #   FileNotFoundException → wrong path or missing permissions.
# MAGIC #   AnalysisException → column/table doesn't exist.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7: Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("HOMEWORK — Spark UI & Debugging")
print("="*70)

print("\n--- Level 1: Find Spark UI ---")
print("  Cluster page → 'Spark UI' tab. Or click job link in cell output.")

print("\n--- Level 2: Read Jobs tab ---")
print("  Each action (.count, .show, .write) creates a Job.")
print("  Green = success. Red = failed. Blue = running.")

print("\n--- Level 3: Read Stages tab ---")
print("  Stages are separated by shuffles (Exchange operators).")
print("  Check: duration, shuffle read/write, spill.")

print("\n--- Level 4: Use .explain() ---")
df = spark.range(100).groupBy((col("id") % 5).alias("g")).count()
df.explain(mode="simple")  # Shows physical plan.
# WHY: Free, instant — shows join type, shuffles, pushdowns.

print("\n--- Level 5: Detect skew ---")
print("  Spark UI → Stage → Task Duration. If max >> median = skew.")
print("  Also: Summary Metrics shows min/25th/median/75th/max.")

print("\n--- Level 6: Check shuffle ---")
print("  Stages tab: 'Shuffle Read' and 'Shuffle Write' columns.")
print("  High shuffle = potential bottleneck. Reduce with broadcast joins.")

print("\n--- Level 7: Check memory ---")
print("  Executors tab: Memory Used, GC Time, Disk Used.")
print("  Stages tab → Tasks: Spill (Memory) and Spill (Disk).")

print("\n--- Level 8: SQL tab ---")
print("  Shows DAG with actual row counts per operator.")
print("  Click any node to see: time, output rows, shuffle bytes.")

print("\n--- Level 9: Driver/Executor logs ---")
print("  Cluster → Driver Logs (stdout, stderr, log4j).")
print("  Spark UI → Executors → Click 'stderr' for each executor.")

print("\n--- Level 10: Teach Spark UI debugging ---")
print("""
"Spark UI debugging workflow:
  1. Run query. 2. Open Spark UI (SQL tab for plan, Stages for metrics).
  3. Check: shuffle size, spill, GC time, task duration distribution.
  4. Red flags: max task >> median (skew), spill > 0 (memory),
     GC > 10% (pressure), huge shuffle (wrong join strategy).
  5. Fix: broadcast joins, AQE, increase partitions/memory,
     salt skewed keys, add filters early.
  Always: .explain() BEFORE running expensive queries."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 101")
print("="*70)