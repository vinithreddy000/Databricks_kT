# Databricks notebook source
# DBTITLE 1,Full Notebook Content
# MAGIC %md
# MAGIC # Notebook 64: Spark Execution Model — Jobs, Stages, Tasks
# MAGIC ## Module 11: Performance Optimization
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC When you click "Run" on a Spark query, a LOT happens behind the scenes. Your code goes through a **hierarchy of execution**: your code becomes a **Job**, which splits into **Stages**, which split into **Tasks**. Understanding this hierarchy is THE key skill for diagnosing and fixing slow queries.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Think of **building a house**:
# MAGIC - **Application** = The entire home construction project (lasts months)
# MAGIC - **Job** = A major milestone that someone asks for ("build the kitchen", "install plumbing")
# MAGIC - **Stage** = A phase within a milestone that must complete before the next starts ("pour concrete" must finish before "build walls")
# MAGIC - **Task** = One worker doing one piece of work in parallel with other workers (10 workers each pouring one section of the foundation simultaneously)
# MAGIC
# MAGIC The key insight: **Tasks run in parallel** (like workers), but **Stages run sequentially** (concrete must dry before you build on it). More tasks = more parallelism = faster.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Spark Execution Hierarchy:
# MAGIC
# MAGIC   Application (your notebook session — lives as long as cluster is up)
# MAGIC    │
# MAGIC    ├── Job 1 (triggered by .count() action)
# MAGIC    │    ├── Stage 0 (read data from files)
# MAGIC    │    │    ├── Task 0  ──┐
# MAGIC    │    │    ├── Task 1    │  Run in PARALLEL
# MAGIC    │    │    ├── Task 2    │  (one per partition)
# MAGIC    │    │    └── Task N  ──┘
# MAGIC    │    │         ↓ SHUFFLE (data redistributed)
# MAGIC    │    └── Stage 1 (aggregate after shuffle)
# MAGIC    │         ├── Task 0  ──┐
# MAGIC    │         ├── Task 1    │  Run in PARALLEL
# MAGIC    │         └── Task M  ──┘
# MAGIC    │
# MAGIC    ├── Job 2 (triggered by .show() action)
# MAGIC    │    └── Stage 2 ...
# MAGIC    │
# MAGIC    └── Job N ...
# MAGIC
# MAGIC Key Rules:
# MAGIC   ┌─────────────────────────────────────────────────────────────┐
# MAGIC   │ New JOB starts at:   Every ACTION                           │
# MAGIC   │   .count(), .show(), .collect(), .write(), display()        │
# MAGIC   │                                                             │
# MAGIC   │ New STAGE starts at: Every SHUFFLE (wide transformation)    │
# MAGIC   │   groupBy(), join(), orderBy(), distinct(), repartition()   │
# MAGIC   │                                                             │
# MAGIC   │ Number of TASKS:     = Number of PARTITIONS in that stage   │
# MAGIC   │   Input stage: num file splits                              │
# MAGIC   │   After shuffle: spark.sql.shuffle.partitions (default 200) │
# MAGIC   └─────────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC Narrow vs Wide Transformations:
# MAGIC   NARROW (same stage, no shuffle):    WIDE (new stage, causes shuffle):
# MAGIC     filter(), select(), map()           groupBy(), join()
# MAGIC     withColumn(), union()               orderBy(), distinct()
# MAGIC     coalesce() (decrease only)          repartition()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3 - Understanding Execution
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, expr, count, avg, spark_partition_id  # Import all functions we need.

print("="*70)  # Visual separator for readability.
print("SECTION 3 — BEGINNER EXAMPLES: Understanding Jobs, Stages, Tasks")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: One action = One job
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: One action triggers exactly ONE job")
print("-"*60)

# Step 1: Create a DataFrame. This is LAZY — nothing runs yet.
df = spark.range(1000000)  # Creates a DataFrame with 1 million rows (id: 0 to 999999).

# Step 2: Call an action. NOW Spark creates a job and executes.
result = df.count()  # ACTION! This is the moment Spark actually does work.

print(f"Result: {result:,} rows")  # Expected: 1,000,000 rows.
print("")
print("What happened behind the scenes:")
print("  1. spark.range(1000000) → LAZY, no execution")
print("  2. df.count()           → ACTION! Triggers Job 0")
print("  3. Job 0 has 1 Stage (no shuffle needed for a simple count)")
print(f"  4. Stage 0 has {df.rdd.getNumPartitions()} tasks (one per partition)")
print("")
print("👉 Go to Spark UI → Jobs tab → you'll see 'Job 0' with status SUCCEEDED")

# Expected output:
# Result: 1,000,000 rows
# Stage 0 has N tasks (depends on cluster cores)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Two actions = Two jobs
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Multiple actions = Multiple jobs")
print("-"*60)

# Create a DataFrame with a computed column.
df2 = spark.range(1000000).withColumn("value", rand())  # Still LAZY — no execution.

# Action 1: triggers Job 1.
count_result = df2.count()  # Job 1 created and executed.
print(f"Action 1 (count): {count_result:,} rows → triggered Job 1")

# Action 2: triggers Job 2.
avg_result = df2.agg(avg("value")).collect()[0][0]  # Job 2 created and executed.
print(f"Action 2 (avg):   {avg_result:.4f}       → triggered Job 2")

print("")
print("Key Learning: Each ACTION creates a SEPARATE job.")
print("If you call .count() and then .show(), that's 2 jobs.")
print("👉 Spark UI will show 2 completed jobs.")

# Expected output:
# Action 1 (count): 1,000,000 rows → triggered Job 1
# Action 2 (avg):   ~0.5000         → triggered Job 2

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: groupBy creates a shuffle = stage boundary
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: groupBy creates a shuffle (= new stage)")
print("-"*60)

# Create sample data: 100K rows with a category column (0-9).
data = spark.range(100000).select(
    col("id"),                              # Row identifier.
    (rand() * 10).cast("int").alias("category"),  # Random category 0-9.
    (rand() * 100).alias("value")           # Random value 0-100.
)

# This query causes a SHUFFLE because groupBy needs to move all rows
# with the same category to the same partition.
agg_result = data.groupBy("category").agg(
    count("*").alias("cnt"),   # Count rows per category.
    avg("value").alias("avg_val")  # Average value per category.
).collect()  # ACTION — triggers the job.

print(f"Got {len(agg_result)} categories")  # Expected: 10 (categories 0-9).
print("")
print("What happened:")
print("  Job triggered by .collect() has TWO stages:")
print("  Stage 0: Read data + compute partial aggregates (per partition)")
print("         ↓ SHUFFLE: redistribute data so same category = same partition")
print("  Stage 1: Final aggregate (combine partial results)")
print("")
print("👉 Spark UI → Click job → You'll see 2 stages with a shuffle between them.")

# Expected output:
# Got 10 categories

# COMMAND ----------

# DBTITLE 1,Section 4-5 Advanced and Exercises
# ═══════════════════════════════════════════════════════════════════
# SECTION 4 — INTERMEDIATE EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, expr, count, avg, sum as spark_sum  # Imports.
from pyspark.sql.functions import spark_partition_id  # To inspect partition placement.

print("="*70)
print("SECTION 4 — INTERMEDIATE EXAMPLES")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Visualizing the DAG (Directed Acyclic Graph)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: A query with 3 stages (filter → groupBy → orderBy)")
print("-"*60)

# Create sample orders data.
orders = spark.range(100000).select(
    col("id").alias("order_id"),                      # Unique order ID.
    (rand() * 100).cast("int").alias("customer_id"),  # Random customer (0-99).
    (rand() * 500 + 10).alias("amount"),              # Order amount ($10-$510).
    expr("CASE WHEN rand()<0.25 THEN 'North' WHEN rand()<0.5 THEN 'South' "
         "WHEN rand()<0.75 THEN 'East' ELSE 'West' END").alias("region")  # Region.
)

# This query creates 3 stages because it has 2 wide transformations:
result = (
    orders
    .filter(col("amount") > 100)              # NARROW: same stage, no shuffle.
    .groupBy("region")                         # WIDE: shuffle! Stage boundary #1.
    .agg(
        count("*").alias("num_orders"),         # Count per region.
        spark_sum("amount").alias("total_rev")  # Total revenue per region.
    )
    .orderBy(col("total_rev").desc())          # WIDE: shuffle! Stage boundary #2.
)

display(result)  # ACTION triggers execution.

print("")
print("DAG for this query:")
print("  Stage 0: [range] → [filter amount>100] → [partial aggregate per partition]")
print("         ↓ SHUFFLE: redistribute data by region")
print("  Stage 1: [final aggregate] (combine partial aggregates)")
print("         ↓ SHUFFLE: redistribute for global sort")
print("  Stage 2: [sort by total_rev DESC] → [output]")
print("")
print("👉 Spark UI → SQL tab → click query → see the visual DAG plan")

# Expected output: 4 rows (North, South, East, West) sorted by revenue.

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: Counting tasks per stage using spark_partition_id()
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: Inspecting partitions = inspecting tasks")
print("-"*60)

# spark_partition_id() tells you which partition each row is in.
# Number of distinct partition IDs = number of tasks in that stage.

df = spark.range(1000000)  # Create 1M rows.

# Before any shuffle: partitions determined by input parallelism.
print(f"Input partitions (= tasks in Stage 0): {df.rdd.getNumPartitions()}")

# See how rows distribute across partitions.
partition_dist = df.withColumn("pid", spark_partition_id()) \
    .groupBy("pid").count() \
    .orderBy("pid")

print("\nRows per partition (first 5):")
partition_dist.show(5, truncate=False)  # Each partition processes roughly equal rows.

# After a shuffle: partitions = spark.sql.shuffle.partitions.
shuffle_parts = spark.conf.get("spark.sql.shuffle.partitions")
print(f"spark.sql.shuffle.partitions = {shuffle_parts}")
print(f"After a groupBy/join, the next stage will have {shuffle_parts} tasks.")
print("")
print("Rule of thumb:")
print("  Small data (<1GB):  set shuffle.partitions = 8-50")
print("  Medium data (1-100GB): set shuffle.partitions = 200 (default)")
print("  Large data (>100GB): set shuffle.partitions = 500-2000")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Two-table join = seeing parallel stages
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: JOIN creates parallel input stages")
print("-"*60)

# Two DataFrames that need to be joined.
orders_df = spark.range(50000).select(
    col("id").alias("order_id"),           # Order ID.
    (rand() * 100).cast("int").alias("customer_id"),  # FK to customers.
    (rand() * 500).alias("amount")        # Order amount.
)

customers_df = spark.range(100).select(
    col("id").alias("customer_id"),        # Customer ID.
    expr("concat('Customer_', id)").alias("name")  # Customer name.
)

# JOIN: Both sides need to be shuffled by the join key (customer_id).
joined = orders_df.join(customers_df, "customer_id")  # Inner join.
result = joined.groupBy("name").agg(spark_sum("amount").alias("total_spent"))  # Aggregate.
result.count()  # Trigger execution.

print("Join execution plan:")
print("  Stage 0a: Read orders_df, shuffle by customer_id     ┐ (parallel)")
print("  Stage 0b: Read customers_df, shuffle by customer_id  ┘")
print("         ↓ SHUFFLE COMPLETE")
print("  Stage 1: Perform join (matching customer_id values)")
print("         ↓ SHUFFLE for groupBy")
print("  Stage 2: Aggregate by name")
print("")
print("Key insight: Stages 0a and 0b run IN PARALLEL!")
print("Both tables are shuffled simultaneously to save time.")
print("👉 Spark UI → job DAG shows parallel branches converging at join.")

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 5 — ADVANCED EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, expr, count, avg, sum as spark_sum  # Imports.
from pyspark.sql.functions import spark_partition_id, window  # Extra imports.
import time  # For measuring execution time.

print("="*70)
print("SECTION 5 — ADVANCED EXAMPLES (Production-Style)")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 7: Reading the Spark UI like a pro
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 7: Production query — analyze Spark UI output")
print("-"*60)

# A realistic multi-step query.
print("Running a production-style analytics query...")
start = time.time()  # Start timer.

# Simulate a fact table (orders) and dimension table (products).
orders = spark.range(500000).select(
    col("id").alias("order_id"),
    (rand() * 1000).cast("int").alias("product_id"),
    (rand() * 200).cast("int").alias("customer_id"),
    (rand() * 1000 + 5).alias("amount"),
    expr("date_add('2024-01-01', cast(rand()*180 as int))").alias("order_date")
)

products = spark.range(1000).select(
    col("id").alias("product_id"),
    expr("concat('Product_', id)").alias("product_name"),
    expr("CASE WHEN rand()<0.3 THEN 'Electronics' WHEN rand()<0.6 THEN 'Clothing' ELSE 'Food' END").alias("category")
)

# Multi-step pipeline: join → groupBy → filter → sort.
result = (
    orders.join(products, "product_id")     # SHUFFLE: redistribute both tables by product_id.
    .groupBy("category", "product_name")    # SHUFFLE: redistribute by category+product_name.
    .agg(
        count("*").alias("order_count"),
        spark_sum("amount").alias("revenue")
    )
    .filter(col("order_count") > 100)       # NARROW: no shuffle.
    .orderBy(col("revenue").desc())          # SHUFFLE: global sort.
    .limit(20)                               # Take top 20.
)

display(result)  # Trigger execution.
elapsed = time.time() - start

print(f"\nExecution time: {elapsed:.2f} seconds")
print("")
print("Spark UI Analysis Guide for this query:")
print("─"*50)
print("JOBS TAB:")
print("  • 1 job (triggered by display/collect)")
print("  • Duration tells overall time")
print("")
print("STAGES TAB (expect ~5 stages):")
print("  Stage 0: Scan orders (check Input size)")
print("  Stage 1: Scan products (check Input size)")
print("  Stage 2: Shuffle for JOIN (check Shuffle Write)")
print("  Stage 3: Shuffle for GROUP BY (check Shuffle Read/Write)")
print("  Stage 4: Shuffle for ORDER BY + limit")
print("")
print("WHAT TO LOOK FOR:")
print("  • Biggest Shuffle Read/Write = most expensive stage")
print("  • Task duration distribution: if 1 task takes 10x longer = SKEW")
print("  • GC time > 10% of task time = MEMORY PRESSURE")
print("  • Spill (Memory/Disk) > 0 = NOT ENOUGH MEMORY")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 8: Measuring job/stage duration programmatically
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 8: Using .explain() to predict stages BEFORE running")
print("-"*60)

# Instead of running and checking UI, use explain() to see the plan first.
print("\nQuery plan (physical):")
result2 = (
    orders.join(products, "product_id")
    .groupBy("category")
    .agg(count("*").alias("cnt"))
    .orderBy("cnt")
)
result2.explain()  # Shows Exchange = shuffle = stage boundary.

print("")
print("Reading the explain output:")
print("  'Exchange' = SHUFFLE = new stage boundary")
print("  'HashAggregate' = aggregation (appears twice: partial + final)")
print("  'SortMergeJoin' = join strategy (requires both sides shuffled)")
print("  'Sort' = the orderBy() operator")
print("")
print("Count 'Exchange' nodes = number of shuffles = stages - 1")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 9: Identifying the bottleneck stage
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 9: Comparing shuffle sizes to find bottleneck")
print("-"*60)

# Run two different approaches and compare.
data_large = spark.range(2000000).select(
    col("id"),
    (rand() * 50).cast("int").alias("group_key"),  # 50 unique groups.
    (rand() * 100).alias("metric")
)

# Approach A: groupBy on high-cardinality key.
print("\nApproach A: Group by 50 unique keys")
start_a = time.time()
result_a = data_large.groupBy("group_key").agg(avg("metric")).collect()
time_a = time.time() - start_a
print(f"  Time: {time_a:.2f}s | Result rows: {len(result_a)}")

# Approach B: groupBy after filter (less data shuffled).
print("\nApproach B: Filter FIRST, then group (less data to shuffle)")
start_b = time.time()
result_b = data_large.filter(col("metric") > 50).groupBy("group_key").agg(avg("metric")).collect()
time_b = time.time() - start_b
print(f"  Time: {time_b:.2f}s | Result rows: {len(result_b)}")

print(f"\n→ Approach B shuffles ~50% less data because filter reduces rows BEFORE shuffle.")
print("  Always push filters as early as possible in your pipeline!")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Calling `.collect()` on large data
# MAGIC ```python
# MAGIC # BAD: Pulls ALL data to driver → OutOfMemoryError
# MAGIC all_rows = huge_df.collect()  # If 100GB, driver dies!
# MAGIC
# MAGIC # GOOD: Use .show(), .take(), or .limit()
# MAGIC huge_df.show(20)           # Only first 20 rows
# MAGIC huge_df.take(10)           # First 10 rows as list
# MAGIC huge_df.limit(100).toPandas()  # Small sample to pandas
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Not understanding lazy evaluation
# MAGIC ```python
# MAGIC # This does NOT run anything:
# MAGIC df = spark.read.parquet("/data")  # Lazy
# MAGIC df2 = df.filter("col > 5")       # Lazy
# MAGIC df3 = df2.groupBy("x").count()   # Lazy
# MAGIC
# MAGIC # NOTHING happens until you call an action:
# MAGIC df3.show()  # NOW it runs everything above!
# MAGIC ```
# MAGIC **Why it matters**: If you time transformations, you get 0 seconds. The work happens at the action.
# MAGIC
# MAGIC ### Mistake 3: Too many actions (triggering redundant jobs)
# MAGIC ```python
# MAGIC # BAD: 3 actions = 3 separate jobs (data read 3 times!)
# MAGIC print(df.count())     # Job 1
# MAGIC print(df.first())     # Job 2
# MAGIC df.show()             # Job 3
# MAGIC
# MAGIC # GOOD: Cache if you need multiple actions on same data
# MAGIC df.cache()            # Mark for caching.
# MAGIC df.count()            # Job 1: computes + caches result.
# MAGIC df.show()             # Job 2: reads from cache (fast!)
# MAGIC df.unpersist()        # Release memory when done.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Ignoring shuffle.partitions for small data
# MAGIC ```python
# MAGIC # BAD: 200 shuffle partitions for a 1000-row table
# MAGIC # Result: 200 tasks, 198 of them process 0 rows (wasted overhead)
# MAGIC
# MAGIC # GOOD: Set appropriate shuffle partitions
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", "8")  # For small data
# MAGIC # Or rely on AQE to auto-coalesce (enabled by default in DBR 12+)
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not using the Spark UI
# MAGIC ```
# MAGIC Your query is slow. Where do you look?
# MAGIC   1. Spark UI → Jobs tab → find the slow job
# MAGIC   2. Click it → Stages tab → find the slow stage
# MAGIC   3. Click stage → Tasks tab → find the slow task
# MAGIC   4. Check: Shuffle Read/Write, GC time, Spill, Duration distribution
# MAGIC   
# MAGIC If 1 task takes 100x longer than others = DATA SKEW.
# MAGIC If all tasks are slow = need more parallelism or bigger executors.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, count, avg, spark_partition_id, sum as spark_sum  # Imports.

print("="*70)
print("HOMEWORK — Spark Execution Model (Jobs, Stages, Tasks)")
print("="*70)

# ────────────────────────────────────────────────────────────
# Level 1 (Just read and run): Run this and check Spark UI.
# HINT: Look at the Jobs tab.
# ────────────────────────────────────────────────────────────
print("\n--- Level 1: Run and check Spark UI ---")
df_l1 = spark.range(500000)  # Create 500K rows.
print(f"Count: {df_l1.count()}")  # Triggers 1 job.
# WHY: .count() is an action, which triggers a job. Check Spark UI > Jobs.

# ────────────────────────────────────────────────────────────
# Level 2 (Tiny change): How many partitions does this DataFrame have?
# HINT: Use .rdd.getNumPartitions()
# ────────────────────────────────────────────────────────────
print("\n--- Level 2: Check partition count ---")
df_l2 = spark.range(1000000)  # 1M rows.
num_parts = df_l2.rdd.getNumPartitions()  # Get partition count.
print(f"Partitions: {num_parts}")  # This = number of tasks in Stage 0.
# WHY: Each partition gets exactly 1 task. More partitions = more parallelism.

# ────────────────────────────────────────────────────────────
# Level 3 (Combine two things): groupBy triggers a shuffle. Verify.
# HINT: Use .explain() and look for "Exchange".
# ────────────────────────────────────────────────────────────
print("\n--- Level 3: Verify shuffle with explain() ---")
df_l3 = spark.range(10000).select(col("id"), (col("id") % 5).alias("grp"))
df_l3.groupBy("grp").count().explain()  # Look for 'Exchange hashpartitioning'.
# WHY: "Exchange" in the plan = shuffle = new stage boundary.

# ────────────────────────────────────────────────────────────
# Level 4 (New scenario): How many jobs does this code create?
# HINT: Count the actions.
# ────────────────────────────────────────────────────────────
print("\n--- Level 4: Count the jobs ---")
df_l4 = spark.range(100).withColumn("v", rand())
df_l4.count()               # Action 1 → Job.
df_l4.show(5)               # Action 2 → Job.
df_l4.agg(avg("v")).show()  # Action 3 → Job.
print("Answer: 3 jobs (one per action: count, show, show)")
# WHY: Every action triggers a separate job.

# ────────────────────────────────────────────────────────────
# Level 5 (Intermediate project): Build a query with exactly 3 stages.
# HINT: You need 2 wide transformations (e.g., groupBy + orderBy).
# ────────────────────────────────────────────────────────────
print("\n--- Level 5: Build a 3-stage query ---")
df_l5 = spark.range(50000).select(
    col("id"),
    (rand() * 10).cast("int").alias("dept"),  # 10 departments.
    (rand() * 100000).alias("salary")          # Random salary.
)
# Stage 1: read data. Stage 2: shuffle for groupBy. Stage 3: shuffle for orderBy.
result_l5 = df_l5.groupBy("dept").agg(avg("salary").alias("avg_sal")).orderBy("avg_sal")
result_l5.show()  # 3 stages.
result_l5.explain()  # Verify: 2 Exchange nodes = 3 stages.
# WHY: groupBy and orderBy each cause a shuffle, creating stage boundaries.

# ────────────────────────────────────────────────────────────
# Level 6 (Design first): Predict the number of stages before running.
# HINT: Count the wide transformations + 1.
# ────────────────────────────────────────────────────────────
print("\n--- Level 6: Predict stages for this pipeline ---")
print("""Pipeline:
  df.filter(...)         ← narrow
  .join(df2, key)        ← WIDE (shuffle both sides)
  .groupBy(col)          ← WIDE
  .agg(sum(...))         ← part of groupBy
  .distinct()            ← WIDE
  .orderBy(col)          ← WIDE

Prediction: 1 (input) + 4 (shuffles) = ~5 stages
  (join counts as 1 shuffle even though both sides shuffle, 
   because Spark creates parallel input stages)
""")
# WHY: stages = number of shuffle boundaries + 1 (the first read stage).

# ────────────────────────────────────────────────────────────
# Level 7 (Optimize it): Reduce stages by eliminating unnecessary shuffles.
# HINT: .distinct() after groupBy is redundant if groupBy already deduplicates.
# ────────────────────────────────────────────────────────────
print("\n--- Level 7: Eliminate unnecessary shuffle ---")
df_l7 = spark.range(100000).select(col("id"), (col("id") % 20).alias("grp"), rand().alias("v"))
# BAD: distinct() after groupBy adds an extra shuffle for no benefit.
bad = df_l7.groupBy("grp").agg(count("*").alias("cnt")).distinct()  # Redundant distinct!
# GOOD: groupBy result is already unique by group key.
good = df_l7.groupBy("grp").agg(count("*").alias("cnt"))  # No extra shuffle.
print("BAD plan (extra Exchange for distinct):")
bad.explain()
print("\nGOOD plan (no redundant shuffle):")
good.explain()
# WHY: Each shuffle adds network I/O and disk I/O. Remove any that don't change results.

# ────────────────────────────────────────────────────────────
# Level 8 (Edge cases): What if one partition has 100x more data?
# HINT: This is DATA SKEW. One task takes forever.
# ────────────────────────────────────────────────────────────
print("\n--- Level 8: Data skew creates uneven tasks ---")
# Create skewed data: key=0 gets 90% of rows.
skewed = spark.range(100000).select(
    col("id"),
    (col("id") < 90000).cast("int").alias("skewed_key")  # 0=90K rows, 1=10K rows.
)
display(skewed.groupBy("skewed_key").count())  # See the imbalance.
print("After groupBy, one task processes 90K rows, other processes 10K.")
print("The slow task holds up the entire stage!")
# WHY: Spark divides work by key. If one key has most data, that partition is huge.

# ────────────────────────────────────────────────────────────
# Level 9 (Production-grade): Tune shuffle partitions for your data size.
# HINT: Target 128-256MB per partition.
# ────────────────────────────────────────────────────────────
print("\n--- Level 9: Calculate optimal shuffle partitions ---")
print("""Formula:
  optimal_partitions = total_shuffle_data_size / target_partition_size

Example:
  Data after filter: 50GB
  Target partition: 200MB
  Optimal partitions: 50000MB / 200MB = 250

  spark.conf.set("spark.sql.shuffle.partitions", "250")

Or just enable AQE and let Spark figure it out:
  spark.conf.set("spark.sql.adaptive.enabled", "true")  # Default in DBR 12+
""")
# WHY: Too few partitions = memory pressure/spill. Too many = scheduling overhead.

# ────────────────────────────────────────────────────────────
# Level 10 (Teach it): Explain to a colleague.
# ────────────────────────────────────────────────────────────
print("\n--- Level 10: Teach this to someone new ---")
print("""
Explain to a new colleague in 60 seconds:

"When you run a Spark query:
  1. Your code is LAZY — nothing happens until you call an ACTION like .count()
  2. Each action creates a JOB — a unit of work for the cluster
  3. Each job splits into STAGES — separated by SHUFFLES (data redistribution)
  4. Each stage splits into TASKS — one per data partition, running in parallel

  The key to making queries fast:
    - Minimize shuffles (fewer stages)
    - Balance data across partitions (no skew)
    - Set partition count based on data size (not too many, not too few)
    - Use the Spark UI to find which stage is slowest"
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 64")
print("="*70)