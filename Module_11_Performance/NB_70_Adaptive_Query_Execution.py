# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Notebook 70: Adaptive Query Execution (AQE)
# MAGIC ## Module 11: Performance Optimization
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **AQE** (Adaptive Query Execution) is Spark's ability to **change its execution plan MID-FLIGHT** based on what it learns from the data as it runs. Instead of committing to a plan before seeing any data, AQE collects real statistics at each shuffle boundary and re-optimizes for the next stage.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine you're a delivery driver with a GPS:
# MAGIC - **Without AQE** = You plan your route at home and follow it no matter what. If there's a traffic jam, you're stuck.
# MAGIC - **With AQE** = Your GPS updates in real-time. It detects traffic after each segment and reroutes you to faster roads. It adapts as you drive.
# MAGIC
# MAGIC AQE does 3 things automatically:
# MAGIC 1. **Coalesces small partitions** — Merges empty/tiny partitions into optimal sizes
# MAGIC 2. **Switches join strategies** — If data is small at runtime, switches to broadcast join
# MAGIC 3. **Handles skew** — Splits oversized partitions into smaller ones
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC AQE Decision Points (at each shuffle boundary):
# MAGIC
# MAGIC   Stage 0: [Read + Filter]  ─── SHUFFLE ───╮
# MAGIC                                           │ ← AQE inspects:
# MAGIC                                           │   • How much data came through?
# MAGIC                                           │   • Are partitions balanced?
# MAGIC                                           │   • Is one side small enough to broadcast?
# MAGIC                                           ╰─────────────────────────
# MAGIC   Stage 1: [Optimized by AQE]             │ AQE may:
# MAGIC                                           │   1. Coalesce 200 → 10 partitions
# MAGIC                                           │   2. Switch SortMerge → Broadcast join
# MAGIC                                           │   3. Split skewed partition into 5 smaller
# MAGIC
# MAGIC Feature 1: COALESCE PARTITIONS
# MAGIC   Before AQE:  [200 partitions after shuffle, 190 are empty] = waste
# MAGIC   After AQE:   [10 partitions with ~equal data] = optimal
# MAGIC
# MAGIC Feature 2: DYNAMIC JOIN STRATEGY
# MAGIC   Plan time:   Both tables look like 100GB → plan SortMergeJoin
# MAGIC   Runtime:     After filter, right side is only 8MB!
# MAGIC   AQE action:  Switch to BroadcastHashJoin (no shuffle needed!)
# MAGIC
# MAGIC Feature 3: SKEW JOIN
# MAGIC   Before AQE:  Partition A = 10GB, Partitions B-Z = 100MB each
# MAGIC   After AQE:   Partition A split into A1(2GB), A2(2GB)... A5(2GB)
# MAGIC                Other side replicated to match. All tasks balanced.
# MAGIC
# MAGIC Key configs:
# MAGIC   spark.sql.adaptive.enabled = true (default in DBR 12+)
# MAGIC   spark.sql.adaptive.coalescePartitions.enabled = true
# MAGIC   spark.sql.adaptive.skewJoin.enabled = true
# MAGIC   spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5
# MAGIC   spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,AQE Demo
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-5 — BEGINNER TO ADVANCED EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, count, avg, expr  # Imports.

print("="*70)
print("SECTIONS 3-5: AQE in Practice")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Check AQE configuration
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: AQE Configuration Status")
print("-"*60)

# All AQE-related configs.
aqe_configs = [
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum",
    "spark.sql.adaptive.coalescePartitions.minPartitionSize",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
    "spark.sql.adaptive.autoBroadcastJoinThreshold",
]

print("\nCurrent AQE settings:")
for cfg in aqe_configs:
    val = spark.conf.get(cfg, "(not set)")  # Get config value.
    print(f"  {cfg.split('.')[-1]:45s} = {val}")

print("")
print("✓ In Databricks (DBR 12+), AQE is ON by default.")
print("  You get all 3 features for free without any code changes.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Coalescing partition demo
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: AQE auto-coalesces empty partitions")
print("-"*60)

# Small data with many shuffle partitions.
df_small = spark.range(1000).select(
    col("id"),
    (rand() * 5).cast("int").alias("key")  # Only 5 unique keys.
)

# Default shuffle.partitions = 200.
print(f"\nspark.sql.shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")

# After groupBy: without AQE = 200 partitions (195 empty!).
# With AQE = partitions coalesced to actual data needs.
result = df_small.groupBy("key").agg(count("*").alias("cnt"))
result_count = result.collect()  # Trigger execution.

print(f"Result: {len(result_count)} groups (5 unique keys)")
print(f"Output partitions: {result.rdd.getNumPartitions()}")
print("")
print("What AQE did:")
print("  Without AQE: 200 shuffle partitions (195 completely empty)")
print("  With AQE: Coalesced to ~5 partitions (one per actual key)")
print("  This eliminates 195 empty tasks = less scheduling overhead.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Dynamic join strategy switch
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: AQE dynamic join strategy")
print("-"*60)

# Left side: large (100K rows).
left = spark.range(100000).select(col("id").alias("key"), rand().alias("val"))

# Right side: LOOKS large (100K rows) but becomes tiny after filter.
right = spark.range(100000).select(col("id").alias("key"), rand().alias("lookup"))

# At plan time: both sides look like 100K rows → plan SortMergeJoin.
# At runtime: right side filtered to only 50 rows → AQE may switch to broadcast!
result = left.join(right.filter("key < 50"), "key")

print("\nPlan (AQE may show 'AdaptiveSparkPlan'):")
result.explain()
result_rows = result.count()  # Trigger.
print(f"\nJoin result: {result_rows} rows")
print("")
print("What AQE did:")
print("  Plan time: Planned SortMergeJoin (both sides look large)")
print("  Runtime: Right side after filter is only 50 rows!")
print("  AQE detects this and may switch to BroadcastHashJoin.")
print("  Result: No shuffle needed for the join = much faster.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: AQE skew join detection
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Skew join handling")
print("-"*60)

print("""
AQE Skew Join triggers when:
  1. A partition after shuffle is > 5x the median partition size
  2. AND that partition is > 256MB

What happens:
  - The skewed partition is SPLIT into smaller sub-partitions.
  - The non-skewed side is REPLICATED to join with each sub-partition.
  - All sub-partitions run in parallel (no more bottleneck).

Example:
  Before AQE: Partition for 'Amazon' = 10GB, others = 200MB each.
    → The 'Amazon' task takes 50x longer than others.

  After AQE detects skew:
    Partition 'Amazon' split into: Amazon_1(2GB), Amazon_2(2GB)...
    Other side replicated to match.
    → All tasks complete in roughly equal time!

You see this in Spark UI as:
  'CustomShuffleReader' or 'AQEShuffleRead' in the plan.
  Tasks tab: more tasks than expected (the split sub-partitions).
""")

# Demonstrate with skewed data.
skewed = spark.range(500000).select(
    col("id"),
    (col("id") < 450000).cast("int").alias("key")  # key=1 gets 90%.
)
lookup = spark.range(2).select(col("id").alias("key"), rand().alias("info"))

# AQE should detect skew in the join.
result = skewed.join(lookup, "key")
result.explain()
print(f"\nJoin result: {result.count():,} rows")
print("Check Spark UI → SQL tab for 'AQEShuffleRead' or 'CustomShuffleReader'.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: When AQE CANNOT help
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: When AQE can't help")
print("-"*60)

print("""
AQE has LIMITATIONS:

  1. FIRST STAGE: No prior shuffle stats. AQE can't optimize
     the initial data read (no runtime info yet).

  2. REPARTITION HINT: If you explicitly call .repartition(200),
     AQE won't coalesce those partitions (you asked for 200).

  3. VERY FAST QUERIES: AQE adds slight overhead for re-planning.
     For sub-second queries, this overhead may exceed the benefit.

  4. STREAMING: AQE doesn't apply to structured streaming queries
     (they use a different execution model).

  5. EXTREME SKEW: If skew is > 1000x, AQE's splitting may not
     be aggressive enough. Manual salting still needed.

Best practice:
  - Let AQE handle most optimization automatically.
  - Only add manual optimizations (broadcast hint, salting)
    when AQE isn't solving the specific problem.
  - Check Spark UI to verify AQE is actually activating.
""")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Disabling AQE without understanding the impact
# MAGIC ```python
# MAGIC # BAD: Turning off AQE because "it's doing something unexpected."
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", "false")  # Loses all 3 benefits!
# MAGIC
# MAGIC # GOOD: Keep AQE on. If something seems wrong, check the Spark UI.
# MAGIC # AQE is tested extensively and almost always improves performance.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Manually setting shuffle.partitions AND relying on AQE
# MAGIC ```python
# MAGIC # CONFUSION: If you set shuffle.partitions=8, AQE won't coalesce further.
# MAGIC # But it also won't INCREASE beyond 8 if data needs more.
# MAGIC
# MAGIC # GOOD: Leave at default (200) and let AQE coalesce down as needed.
# MAGIC # Or set to a high number (2000) and let AQE coalesce the empties.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Using repartition() which blocks AQE coalescing
# MAGIC ```python
# MAGIC # BAD: Explicit repartition blocks AQE from reducing partitions.
# MAGIC df.repartition(200).groupBy("key").count()  # AQE won't coalesce!
# MAGIC
# MAGIC # GOOD: Let AQE handle partition count after the shuffle.
# MAGIC df.groupBy("key").count()  # AQE auto-coalesces empty partitions.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Expecting AQE to fix the first stage
# MAGIC ```python
# MAGIC # AQE only kicks in AFTER the first shuffle (needs runtime stats).
# MAGIC # The initial file read parallelism is still determined by:
# MAGIC #   - Number of files/blocks
# MAGIC #   - spark.sql.files.maxPartitionBytes
# MAGIC # You still need to manage input partitioning yourself.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not checking Spark UI to verify AQE activated
# MAGIC ```
# MAGIC In Spark UI → SQL tab → click your query:
# MAGIC   Look for: "AdaptiveSparkPlan" at the top of the plan.
# MAGIC   Look for: "CustomShuffleReader" (coalescing or splitting).
# MAGIC   If you don't see these: AQE didn't activate for this query.
# MAGIC   Possible reasons: query too simple, no shuffle, or AQE disabled.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, count  # Imports.

print("="*70)
print("HOMEWORK — Adaptive Query Execution (AQE)")
print("="*70)

# Level 1: Check if AQE is enabled.
print("\n--- Level 1: Verify AQE is ON ---")
print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled', 'not set')}")
print(f"Coalesce enabled: {spark.conf.get('spark.sql.adaptive.coalescePartitions.enabled', 'not set')}")
print(f"Skew join enabled: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled', 'not set')}")
# WHY: AQE must be ON for its optimizations to apply.

# Level 2: Observe coalescing.
print("\n--- Level 2: See AQE coalesce in action ---")
df = spark.range(100).select(col("id"), (col("id")%3).alias("grp"))
result = df.groupBy("grp").count()
result.collect()  # Trigger.
print(f"Output partitions: {result.rdd.getNumPartitions()}")
print("Without AQE: would be 200. With AQE: coalesced to few.")
# WHY: AQE detects most partitions are empty and merges them.

# Level 3: Look for AdaptiveSparkPlan in explain.
print("\n--- Level 3: Spot AQE in the plan ---")
df2 = spark.range(10000).select(col("id"), (rand()*10).cast("int").alias("key"))
df2.groupBy("key").count().explain()
print("Look for 'AdaptiveSparkPlan' at the top = AQE is active.")
# WHY: AdaptiveSparkPlan wrapper means AQE will re-optimize at runtime.

# Level 4: Dynamic join switch.
print("\n--- Level 4: Dynamic join strategy ---")
large = spark.range(50000).select(col("id").alias("key"), rand().alias("v"))
small_after_filter = spark.range(50000).select(col("id").alias("key"), rand().alias("w"))
result = large.join(small_after_filter.filter("key < 10"), "key")
result.explain()
print(f"Result rows: {result.count()}")
print("AQE may switch to BroadcastHashJoin if filtered side is tiny.")
# WHY: AQE sees actual data size after filter and can switch join type.

# Level 5: Verify skew configs.
print("\n--- Level 5: Skew join thresholds ---")
factor = spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
threshold = spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
print(f"Skew factor: {factor}x median (partition must be {factor}x larger)")
print(f"Skew threshold: {threshold} (partition must also exceed this size)")
print("Both conditions must be true for AQE to split the partition.")
# WHY: Prevents false positives on small but imbalanced data.

# Levels 6-10: Conceptual.
print("\n--- Level 6: When AQE can't help (first stage) ---")
print("AQE needs shuffle stats. First stage has no prior shuffle.")
print("Manage input parallelism with maxPartitionBytes or file count.")

print("\n--- Level 7: repartition blocks AQE ---")
print("If you call repartition(200), AQE won't coalesce those.")
print("Use default shuffle.partitions and let AQE handle it.")

print("\n--- Level 8: When to still manually optimize ---")
print("Extreme skew (1000x+), specific broadcast hints, streaming.")

print("\n--- Level 9: Monitor AQE in Spark UI ---")
print("SQL tab → click query → look for CustomShuffleReader.")
print("If present: AQE coalesced or split partitions.")

print("\n--- Level 10: Teach AQE ---")
print("""
"AQE = Spark adapts its plan while running, using real data stats.
  3 automatic fixes:
  1. Merges empty partitions (no wasted tasks)
  2. Switches to broadcast join if data is small at runtime
  3. Splits skewed partitions (no manual salting needed)
  It's ON by default in Databricks. Just let it work."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 70")
print("="*70)