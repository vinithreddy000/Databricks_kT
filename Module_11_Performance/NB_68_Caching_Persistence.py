# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Notebook 68: Caching and Persistence
# MAGIC ## Module 11: Performance Optimization
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Caching** stores a DataFrame's computed results in memory (or disk) so Spark doesn't have to recompute it from scratch every time you use it. Without caching, every action on the same DataFrame triggers a full recomputation from the source files.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine you're a chef preparing a complex sauce that takes 30 minutes:
# MAGIC - **Without caching**: Every time a customer orders the sauce, you make it from scratch (30 min each).
# MAGIC - **With caching**: You make a big batch once, store it in the fridge, and scoop from it for each order (30 seconds each).
# MAGIC - **Unpersist**: Throwing out the stored batch when you're done for the night (free up fridge space).
# MAGIC
# MAGIC The fridge = memory. The freezer = disk. You choose based on how much space you have and how fast you need access.
# MAGIC
# MAGIC ### When to Cache:
# MAGIC - You use the **same DataFrame in multiple actions** (count + show + groupBy)
# MAGIC - **Iterative algorithms** (ML training that reads the same data 100 times)
# MAGIC - **Interactive exploration** (Databricks notebook where you run multiple queries on same data)
# MAGIC
# MAGIC ### When NOT to Cache:
# MAGIC - **Single-use DataFrames** (read → transform → write → done)
# MAGIC - **Very large DataFrames** that won't fit in memory (causes spill + GC pressure)
# MAGIC - **Delta tables** (Delta has its own caching layer; double-caching wastes memory)
# MAGIC - **Streaming DataFrames** (can't cache streaming)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Without Caching (recomputes every time):
# MAGIC
# MAGIC   df = spark.read.parquet("/data")     ← LAZY (no work yet)
# MAGIC   df2 = df.filter(...).groupBy(...)    ← LAZY (no work yet)
# MAGIC
# MAGIC   df2.count()   → [Read files] → [Filter] → [GroupBy] → [Count]     (30 sec)
# MAGIC   df2.show()    → [Read files] → [Filter] → [GroupBy] → [Show]      (30 sec)
# MAGIC   df2.toPandas()→ [Read files] → [Filter] → [GroupBy] → [toPandas]  (30 sec)
# MAGIC                    Total: 90 seconds (everything computed 3 times!)
# MAGIC
# MAGIC With Caching (compute once, reuse):
# MAGIC
# MAGIC   df2.cache()     ← MARKS for caching (no work yet)
# MAGIC   df2.count()   → [Read] → [Filter] → [GroupBy] → [Count] + STORE IN MEMORY  (30 sec)
# MAGIC   df2.show()    → [Read from memory]  → [Show]                                (2 sec)
# MAGIC   df2.toPandas()→ [Read from memory]  → [toPandas]                            (2 sec)
# MAGIC                    Total: 34 seconds (10x faster for actions 2 and 3!)
# MAGIC
# MAGIC Storage Levels:
# MAGIC   ┌─────────────────────┬───────┬──────┬────────────┬──────────────────────────────┐
# MAGIC   │ Level               │ Mem   │ Disk │ Serialized │ Best For                       │
# MAGIC   ├─────────────────────┼───────┼──────┼────────────┼──────────────────────────────┤
# MAGIC   │ MEMORY_ONLY         │ Yes   │ No   │ No         │ Small data, fastest access      │
# MAGIC   │ MEMORY_AND_DISK     │ Yes   │ Spill│ No         │ Default (.cache()), safe choice │
# MAGIC   │ MEMORY_ONLY_SER     │ Yes   │ No   │ Yes        │ Fit more data in same memory    │
# MAGIC   │ MEMORY_AND_DISK_SER │ Yes   │ Spill│ Yes        │ Large data, balanced            │
# MAGIC   │ DISK_ONLY           │ No    │ Yes  │ Yes        │ Very large, infrequent access   │
# MAGIC   │ OFF_HEAP            │ Off-H │ No   │ Yes        │ Avoid GC pauses (advanced)      │
# MAGIC   └─────────────────────┴───────┴──────┴────────────┴──────────────────────────────┘
# MAGIC
# MAGIC   .cache() = shortcut for .persist(StorageLevel.MEMORY_AND_DISK)
# MAGIC   .unpersist() = remove from cache, free memory
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Caching Demo
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, count, avg, sum as spark_sum  # Import functions.
from pyspark import StorageLevel  # Import all storage level options.
import time  # For measuring performance difference.

print("="*70)
print("SECTION 3 — BEGINNER EXAMPLES: Basic Caching")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Without cache vs With cache (timing comparison)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Timing — Without cache vs With cache")
print("-"*60)

# Create a DataFrame with an "expensive" computation.
# (In production, this would be reading from files + complex transforms.)
df = spark.range(5000000).select(  # 5 million rows.
    col("id"),
    (rand() * 1000).alias("value"),       # Random value 0-1000.
    (rand() * 10).cast("int").alias("category")  # Random category 0-9.
).filter(col("value") > 100)  # Keep ~90% of rows.

# --- WITHOUT CACHE: DataFrame recomputed for EVERY action. ---
print("\n--- WITHOUT cache (recomputed each time) ---")
start = time.time()  # Start timer.
df.count()  # Action 1: Spark reads source, filters, counts.
df.groupBy("category").agg(avg("value")).collect()  # Action 2: Reads source AGAIN!
df.agg(spark_sum("value")).collect()  # Action 3: Reads source AGAIN!
time_no_cache = time.time() - start  # Total time.
print(f"  3 actions without cache: {time_no_cache:.2f} seconds")
print("  (DataFrame was recomputed 3 separate times!)")

# --- WITH CACHE: Compute once, store in memory, reuse. ---
print("\n--- WITH cache (computed once, reused) ---")
df_cached = df.cache()  # Mark for caching. Nothing happens yet (lazy).
start = time.time()  # Start timer.
df_cached.count()  # Action 1: Compute + STORE in memory.
df_cached.groupBy("category").agg(avg("value")).collect()  # Action 2: Read from MEMORY!
df_cached.agg(spark_sum("value")).collect()  # Action 3: Read from MEMORY!
time_with_cache = time.time() - start  # Total time.
print(f"  3 actions with cache: {time_with_cache:.2f} seconds")
print(f"  Speedup: {time_no_cache/max(time_with_cache,0.01):.1f}x faster")

# Clean up.
df_cached.unpersist()  # Release cached data from memory.
print("\n  ✓ Unpersisted. Memory freed.")

# Expected: Cache version significantly faster for actions 2 & 3.

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: .cache() vs .persist() with different storage levels
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: .cache() vs .persist(StorageLevel)")
print("-"*60)

df2 = spark.range(2000000).select(col("id"), (rand() * 100).alias("metric"))  # 2M rows.

# .cache() is just a shortcut for .persist(MEMORY_AND_DISK).
print("\n.cache() = .persist(StorageLevel.MEMORY_AND_DISK)")
print("  If it fits in memory: stays in memory (fast).")
print("  If it doesn't fit: spills to disk (slower but won't crash).")

# Demonstrate different storage levels.
print("\n--- MEMORY_ONLY: fastest, but drops data if memory runs out ---")
df2.persist(StorageLevel.MEMORY_ONLY)  # Only in memory. If no room, data is LOST (recomputed).
df2.count()  # Trigger caching.
print(f"  Persisted MEMORY_ONLY. Is cached: {df2.is_cached}")
df2.unpersist()  # Clean up.

print("\n--- MEMORY_AND_DISK: safe default (spills to disk if needed) ---")
df2.persist(StorageLevel.MEMORY_AND_DISK)  # Memory first, overflow to disk.
df2.count()  # Trigger.
print(f"  Persisted MEMORY_AND_DISK. Is cached: {df2.is_cached}")
df2.unpersist()  # Clean up.

print("\n--- DISK_ONLY: when data is too large for memory ---")
df2.persist(StorageLevel.DISK_ONLY)  # Everything on disk. Slower but no memory pressure.
df2.count()  # Trigger.
print(f"  Persisted DISK_ONLY. Is cached: {df2.is_cached}")
df2.unpersist()  # Clean up.

print("\n--- MEMORY_ONLY_SER: serialized = smaller footprint, slight CPU cost ---")
df2.persist(StorageLevel.MEMORY_ONLY_SER)  # Compressed in memory. Uses less space.
df2.count()  # Trigger.
print(f"  Persisted MEMORY_ONLY_SER. Is cached: {df2.is_cached}")
df2.unpersist()  # Clean up.

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Checking if data is cached + the Storage tab
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Checking cache status and using .unpersist()")
print("-"*60)

df3 = spark.range(1000000).select(col("id"), rand().alias("val"))  # 1M rows.

# Before caching.
print(f"\nBefore cache: df3.is_cached = {df3.is_cached}")  # False.

# Cache it.
df3.cache()  # Mark for caching.
df3.count()  # First action triggers the actual caching.
print(f"After cache + count: df3.is_cached = {df3.is_cached}")  # True.

# Check the Storage tab in Spark UI.
print("\n👉 Go to Spark UI → Storage tab:")
print("  You'll see this DataFrame listed with:")
print("  - Size in Memory (how much RAM it uses)")
print("  - Size on Disk (if it spilled)")
print("  - Fraction Cached (% of partitions stored)")

# Unpersist: ALWAYS do this when you're done!
df3.unpersist()  # Releases the memory.
print(f"\nAfter unpersist: df3.is_cached = {df3.is_cached}")  # False.
print("  ✓ Memory freed. Always unpersist when done to avoid memory waste.")

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 4 — INTERMEDIATE EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, count, avg, sum as spark_sum, expr  # Imports.
from pyspark import StorageLevel  # Storage levels.
import time  # Timing.

print("="*70)
print("SECTION 4 — INTERMEDIATE EXAMPLES")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Caching in iterative algorithms (ML pattern)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Iterative algorithm pattern (cache before loop)")
print("-"*60)

# Scenario: Training data is read once but used in 10 iterations.
training_data = spark.range(2000000).select(
    col("id"),
    (rand() * 100).alias("feature1"),  # Feature 1.
    (rand() * 50).alias("feature2"),   # Feature 2.
    (rand() > 0.5).cast("int").alias("label")  # Binary label.
)

# Cache the training data BEFORE the loop.
training_data.cache()  # Mark for caching.
training_data.count()  # Trigger caching (first action materializes it).
print(f"Training data cached: {training_data.is_cached}")
print(f"Rows: {training_data.count():,}")

# Simulate 5 iterations of an algorithm accessing the same data.
print("\nSimulating 5 iterations of an algorithm:")
for i in range(5):
    start = time.time()
    # Each iteration computes something from the cached data.
    result = training_data.groupBy("label").agg(
        avg("feature1").alias("avg_f1"),
        avg("feature2").alias("avg_f2")
    ).collect()  # Reads from CACHE (not re-reading source files!).
    elapsed = time.time() - start
    print(f"  Iteration {i+1}: {elapsed:.3f}s (reading from cache)")

# Clean up.
training_data.unpersist()  # Free memory after training.
print("\n  ✓ Training complete. Cache released.")
print("  Without cache: each iteration would re-read source + recompute.")
print("  With cache: iterations 2-5 are nearly instant.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: SQL caching with CACHE TABLE
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: SQL-style caching with CACHE TABLE")
print("-"*60)

# Create a temp view.
df_sql = spark.range(1000000).select(col("id"), (rand()*100).alias("score"))
df_sql.createOrReplaceTempView("student_scores")  # Register as SQL view.

# Cache the view using SQL.
print("\n--- CACHE TABLE (SQL) ---")
spark.sql("CACHE TABLE student_scores")  # Caches the ENTIRE view in memory.
print("  CACHE TABLE student_scores → cached!")

# Now SQL queries on this view read from cache.
result1 = spark.sql("SELECT avg(score) FROM student_scores").collect()  # From cache.
result2 = spark.sql("SELECT count(*) FROM student_scores WHERE score > 50").collect()  # From cache.
print(f"  Avg score: {result1[0][0]:.2f}")
print(f"  Count > 50: {result2[0][0]:,}")

# Uncache.
spark.sql("UNCACHE TABLE student_scores")  # Release.
print("  UNCACHE TABLE student_scores → freed!")
print("")
print("  Use CACHE TABLE for SQL-heavy workflows in Databricks SQL.")
print("  Use .cache()/.persist() for DataFrame API workflows.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Cache invalidation — when cached data becomes stale
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Cache invalidation (stale data problem)")
print("-"*60)

# Write initial data.
cache_path = "/tmp/delta_kt/cache_invalidation_demo"
spark.range(100).select(col("id"), lit(1).alias("version")) \
    .write.format("delta").mode("overwrite").save(cache_path)  # v1.

# Read and cache.
from pyspark.sql.functions import lit  # Import.
df_cached = spark.read.format("delta").load(cache_path).cache()  # Cache v1.
df_cached.count()  # Materialize cache.
print(f"Cached version 1: {df_cached.filter('version=1').count()} rows")

# Now the underlying data changes!
spark.range(100, 200).select(col("id"), lit(2).alias("version")) \
    .write.format("delta").mode("append").save(cache_path)  # v2 appended.

# The CACHE still shows old data!
print(f"Cached (STALE): {df_cached.count()} rows (should be 200, shows 100!)")
print("  ⚠️ Cache is STALE! It doesn't know about the new data.")

# Fix: Unpersist and re-read.
df_cached.unpersist()  # Release stale cache.
df_fresh = spark.read.format("delta").load(cache_path).cache()  # Re-read fresh.
df_fresh.count()  # Materialize.
print(f"Fresh cache: {df_fresh.count()} rows (correct!)")
df_fresh.unpersist()  # Clean up.

print("")
print("Key lesson: Cached data does NOT auto-refresh!")
print("If underlying data changes, you must unpersist + re-cache.")
print("This is why caching is best for STATIC data (training sets, lookups).")

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 5 — ADVANCED EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, count, avg  # Imports.
from pyspark import StorageLevel  # Storage levels.
import time  # Timing.

print("="*70)
print("SECTION 5 — ADVANCED EXAMPLES (Production-Style)")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 7: Choosing the right storage level for your data size
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 7: Decision tree for choosing storage level")
print("-"*60)

print("""
┌─────────────────────────────────────────────────────────┐
│         STORAGE LEVEL DECISION TREE                     │
└─────────────────────────────────────────────────────────┘

  Does your data fit in executor memory?
    │
    ├─ YES: Does it fit comfortably (< 60% of executor memory)?
    │    ├─ YES: Use MEMORY_ONLY (fastest, no disk fallback)
    │    └─ BARELY: Use MEMORY_ONLY_SER (serialized = 2-5x smaller)
    │
    └─ NO: Do you still want fast access?
         ├─ YES: Use MEMORY_AND_DISK (default .cache())
         │       Memory for what fits, disk for overflow.
         └─ Access is infrequent:
                  Use DISK_ONLY (saves memory for other ops)

  In Databricks specifically:
    - Delta tables have their own disk cache (Delta Cache).
    - Don't double-cache: if reading from Delta, the disk cache
      already handles repeated reads efficiently.
    - Use .cache() mainly for: intermediate DataFrames that 
      you've built with complex transformations.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 8: Production pattern — cache checkpoint in a pipeline
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 8: Cache at pipeline branch points")
print("-"*60)

# Scenario: One expensive transformation feeds MULTIPLE downstream paths.
print("""
Pipeline pattern:

  [Read raw data] → [Clean] → [Transform] → df_clean  ←─ CACHE HERE!
                                               │
                          ┌───────────────────┼───────────────────┐
                          │                   │                   │
                    [Report A]          [Report B]          [Write to Gold]
                    (groupBy region)    (groupBy product)   (Delta table)

Cache df_clean because it feeds 3 separate outputs.
Without cache: the expensive Read+Clean+Transform runs 3 times!
With cache: runs once, stored in memory, used by all 3 branches.
""")

# Demonstrate the pattern.
df_raw = spark.range(3000000).select(
    col("id"),
    (rand() * 100).cast("int").alias("region_id"),
    (rand() * 50).cast("int").alias("product_id"),
    (rand() * 1000).alias("amount")
)

# Expensive transformation.
df_clean = df_raw.filter(col("amount") > 100) \
    .withColumn("normalized", col("amount") / 1000)

# CACHE at the branch point.
df_clean.cache()  # Mark.
df_clean.count()  # Materialize.
print(f"df_clean cached: {df_clean.count():,} rows")

# Branch A: Report by region.
start = time.time()
report_a = df_clean.groupBy("region_id").agg(avg("amount").alias("avg_amt")).collect()
print(f"\n  Report A (by region): {time.time()-start:.3f}s")

# Branch B: Report by product.
start = time.time()
report_b = df_clean.groupBy("product_id").agg(count("*").alias("cnt")).collect()
print(f"  Report B (by product): {time.time()-start:.3f}s")

# Branch C: Write to output.
start = time.time()
df_clean.write.format("delta").mode("overwrite").save("/tmp/delta_kt/cache_branch_output")
print(f"  Write to Delta: {time.time()-start:.3f}s")

# Clean up.
df_clean.unpersist()
print("\n  ✓ All 3 branches completed from cache. Memory released.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 9: Monitoring cache memory usage
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 9: Monitoring cache with spark.catalog")
print("-"*60)

# Cache two DataFrames.
df_a = spark.range(500000).select(col("id"), rand().alias("x")).cache()
df_b = spark.range(500000).select(col("id"), rand().alias("y")).cache()
df_a.count()  # Materialize.
df_b.count()  # Materialize.

# Check what's cached using Catalog API.
print("\nCurrently cached tables/DataFrames:")
print(f"  df_a is cached: {df_a.is_cached}")
print(f"  df_b is cached: {df_b.is_cached}")
print("")
print("👉 Spark UI → Storage tab shows:")
print("  - RDD Name (DataFrame identity)")
print("  - Storage Level (MEMORY_AND_DISK, etc.)")
print("  - Size in Memory / Size on Disk")
print("  - Fraction Cached (what % of partitions are stored)")

# Clear ALL caches at once.
spark.catalog.clearCache()  # Nuclear option: drops ALL cached data.
print("\n  spark.catalog.clearCache() → All caches cleared!")
print(f"  df_a is cached: {df_a.is_cached}")  # False.
print(f"  df_b is cached: {df_b.is_cached}")  # False.

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Caching a DataFrame you only use once
# MAGIC ```python
# MAGIC # BAD: Cache overhead with zero benefit.
# MAGIC df = spark.read.parquet("/data")
# MAGIC df.cache()  # Wastes memory!
# MAGIC df.write.parquet("/output")  # Used only once. Cache was pointless.
# MAGIC df.unpersist()
# MAGIC
# MAGIC # GOOD: Only cache if used in MULTIPLE actions.
# MAGIC df.write.parquet("/output")  # Just write directly. No cache needed.
# MAGIC ```
# MAGIC **Why**: Caching has overhead (memory allocation, serialization). If you only use the data once, caching costs more than it saves.
# MAGIC
# MAGIC ### Mistake 2: Forgetting to call .unpersist()
# MAGIC ```python
# MAGIC # BAD: Cache accumulates, eating all memory.
# MAGIC for table in table_list:
# MAGIC     df = spark.read.table(table).cache()  # Caches pile up!
# MAGIC     process(df)
# MAGIC     # Forgot unpersist! Memory keeps growing.
# MAGIC
# MAGIC # GOOD: Always unpersist when done.
# MAGIC for table in table_list:
# MAGIC     df = spark.read.table(table).cache()
# MAGIC     process(df)
# MAGIC     df.unpersist()  # Free memory immediately.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Caching before the action (thinking cache is eager)
# MAGIC ```python
# MAGIC # MISUNDERSTANDING: .cache() does NOT immediately cache.
# MAGIC df.cache()  # Just marks it. NO data stored yet!
# MAGIC print("Cached!")  # WRONG — nothing is cached yet.
# MAGIC
# MAGIC # CORRECT: An action must trigger the caching.
# MAGIC df.cache()  # Mark.
# MAGIC df.count()  # THIS triggers actual caching (first action).
# MAGIC # NOW it's in memory.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Double-caching Delta tables
# MAGIC ```python
# MAGIC # BAD: Delta already has its own disk cache.
# MAGIC df = spark.read.format("delta").load("/delta/table")
# MAGIC df.cache()  # Redundant! Delta Cache already handles repeated reads.
# MAGIC
# MAGIC # GOOD: Trust Delta's built-in caching for table reads.
# MAGIC # Only cache TRANSFORMED DataFrames that won't benefit from Delta Cache.
# MAGIC df_transformed = spark.read.format("delta").load("/table") \
# MAGIC     .filter(...).join(...).groupBy(...)  # Complex transforms.
# MAGIC df_transformed.cache()  # THIS makes sense — caching the transform result.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Caching too much data (OOM / GC pressure)
# MAGIC ```python
# MAGIC # BAD: Caching a 50GB DataFrame on a cluster with 32GB RAM.
# MAGIC huge_df.cache()  # Causes: spill to disk, GC storms, OOM errors.
# MAGIC
# MAGIC # GOOD: Either sample or use DISK_ONLY for large data.
# MAGIC huge_df.persist(StorageLevel.DISK_ONLY)  # No memory pressure.
# MAGIC # Or: Don't cache at all — let Spark recompute if needed.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, count, avg  # Imports.
from pyspark import StorageLevel  # Storage levels.
import time  # Timing.

print("="*70)
print("HOMEWORK — Caching and Persistence")
print("="*70)

# ────────────────────────────────────────────────────────────
# Level 1 (Just read and run): Cache a DataFrame and check is_cached.
# HINT: Use .cache() then .count() to materialize.
# ────────────────────────────────────────────────────────────
print("\n--- Level 1: Basic cache ---")
df1 = spark.range(100000).select(col("id"), rand().alias("val"))
df1.cache()         # Mark for caching.
df1.count()         # Triggers actual caching.
print(f"is_cached: {df1.is_cached}")  # True.
df1.unpersist()     # Clean up.
print(f"After unpersist: {df1.is_cached}")  # False.
# WHY: .cache() marks it, an action materializes it, .unpersist() frees it.

# ────────────────────────────────────────────────────────────
# Level 2 (Tiny change): Use persist(DISK_ONLY) instead of .cache().
# HINT: from pyspark import StorageLevel
# ────────────────────────────────────────────────────────────
print("\n--- Level 2: Persist with DISK_ONLY ---")
df2 = spark.range(100000).select(col("id"), rand().alias("val"))
df2.persist(StorageLevel.DISK_ONLY)  # Store on disk only (no memory).
df2.count()  # Materialize.
print(f"is_cached: {df2.is_cached}")  # True (even DISK_ONLY counts as cached).
df2.unpersist()
# WHY: DISK_ONLY saves memory for other operations when data is large.

# ────────────────────────────────────────────────────────────
# Level 3 (Combine two things): Time cached vs non-cached with 3 actions.
# HINT: Use time.time() around actions.
# ────────────────────────────────────────────────────────────
print("\n--- Level 3: Timing comparison ---")
df3 = spark.range(3000000).select(col("id"), rand().alias("v"), (col("id")%5).alias("g"))
df3_cached = df3.cache()
start = time.time()
df3_cached.count(); df3_cached.groupBy("g").count().collect(); df3_cached.agg(avg("v")).collect()
print(f"3 actions with cache: {time.time()-start:.2f}s")
df3_cached.unpersist()
# WHY: First action caches, subsequent actions read from memory (fast).

# ────────────────────────────────────────────────────────────
# Level 4 (New scenario): Cache a SQL table view.
# HINT: Use spark.sql("CACHE TABLE viewname").
# ────────────────────────────────────────────────────────────
print("\n--- Level 4: SQL CACHE TABLE ---")
spark.range(50000).select(col("id"), rand().alias("score")).createOrReplaceTempView("hw_view")
spark.sql("CACHE TABLE hw_view")
print(f"SQL cached. Count: {spark.sql('SELECT count(*) FROM hw_view').collect()[0][0]}")
spark.sql("UNCACHE TABLE hw_view")
# WHY: SQL-based caching works for views used in multiple SQL queries.

# ────────────────────────────────────────────────────────────
# Level 5 (Intermediate project): Cache at a branch point.
# HINT: One DataFrame used by 2+ downstream outputs.
# ────────────────────────────────────────────────────────────
print("\n--- Level 5: Cache at branch point ---")
base = spark.range(1000000).select(col("id"), rand().alias("metric"), (col("id")%3).alias("group"))
base_clean = base.filter(col("metric") > 0.2).cache()  # Branch point.
base_clean.count()  # Materialize.
report1 = base_clean.groupBy("group").agg(avg("metric")).collect()  # Branch A.
report2 = base_clean.agg(count("*")).collect()  # Branch B.
base_clean.unpersist()  # Clean up.
print(f"Branch A: {len(report1)} groups. Branch B: {report2[0][0]:,} rows.")
# WHY: Without cache, the filter computation runs twice (once per branch).

# ────────────────────────────────────────────────────────────
# Level 6 (Design first): Should you cache this pipeline?
# ────────────────────────────────────────────────────────────
print("\n--- Level 6: Design decision ---")
print("""
Scenario A: Read CSV → clean → write to Delta. (One action: write.)
  Answer: DON'T cache. Single-use pipeline.

Scenario B: Read CSV → clean → report1 + report2 + report3. (Three actions.)
  Answer: CACHE after clean. Used by 3 downstream actions.

Scenario C: Read Delta table → groupBy → show.
  Answer: DON'T cache. Delta has its own disk cache. Single use.

Scenario D: ML training: read data → iterate 100 times.
  Answer: CACHE! Same data accessed 100 times.
""")
# WHY: Cache only when same DataFrame is accessed by multiple actions.

# ────────────────────────────────────────────────────────────
# Level 7 (Optimize it): Replace .cache() with better storage level.
# ────────────────────────────────────────────────────────────
print("\n--- Level 7: Choose optimal storage level ---")
print("Scenario: 500MB DataFrame, 4GB executor memory, accessed 10 times.")
print("Answer: MEMORY_ONLY (fits easily, fastest access).")
print("")
print("Scenario: 10GB DataFrame, 4GB executor memory, accessed 3 times.")
print("Answer: MEMORY_AND_DISK or DISK_ONLY (won't fit in memory alone).")
# WHY: Match storage level to your data size vs available memory.

# ────────────────────────────────────────────────────────────
# Level 8 (Edge cases): What happens if you cache too much?
# ────────────────────────────────────────────────────────────
print("\n--- Level 8: Over-caching consequences ---")
print("""
If you cache more than executor memory can hold:
  1. MEMORY_ONLY: Oldest cached partitions get EVICTED (lost, must recompute).
  2. MEMORY_AND_DISK: Excess spills to disk (slower but safe).
  3. Too much caching: Less memory for shuffles/joins = SPILL TO DISK everywhere.
  4. Extreme case: GC overhead limit exceeded (all time spent garbage collecting).

Fix: Monitor Storage tab. If cache uses > 50% of executor memory, reduce caching.
""")
# WHY: Cache competes with shuffle/execution memory. Too much = slower everything.

# ────────────────────────────────────────────────────────────
# Level 9 (Production-grade): Implement cache with error handling.
# ────────────────────────────────────────────────────────────
print("\n--- Level 9: Production cache pattern ---")
def process_with_cache(df, operations):
    """Cache a DataFrame, run multiple operations, then unpersist."""
    try:
        df.cache()        # Mark.
        df.count()        # Materialize.
        results = []
        for op in operations:
            results.append(op(df))  # Each operation reads from cache.
        return results
    finally:
        df.unpersist()    # ALWAYS unpersist, even on error.

# Usage.
df9 = spark.range(500000).select(col("id"), rand().alias("v"), (col("id")%3).alias("g"))
results = process_with_cache(df9, [
    lambda d: d.count(),
    lambda d: d.groupBy("g").count().collect(),
    lambda d: d.agg(avg("v")).collect()
])
print(f"Results: count={results[0]}, groups={len(results[1])}, avg={results[2][0][0]:.4f}")
# WHY: try/finally ensures unpersist runs even if processing fails.

# ────────────────────────────────────────────────────────────
# Level 10 (Teach it): Explain caching to a new colleague.
# ────────────────────────────────────────────────────────────
print("\n--- Level 10: Teach caching ---")
print("""
"Caching in Spark:
  Without cache: every .count(), .show(), .write() recomputes everything from scratch.
  With cache: compute once, store result in memory, reuse for all subsequent actions.

  How to use:
    df.cache()     → Mark for caching (nothing happens yet).
    df.count()     → First action materializes the cache.
    df.show()      → Second action reads from memory (instant!).
    df.unpersist() → Free the memory when done.

  When to cache:  Same DataFrame used in 2+ actions.
  When NOT to:    Single-use DataFrames, very large data, Delta table reads.
  Always:         Call .unpersist() when done to avoid memory leaks."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 68")
print("="*70)