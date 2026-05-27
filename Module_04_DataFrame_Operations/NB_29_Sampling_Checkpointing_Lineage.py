# Databricks notebook source
# DBTITLE 1,Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 29: Sampling, Checkpointing, and Lineage
# MAGIC # Module: DataFrame Operations
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 40 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC
# MAGIC - **Sampling** = Tasting one spoonful from a pot to check if the soup is good (don’t eat the whole pot)
# MAGIC - **Checkpointing** = Saving your video game — if you die, restart from the checkpoint, not the beginning
# MAGIC - **Lineage** = The recipe card that tracks every step you took to cook the dish
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Why You Need These
# MAGIC
# MAGIC | Concept | Problem It Solves |
# MAGIC |---------|------------------|
# MAGIC | `sample()` | Working with 1B rows? Sample 1% for fast exploration |
# MAGIC | `sampleBy()` | Need proportional representation per category (stratified) |
# MAGIC | Lineage | Spark tracks every transformation — the full "recipe" |
# MAGIC | `checkpoint()` | Recipe gets too long (100+ steps) → performance degrades |
# MAGIC | `localCheckpoint()` | Faster checkpoint, but less reliable (no disk backup) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Insight
# MAGIC
# MAGIC Spark is **lazy** — it remembers every transformation as a lineage graph. When that graph gets very deep (iterative ML, complex pipelines), `checkpoint()` truncates it to avoid recomputation overhead.

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Sampling Mechanics
# MAGIC
# MAGIC ```
# MAGIC df.sample(fraction=0.1, seed=42)
# MAGIC   → Each row has 10% probability of being included
# MAGIC   → Result size is APPROXIMATE (not exactly 10% of rows)
# MAGIC   → seed makes it reproducible
# MAGIC
# MAGIC df.sampleBy("category", fractions={"A": 0.5, "B": 0.1}, seed=42)
# MAGIC   → 50% of category A rows, 10% of category B rows
# MAGIC   → Stratified: maintains proportional representation
# MAGIC ```
# MAGIC
# MAGIC ### What is Lineage?
# MAGIC
# MAGIC ```
# MAGIC Lineage = the DAG of transformations Spark remembers:
# MAGIC
# MAGIC   read_csv → filter → withColumn → join → groupBy → filter → ...
# MAGIC
# MAGIC Why it matters:
# MAGIC   - If a partition is lost, Spark replays the lineage to recompute it
# MAGIC   - Very long lineage = slow plan compilation + deep stack traces
# MAGIC   - After 50+ transformations, consider checkpointing
# MAGIC ```
# MAGIC
# MAGIC ### Checkpoint vs LocalCheckpoint
# MAGIC
# MAGIC ```
# MAGIC checkpoint():
# MAGIC   → Writes data to reliable storage (HDFS/DBFS)
# MAGIC   → Cuts lineage completely
# MAGIC   → Data survives executor failures
# MAGIC   → Requires: sc.setCheckpointDir("path")
# MAGIC
# MAGIC localCheckpoint():
# MAGIC   → Writes to local executor disk/memory
# MAGIC   → Cuts lineage
# MAGIC   → Data lost if executor dies
# MAGIC   → No checkpoint dir needed
# MAGIC   → Faster than checkpoint()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: sample()
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: sample()
# ═══════════════════════════════════════════════════════

print("=== DataFrame.sample() ===")
print()

# Create a large DataFrame
df = spark.range(10000).withColumn("value", (col("id") * 7 % 100))  # 10K rows
print(f"Original: {df.count():,} rows")

# --- Basic sample (10%) ---
print("\n--- 1. sample(fraction=0.1) — ~10% of rows ---")
from pyspark.sql.functions import col
sample1 = df.sample(fraction=0.1)  # ~10% probability per row
print(f"  Sampled: {sample1.count()} rows (approx 1000)")

# --- Sample with seed (reproducible!) ---
print("\n--- 2. sample(fraction=0.1, seed=42) — reproducible ---")
sample_a = df.sample(fraction=0.1, seed=42)  # Same seed
sample_b = df.sample(fraction=0.1, seed=42)  # Same seed again
print(f"  Run A: {sample_a.count()} rows")
print(f"  Run B: {sample_b.count()} rows")
print(f"  Same result: {sample_a.count() == sample_b.count()}")  # True!

# --- Sample with replacement ---
print("\n--- 3. sample(withReplacement=True) — allows duplicates ---")
sample_wr = df.sample(withReplacement=True, fraction=0.1, seed=42)
print(f"  With replacement: {sample_wr.count()} rows (some may be duplicated)")
print("  withReplacement=True: same row can be picked multiple times")

# --- Quick exploration pattern ---
print("\n--- 4. Quick exploration: limit vs sample ---")
print("  df.limit(100)   = first 100 rows (biased! may be sorted)")
print("  df.sample(0.01) = random ~1% (unbiased representation)")
print("  For exploration: sample > limit (more representative)")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: sampleBy (stratified)
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: sampleBy() (stratified sampling)
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== sampleBy() — Stratified Sampling ===")
print()
print("Use when you need proportional representation per category.")
print("Example: 90% fraud-free, 10% fraud → sample should maintain this ratio.")
print()

# Create imbalanced data (like fraud detection)
df = spark.range(10000).withColumn(
    "category",
    col("id").cast("int") % 10  # Categories 0-9
)

# Show original distribution
print("--- Original distribution ---")
df.groupBy("category").count().orderBy("category").show()

# --- sampleBy: Different fractions per category ---
print("--- sampleBy: 50% of cat 0, 10% of cat 1, 5% of rest ---")
fractions = {0: 0.5, 1: 0.1, 2: 0.05, 3: 0.05, 4: 0.05,
             5: 0.05, 6: 0.05, 7: 0.05, 8: 0.05, 9: 0.05}

stratified = df.sampleBy("category", fractions=fractions, seed=42)
print(f"  Total sampled: {stratified.count()} rows")
print("\n  Sampled distribution:")
stratified.groupBy("category").count().orderBy("category").show(5)

print("--- Use cases ---")
print("  1. ML: Balance classes for training (oversample minority)")
print("  2. Testing: Ensure all categories represented in test data")
print("  3. Cost control: Sample more from important categories")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Understanding lineage
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: Understanding lineage
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, upper, lit

print("=== Understanding Lineage (Transformation History) ===")
print()
print("Spark remembers every step as a DAG (Directed Acyclic Graph).")
print("This is the LINEAGE — the recipe to reproduce any partition.")
print()

# Build a chain of transformations
df = spark.range(1000)  # Step 1: Generate
df = df.withColumn("doubled", col("id") * 2)  # Step 2: Add column
df = df.filter(col("id") > 500)  # Step 3: Filter
df = df.withColumn("label", lit("active"))  # Step 4: Add constant
df = df.withColumn("tripled", col("id") * 3)  # Step 5: Another column

# --- View the lineage (logical plan) ---
print("--- Logical Plan (lineage) ---")
df.explain(True)  # Shows all plans including parsed/analyzed/optimized/physical

# --- RDD lineage (deeper view) ---
print("\n--- RDD Lineage (toDebugString) ---")
print(df.rdd.toDebugString().decode("utf-8")[:500])  # First 500 chars

print("\n--- Why lineage matters ---")
print("  1. FAULT TOLERANCE: If a partition is lost, Spark replays lineage")
print("  2. OPTIMIZATION: Catalyst optimizer rearranges steps for speed")
print("  3. LAZY: Nothing executes until an action (show, count, write)")
print("  4. PROBLEM: Very long lineage (100+ steps) = slow plan compilation")
print("     Solution: checkpoint() to truncate lineage")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: checkpoint()
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: checkpoint()
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== checkpoint() — Cutting Long Lineage ===")
print()

# --- Step 1: Set checkpoint directory (REQUIRED!) ---
sc = spark.sparkContext
sc.setCheckpointDir("/tmp/spark_checkpoints")  # Where to save checkpoint data
print("Checkpoint directory set: /tmp/spark_checkpoints")

# --- Build a long lineage ---
df = spark.range(10000)
for i in range(20):  # 20 transformations = long lineage!
    df = df.withColumn(f"col_{i}", col("id") + i)

print(f"\n--- Before checkpoint: {len(df.columns)} columns, deep lineage ---")
print(f"  Plan length: ~{df.rdd.toDebugString().count(b'\n')} lines")

# --- Checkpoint: saves data and truncates lineage ---
df_checkpointed = df.checkpoint()  # Materializes + cuts lineage!

print(f"\n--- After checkpoint: lineage is FLAT ---")
print(f"  Plan length: ~{df_checkpointed.rdd.toDebugString().count(b'\n')} lines")
print(f"  Rows: {df_checkpointed.count():,}")

# --- Verify lineage is cut ---
print("\n--- Checkpointed plan (much simpler!) ---")
df_checkpointed.explain()  # Should show just "Scan" (no 20 projections)

print("\n--- Key facts ---")
print("  1. checkpoint() triggers an ACTION (writes data immediately)")
print("  2. Result: new DataFrame with flat lineage (just reads files)")
print("  3. checkpoint(eager=True) = default, materializes immediately")
print("  4. checkpoint(eager=False) = lazy, materializes on first action")
print("  5. Files saved to checkpointDir (auto-cleaned on SparkContext stop)")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: localCheckpoint()
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: localCheckpoint()
# ═══════════════════════════════════════════════════════

import time
from pyspark.sql.functions import col

print("=== localCheckpoint() — Faster, Less Reliable ===")
print()
print("localCheckpoint: saves to executor LOCAL storage (not HDFS/DBFS)")
print("Faster than checkpoint(), but data lost if executor dies.")
print()

# Build long lineage
df = spark.range(50000)
for i in range(30):  # 30 transformations
    df = df.withColumn(f"c{i}", col("id") * (i + 1))

# --- Compare: checkpoint vs localCheckpoint ---
print("--- Timing comparison ---")

# Regular checkpoint
start = time.time()
df_cp = df.checkpoint()
df_cp.count()  # Force materialization
t_cp = time.time() - start

# Local checkpoint
df2 = spark.range(50000)
for i in range(30):
    df2 = df2.withColumn(f"c{i}", col("id") * (i + 1))

start = time.time()
df_lcp = df2.localCheckpoint()
df_lcp.count()  # Force materialization
t_lcp = time.time() - start

print(f"  checkpoint():      {t_cp:.2f}s (writes to reliable storage)")
print(f"  localCheckpoint(): {t_lcp:.2f}s (writes to local executor)")

print("\n--- When to use which ---")
print(f"  {'Method':<20} {'Speed':<10} {'Reliability':<15} {'Use Case'}")
print(f"  {'-'*65}")
print(f"  {'checkpoint()':<20} {'Slow':<10} {'High':<15} {'Production, long jobs'}")
print(f"  {'localCheckpoint()':<20} {'Fast':<10} {'Low':<15} {'Iterative ML, exploration'}")
print(f"  {'cache()':<20} {'N/A':<10} {'Medium':<15} {'Repeated reads (no lineage cut)'}")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: When to checkpoint
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: When to checkpoint
# ═══════════════════════════════════════════════════════

print("=== When to Checkpoint: Decision Guide ===")
print()

# Scenario: Iterative algorithm (like K-means or PageRank)
print("--- Scenario: Iterative ML (PageRank-like) ---")
from pyspark.sql.functions import col, lit

# Simulate iterative computation
df = spark.range(10000).withColumn("score", lit(1.0))  # Initial scores

for iteration in range(5):
    # Each iteration: transform based on previous result
    df = df.withColumn("score", col("score") * 0.85 + 0.15)  # PageRank-like
    
    # Checkpoint every N iterations to prevent lineage explosion!
    if iteration % 3 == 2:  # Every 3 iterations
        df = df.localCheckpoint()  # Cut lineage
        print(f"  Iteration {iteration}: checkpointed (lineage reset)")
    else:
        print(f"  Iteration {iteration}: building lineage")

print(f"\n  Final count: {df.count():,} rows")

print("\n--- Decision Guide: When to Checkpoint ---")
print("  \u2705 Iterative ML (K-means, PageRank, gradient descent)")
print("  \u2705 Very long ETL pipeline (50+ transformations)")
print("  \u2705 DataFrame used as input to multiple downstream branches")
print("  \u2705 Debugging: checkpoint to materialize and inspect mid-pipeline")
print("  \u2705 Streaming: required for stateful operations")
print()
print("  \u274c Single-pass ETL (just cache or nothing)")
print("  \u274c Small data (overhead > benefit)")
print("  \u274c Already writing to Delta (Delta IS your checkpoint!)")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Smart sampling strategy
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Smart sampling strategies
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, count, lit

print("=== Smart Sampling Strategies ===")
print()

def smart_sample(df, target_rows=1000, stratify_col=None, seed=42):
    """
    Intelligent sampling:
    - Calculates exact fraction needed for target_rows
    - Optionally stratifies by a column
    - Ensures minimum representation per category
    """
    total = df.count()
    
    if total <= target_rows:
        print(f"  Data ({total} rows) smaller than target ({target_rows}). No sampling needed.")
        return df
    
    if stratify_col:
        # Calculate per-category fractions
        categories = df.groupBy(stratify_col).count().collect()
        fractions = {}
        for row in categories:
            cat = row[stratify_col]
            cat_count = row["count"]
            # Target proportional representation, min 1 row per category
            frac = max(target_rows / total, 1.0 / cat_count)
            fractions[cat] = min(frac, 1.0)  # Cap at 100%
        
        result = df.sampleBy(stratify_col, fractions, seed)
    else:
        fraction = target_rows / total
        result = df.sample(fraction=fraction, seed=seed)
    
    actual = result.count()
    print(f"  Sampled: {total:,} → {actual:,} rows (target: {target_rows:,})")
    return result

# Demo
df = spark.range(100000).withColumn("category", (col("id") % 5).cast("string"))

print("--- Random sample ---")
s1 = smart_sample(df, target_rows=500)

print("\n--- Stratified sample ---")
s2 = smart_sample(df, target_rows=500, stratify_col="category")
print("  Per-category distribution:")
s2.groupBy("category").count().orderBy("category").show()

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Lineage debugging
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Lineage inspection and debugging
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== Lineage Inspection Tools ===")
print()

def lineage_depth(df):
    """Estimate lineage depth from RDD debug string."""
    debug = df.rdd.toDebugString().decode("utf-8")
    depth = debug.count("\n")
    return depth

def should_checkpoint(df, threshold=30):
    """Check if lineage is deep enough to warrant checkpointing."""
    depth = lineage_depth(df)
    recommend = depth > threshold
    print(f"  Lineage depth: {depth} lines")
    print(f"  Threshold: {threshold}")
    print(f"  Recommend checkpoint: {'YES' if recommend else 'No'}")
    return recommend

# --- Build a deep lineage ---
df = spark.range(1000)
for i in range(25):
    df = df.withColumn(f"col_{i}", col("id") + i * 2)

print("--- Check lineage depth ---")
needs_cp = should_checkpoint(df, threshold=20)

if needs_cp:
    print("\n--- Applying localCheckpoint ---")
    df = df.localCheckpoint()
    should_checkpoint(df, threshold=20)  # After: should be shallow

print("\n--- Production pattern ---")
print("  def transform_pipeline(df):")
print("      df = step_1(df)")
print("      df = step_2(df)")
print("      ...")
print("      if lineage_depth(df) > 50:")
print("          df = df.localCheckpoint()")
print("      return df")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Checkpoint in iterative ML
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Checkpoint in iterative algorithms
# ═══════════════════════════════════════════════════════

import time
from pyspark.sql.functions import col, abs as spark_abs, avg

print("=== Checkpoint in Iterative Algorithm ===")
print()
print("Simulating iterative convergence (like gradient descent).")
print()

# --- Without checkpoint (lineage grows each iteration) ---
print("--- Without checkpoint ---")
df_no_cp = spark.range(10000).withColumn("value", col("id").cast("double") / 10000)

start = time.time()
for i in range(10):
    df_no_cp = df_no_cp.withColumn("value", col("value") * 0.9 + 0.05)  # Converge
t_no_cp = time.time() - start
df_no_cp.select(avg("value")).show()  # Force evaluation
print(f"  Time (no checkpoint): {time.time() - start:.2f}s")

# --- With checkpoint every 5 iterations ---
print("\n--- With checkpoint every 5 iterations ---")
df_with_cp = spark.range(10000).withColumn("value", col("id").cast("double") / 10000)

start = time.time()
for i in range(10):
    df_with_cp = df_with_cp.withColumn("value", col("value") * 0.9 + 0.05)
    if (i + 1) % 5 == 0:  # Checkpoint every 5 iterations
        df_with_cp = df_with_cp.localCheckpoint()  # Cut lineage!
        print(f"  Iteration {i+1}: checkpointed")

df_with_cp.select(avg("value")).show()
print(f"  Time (with checkpoint): {time.time() - start:.2f}s")

print("\n--- Best practice for iterative algorithms ---")
print("  1. Checkpoint every N iterations (e.g., every 5-10)")
print("  2. Use localCheckpoint for speed (acceptable risk for exploration)")
print("  3. Use checkpoint() for production (reliable, survives failures)")
print("  4. Alternative: Write to Delta table as intermediate step")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Thinking sample() gives exact row count
# MAGIC **Problem:** `sample(fraction=0.1)` on 10K rows gives ~1000 rows, not exactly 1000.  
# MAGIC **Fix:** For exact N rows, use `df.orderBy(rand()).limit(N)` (but slower).
# MAGIC
# MAGIC ### Mistake #2: Forgetting to set checkpointDir
# MAGIC **Problem:** `df.checkpoint()` throws error: "checkpoint directory not set".  
# MAGIC **Fix:** Call `spark.sparkContext.setCheckpointDir("/tmp/checkpoints")` before checkpointing.
# MAGIC
# MAGIC ### Mistake #3: Checkpointing without counting first
# MAGIC **Problem:** Checkpointing a DataFrame with no action = nothing is actually materialized.  
# MAGIC **Fix:** Use `checkpoint(eager=True)` (default) or call `.count()` after lazy checkpoint.
# MAGIC
# MAGIC ### Mistake #4: Using sample() without seed for reproducibility
# MAGIC **Problem:** Results change every run, making debugging impossible.  
# MAGIC **Fix:** Always specify `seed=42` (or any fixed number) for reproducible results.
# MAGIC
# MAGIC ### Mistake #5: Over-checkpointing (every single step)
# MAGIC **Problem:** Checkpointing after every transformation = writing data to disk constantly = slow!  
# MAGIC **Fix:** Only checkpoint when lineage is truly deep (30+ steps) or branching.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC **Level 1:** Sample 10% of a 10K-row DataFrame. Verify the count is approximately 1000.
# MAGIC
# MAGIC **Level 2:** Use `seed` to make sampling reproducible. Run twice and verify same count.
# MAGIC
# MAGIC **Level 3:** Use `sampleBy()` to get 20% of category "A" and 5% of category "B".
# MAGIC
# MAGIC **Level 4:** View a DataFrame's lineage with `explain(True)`. Count the transformation steps.
# MAGIC
# MAGIC **Level 5:** Build 20 chained transformations. Checkpoint. Compare explain() before/after.
# MAGIC
# MAGIC **Level 6:** Compare `checkpoint()` vs `localCheckpoint()` timing on 100K rows with 30 transforms.
# MAGIC
# MAGIC **Level 7:** Implement the "checkpoint every N iterations" pattern for a simulated algorithm.
# MAGIC
# MAGIC **Level 8:** Build a `smart_sample()` function that auto-calculates fraction for target row count.
# MAGIC
# MAGIC **Level 9:** Build a `lineage_monitor()` that warns when depth exceeds threshold and auto-checkpoints.
# MAGIC
# MAGIC **Level 10:** Explain to a teammate: When should you checkpoint vs cache vs write to Delta?

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

# Level 1 & 2: Sample with seed
print("=== Level 1 & 2: Sample with seed ===")
df = spark.range(10000)
s1 = df.sample(fraction=0.1, seed=42)
s2 = df.sample(fraction=0.1, seed=42)
print(f"  Sample 1: {s1.count()} rows")
print(f"  Sample 2: {s2.count()} rows")
print(f"  Reproducible: {s1.count() == s2.count()}")

# Level 3: Stratified
print("\n=== Level 3: sampleBy ===")
df3 = spark.range(5000).withColumn("cat", (col("id") % 2).cast("string"))
# Rename: 0="A", 1="B" (simulate)
stratified = df3.sampleBy("cat", fractions={"0": 0.2, "1": 0.05}, seed=42)
stratified.groupBy("cat").count().show()

# Level 5: Checkpoint before/after
print("\n=== Level 5: Checkpoint effect ===")
df5 = spark.range(1000)
for i in range(20):
    df5 = df5.withColumn(f"c{i}", col("id") + i)
print(f"  Before checkpoint — lineage lines: ~{df5.rdd.toDebugString().count(b'\n')}")

df5_cp = df5.localCheckpoint()
print(f"  After checkpoint — lineage lines: ~{df5_cp.rdd.toDebugString().count(b'\n')}")

# Level 10: Decision guide
print("\n=== Level 10: When to use what ===")
print("  cache():            Repeated reads, same session, fits in memory")
print("  localCheckpoint():  Cut lineage fast, acceptable risk, exploration")
print("  checkpoint():       Cut lineage reliably, production, survives failures")
print("  Write to Delta:     Permanent persistence, cross-session, audit trail")
print("                      Also cuts lineage! (Reading Delta = fresh lineage)")

print("\n\u2705 All homework solutions complete!")