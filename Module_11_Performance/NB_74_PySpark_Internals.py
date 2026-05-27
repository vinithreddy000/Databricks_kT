# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Notebook 74: PySpark Internals — Catalyst, Tungsten, Codegen
# MAGIC ## Module 11: Performance Optimization
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Spark's internals are what make DataFrames **10-100x faster** than RDDs. Three key engines work together:
# MAGIC 1. **Catalyst Optimizer** — Rewrites your query for maximum efficiency (like a SQL optimizer)
# MAGIC 2. **Tungsten Engine** — Manages memory directly (off-heap) and processes data in batches
# MAGIC 3. **Whole-Stage Codegen** — Compiles multiple operators into a single optimized loop
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC **Without optimization (RDD)**: You're a chef who follows the recipe exactly as written, step by step, looking up each instruction one at a time. Every step involves opening and closing the recipe book.
# MAGIC
# MAGIC **With optimization (DataFrame + Catalyst + Tungsten + Codegen)**: A food scientist redesigns your recipe: reorders steps for efficiency, pre-measures all ingredients (no lookups during cooking), combines compatible steps into single motions, and even uses industrial equipment instead of hand tools.
# MAGIC
# MAGIC Same dish. Same ingredients. 100x faster.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC The Catalyst Optimizer (4 Phases):
# MAGIC
# MAGIC   Your Code → [1. Analysis] → [2. Logical Optimization] → [3. Physical Planning] → [4. Code Generation]
# MAGIC               Resolve names    Push filters down           Choose algorithms      Compile to bytecode
# MAGIC               Check types      Eliminate redundancy        Pick join strategies    Fuse operators
# MAGIC               Verify tables    Fold constants              Generate candidates     Vectorize
# MAGIC
# MAGIC   Phase 1 - Analysis:
# MAGIC     df.filter("amount > 100")  →  Verify 'amount' exists, check it's numeric.
# MAGIC
# MAGIC   Phase 2 - Logical Optimization:
# MAGIC     SELECT * then filter  →  Filter first, then project (less data moved).
# MAGIC     1 + 2 + 3             →  Replaced with literal 6 (constant folding).
# MAGIC     filter(x>5).filter(x<10) → Combined: filter(x>5 AND x<10).
# MAGIC
# MAGIC   Phase 3 - Physical Planning:
# MAGIC     Join small table?     →  Choose BroadcastHashJoin.
# MAGIC     Join large tables?    →  Choose SortMergeJoin.
# MAGIC     Generate multiple plans, estimate cost, pick cheapest.
# MAGIC
# MAGIC   Phase 4 - Code Generation (Tungsten):
# MAGIC     Multiple operators   →  Single tight loop (no virtual dispatch).
# MAGIC     Row-at-a-time        →  Batch processing (vectorized).
# MAGIC     JVM objects          →  Raw memory (off-heap, no GC).
# MAGIC
# MAGIC Why DataFrames >> RDDs:
# MAGIC   RDD:       rdd.filter(lambda x: x[1] > 100)  ← Opaque! Catalyst can't see inside.
# MAGIC   DataFrame: df.filter(col("amount") > 100)     ← Transparent! Catalyst optimizes fully.
# MAGIC
# MAGIC   RDD: serialize Python objects, GC pressure, no pushdown.
# MAGIC   DF:  Tungsten binary format, off-heap, full pushdown + codegen.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Internals Demo
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-5 — EXAMPLES (Beginner to Advanced)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, expr, count, avg  # Imports.
import time  # For timing comparisons.

print("="*70)
print("SECTIONS 3-5: Catalyst, Tungsten, and Codegen in Action")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Catalyst reorders operations automatically
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Catalyst reorders for efficiency")
print("-"*60)

df = spark.range(1000000).select(
    col("id"),
    (rand() * 100).alias("a"),
    (rand() * 100).alias("b")
)

# You write: select all → filter → select two.
# Catalyst optimizes to: filter first → project only needed columns.
result = df.select("id", "a", "b").filter("a > 50").select("id", "a")

print("\nYour code order: select(all) → filter(a>50) → select(id,a)")
print("Catalyst reorder: filter(a>50) → select(id,a)")
print("\nOptimized plan:")
result.explain(True)
print("✓ Filter pushed BEFORE project. Less data moves through pipeline.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Constant folding
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Constant folding (pre-compute at plan time)")
print("-"*60)

# 1+2+3 is computed at PLAN time, not at runtime for each row.
result2 = df.withColumn("constant", expr("1 + 2 + 3"))  # Folded to 6.
print("\nPlan for withColumn('constant', 1+2+3):")
result2.explain(True)
print("✓ Catalyst pre-computes 1+2+3=6. Runtime sees literal 6, not addition.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Whole-Stage Code Generation
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Whole-Stage Code Generation (WholeStageCodegen)")
print("-"*60)

result3 = df.filter("a > 50").groupBy((col("id") % 10).alias("grp")).agg(count("*"))
print("\nPlan with WholeStageCodegen:")
result3.explain()

print("")
print("Look for: *(1), *(2), *(3) prefixes in the plan.")
print("  *(1) = WholeStageCodegen stage 1.")
print("  Multiple operators inside *(1) are FUSED into one loop.")
print("  Instead of: for each row { filter(row); project(row); aggregate(row); }")
print("  Codegen:    for each row { single_fused_operation(row); }")
print("  This eliminates virtual dispatch overhead between operators.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: DataFrame vs RDD performance comparison
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: DataFrame vs RDD speed")
print("-"*60)

n = 2000000  # 2M rows.

# DataFrame approach (Catalyst + Tungsten + Codegen).
start = time.time()
df_result = spark.range(n).select(col("id"), (col("id") * 2).alias("doubled")) \
    .filter(col("doubled") > n) \
    .groupBy((col("id") % 10).alias("grp")).count().collect()
df_time = time.time() - start

# RDD approach (no Catalyst, no Tungsten, no Codegen).
start = time.time()
rdd_result = spark.sparkContext.range(n) \
    .map(lambda x: (x, x * 2)) \
    .filter(lambda x: x[1] > n) \
    .map(lambda x: (x[0] % 10, 1)) \
    .reduceByKey(lambda a, b: a + b).collect()
rdd_time = time.time() - start

print(f"\n  DataFrame: {df_time:.2f}s (Catalyst + Tungsten + Codegen)")
print(f"  RDD:       {rdd_time:.2f}s (raw Python lambdas)")
print(f"  Speedup:   {rdd_time/max(df_time,0.01):.1f}x faster with DataFrame")
print("")
print("Why DataFrame wins:")
print("  1. Catalyst pushes filter before groupBy")
print("  2. Tungsten uses off-heap memory (no GC)")
print("  3. Codegen fuses filter+project+aggregate")
print("  4. No Python serialization overhead")
print("")
print("👉 Rule: ALWAYS use DataFrames/SQL. Only use RDDs when absolutely necessary.")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Using RDDs when DataFrames would work
# MAGIC ```python
# MAGIC # BAD: Python lambdas bypass ALL Spark optimizations.
# MAGIC rdd.filter(lambda x: x[1] > 100).map(lambda x: (x[0], x[1]*2))
# MAGIC
# MAGIC # GOOD: Column expressions let Catalyst optimize fully.
# MAGIC df.filter(col("amount") > 100).withColumn("doubled", col("amount") * 2)
# MAGIC ```
# MAGIC **Why**: RDD lambdas are opaque to Catalyst. No pushdown, no codegen, no Tungsten.
# MAGIC
# MAGIC ### Mistake 2: Using Python UDFs instead of built-in functions
# MAGIC ```python
# MAGIC # BAD: Python UDF = serialize to Python, execute, serialize back. Slow!
# MAGIC @udf("int")
# MAGIC def double_it(x): return x * 2
# MAGIC df.withColumn("d", double_it(col("v")))  # 10-100x slower!
# MAGIC
# MAGIC # GOOD: Built-in function runs in JVM with codegen.
# MAGIC df.withColumn("d", col("v") * 2)  # Full Tungsten speed.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Not understanding that .explain() is free
# MAGIC ```python
# MAGIC # explain() does NOT execute the query. It just shows the plan.
# MAGIC # Use it liberally to understand what Spark will do.
# MAGIC complex_query.explain()  # Free! No data processed.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Thinking more code = more work for Spark
# MAGIC ```python
# MAGIC # WRONG assumption: "This has 10 transformations, it must be slow."
# MAGIC result = df.filter(...).select(...).withColumn(...).filter(...) # ...
# MAGIC
# MAGIC # REALITY: Catalyst fuses/reorders everything into a minimal plan.
# MAGIC # 10 transformations might become just 2 physical operations.
# MAGIC # Always check explain() — the plan is what matters, not your code structure.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Fighting Catalyst (forcing suboptimal plans)
# MAGIC ```python
# MAGIC # BAD: Forcing a specific join order when Catalyst knows better.
# MAGIC df_a.hint("MERGE").join(df_b, "key")  # Forces SortMergeJoin even if broadcast is better.
# MAGIC
# MAGIC # GOOD: Let Catalyst choose. Only add hints when you KNOW better.
# MAGIC df_a.join(df_b, "key")  # Catalyst picks optimal strategy.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, expr, count  # Imports.
import time  # Timing.

print("="*70)
print("HOMEWORK — PySpark Internals")
print("="*70)

# Level 1: See Catalyst reorder.
print("\n--- Level 1: Catalyst reorders operations ---")
df = spark.range(10000).select(col("id"), rand().alias("v"))
# You write select then filter; Catalyst pushes filter first.
df.select("id", "v").filter("v > 0.5").select("id").explain()
print("✓ Filter is before final Project in the plan.")

# Level 2: See constant folding.
print("\n--- Level 2: Constant folding ---")
df.withColumn("x", expr("10 * 10 * 10")).explain()
print("✓ 10*10*10 folded to literal 1000 at plan time.")

# Level 3: Spot WholeStageCodegen.
print("\n--- Level 3: WholeStageCodegen ---")
df.filter("v > 0.5").groupBy((col("id")%5).alias("g")).count().explain()
print("✓ *(1) and *(2) = whole-stage codegen stages.")

# Level 4: DataFrame vs RDD timing.
print("\n--- Level 4: DataFrame vs RDD speed ---")
start = time.time()
spark.range(1000000).filter(col("id") > 500000).count()
df_t = time.time() - start
start = time.time()
sc = spark.sparkContext
sc.range(1000000).filter(lambda x: x > 500000).count()
rdd_t = time.time() - start
print(f"DataFrame: {df_t:.3f}s | RDD: {rdd_t:.3f}s | DF is {rdd_t/max(df_t,0.001):.1f}x faster")

# Level 5-10: Conceptual.
print("\n--- Level 5: What does Catalyst do? ---")
print("Resolves names, pushes filters, chooses join strategies, generates code.")

print("\n--- Level 6: What does Tungsten do? ---")
print("Off-heap memory, cache-aware computation, binary serialization.")

print("\n--- Level 7: What does Codegen do? ---")
print("Fuses operators into single loop. Eliminates virtual dispatch.")

print("\n--- Level 8: Why are UDFs slow? ---")
print("UDF = serialize to Python + deserialize result. Bypasses Tungsten.")
print("Fix: Use pandas_udf (vectorized) or built-in functions.")

print("\n--- Level 9: When to use RDDs? ---")
print("Graph algorithms, custom partitioners, libraries requiring RDDs.")
print("For 95% of work: DataFrames are faster AND simpler.")

print("\n--- Level 10: Teach internals to a colleague ---")
print("""
"Spark's secret sauce: Catalyst + Tungsten + Codegen.
  Catalyst: rewrites your query for max efficiency (like a SQL optimizer).
  Tungsten: manages memory directly (no garbage collection pauses).
  Codegen: compiles operators into one tight loop (no overhead).
  This is why DataFrames are 10-100x faster than RDDs.
  Rule: Use DataFrames + built-in functions. Avoid RDDs and Python UDFs."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 74")
print("\n🎉 MODULE 11 COMPLETE! All 11 notebooks (64-74) rebuilt with full format.")
print("="*70)