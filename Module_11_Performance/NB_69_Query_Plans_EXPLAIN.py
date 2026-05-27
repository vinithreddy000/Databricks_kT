# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Notebook 69: Reading Query Plans — EXPLAIN Deep Dive
# MAGIC ## Module 11: Performance Optimization
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **EXPLAIN** shows you the exact plan Spark will use to execute your query — **before it actually runs**. It's like a GPS showing the route before you start driving. Learning to read query plans is the **#1 skill** for performance tuning.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC You're planning a road trip from Berlin to Paris:
# MAGIC - **Logical Plan** = "I need to go from Berlin to Paris" (the WHAT)
# MAGIC - **Optimized Plan** = "Take the highway via Cologne" (the HOW, after optimization)
# MAGIC - **Physical Plan** = "Take A2 to Dortmund, merge onto A1, exit 23 to Cologne, take A4 to Paris" (the exact step-by-step)
# MAGIC
# MAGIC Spark does the same: it takes your query (logical) and creates an optimized physical execution plan. EXPLAIN shows you ALL these plans.
# MAGIC
# MAGIC ### Key Operators to Recognize:
# MAGIC | Operator | Meaning | Performance Impact |
# MAGIC |----------|---------|-------------------|
# MAGIC | `FileScan` | Reading from files | Check PushedFilters! |
# MAGIC | `Filter` | Filtering rows | Narrow (good) |
# MAGIC | `Project` | Selecting columns | Narrow (good) |
# MAGIC | `Exchange` | **SHUFFLE!** | Expensive (network I/O) |
# MAGIC | `HashAggregate` | Aggregation | Usually efficient |
# MAGIC | `BroadcastHashJoin` | Join with broadcast | No shuffle (good!) |
# MAGIC | `SortMergeJoin` | Standard join | Requires shuffle (expensive) |
# MAGIC | `Sort` | Sorting data | May cause shuffle |
# MAGIC | `CartesianProduct` | Cross join | **DANGER!** N×M rows |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Spark Query Optimization Pipeline:
# MAGIC
# MAGIC   Your Code            Catalyst Optimizer              Execution
# MAGIC   ─────────            ──────────────────              ─────────
# MAGIC   df.filter(...)  →  [Parsed Logical Plan]  →  [Actual Execution]
# MAGIC   .groupBy(...)      [Analyzed Logical Plan]     on cluster
# MAGIC   .agg(...)          [Optimized Logical Plan]
# MAGIC   .orderBy(...)      [Physical Plan] ← THIS is what explain() shows!
# MAGIC
# MAGIC The 4 Plans (shown by explain(True)):
# MAGIC
# MAGIC   1. PARSED LOGICAL PLAN   — Raw translation of your code.
# MAGIC   2. ANALYZED LOGICAL PLAN — With column types resolved.
# MAGIC   3. OPTIMIZED LOGICAL PLAN— After Catalyst optimizations:
# MAGIC        • Predicate pushdown (push filters closer to source)
# MAGIC        • Column pruning (don't read unused columns)
# MAGIC        • Constant folding (pre-compute constants)
# MAGIC   4. PHYSICAL PLAN         — The actual execution steps:
# MAGIC        • Which join algorithm (Broadcast vs SortMerge)
# MAGIC        • Where shuffles happen (Exchange nodes)
# MAGIC        • Scan details (PushedFilters, file format)
# MAGIC
# MAGIC Reading the Physical Plan (bottom to top):
# MAGIC
# MAGIC   == Physical Plan ==
# MAGIC   *(3) Sort [revenue DESC]                 ← Step 5: Final sort
# MAGIC   +- Exchange rangepartitioning(revenue)    ← Step 4: SHUFFLE for sort
# MAGIC      +- *(2) HashAggregate(keys=[cat])      ← Step 3: Final aggregate
# MAGIC         +- Exchange hashpartitioning(cat)    ← Step 2: SHUFFLE for groupBy
# MAGIC            +- *(1) HashAggregate(partial)   ← Step 1b: Partial aggregate
# MAGIC               +- *(1) Filter (amount > 100) ← Step 1a: Filter
# MAGIC                  +- *(1) FileScan parquet   ← Step 0: Read files
# MAGIC
# MAGIC   Read BOTTOM-UP: data flows from FileScan upward to final Sort.
# MAGIC   Count 'Exchange' = number of shuffles = performance cost.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,EXPLAIN Demo
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, count, avg, broadcast, expr  # Imports.

print("="*70)
print("SECTION 3 — BEGINNER EXAMPLES: Reading Query Plans")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: explain() — the physical plan only (quickest view)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: .explain() shows the physical execution plan")
print("-"*60)

# Create test data.
df = spark.range(100000).select(
    col("id"),
    (rand() * 10).cast("int").alias("category"),  # Random 0-9.
    (rand() * 1000).alias("amount")               # Random 0-1000.
)

# Simple query: filter then aggregate.
query = df.filter(col("amount") > 500).groupBy("category").agg(count("*").alias("cnt"))

print("\nPhysical Plan:")
query.explain()  # Shows ONLY the physical plan.

print("")
print("How to read this (bottom to top):")
print("  1. FileScan / Range → generates the initial data")
print("  2. Filter (amount > 500) → narrow operation, no shuffle")
print("  3. HashAggregate (partial) → partial count per partition")
print("  4. Exchange hashpartitioning → SHUFFLE by category")
print("  5. HashAggregate (final) → combine partial counts")
print("")
print("  Key: 'Exchange' = SHUFFLE. Count them to know your query's cost.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: explain(True) — all 4 plans
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: explain(True) shows ALL 4 plans")
print("-"*60)

# Simple filter query.
print("\nAll plans for: df.filter(amount > 500):")
df.filter(col("amount") > 500).explain(True)  # Shows parsed, analyzed, optimized, physical.

print("")
print("The 4 plans you see:")
print("  1. == Parsed Logical Plan ==     → raw code translation")
print("  2. == Analyzed Logical Plan ==   → types resolved")
print("  3. == Optimized Logical Plan ==  → after Catalyst optimizations")
print("  4. == Physical Plan ==           → actual execution steps")
print("")
print("  For performance tuning, focus on the PHYSICAL plan (last one).")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: explain("formatted") — readable, structured output
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: explain('formatted') — human-readable layout")
print("-"*60)

print("\nFormatted plan for groupBy + avg:")
df.groupBy("category").agg(avg("amount").alias("avg_amt")).explain("formatted")

print("")
print("The 'formatted' mode shows:")
print("  - Numbered steps (easier to follow)")
print("  - Input/Output columns for each step")
print("  - Details like partition expressions")
print("")
print("  explain() modes:")
print("    explain()             → physical plan only (quick check)")
print("    explain(True)         → all 4 plans (deep analysis)")
print("    explain('formatted')  → physical plan, nicely formatted")
print("    explain('extended')   → same as explain(True)")
print("    explain('codegen')    → generated Java code (advanced)")
print("    explain('cost')       → with estimated costs")

# COMMAND ----------

# DBTITLE 1,Section 4-5: Intermediate and Advanced
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4 & 5 — INTERMEDIATE & ADVANCED EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, count, avg, broadcast, expr, sum as spark_sum  # Imports.

print("="*70)
print("SECTIONS 4-5: Identifying Good and Bad Plans")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: BroadcastHashJoin vs SortMergeJoin in plans
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Good plan (BroadcastHashJoin) vs Bad plan (SortMergeJoin)")
print("-"*60)

large_tbl = spark.range(100000).select(col("id"), (col("id")%50).alias("key"), rand().alias("val"))
small_tbl = spark.range(50).select(col("id").alias("key"), rand().alias("lookup"))

# GOOD: Broadcast join (no shuffle).
print("\n✓ GOOD PLAN (BroadcastHashJoin):")
large_tbl.join(broadcast(small_tbl), "key").explain()
print("  → No 'Exchange' for the join! BroadcastHashJoin = no shuffle.")

# BAD: Sort-merge join (both sides shuffled).
print("\n✗ LESS OPTIMAL PLAN (SortMergeJoin):")
# Disable auto-broadcast to force SortMergeJoin.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable auto-broadcast.
large_tbl.join(small_tbl, "key").explain()
print("  → Two 'Exchange' nodes = BOTH tables shuffled (expensive).")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # Reset to 10MB.

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: Filter pushdown (good sign in plans)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: Predicate pushdown (filter applied at scan level)")
print("-"*60)

# Write test data to Delta.
path = "/tmp/delta_kt/explain_pushdown_demo"
spark.range(10000).select(col("id"), (rand()*100).alias("score")) \
    .write.format("delta").mode("overwrite").save(path)

# Query with pushdown.
print("\nPlan with filter (look for PushedFilters):")
spark.read.format("delta").load(path).filter("id > 5000").explain(True)

print("")
print("What to look for in FileScan:")
print("  PushedFilters: [IsNotNull(id), GreaterThan(id,5000)]")
print("  → Filter was pushed DOWN to the file scan level!")
print("  → Spark skips reading rows that don't match (less I/O).")
print("")
print("  If PushedFilters is empty when you have a filter → problem!")
print("  Common cause: UDFs block predicate pushdown.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Comparing plans before and after optimization
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Using explain to compare query approaches")
print("-"*60)

df = spark.range(200000).select(
    col("id"),
    (rand() * 10).cast("int").alias("dept"),
    (rand() * 100000).alias("salary")
)

# Approach A: filter after groupBy (filter on aggregate result).
print("\nApproach A plan (groupBy then filter on result):")
a = df.groupBy("dept").agg(avg("salary").alias("avg_sal")).filter(col("avg_sal") > 50000)
a.explain()

# Approach B: filter before groupBy (less data to shuffle).
print("\nApproach B plan (filter raw data, then groupBy):")
b = df.filter(col("salary") > 50000).groupBy("dept").agg(avg("salary").alias("avg_sal"))
b.explain()

print("")
print("Compare the plans:")
print("  A: Filter is AFTER Exchange (shuffle processes all data)")
print("  B: Filter is BEFORE Exchange (shuffle processes less data)")
print("  B is better when you can push filters before the shuffle.")
print("  (Note: Catalyst may optimize A=B in some cases.)")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 7: Dangerous plan pattern — CartesianProduct
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 7: DANGER — CartesianProduct in explain output")
print("-"*60)

df_a = spark.range(100)  # 100 rows.
df_b = spark.range(100)  # 100 rows.

# Cross join: 100 × 100 = 10,000 rows. With big tables this EXPLODES.
print("\nCross join plan (CartesianProduct):")
df_a.crossJoin(df_b).explain()

print("")
print("⚠️  'CartesianProduct' in your plan = DANGER!")
print("  100 rows × 100 rows = 10K rows (fine).")
print("  1M rows × 1M rows = 1 TRILLION rows (cluster dies!).")
print("")
print("  If you see CartesianProduct unexpectedly, you probably:")
print("  1. Forgot the join condition: df1.join(df2)  ← no 'on' clause!")
print("  2. Used a non-equi join without realizing it creates cross join.")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Never checking explain() before running expensive queries
# MAGIC ```python
# MAGIC # BAD: Run a 2-hour query, then wonder why it's slow.
# MAGIC result = massive_query.collect()  # 2 hours later... what happened?
# MAGIC
# MAGIC # GOOD: Check explain() FIRST (free, instant, no execution).
# MAGIC massive_query.explain()  # See the plan. Count shuffles. Spot problems.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Not reading the plan bottom-to-top
# MAGIC ```
# MAGIC The physical plan reads BOTTOM to TOP:
# MAGIC   Top = final output
# MAGIC   Bottom = data source (FileScan)
# MAGIC
# MAGIC Data flows UPWARD through each operator.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Confusing logical and physical plans
# MAGIC ```python
# MAGIC # Logical plan shows WHAT you asked for.
# MAGIC # Physical plan shows HOW Spark will do it.
# MAGIC # Always look at the Physical Plan for performance analysis.
# MAGIC df.explain()       # Shows physical plan (this is what you need).
# MAGIC df.explain(True)   # Shows all 4 (logical + physical).
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Ignoring missing PushedFilters
# MAGIC ```python
# MAGIC # If your filter isn't pushed down, you're reading unnecessary data.
# MAGIC # Common causes of blocked pushdown:
# MAGIC #   1. UDF in filter: df.filter(my_udf(col) > 5)  → can't push UDF to storage.
# MAGIC #   2. Complex expressions: some aren't pushable.
# MAGIC #   3. Non-supported predicates for the file format.
# MAGIC # Fix: Use built-in functions instead of UDFs when possible.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not comparing plans when optimizing
# MAGIC ```python
# MAGIC # When trying to improve a query:
# MAGIC # 1. Get the BEFORE plan: slow_query.explain()
# MAGIC # 2. Make your optimization
# MAGIC # 3. Get the AFTER plan: optimized_query.explain()
# MAGIC # 4. Compare: fewer Exchange nodes? Better join type? Pushdown working?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, count, avg, broadcast  # Imports.

print("="*70)
print("HOMEWORK — Query Plans (EXPLAIN)")
print("="*70)

# Level 1: Run explain() on a simple query.
print("\n--- Level 1: Basic explain ---")
df = spark.range(10000).select(col("id"), (col("id")%5).alias("g"), rand().alias("v"))
df.filter(col("v") > 0.5).explain()  # No Exchange (just filter).
print("✓ No Exchange = no shuffle. Simple filter is a narrow operation.")

# Level 2: Count Exchange nodes.
print("\n--- Level 2: Count shuffles ---")
df.groupBy("g").count().orderBy("g").explain()
print("✓ 2 Exchange nodes = 2 shuffles (groupBy + orderBy).")

# Level 3: Use explain(True) to see all plans.
print("\n--- Level 3: All 4 plans ---")
df.filter(col("v") > 0.5).explain(True)
print("✓ Shows Parsed → Analyzed → Optimized → Physical.")

# Level 4: Spot the BroadcastHashJoin.
print("\n--- Level 4: Identify join type ---")
small = spark.range(5).select(col("id").alias("g"), rand().alias("info"))
df.join(broadcast(small), "g").explain()
print("✓ BroadcastHashJoin confirmed (no Exchange for join).")

# Level 5: Force SortMergeJoin and compare.
print("\n--- Level 5: Force SortMergeJoin ---")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
df.join(small, "g").explain()  # SortMergeJoin with Exchange.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
print("✓ SortMergeJoin has Exchange nodes = more expensive.")

# Level 6: Detect CartesianProduct.
print("\n--- Level 6: Detect dangerous cross join ---")
spark.range(5).crossJoin(spark.range(5)).explain()
print("✓ CartesianProduct detected. Avoid on large tables!")

# Level 7: Use formatted explain.
print("\n--- Level 7: Formatted plan ---")
df.groupBy("g").agg(avg("v")).explain("formatted")
print("✓ Formatted output is easier to read for complex queries.")

# Level 8: Check for PushedFilters.
print("\n--- Level 8: Verify pushdown ---")
path = "/tmp/delta_kt/explain_pushdown_demo"
spark.read.format("delta").load(path).filter("id > 5000").explain()
print("✓ PushedFilters should show the id > 5000 condition.")

# Level 9: Compare two approaches.
print("\n--- Level 9: Compare query plans ---")
print("Plan A (groupBy + filter result):")
df.groupBy("g").agg(avg("v").alias("a")).filter("a > 0.5").explain()
print("\nPlan B (filter data + groupBy):")
df.filter(col("v") > 0.5).groupBy("g").agg(avg("v")).explain()
print("✓ Plan B shuffles less data (filter before shuffle).")

# Level 10: Teach explain to a colleague.
print("\n--- Level 10: Teach it ---")
print("""
"explain() shows Spark's execution plan WITHOUT running the query.
 Read the physical plan bottom-to-top.
 Count 'Exchange' nodes = number of shuffles = performance cost.
 Look for: BroadcastHashJoin (good), PushedFilters (good),
           CartesianProduct (bad), many Exchange nodes (bad).
 Always compare plans before and after optimization."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 69")
print("="*70)