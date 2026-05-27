# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Notebook 71: Predicate and Projection Pushdown
# MAGIC ## Module 11: Performance Optimization
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 40 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Two **free** performance wins that Spark gives you automatically:
# MAGIC 1. **Predicate Pushdown** = Push your WHERE filters INTO the data source so Spark reads **less data from disk**.
# MAGIC 2. **Projection Pushdown** = Read ONLY the columns you actually use (columnar formats like Parquet/Delta support this natively).
# MAGIC
# MAGIC These are the **easiest** performance gains — they happen automatically unless your code accidentally blocks them.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC **Predicate Pushdown**: You're ordering from a warehouse. Instead of ordering ALL 10,000 items and throwing away 9,900, you tell the warehouse: "Only ship the 100 items matching my criteria." Less shipping, less handling.
# MAGIC
# MAGIC **Projection Pushdown**: A spreadsheet has 50 columns but you need only 3. Instead of loading all 50 and hiding 47, you ask: "Only send me columns A, B, C." 94% less data transferred.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Predicate Pushdown:
# MAGIC
# MAGIC   WITHOUT pushdown:                     WITH pushdown:
# MAGIC   [Read ALL 100M rows from disk]        [Read only matching rows]
# MAGIC   [Transfer 100M to executors]          [Storage layer filters first]
# MAGIC   [Filter in Spark: keep 1M]            [Only 1M rows leave disk]
# MAGIC   Result: 100M rows read, 99M wasted    Result: 1M rows read, 0 wasted
# MAGIC
# MAGIC   HOW IT WORKS with Delta/Parquet:
# MAGIC   1. Spark pushes filter predicate (e.g., id > 50000) to the file reader.
# MAGIC   2. Parquet uses column statistics (min/max) to SKIP entire row groups.
# MAGIC   3. Delta adds file-level statistics to skip ENTIRE FILES.
# MAGIC   4. Result: Only matching data ever leaves storage.
# MAGIC
# MAGIC Projection Pushdown:
# MAGIC
# MAGIC   WITHOUT projection pushdown:          WITH projection pushdown:
# MAGIC   Read: [id, name, email, addr,         Read: [id, amount]
# MAGIC          phone, amount, ...]            (only 2 columns from disk)
# MAGIC   (all 50 columns from disk)            90% less I/O!
# MAGIC
# MAGIC   HOW IT WORKS:
# MAGIC   Parquet stores data column-by-column (columnar format).
# MAGIC   If you SELECT id, amount → only those 2 column chunks are read.
# MAGIC   The other 48 columns never leave disk.
# MAGIC
# MAGIC What BLOCKS pushdown:
# MAGIC   ┌─────────────────────────┬───────────────────────────────────────┐
# MAGIC   │ Blocker                 │ Fix                                   │
# MAGIC   ├─────────────────────────┼───────────────────────────────────────┤
# MAGIC   │ Python UDFs             │ Use built-in SQL functions instead     │
# MAGIC   │ Complex casts           │ Cast before filter or use native types │
# MAGIC   │ OR with different cols   │ Split into UNION if possible           │
# MAGIC   │ Non-deterministic funcs │ Avoid rand()/now() in predicates       │
# MAGIC   └─────────────────────────┴───────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Pushdown Demo
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-5 — EXAMPLES (Beginner to Advanced)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, expr, udf, upper  # Imports.
from pyspark.sql.types import BooleanType  # For UDF demo.

print("="*70)
print("SECTIONS 3-5: Predicate & Projection Pushdown")
print("="*70)

# Setup: Create a Delta table.
path = "/tmp/delta_kt/pushdown_demo"
spark.range(100000).select(
    col("id"),
    (rand() * 1000).alias("amount"),
    expr("CASE WHEN id%4=0 THEN 'A' WHEN id%4=1 THEN 'B' WHEN id%4=2 THEN 'C' ELSE 'D' END").alias("category"),
    expr("date_add('2024-01-01', cast(id%365 as int))").alias("dt")
).write.format("delta").mode("overwrite").save(path)

df = spark.read.format("delta").load(path)  # Read the table.

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Predicate pushdown (filter pushed to scan)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Predicate pushdown (filter at scan level)")
print("-"*60)

print("\nPlan for: WHERE id > 50000 AND category = 'A':")
df.filter("id > 50000 AND category = 'A'").explain(True)

print("")
print("✓ Look for 'PushedFilters: [IsNotNull(id), GreaterThan(id,50000)]'")
print("  This means the filter was pushed to the file reader!")
print("  Only matching row groups are read from disk.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Projection pushdown (only needed columns read)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Projection pushdown (column pruning)")
print("-"*60)

# Select only 2 of 4 columns.
print("\nPlan for: SELECT id, amount (only 2 of 4 columns):")
df.select("id", "amount").explain(True)

print("")
print("✓ In the FileScan, look at 'ReadSchema: struct<id:bigint,amount:double>'")
print("  Only id and amount are read from disk.")
print("  'category' and 'dt' columns never leave storage = 50% less I/O!")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: UDF BLOCKS pushdown (bad pattern)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: UDF blocks pushdown (ANTI-PATTERN)")
print("-"*60)

# Define a simple UDF.
@udf(BooleanType())
def my_filter_udf(x):
    """A UDF that Spark can't push down."""
    return x > 50000  # Same logic as col('id') > 50000.

# BAD: UDF in filter — Spark can't push it to storage.
print("\nPlan with UDF filter (NO pushdown):")
df.filter(my_filter_udf(col("id"))).explain(True)
print("")
print("✗ PushedFilters is EMPTY! UDF is a black box to Spark.")
print("  Spark reads ALL 100K rows, then applies UDF in executor.")

# GOOD: Same logic with built-in function — pushdown works.
print("\nPlan with built-in filter (WITH pushdown):")
df.filter(col("id") > 50000).explain(True)
print("✓ PushedFilters: [GreaterThan(id,50000)] — pushed to scan!")
print("")
print("Rule: ALWAYS prefer built-in functions over UDFs for filters.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Delta data skipping (file-level pushdown)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Delta data skipping")
print("-"*60)

print("""
Delta stores min/max statistics for the first 32 columns of each file.
When you query WHERE id > 90000:

  File 1: min_id=0, max_id=25000      → SKIP (no matches possible)
  File 2: min_id=25001, max_id=50000   → SKIP
  File 3: min_id=50001, max_id=75000   → SKIP
  File 4: min_id=75001, max_id=100000  → READ (might have matches)

  Result: Only 1 of 4 files read! 75% less I/O.

Boost with ZORDER or Liquid Clustering:
  OPTIMIZE '/path' ZORDER BY (category);
  → Colocates rows with same category in same files.
  → WHERE category='A' skips even more files.
""")

# Count files read with filter vs without.
all_files = len(df.inputFiles())  # Total files.
filtered_plan = df.filter("id > 90000")
print(f"Total files in table: {all_files}")
print(f"Filtered result rows: {filtered_plan.count():,}")
print("Delta skipped files where max(id) < 90000!")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Using UDFs in filters (kills pushdown)
# MAGIC ```python
# MAGIC # BAD: UDF prevents predicate pushdown.
# MAGIC @udf(BooleanType())
# MAGIC def is_premium(amount): return amount > 1000
# MAGIC df.filter(is_premium(col("amount")))  # Reads ALL data!
# MAGIC
# MAGIC # GOOD: Same logic with built-in expression.
# MAGIC df.filter(col("amount") > 1000)  # Pushed to scan, skips data!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: SELECT * when you only need 2 columns
# MAGIC ```python
# MAGIC # BAD: Reads all 50 columns from disk.
# MAGIC df.select("*").filter("id > 1000")  # Columnar benefit wasted!
# MAGIC
# MAGIC # GOOD: Select only needed columns.
# MAGIC df.select("id", "amount").filter("id > 1000")  # Reads only 2 columns.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Filtering on computed columns blocks pushdown
# MAGIC ```python
# MAGIC # BAD: Computed expression can't be pushed to storage.
# MAGIC df.filter(col("amount") * 1.1 > 500)  # Not pushable!
# MAGIC
# MAGIC # GOOD: Rearrange so raw column is compared to constant.
# MAGIC df.filter(col("amount") > 500 / 1.1)  # Equivalent, but pushable!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Not using Delta format (missing data skipping)
# MAGIC ```python
# MAGIC # CSV/JSON: No column statistics, no data skipping. Reads everything.
# MAGIC # Parquet: Row-group statistics, some skipping.
# MAGIC # Delta: File-level statistics + row-group stats = maximum skipping.
# MAGIC # Always use Delta for analytical workloads!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not checking the plan to verify pushdown
# MAGIC ```python
# MAGIC # Always verify pushdown is working:
# MAGIC df.filter("category = 'A'").explain(True)
# MAGIC # Look for: PushedFilters: [..., EqualTo(category,A)]
# MAGIC # If empty: something is blocking pushdown. Investigate!
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, udf  # Imports.
from pyspark.sql.types import BooleanType  # Types.

print("="*70)
print("HOMEWORK — Predicate & Projection Pushdown")
print("="*70)

path = "/tmp/delta_kt/pushdown_demo"  # Reuse table from above.
df = spark.read.format("delta").load(path)

# Level 1: Verify predicate pushdown.
print("\n--- Level 1: Check PushedFilters ---")
df.filter("id > 80000").explain()
print("✓ PushedFilters shows the filter was pushed down.")

# Level 2: Verify projection pushdown.
print("\n--- Level 2: Check column pruning ---")
df.select("id").explain()
print("✓ ReadSchema shows only 'id' column read.")

# Level 3: UDF blocks pushdown.
print("\n--- Level 3: UDF blocks pushdown ---")
@udf(BooleanType())
def bad_filter(x): return x > 80000
df.filter(bad_filter(col("id"))).explain()
print("✗ PushedFilters is empty = UDF blocked pushdown.")

# Level 4: Fix the UDF with built-in.
print("\n--- Level 4: Fix with built-in ---")
df.filter(col("id") > 80000).explain()
print("✓ Fixed! Built-in > gets pushed down.")

# Level 5: Combined predicate + projection.
print("\n--- Level 5: Both pushdowns together ---")
df.select("id", "amount").filter("id > 50000").explain()
print("✓ Only 2 columns read AND filter pushed to scan.")

# Level 6-10: Conceptual.
print("\n--- Level 6: What blocks pushdown? ---")
print("UDFs, complex casts, non-deterministic functions, OR across columns.")

print("\n--- Level 7: Delta data skipping ---")
print("Delta stores min/max per file. Filter skips entire files.")
print("ZORDER/Liquid Clustering improves data co-location for better skipping.")

print("\n--- Level 8: CSV vs Parquet vs Delta ---")
print("CSV: No pushdown (reads everything). Parquet: Row-group stats.")
print("Delta: File-level stats + row-group stats = best skipping.")

print("\n--- Level 9: Production checklist ---")
print("1. Always check explain() for PushedFilters.")
print("2. Replace UDFs with built-ins where possible.")
print("3. Select only needed columns (never SELECT *).")
print("4. Use Delta format for maximum data skipping.")

print("\n--- Level 10: Teach pushdown ---")
print("""
"Predicate pushdown: Tell storage to filter BEFORE sending data.
 Projection pushdown: Only read the columns you actually use.
 Both save massive I/O. They're free if you don't block them.
 Blockers: UDFs, SELECT *, complex expressions in filters.
 Always verify with explain() → look for PushedFilters."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 71")
print("="*70)