# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 91: PySpark Pandas API (pandas-on-Spark)
# MAGIC ## Module 15: PySpark Pandas & Python Integration
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC The **PySpark Pandas API** (formerly Koalas) lets you write **pandas code** that runs on Spark's distributed engine. Same familiar `df.groupby()`, `df.plot()`, `df.describe()` syntax — but instead of hitting a memory limit at 10GB, it scales to terabytes across your cluster.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine you're a chef who knows how to cook in a home kitchen (pandas). Now you're given a **commercial kitchen with 20 stoves** (Spark). The PySpark Pandas API is like having the same recipe book work in both kitchens — same instructions, but the commercial kitchen processes 20x more food in parallel.
# MAGIC
# MAGIC ### When to Use Each:
# MAGIC | API | Best For | Limits |
# MAGIC |-----|----------|--------|
# MAGIC | pandas | Small data (<10GB), exploration, prototyping | Single machine memory |
# MAGIC | PySpark Pandas API | Medium-large data (10GB-TB), pandas-style code at scale | Slightly slower for small data |
# MAGIC | PySpark DataFrame API | Very large data (TB+), production ETL, streaming | More verbose syntax |
# MAGIC
# MAGIC ### Import:
# MAGIC ```python
# MAGIC import pyspark.pandas as ps  # The PySpark Pandas API
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Architecture:
# MAGIC
# MAGIC   Your Code (pandas syntax)     PySpark Pandas API       Spark Engine
# MAGIC   ─────────────────────────     ──────────────────       ────────────
# MAGIC   ps.DataFrame(...)        →    Translates to Spark  →   Distributed
# MAGIC   df.groupby('col').mean() →    DataFrame operations →   Parallel
# MAGIC   df.plot()                →    Collects for display →   execution
# MAGIC
# MAGIC   Key Insight:
# MAGIC     A ps.DataFrame IS a Spark DataFrame underneath.
# MAGIC     It just exposes the pandas API on top.
# MAGIC
# MAGIC Conversion Between APIs:
# MAGIC
# MAGIC   # pandas → PySpark Pandas
# MAGIC   ps_df = ps.from_pandas(pandas_df)
# MAGIC
# MAGIC   # PySpark Pandas → pandas (collects to driver!)
# MAGIC   pandas_df = ps_df.to_pandas()
# MAGIC
# MAGIC   # PySpark Pandas → Spark DataFrame
# MAGIC   spark_df = ps_df.to_spark()
# MAGIC
# MAGIC   # Spark DataFrame → PySpark Pandas
# MAGIC   ps_df = ps.DataFrame(spark_df)  # or spark_df.pandas_api()
# MAGIC
# MAGIC What Works (same as pandas):
# MAGIC   ✓ df['col'], df.col          (column access)
# MAGIC   ✓ df.groupby().agg()         (aggregations)
# MAGIC   ✓ df.merge(), df.join()      (joins)
# MAGIC   ✓ df.sort_values()           (sorting)
# MAGIC   ✓ df.describe()              (statistics)
# MAGIC   ✓ df.plot()                  (visualization)
# MAGIC   ✓ df.apply(func)             (row/column apply)
# MAGIC   ✓ df.fillna(), df.dropna()   (missing values)
# MAGIC
# MAGIC What's Different:
# MAGIC   ✗ Index works differently (Spark has no natural row order)
# MAGIC   ✗ In-place operations limited
# MAGIC   ✗ Some pandas functions not yet implemented
# MAGIC   ✗ Performance: some ops trigger shuffles (sort, groupby)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: Beginner Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

import pyspark.pandas as ps  # PySpark Pandas API.
from pyspark.sql.functions import col  # For Spark conversions.

print("="*70)
print("SECTION 3 — BEGINNER: PySpark Pandas Basics")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Creating a PySpark Pandas DataFrame
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Creating DataFrames")
print("-"*60)

# From a dictionary (just like pandas!).
ps_df = ps.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
    'age': [25, 30, 35, 28, 32],
    'department': ['Engineering', 'Marketing', 'Engineering', 'Sales', 'Marketing'],
    'salary': [75000, 65000, 85000, 60000, 70000]
})

print("\nPySpark Pandas DataFrame:")
print(ps_df)  # Displays like pandas!
print(f"\nType: {type(ps_df)}")
print(f"Shape: {ps_df.shape}")  # (rows, cols) just like pandas.
print(f"Dtypes:\n{ps_df.dtypes}")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: pandas-style operations
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Familiar pandas operations")
print("-"*60)

# Column access (same as pandas).
print("\nColumn access:")
print(ps_df['name'].head(3))  # .head() works!

# Filtering (same syntax).
print("\nFilter salary > 70000:")
high_salary = ps_df[ps_df['salary'] > 70000]
print(high_salary)

# New column.
ps_df['bonus'] = ps_df['salary'] * 0.1  # Vectorized operation.
print("\nWith bonus column:")
print(ps_df[['name', 'salary', 'bonus']])

# Descriptive statistics.
print("\n.describe():")
print(ps_df.describe())

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: GroupBy and Aggregation
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: GroupBy (same as pandas!)")
print("-"*60)

# GroupBy + multiple aggregations.
print("\nGroup by department:")
grouped = ps_df.groupby('department').agg({
    'salary': ['mean', 'max', 'count'],
    'age': 'mean'
})
print(grouped)

# Value counts.
print("\nDepartment value counts:")
print(ps_df['department'].value_counts())

# Sort.
print("\nSorted by salary (descending):")
print(ps_df.sort_values('salary', ascending=False)[['name', 'salary']])

# COMMAND ----------

# DBTITLE 1,Sections 4-5: Intermediate and Advanced
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4-5 — INTERMEDIATE & ADVANCED
# ═══════════════════════════════════════════════════════════════════

import pyspark.pandas as ps  # PySpark Pandas.
import pandas as pd  # Regular pandas for comparison.
import numpy as np  # NumPy.

print("="*70)
print("SECTIONS 4-5: Intermediate & Advanced PySpark Pandas")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Converting between Spark, pandas, and ps DataFrames
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Conversions between APIs")
print("-"*60)

# Start with Spark DataFrame.
spark_df = spark.range(1000).selectExpr("id", "id * 2 as doubled", "id % 5 as group")

# Spark → PySpark Pandas.
ps_from_spark = spark_df.pandas_api()  # Zero-copy conversion!
print(f"Spark → PySpark Pandas: type={type(ps_from_spark)}, shape={ps_from_spark.shape}")

# PySpark Pandas → Spark DataFrame.
back_to_spark = ps_from_spark.to_spark()  # Back to Spark.
print(f"PySpark Pandas → Spark: type={type(back_to_spark)}, count={back_to_spark.count()}")

# PySpark Pandas → regular pandas (CAUTION: collects to driver!).
small_ps = ps_from_spark.head(10)  # Take small subset first!
pandas_local = small_ps.to_pandas()  # Now safe to collect.
print(f"PySpark Pandas → pandas: type={type(pandas_local)}, shape={pandas_local.shape}")

# Regular pandas → PySpark Pandas.
pd_df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
ps_from_pd = ps.from_pandas(pd_df)  # Distributes to Spark.
print(f"pandas → PySpark Pandas: type={type(ps_from_pd)}")

print("")
print("Conversion summary:")
print("  spark_df.pandas_api()    → PySpark Pandas (fast, zero-copy)")
print("  ps_df.to_spark()         → Spark DataFrame (fast)")
print("  ps_df.to_pandas()        → pandas (COLLECTS! only for small data)")
print("  ps.from_pandas(pd_df)    → PySpark Pandas (distributes)")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: Working with large data (millions of rows)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: Scaling to millions of rows")
print("-"*60)

import time

# Create 5 million rows (would be slow in pandas, fast in ps).
ps_large = spark.range(5000000).selectExpr(
    "id",
    "rand() * 1000 as revenue",
    "id % 100 as customer_id",
    "id % 12 + 1 as month"
).pandas_api()

print(f"DataFrame shape: {ps_large.shape} (5M rows!)")

# GroupBy on 5M rows (runs distributed!).
start = time.time()
result = ps_large.groupby('customer_id')['revenue'].agg(['sum', 'mean', 'count'])
elapsed = time.time() - start
print(f"\nGroupBy on 5M rows: {elapsed:.2f}s")
print(f"Result shape: {result.shape}")
print(result.head(5))

# This would be MUCH slower (or OOM) with regular pandas!
print("\n✓ Same pandas syntax, but running on Spark across the cluster.")
print("  Regular pandas would need 5M rows in driver memory.")
print("  PySpark Pandas distributes the work.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Apply functions (UDF-like)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: apply() and map() functions")
print("-"*60)

ps_df2 = ps.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'score': [85, 92, 78],
    'grade_str': ['B', 'A', 'C']
})

# apply on column (element-wise).
ps_df2['grade'] = ps_df2['score'].apply(lambda x: 'Pass' if x >= 80 else 'Fail')
print("\nApply (element-wise):")
print(ps_df2)

# map (rename values).
grade_map = {'A': 'Excellent', 'B': 'Good', 'C': 'Average'}
ps_df2['grade_label'] = ps_df2['grade_str'].map(grade_map)
print("\nMap (value replacement):")
print(ps_df2)

print("\n⚠️  apply() runs as a pandas UDF under the hood (slower than vectorized).")
print("  Prefer vectorized operations when possible:")
print("  Good: ps_df['x'] * 2  (vectorized, fast)")
print("  Slow: ps_df['x'].apply(lambda x: x*2)  (row-by-row, slow)")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Calling .to_pandas() on a large DataFrame
# MAGIC ```python
# MAGIC # BAD: Collects 100M rows to driver memory → OOM!
# MAGIC big_df = spark.range(100000000).pandas_api()
# MAGIC result = big_df.to_pandas()  # CRASH!
# MAGIC
# MAGIC # GOOD: Aggregate first, then collect small result.
# MAGIC result = big_df.groupby('key')['val'].mean()  # Distributed aggregation.
# MAGIC small_result = result.to_pandas()  # Only collect the summary.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Using apply() when vectorized operations exist
# MAGIC ```python
# MAGIC # BAD: Row-by-row apply (triggers slow pandas UDF).
# MAGIC df['doubled'] = df['value'].apply(lambda x: x * 2)  # Slow!
# MAGIC
# MAGIC # GOOD: Vectorized operation (runs natively in Spark).
# MAGIC df['doubled'] = df['value'] * 2  # Fast!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Expecting pandas-identical index behavior
# MAGIC ```python
# MAGIC # PySpark Pandas doesn't preserve row order like pandas.
# MAGIC # BAD: Relying on positional indexing.
# MAGIC row = df.iloc[5]  # Works but requires expensive sorting!
# MAGIC
# MAGIC # GOOD: Use column-based filtering.
# MAGIC row = df[df['id'] == 5]  # Fast, distributed.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Not using .pandas_api() for Spark→ps conversion
# MAGIC ```python
# MAGIC # BAD: Creating ps.DataFrame from scratch when you already have Spark DF.
# MAGIC ps_df = ps.DataFrame(spark_df.toPandas())  # Collects first! Defeats purpose!
# MAGIC
# MAGIC # GOOD: Zero-copy conversion.
# MAGIC ps_df = spark_df.pandas_api()  # No data movement!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Mixing ps and Spark operations incorrectly
# MAGIC ```python
# MAGIC # BAD: Can't use Spark functions on ps DataFrame directly.
# MAGIC from pyspark.sql.functions import col
# MAGIC ps_df.select(col('x'))  # ERROR! ps doesn't have .select()
# MAGIC
# MAGIC # GOOD: Convert to Spark first if you need Spark API.
# MAGIC spark_df = ps_df.to_spark()
# MAGIC spark_df.select(col('x'))  # Works!
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

import pyspark.pandas as ps  # Import.

print("="*70)
print("HOMEWORK — PySpark Pandas API")
print("="*70)

# Level 1: Create a ps DataFrame.
print("\n--- Level 1: Create DataFrame ---")
df1 = ps.DataFrame({'a': [1,2,3,4,5], 'b': [10,20,30,40,50]})
print(f"Shape: {df1.shape}, Sum of 'a': {df1['a'].sum()}")
# WHY: ps.DataFrame works just like pd.DataFrame.

# Level 2: Filter and select.
print("\n--- Level 2: Filter ---")
filtered = df1[df1['a'] > 2][['a', 'b']]
print(filtered)
# WHY: Boolean indexing works identically to pandas.

# Level 3: GroupBy.
print("\n--- Level 3: GroupBy ---")
df3 = ps.DataFrame({'cat': ['A','B','A','B','A'], 'val': [10,20,30,40,50]})
print(df3.groupby('cat')['val'].mean())
# WHY: groupby().agg() is distributed across the cluster.

# Level 4: Convert Spark ↔ PySpark Pandas.
print("\n--- Level 4: Conversions ---")
spark_df = spark.range(100).selectExpr("id", "id*2 as doubled")
ps_df = spark_df.pandas_api()  # Spark → ps.
print(f"ps shape: {ps_df.shape}")
back = ps_df.to_spark()  # ps → Spark.
print(f"Back to Spark: {back.count()} rows")
# WHY: .pandas_api() is zero-copy. .to_spark() is also instant.

# Level 5: Descriptive stats.
print("\n--- Level 5: describe() ---")
print(df1.describe())
# WHY: .describe() gives count, mean, std, min, max like pandas.

# Levels 6-10: Conceptual.
print("\n--- Level 6: When to use ps vs pandas vs Spark DF ---")
print("pandas: <10GB, prototyping. ps: 10GB-TB, pandas syntax at scale.")
print("Spark DF: TB+, production ETL, streaming.")

print("\n--- Level 7: Performance tips ---")
print("1. Use vectorized ops (df['x']*2) not apply().")
print("2. Don't call .to_pandas() on large data.")
print("3. Use .pandas_api() for Spark↔ps (zero-copy).")

print("\n--- Level 8: Plotting ---")
print("ps_df['col'].plot.hist()  → works like pandas plotting!")
print("ps_df.plot.scatter(x='a', y='b')  → scatter plot.")

print("\n--- Level 9: Missing values ---")
print("ps_df.fillna(0), ps_df.dropna(), ps_df.isna().sum()")
print("All work identically to pandas.")

print("\n--- Level 10: Teach PySpark Pandas ---")
print("""
"PySpark Pandas = pandas API running on Spark engine.
  import pyspark.pandas as ps
  Same syntax: df.groupby(), df.describe(), df.merge().
  Scales to TB (pandas limited to driver memory).
  Convert: spark_df.pandas_api() ↔ ps_df.to_spark().
  Avoid: .to_pandas() on large data, .apply() when vectorized works."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 91")
print("="*70)