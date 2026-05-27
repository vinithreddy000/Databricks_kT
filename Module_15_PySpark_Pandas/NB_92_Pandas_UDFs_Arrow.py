# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 92: Pandas UDFs & Apache Arrow
# MAGIC ## Module 15: PySpark Pandas & Python Integration
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Pandas UDFs** (vectorized UDFs) let you write custom Python functions that process data in **batches** (pandas Series/DataFrames) rather than row-by-row. They're 10-100x faster than regular Python UDFs because they use **Apache Arrow** for efficient data transfer between JVM and Python.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC - **Regular UDF** = A factory worker who picks up ONE item, processes it, puts it down, picks up the next. Very slow for millions of items.
# MAGIC - **Pandas UDF** = A factory worker who receives a TRAY of 1000 items, processes them all at once with a machine, then sends the tray back. Same worker, 1000x throughput.
# MAGIC
# MAGIC ### Types of Pandas UDFs:
# MAGIC | Type | Input → Output | Use Case |
# MAGIC |------|----------------|----------|
# MAGIC | Series → Series | pd.Series → pd.Series | Column transformation (element-wise) |
# MAGIC | Iterator of Series | Iterator[pd.Series] → Iterator[pd.Series] | Expensive init (load model once) |
# MAGIC | Series → Scalar | pd.Series → scalar | Custom aggregation (per group) |
# MAGIC | DataFrame → DataFrame | pd.DataFrame → pd.DataFrame | applyInPandas (group map) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Regular UDF vs Pandas UDF:
# MAGIC
# MAGIC   Regular Python UDF (ROW-BY-ROW):
# MAGIC     JVM ── serialize row 1 ──▶ Python ── process ──▶ JVM
# MAGIC     JVM ── serialize row 2 ──▶ Python ── process ──▶ JVM
# MAGIC     JVM ── serialize row 3 ──▶ Python ── process ──▶ JVM
# MAGIC     ... (1 million times! Huge serialization overhead.)
# MAGIC
# MAGIC   Pandas UDF (VECTORIZED, via Apache Arrow):
# MAGIC     JVM ── Arrow batch (10K rows) ──▶ Python (pandas) ── process ALL ──▶ JVM
# MAGIC     JVM ── Arrow batch (10K rows) ──▶ Python (pandas) ── process ALL ──▶ JVM
# MAGIC     ... (only 100 batches for 1M rows! Minimal overhead.)
# MAGIC
# MAGIC   Apache Arrow:
# MAGIC     - Columnar in-memory format shared between JVM and Python.
# MAGIC     - Zero-copy reads (no serialization/deserialization).
# MAGIC     - Enables vectorized numpy/pandas operations on batches.
# MAGIC
# MAGIC Code Pattern:
# MAGIC
# MAGIC   from pyspark.sql.functions import pandas_udf
# MAGIC   import pandas as pd
# MAGIC
# MAGIC   # Decorator marks it as a pandas UDF.
# MAGIC   @pandas_udf("double")  # Return type.
# MAGIC   def my_func(series: pd.Series) -> pd.Series:
# MAGIC       return series * 2 + 1  # Vectorized pandas operation.
# MAGIC
# MAGIC   # Use just like a regular Spark function!
# MAGIC   df.withColumn("result", my_func(col("value")))
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: Pandas UDF Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import pandas_udf, col, rand  # Spark function imports.
from pyspark.sql.functions import udf  # Regular UDF for comparison.
from pyspark.sql.types import DoubleType, StringType  # Return type declarations.
import pandas as pd  # For pandas UDF type hints.
import numpy as np  # For numpy vectorized ops in UDF.

print("="*70)
print("SECTION 3 — BEGINNER: Pandas UDF Basics")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Simple Series → Series Pandas UDF
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Series → Series (simple column transform)")
print("-"*60)

# Define a pandas UDF that doubles and adds 1.
@pandas_udf("double")  # Return type specified as string.
def double_plus_one(s: pd.Series) -> pd.Series:
    """Vectorized: processes entire batch of values at once."""
    return s * 2 + 1  # Numpy-style vectorized operation on pandas Series.

# Create sample DataFrame.
df = spark.createDataFrame(
    [(1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0)],
    ["id", "value"]
)

# Apply the pandas UDF (just like a built-in Spark function!).
result = df.withColumn("transformed", double_plus_one(col("value")))  # Use in withColumn.
print("\nAfter applying pandas UDF:")
display(result)  # display() for Spark DataFrame.

print("\n✓ @pandas_udf('double') decorator marks this as a vectorized UDF.")
print("  Input: pd.Series (batch of rows). Output: pd.Series (same size batch).")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Multi-column input Pandas UDF
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Multi-column input")
print("-"*60)

# UDF that takes two columns and returns one.
@pandas_udf("double")
def compute_bmi(weight_kg: pd.Series, height_m: pd.Series) -> pd.Series:
    """Compute BMI from weight and height columns."""
    return weight_kg / (height_m ** 2)  # Vectorized division.

# Sample health data.
health_df = spark.createDataFrame([
    ("Alice", 60.0, 1.65), ("Bob", 80.0, 1.80),
    ("Charlie", 75.0, 1.75), ("Diana", 55.0, 1.60)
], ["name", "weight_kg", "height_m"])

# Apply multi-input UDF.
bmi_result = health_df.withColumn(
    "bmi", compute_bmi(col("weight_kg"), col("height_m"))  # Pass multiple columns.
)
print("\nBMI calculated with pandas UDF:")
display(bmi_result)  # display() for output.

print("✓ Multiple input Series → one output Series. Each Series = one column batch.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Comparison — Regular UDF vs Pandas UDF syntax
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Regular UDF vs Pandas UDF (syntax comparison)")
print("-"*60)

# REGULAR UDF: processes ONE row at a time.
@udf("string")  # Regular UDF decorator.
def categorize_regular(value):
    """Row-by-row: receives a single value."""
    if value is None:  # Must handle None explicitly.
        return "unknown"
    elif value > 75:  # Single scalar comparison.
        return "high"
    elif value > 25:
        return "medium"
    else:
        return "low"

# PANDAS UDF: processes a BATCH (pd.Series) at once.
@pandas_udf("string")
def categorize_pandas(s: pd.Series) -> pd.Series:
    """Vectorized: receives entire batch as pandas Series."""
    # Use numpy where for vectorized conditionals.
    return pd.Series(
        np.where(s > 75, "high",           # Condition 1.
        np.where(s > 25, "medium", "low"))  # Condition 2 + else.
    )

# Apply both to same data.
test_df = spark.createDataFrame([(10.0,), (50.0,), (90.0,), (30.0,)], ["score"])
result_comparison = test_df.withColumn(
    "cat_regular", categorize_regular(col("score"))  # Regular UDF.
).withColumn(
    "cat_pandas", categorize_pandas(col("score"))    # Pandas UDF.
)
print("\nBoth give same results (but pandas is 10-100x faster):")
display(result_comparison)  # display() shows both columns.
print("\n✓ Same logic, same results. Pandas UDF processes batches = much faster.")

# COMMAND ----------

# DBTITLE 1,Sections 4-5: Intermediate and Advanced
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4-5 — INTERMEDIATE & ADVANCED
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import pandas_udf, col, rand  # Spark imports.
from pyspark.sql.functions import udf  # Regular UDF for comparison.
import pandas as pd  # Pandas for type hints.
import numpy as np  # NumPy for vectorized ops.
import time  # For performance timing.
from typing import Iterator  # For Iterator pattern.

print("="*70)
print("SECTIONS 4-5: Intermediate & Advanced Pandas UDFs")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Speed Benchmark (Regular vs Pandas UDF on 2M rows)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Speed benchmark — Regular UDF vs Pandas UDF")
print("-"*60)

# Regular UDF (row-by-row, SLOW).
@udf("double")
def regular_square(x):
    """Process one row at a time."""
    return float(x) ** 2 if x else 0.0  # Single scalar operation.

# Pandas UDF (vectorized, FAST).
@pandas_udf("double")
def pandas_square(s: pd.Series) -> pd.Series:
    """Process entire batch with numpy vectorization."""
    return s ** 2  # Vectorized: squares entire Series at once.

# Create 2 million row test DataFrame.
df_bench = spark.range(2000000).select(
    (rand() * 100).alias("val")  # Random values 0-100.
)

# Time regular UDF.
start = time.time()  # Start timer.
df_bench.withColumn("sq", regular_square(col("val"))).write.format("noop").mode("overwrite").save()  # Force execution.
reg_time = time.time() - start  # Measure elapsed.

# Time pandas UDF.
start = time.time()  # Start timer.
df_bench.withColumn("sq", pandas_square(col("val"))).write.format("noop").mode("overwrite").save()  # Force execution.
pd_time = time.time() - start  # Measure elapsed.

print(f"\n  Regular UDF (2M rows): {reg_time:.2f}s")
print(f"  Pandas UDF  (2M rows): {pd_time:.2f}s")
print(f"  Speedup: {reg_time/max(pd_time, 0.01):.1f}x faster with Pandas UDF!")
print("\n✓ Arrow-based batching eliminates per-row serialization overhead.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: applyInPandas (grouped map UDF)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: applyInPandas (per-group pandas operations)")
print("-"*60)

# Data with groups.
df_groups = spark.createDataFrame([
    ("Engineering", "Alice", 85000.0),
    ("Engineering", "Bob", 92000.0),
    ("Engineering", "Charlie", 78000.0),
    ("Marketing", "Diana", 72000.0),
    ("Marketing", "Eve", 68000.0),
    ("Marketing", "Frank", 75000.0),
    ("Sales", "Grace", 65000.0),
    ("Sales", "Hank", 70000.0),
    ("Sales", "Ivy", 62000.0)
], ["dept", "name", "salary"])

# Function that receives ONE group as a pandas DataFrame.
def zscore_within_group(pdf: pd.DataFrame) -> pd.DataFrame:
    """Compute z-score of salary WITHIN each department."""
    mean = pdf['salary'].mean()  # Group mean.
    std = pdf['salary'].std()    # Group std.
    pdf['salary_zscore'] = (pdf['salary'] - mean) / std  # Per-group z-score.
    pdf['dept_avg'] = mean       # Add group average for context.
    return pdf  # Return modified pandas DataFrame.

# Apply per group (each dept processed separately as pandas DF).
result_schema = "dept string, name string, salary double, salary_zscore double, dept_avg double"
result = df_groups.groupby("dept").applyInPandas(
    zscore_within_group,  # Function to apply.
    schema=result_schema  # Output schema (must declare).
)
print("\nPer-group z-score normalization:")
display(result)  # display() for output.

print("\n✓ applyInPandas: full pandas power per group.")
print("  Use for: custom models per group, scipy stats, complex transforms.")
print("  Each group is a separate pandas DataFrame in the function.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Iterator Pattern (expensive initialization)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Iterator pattern (load model ONCE per worker)")
print("-"*60)

# Iterator pattern: initialize expensive resource ONCE, process many batches.
@pandas_udf("double")
def predict_with_model(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """Load model once, then score many batches."""
    # EXPENSIVE INIT: runs only ONCE per worker partition.
    # In production: model = mlflow.sklearn.load_model("models:/MyModel/Production")
    coefficients = np.array([0.5, 1.2, -0.3])  # Simulated model weights.
    print("Model loaded!")  # Only prints once per partition.
    
    # Process each batch (runs many times).
    for batch in iterator:  # Each batch is a pd.Series.
        yield batch * coefficients[0] + 10  # Simulated prediction.

# Apply to data.
df_predict = spark.range(100).select((rand() * 50).alias("feature"))
predictions = df_predict.withColumn("prediction", predict_with_model(col("feature")))
print("\nPredictions using Iterator pattern:")
display(predictions.limit(5))  # display() for output.

print("\n✓ Iterator[pd.Series] → Iterator[pd.Series]:")
print("  Model loads ONCE, then processes many batches.")
print("  Without Iterator: model reloads for EVERY batch = very slow!")
print("  Use for: ML inference, loading lookup tables, DB connections.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 7: Grouped Aggregate Pandas UDF
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 7: Grouped Aggregate (custom aggregation function)")
print("-"*60)

# Custom aggregation: coefficient of variation (std/mean * 100).
@pandas_udf("double")
def coeff_of_variation(s: pd.Series) -> float:
    """Custom aggregate: returns a SINGLE scalar from a Series."""
    return float(s.std() / s.mean() * 100)  # CV as percentage.

# Apply as a grouped aggregation.
agg_result = df_groups.groupby("dept").agg(
    coeff_of_variation(col("salary")).alias("salary_cv_pct")  # Custom agg per group.
)
print("\nCoefficient of Variation by department:")
display(agg_result)  # display() for output.

print("\n✓ Series → scalar: custom aggregation function.")
print("  Use in .groupby().agg() just like built-in avg(), sum(), etc.")
print("  Returns one value per group (not one per row).")

# COMMAND ----------

# DBTITLE 1,Section 6: Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Using regular @udf when @pandas_udf is better
# MAGIC ```python
# MAGIC # BAD: Row-by-row processing (10-100x slower).
# MAGIC @udf("double")
# MAGIC def slow_square(x):
# MAGIC     return float(x) ** 2
# MAGIC
# MAGIC # GOOD: Vectorized batch processing.
# MAGIC @pandas_udf("double")
# MAGIC def fast_square(s: pd.Series) -> pd.Series:
# MAGIC     return s ** 2  # Entire batch at once!
# MAGIC ```
# MAGIC **Rule**: If your logic can be expressed with pandas/numpy vectorized ops, ALWAYS use `@pandas_udf`.
# MAGIC
# MAGIC ### Mistake 2: Importing heavy libraries INSIDE a regular pandas UDF
# MAGIC ```python
# MAGIC # BAD: Model loads on EVERY batch (1000s of times!).
# MAGIC @pandas_udf("double")
# MAGIC def predict_bad(s: pd.Series) -> pd.Series:
# MAGIC     import joblib
# MAGIC     model = joblib.load("/path/model.pkl")  # Reloads every batch!
# MAGIC     return pd.Series(model.predict(s.values.reshape(-1, 1)))
# MAGIC
# MAGIC # GOOD: Use Iterator pattern to load model ONCE.
# MAGIC @pandas_udf("double")
# MAGIC def predict_good(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
# MAGIC     import joblib
# MAGIC     model = joblib.load("/path/model.pkl")  # Loaded ONCE per worker!
# MAGIC     for batch in iterator:
# MAGIC         yield pd.Series(model.predict(batch.values.reshape(-1, 1)))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Forgetting to specify the return type
# MAGIC ```python
# MAGIC # BAD: No return type → error!
# MAGIC @pandas_udf  # Missing return type!
# MAGIC def my_func(s: pd.Series) -> pd.Series:
# MAGIC     return s * 2
# MAGIC
# MAGIC # GOOD: Always specify return type as first argument.
# MAGIC @pandas_udf("double")  # Return type declared.
# MAGIC def my_func(s: pd.Series) -> pd.Series:
# MAGIC     return s * 2
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Returning wrong size from Series → Series UDF
# MAGIC ```python
# MAGIC # BAD: Output has different length than input!
# MAGIC @pandas_udf("double")
# MAGIC def bad_filter(s: pd.Series) -> pd.Series:
# MAGIC     return s[s > 0]  # WRONG! Returns fewer rows than input.
# MAGIC
# MAGIC # GOOD: Series→Series must return SAME number of rows.
# MAGIC @pandas_udf("double")
# MAGIC def good_filter(s: pd.Series) -> pd.Series:
# MAGIC     return s.where(s > 0, 0.0)  # Replace negatives with 0, keep length.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not declaring output schema in applyInPandas
# MAGIC ```python
# MAGIC # BAD: Missing schema → runtime error.
# MAGIC df.groupby("key").applyInPandas(my_func)  # ERROR! Schema required.
# MAGIC
# MAGIC # GOOD: Always declare output schema as string or StructType.
# MAGIC df.groupby("key").applyInPandas(
# MAGIC     my_func,
# MAGIC     schema="key string, value double, computed double"  # Explicit schema.
# MAGIC )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7: Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import pandas_udf, col, rand  # Imports.
import pandas as pd  # Pandas.
import numpy as np  # NumPy.
from typing import Iterator  # Iterator type.

print("="*70)
print("HOMEWORK — Pandas UDFs & Apache Arrow")
print("="*70)

# Level 1: Write a simple pandas UDF.
print("\n--- Level 1: Simple Series → Series UDF ---")

@pandas_udf("double")  # Return type.
def cube_values(s: pd.Series) -> pd.Series:
    """Cube each value in the column."""
    return s ** 3  # Vectorized power operation.

df1 = spark.createDataFrame([(1.0,), (2.0,), (3.0,), (4.0,)], ["x"])  # Test data.
display(df1.withColumn("x_cubed", cube_values(col("x"))))  # Apply and display.
# WHY: @pandas_udf processes batches via Arrow = 10-100x faster than @udf.

# Level 2: Multi-input UDF.
print("\n--- Level 2: Multi-column input ---")

@pandas_udf("double")
def distance_2d(x: pd.Series, y: pd.Series) -> pd.Series:
    """Euclidean distance from origin."""
    return np.sqrt(x**2 + y**2)  # Vectorized numpy operation.

df2 = spark.createDataFrame([(3.0, 4.0), (5.0, 12.0), (8.0, 15.0)], ["x", "y"])
display(df2.withColumn("dist", distance_2d(col("x"), col("y"))))  # 5.0, 13.0, 17.0.
# WHY: Multi-input pandas UDFs accept multiple pd.Series arguments.

# Level 3: String processing UDF.
print("\n--- Level 3: String transformation ---")

@pandas_udf("string")
def clean_text(s: pd.Series) -> pd.Series:
    """Lowercase, strip whitespace, remove special chars."""
    return s.str.lower().str.strip().str.replace(r'[^a-z0-9\s]', '', regex=True)

df3 = spark.createDataFrame([("  Hello World! ",), ("TESTING 123.",)], ["text"])
display(df3.withColumn("clean", clean_text(col("text"))))  # Cleaned text.
# WHY: pandas .str accessor provides powerful vectorized string ops.

# Level 4: applyInPandas.
print("\n--- Level 4: applyInPandas (per-group) ---")

def rank_within_group(pdf: pd.DataFrame) -> pd.DataFrame:
    """Add rank column within each group."""
    pdf['rank'] = pdf['score'].rank(ascending=False).astype(int)  # Rank by score.
    return pdf  # Return modified DataFrame.

df4 = spark.createDataFrame([
    ("A", 90), ("A", 85), ("A", 95), ("B", 70), ("B", 80)
], ["group", "score"])
result4 = df4.groupby("group").applyInPandas(
    rank_within_group, schema="group string, score int, rank int"
)
display(result4)  # Rank within each group.
# WHY: applyInPandas gives full pandas power per group (custom logic).

# Level 5: Iterator pattern.
print("\n--- Level 5: Iterator pattern (load once) ---")

@pandas_udf("double")
def score_with_lookup(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """Expensive init ONCE, then process many batches."""
    # Simulated expensive load (model, lookup table, DB connection).
    lookup = {0: 1.5, 1: 2.0, 2: 0.8, 3: 1.2, 4: 1.0}  # Loaded ONCE.
    for batch in iterator:  # Process each batch.
        yield batch.map(lambda x: lookup.get(int(x) % 5, 1.0))  # Apply lookup.

df5 = spark.range(10).select(col("id").cast("double").alias("val"))
display(df5.withColumn("scored", score_with_lookup(col("val"))))  # Scored values.
# WHY: Iterator avoids reloading expensive resources per batch.

# Level 6: Grouped aggregate UDF.
print("\n--- Level 6: Custom aggregation ---")

@pandas_udf("double")
def iqr_agg(s: pd.Series) -> float:
    """Interquartile range (custom aggregation)."""
    return float(s.quantile(0.75) - s.quantile(0.25))  # Q3 - Q1.

df6 = spark.createDataFrame([
    ("A", 10.0), ("A", 20.0), ("A", 30.0), ("A", 40.0),
    ("B", 5.0), ("B", 50.0), ("B", 75.0), ("B", 100.0)
], ["grp", "val"])
display(df6.groupby("grp").agg(iqr_agg(col("val")).alias("iqr")))  # IQR per group.
# WHY: Series → scalar lets you write custom agg functions for groupby.

# Levels 7-10: Conceptual.
print("\n--- Level 7: When to use each UDF type ---")
print("  Series→Series: column transforms (most common, 90% of cases).")
print("  Iterator: expensive init (ML models, DB connections).")
print("  applyInPandas: per-group custom logic (scipy, custom models).")
print("  Scalar (agg): custom aggregations (IQR, weighted mean, etc).")

print("\n--- Level 8: Arrow configuration ---")
print("  spark.sql.execution.arrow.pyspark.enabled = true (default in DBR).")
print("  spark.sql.execution.arrow.maxRecordsPerBatch = 10000 (batch size).")
print("  Larger batches = more memory, fewer transfers, usually faster.")

print("\n--- Level 9: Limitations ---")
print("  - Cannot use Spark functions inside pandas UDFs (only pandas/numpy).")
print("  - Series→Series must return same row count as input.")
print("  - applyInPandas schema must be declared (no auto-inference).")
print("  - Very large batches can OOM the executor Python process.")

print("\n--- Level 10: Teach Pandas UDFs ---")
print("""
"Pandas UDFs process data in vectorized batches via Apache Arrow.
  @pandas_udf('return_type') = 10-100x faster than @udf.
  Types:
    Series→Series: column transform (most common).
    Iterator: load expensive resource ONCE per worker.
    applyInPandas: full pandas per group.
    Scalar: custom aggregation in groupby.
  Always prefer vectorized numpy/pandas ops inside UDFs.
  Never use regular @udf if @pandas_udf can do the job."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 92")
print("="*70)