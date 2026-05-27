# Databricks notebook source
# DBTITLE 1,Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 30: DataFrame.transform() and Functional Patterns
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
# MAGIC - **df.transform()** = An assembly line where each station does ONE job (paint, weld, polish). You can swap stations in/out without redesigning the whole factory.
# MAGIC - **Functional Patterns** = LEGO bricks. Each function is one brick. Snap them together in any order.
# MAGIC - **mapInPandas()** = Hiring a specialist (pandas) for one station only (the rest stays Spark)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Why You Need This
# MAGIC
# MAGIC | Pattern | Problem It Solves |
# MAGIC |---------|------------------|
# MAGIC | `df.transform(func)` | Chaining reusable transformations cleanly |
# MAGIC | Transformation library | Team reuses same logic across 50 notebooks |
# MAGIC | `mapInPandas()` | Use pandas/sklearn/scipy inside Spark |
# MAGIC | `mapInArrow()` | High-performance pandas-like UDF (zero-copy) |
# MAGIC | `applyInPandas()` | Per-group logic with pandas (groupBy + apply) |
# MAGIC | `df.observe()` | Collect metrics mid-pipeline without side effects |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Insight
# MAGIC
# MAGIC `df.transform(func)` enables **pipeline-as-code**: your ETL becomes a sequence of small, testable functions instead of one giant notebook cell.

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### df.transform() Mechanics
# MAGIC
# MAGIC ```
# MAGIC def add_greeting(df):
# MAGIC     return df.withColumn("greeting", lit("Hello"))
# MAGIC
# MAGIC def filter_adults(df):
# MAGIC     return df.filter(col("age") >= 18)
# MAGIC
# MAGIC # Without transform (nested, hard to read):
# MAGIC result = filter_adults(add_greeting(df))
# MAGIC
# MAGIC # With transform (clean chain):
# MAGIC result = df.transform(add_greeting).transform(filter_adults)
# MAGIC
# MAGIC # With parameters:
# MAGIC def filter_by_age(df, min_age):
# MAGIC     return df.filter(col("age") >= min_age)
# MAGIC
# MAGIC result = df.transform(filter_by_age, min_age=21)
# MAGIC ```
# MAGIC
# MAGIC ### mapInPandas / applyInPandas
# MAGIC
# MAGIC ```
# MAGIC mapInPandas(func, schema):
# MAGIC   → Processes ENTIRE DataFrame in pandas chunks (partition by partition)
# MAGIC   → func receives Iterator[pd.DataFrame] → yields Iterator[pd.DataFrame]
# MAGIC   → Use for: scipy, sklearn, any pandas-only library
# MAGIC
# MAGIC applyInPandas(func, schema):
# MAGIC   → Processes EACH GROUP as one pandas DataFrame
# MAGIC   → func receives pd.DataFrame → returns pd.DataFrame
# MAGIC   → Use for: per-group ML, per-customer stats, etc.
# MAGIC ```
# MAGIC
# MAGIC ### df.observe()
# MAGIC
# MAGIC ```
# MAGIC from pyspark.sql import Observation
# MAGIC
# MAGIC obs = Observation("my_metrics")
# MAGIC df.observe(obs, count(lit(1)).alias("total"), avg("amount").alias("avg_amt"))
# MAGIC   → Collects metrics without extra pass over data
# MAGIC   → Metrics available after action completes
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: df.transform()
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: df.transform()
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, upper, lit

print("=== df.transform() — Clean Transformation Chaining ===")
print()

# Sample data: employees
df = spark.createDataFrame([
    ("alice", 28, "engineering", 85000),
    ("bob", 35, "marketing", 72000),
    ("charlie", 42, "engineering", 95000),
    ("diana", 31, "sales", 68000),
    ("eve", 26, "engineering", 78000),
], ["name", "age", "dept", "salary"])

# --- Define reusable transformation functions ---
def uppercase_names(df):
    """Capitalize all names."""
    return df.withColumn("name", upper(col("name")))  # alice → ALICE

def add_salary_grade(df):
    """Classify salary into grades."""
    from pyspark.sql.functions import when
    return df.withColumn("grade",
        when(col("salary") >= 90000, "A")  # High earners
        .when(col("salary") >= 75000, "B")  # Mid earners
        .otherwise("C")  # Entry level
    )

def filter_engineers(df):
    """Keep only engineering dept."""
    return df.filter(col("dept") == "engineering")  # Filter to engineers only

# --- Chain with transform (clean!) ---
print("--- Chained with .transform() ---")
result = (df
    .transform(uppercase_names)       # Step 1: capitalize
    .transform(add_salary_grade)      # Step 2: add grade
    .transform(filter_engineers)      # Step 3: filter
)
result.show()

# --- Without transform (hard to read) ---
print("--- Equivalent without .transform() (ugly nesting) ---")
print("  result = filter_engineers(add_salary_grade(uppercase_names(df)))")
print("  # Inside-out reading = confusing!")

# Expected output:
# +-------+---+-----------+------+-----+
# |   name|age|       dept|salary|grade|
# +-------+---+-----------+------+-----+
# |  ALICE| 28|engineering| 85000|    B|
# |CHARLIE| 42|engineering| 95000|    A|
# |    EVE| 26|engineering| 78000|    B|
# +-------+---+-----------+------+-----+

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: transform with parameters
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: transform with parameters
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, when

print("=== transform() with Parameters ===")
print()
print("Pass extra arguments to make transformations configurable.")
print()

# Sample data
df = spark.createDataFrame([
    ("ProductA", 100, "electronics"),
    ("ProductB", 250, "clothing"),
    ("ProductC", 50, "electronics"),
    ("ProductD", 500, "luxury"),
    ("ProductE", 150, "clothing"),
], ["product", "price", "category"])

# --- Parameterized transformations ---
def filter_by_price(df, min_price=0, max_price=float("inf")):
    """Filter rows within a price range."""
    return df.filter(
        (col("price") >= min_price) &  # Above minimum
        (col("price") <= max_price)    # Below maximum
    )

def apply_discount(df, discount_pct=10):
    """Add a discounted_price column."""
    factor = 1 - (discount_pct / 100)  # 10% discount = 0.9 factor
    return df.withColumn("discounted_price", (col("price") * factor).cast("int"))

def add_category_flag(df, target_category):
    """Flag rows matching a target category."""
    return df.withColumn(
        "is_target",
        when(col("category") == target_category, True).otherwise(False)
    )

# --- Chain with parameters ---
print("--- Affordable electronics with 15% discount ---")
result = (df
    .transform(filter_by_price, min_price=50, max_price=200)  # Price range
    .transform(apply_discount, discount_pct=15)               # 15% off
    .transform(add_category_flag, target_category="electronics")  # Flag electronics
)
result.show()

# --- Same functions, different parameters ---
print("--- Luxury items with 5% discount ---")
result2 = (df
    .transform(filter_by_price, min_price=300)  # Only expensive
    .transform(apply_discount, discount_pct=5)  # Small discount
    .transform(add_category_flag, target_category="luxury")  # Flag luxury
)
result2.show()

print("--- Key benefit: Same functions, different configs = reusable! ---")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Building a transformation library
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: Building a transformation library
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, current_timestamp, lit, lower, trim, regexp_replace

print("=== Building a Reusable Transformation Library ===")
print()
print("Pattern: One function per transformation. Compose them into pipelines.")
print()

# ═════ TRANSFORMATION LIBRARY ═════

def clean_strings(df, columns):
    """Trim and lowercase string columns."""
    for c in columns:
        df = df.withColumn(c, lower(trim(col(c))))  # " Alice " → "alice"
    return df

def remove_special_chars(df, columns):
    """Remove non-alphanumeric characters."""
    for c in columns:
        df = df.withColumn(c, regexp_replace(col(c), "[^a-zA-Z0-9 ]", ""))  # Clean
    return df

def add_audit_columns(df):
    """Add standard audit columns."""
    return (df
        .withColumn("_loaded_at", current_timestamp())  # When was this row processed
        .withColumn("_source", lit("raw_input"))        # Data source tag
    )

def drop_nulls_in(df, columns):
    """Drop rows with nulls in specified columns."""
    return df.dropna(subset=columns)  # Remove rows with nulls in key columns

# ═════ USAGE: Clean ETL Pipeline ═════

# Dirty data
df = spark.createDataFrame([
    (" Alice!!", "  ENGINEERING ", 85000),
    ("Bob#$", "marketing", 72000),
    (None, "  Sales", 68000),
    ("Charlie", None, 95000),
    (" Diana ", "  Engineering ", None),
], ["name", "dept", "salary"])

print("--- Raw data ---")
df.show()

# Clean pipeline
print("--- Cleaned data (composable pipeline) ---")
cleaned = (df
    .transform(drop_nulls_in, columns=["name", "dept"])  # Remove nulls in key cols
    .transform(clean_strings, columns=["name", "dept"])  # Trim + lowercase
    .transform(remove_special_chars, columns=["name"])   # Remove special chars
    .transform(add_audit_columns)                        # Add audit fields
)
cleaned.show(truncate=False)

print("--- Library benefits ---")
print("  1. Each function is independently testable")
print("  2. Functions compose in any order")
print("  3. Same functions used across all notebooks in your project")
print("  4. Easy to add new transformations without changing existing code")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: mapInPandas()
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: mapInPandas()
# ═══════════════════════════════════════════════════════

import pandas as pd
from pyspark.sql.functions import col

print("=== mapInPandas() — Use Pandas Inside Spark ===")
print()
print("Process each Spark partition as a pandas DataFrame.")
print("Use when you need pandas/scipy/sklearn but have big data.")
print()

# Sample data
df = spark.createDataFrame([
    ("A", 10.5), ("B", 20.3), ("C", 30.1),
    ("D", 40.7), ("E", 50.9), ("F", 60.2),
    ("G", 70.4), ("H", 80.6), ("I", 90.8),
], ["id", "value"])

# --- mapInPandas: apply pandas logic to each partition ---
def normalize_partition(iterator):
    """
    Normalize values within each partition using pandas.
    iterator: Iterator[pd.DataFrame] (one per partition)
    yields: Iterator[pd.DataFrame]
    """
    for pdf in iterator:  # Each chunk is a pandas DataFrame
        # Use any pandas operation!
        pdf["normalized"] = (pdf["value"] - pdf["value"].mean()) / pdf["value"].std()
        pdf["rank_in_partition"] = pdf["value"].rank()  # Pandas rank
        yield pdf  # Must yield pandas DataFrame

# Define output schema (must match what we yield)
output_schema = "id string, value double, normalized double, rank_in_partition double"

# Apply mapInPandas
print("--- mapInPandas: normalize within each partition ---")
result = df.mapInPandas(normalize_partition, schema=output_schema)
result.show()

print("--- Key points ---")
print("  1. Function receives Iterator[pd.DataFrame], yields Iterator[pd.DataFrame]")
print("  2. Schema must be specified (Spark can't infer from pandas)")
print("  3. Each partition processed independently (no cross-partition logic)")
print("  4. Use for: scipy stats, sklearn transforms, numpy operations")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: applyInPandas()
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: applyInPandas() (grouped)
# ═══════════════════════════════════════════════════════

import pandas as pd
from pyspark.sql.functions import col

print("=== applyInPandas() — Per-Group Pandas Logic ===")
print()
print("groupBy().applyInPandas(): Each GROUP processed as one pandas DataFrame.")
print("Use for per-customer models, per-group statistics, per-store forecasts.")
print()

# Sales data by store
df = spark.createDataFrame([
    ("StoreA", "2024-01", 100), ("StoreA", "2024-02", 120),
    ("StoreA", "2024-03", 110), ("StoreA", "2024-04", 130),
    ("StoreB", "2024-01", 200), ("StoreB", "2024-02", 180),
    ("StoreB", "2024-03", 220), ("StoreB", "2024-04", 240),
    ("StoreC", "2024-01", 50),  ("StoreC", "2024-02", 55),
    ("StoreC", "2024-03", 60),  ("StoreC", "2024-04", 58),
], ["store", "month", "sales"])

# --- applyInPandas: calculate growth per store ---
def calculate_store_metrics(pdf):
    """
    Per-store: calculate growth rate and rolling average.
    Receives: one store's data as pd.DataFrame
    Returns: pd.DataFrame with same/new columns
    """
    pdf = pdf.sort_values("month")  # Ensure order
    pdf["growth_pct"] = pdf["sales"].pct_change() * 100  # MoM growth
    pdf["rolling_avg"] = pdf["sales"].rolling(2, min_periods=1).mean()  # 2-month avg
    pdf["cumulative"] = pdf["sales"].cumsum()  # Running total
    return pdf

# Output schema must include all columns
schema = "store string, month string, sales long, growth_pct double, rolling_avg double, cumulative long"

# Apply per group
print("--- Per-store metrics (applyInPandas) ---")
result = df.groupBy("store").applyInPandas(calculate_store_metrics, schema=schema)
result.orderBy("store", "month").show()

print("--- applyInPandas vs mapInPandas ---")
print("  mapInPandas: processes each PARTITION (arbitrary split)")
print("  applyInPandas: processes each GROUP (logical, like per-customer)")
print("  \u2192 Use applyInPandas when logic depends on grouping!")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: df.observe()
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: df.observe() — Metrics mid-pipeline
# ═══════════════════════════════════════════════════════

from pyspark.sql import Observation
from pyspark.sql.functions import col, count, avg, min as spark_min, max as spark_max, lit

print("=== df.observe() — Collect Metrics Without Extra Pass ===")
print()
print("observe() attaches metric collection to an existing query plan.")
print("Metrics computed in the SAME PASS as the main query (no extra scan!).")
print()

# Create sample data
df = spark.createDataFrame([
    ("alice", 28, 85000), ("bob", 35, 72000),
    ("charlie", 42, 95000), ("diana", 31, 68000),
    ("eve", 26, 78000), ("frank", 39, 91000),
    ("grace", 33, 82000), ("henry", 45, 105000),
], ["name", "age", "salary"])

# --- Define observation (metrics to collect) ---
obs = Observation("salary_metrics")  # Name your observation

# Attach observation to the DataFrame
df_observed = df.observe(
    obs,
    count(lit(1)).alias("total_rows"),    # Count all rows
    avg("salary").alias("avg_salary"),     # Average salary
    spark_min("salary").alias("min_sal"),  # Min salary
    spark_max("salary").alias("max_sal"),  # Max salary
)

# Trigger an action (observation collects during this action)
print("--- Main query result ---")
filtered = df_observed.filter(col("salary") > 80000)  # Some downstream op
filtered.show()

# Retrieve metrics AFTER the action
print("--- Metrics collected mid-pipeline (no extra pass!) ---")
metrics = obs.get  # Returns dict of metric_name: value
for k, v in metrics.items():
    print(f"  {k}: {v}")

print("\n--- Use cases ---")
print("  1. Data quality: count nulls, check ranges during ETL")
print("  2. Monitoring: track row counts at each pipeline stage")
print("  3. Debugging: verify values without adding .show() everywhere")
print("  4. Auditing: log metrics without materializing intermediate DFs")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Production transformation framework
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Production transformation framework
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, current_timestamp, lit, when, count
from functools import reduce

print("=== Production Transformation Framework ===")
print()

# --- Pipeline builder: compose multiple transforms ---
def build_pipeline(*transforms):
    """
    Compose multiple transformation functions into one.
    Usage: pipeline = build_pipeline(func1, func2, func3)
           result = pipeline(df)
    """
    def pipeline(df):
        return reduce(lambda d, f: f(d), transforms, df)  # Apply all in order
    return pipeline

# --- Individual transformations ---
def validate_not_null(df, columns):
    """Tag rows with null values (don't drop, flag them)."""
    null_check = sum([when(col(c).isNull(), 1).otherwise(0) for c in columns])
    return df.withColumn("_null_count", null_check)  # Count nulls per row

def add_metadata(df):
    """Add processing metadata."""
    return (df
        .withColumn("_processed_at", current_timestamp())
        .withColumn("_version", lit("v2.1"))
    )

def quarantine_bad_rows(df):
    """Separate good rows from bad rows."""
    good = df.filter(col("_null_count") == 0)  # No nulls = good
    bad = df.filter(col("_null_count") > 0)    # Has nulls = quarantine
    return good, bad  # Returns tuple!

# --- Build the pipeline ---
print("--- Building composable pipeline ---")
etl_pipeline = build_pipeline(
    lambda df: validate_not_null(df, ["name", "email"]),  # Step 1
    add_metadata,  # Step 2
)

# Test data
df = spark.createDataFrame([
    ("alice", "alice@co.com", 100),
    ("bob", None, 200),
    (None, "charlie@co.com", 300),
    ("diana", "diana@co.com", 400),
], ["name", "email", "amount"])

# Apply pipeline
result = etl_pipeline(df)

# Split into good/bad
good, bad = quarantine_bad_rows(result)

print("--- Good rows (no nulls) ---")
good.show(truncate=False)

print("--- Quarantined rows (has nulls) ---")
bad.show(truncate=False)

print(f"\n  Good: {good.count()} rows | Bad: {bad.count()} rows")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: mapInArrow for high performance
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: mapInArrow() for high-performance
# ═══════════════════════════════════════════════════════

import pyarrow as pa
import pyarrow.compute as pc

print("=== mapInArrow() — Zero-Copy High-Performance ===")
print()
print("mapInArrow: Like mapInPandas but uses Apache Arrow directly.")
print("Benefits: Zero-copy data transfer, columnar operations, faster.")
print()

# Sample data
df = spark.createDataFrame([
    ("alice", 28, 85000.0), ("bob", 35, 72000.0),
    ("charlie", 42, 95000.0), ("diana", 31, 68000.0),
    ("eve", 26, 78000.0), ("frank", 39, 91000.0),
], ["name", "age", "salary"])

# --- mapInArrow: process with Arrow (zero-copy!) ---
def process_with_arrow(iterator):
    """
    Process each batch using PyArrow compute functions.
    iterator: Iterator[pyarrow.RecordBatch]
    yields: Iterator[pyarrow.RecordBatch]
    """
    for batch in iterator:
        # Arrow compute: vectorized operations (very fast!)
        salary_col = batch.column("salary")  # Get salary column
        
        # Compute tax (20% bracket)
        tax = pc.multiply(salary_col, 0.20)  # Element-wise multiply
        net_salary = pc.subtract(salary_col, tax)  # salary - tax
        
        # Add new columns to the batch
        new_batch = batch.append_column("tax", tax)
        new_batch = new_batch.append_column("net_salary", net_salary)
        yield new_batch

# Define output schema
output_schema = "name string, age int, salary double, tax double, net_salary double"

print("--- mapInArrow result ---")
result = df.mapInArrow(process_with_arrow, schema=output_schema)
result.show()

print("--- mapInArrow vs mapInPandas ---")
print("  mapInPandas: Spark → Arrow → Pandas → Arrow → Spark (2 conversions)")
print("  mapInArrow:  Spark → Arrow → Arrow → Spark (zero-copy!)")
print("  \u2192 mapInArrow is faster for pure compute (no pandas needed)")
print("  \u2192 Use mapInPandas only when you NEED pandas/scipy/sklearn")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Full ETL with observe + transform
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Full ETL with observe + transform
# ═══════════════════════════════════════════════════════

from pyspark.sql import Observation
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, lit, when, upper, trim, current_timestamp
)

print("=== Production ETL: transform() + observe() ===")
print()
print("Combining all patterns into a real-world ETL pipeline.")
print()

# --- Raw data (messy!) ---
raw = spark.createDataFrame([
    ("alice", "US", 100.0, "completed"),
    ("bob", "UK", 250.0, "completed"),
    (" Charlie ", "us", 50.0, "pending"),
    ("diana", "DE", -10.0, "completed"),   # Bad: negative amount
    ("eve", "US", 300.0, "completed"),
    (None, "UK", 150.0, "completed"),       # Bad: null name
    ("frank", "FR", 75.0, "cancelled"),
    ("grace", "US", 200.0, "completed"),
], ["customer", "country", "amount", "status"])

# --- Transformation functions ---
def clean_customer_data(df):
    """Standardize string fields."""
    return (df
        .withColumn("customer", trim(col("customer")))  # Remove spaces
        .withColumn("country", upper(col("country")))   # Uppercase country
    )

def filter_valid_records(df):
    """Remove invalid records."""
    return df.filter(
        col("customer").isNotNull() &  # Must have customer name
        (col("amount") > 0) &          # Positive amounts only
        (col("status") == "completed")  # Only completed orders
    )

def enrich_with_region(df):
    """Add region based on country code."""
    return df.withColumn("region",
        when(col("country").isin("US", "CA"), "Americas")
        .when(col("country").isin("UK", "DE", "FR"), "Europe")
        .otherwise("Other")
    )

# --- Build pipeline with observation ---
obs_input = Observation("input_metrics")
obs_output = Observation("output_metrics")

print("--- Running ETL pipeline ---")
result = (raw
    .observe(obs_input, count(lit(1)).alias("input_rows"))  # Track input
    .transform(clean_customer_data)    # Step 1: clean
    .transform(filter_valid_records)   # Step 2: filter
    .transform(enrich_with_region)     # Step 3: enrich
    .withColumn("_etl_timestamp", current_timestamp())  # Audit
    .observe(obs_output,
        count(lit(1)).alias("output_rows"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )
)

# Trigger execution
result.show(truncate=False)

# Report metrics
print("\n--- Pipeline Metrics ---")
input_m = obs_input.get
output_m = obs_output.get
print(f"  Input rows:  {input_m['input_rows']}")
print(f"  Output rows: {output_m['output_rows']}")
print(f"  Filtered:    {input_m['input_rows'] - output_m['output_rows']} rows removed")
print(f"  Total $:     ${output_m['total_amount']:,.2f}")
print(f"  Avg $:       ${output_m['avg_amount']:,.2f}")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: transform() function doesn't return a DataFrame
# MAGIC **Problem:** `def my_func(df): df.withColumn(...)` (missing `return`)  
# MAGIC **Fix:** Always `return` the DataFrame: `def my_func(df): return df.withColumn(...)`
# MAGIC
# MAGIC ### Mistake #2: Wrong schema in mapInPandas/applyInPandas
# MAGIC **Problem:** Schema doesn't match the columns your function returns → cryptic errors.  
# MAGIC **Fix:** Define schema explicitly matching ALL output columns (name + type).
# MAGIC
# MAGIC ### Mistake #3: applyInPandas with too-large groups
# MAGIC **Problem:** One group has 100M rows → doesn't fit in executor memory.  
# MAGIC **Fix:** Ensure groups are reasonable size, or use mapInPandas with repartition.
# MAGIC
# MAGIC ### Mistake #4: Using observe() without triggering an action
# MAGIC **Problem:** `obs.get` called before any action (show/count/write) → empty/error.  
# MAGIC **Fix:** Always call an action AFTER observe, THEN read `obs.get`.
# MAGIC
# MAGIC ### Mistake #5: Modifying DataFrame in-place inside transform
# MAGIC **Problem:** `def func(df): df = df.filter(...); return df` modifies local variable but not chain.  
# MAGIC **Fix:** This actually works in Python, but be careful not to mutate the original variable outside the function — always return a new DataFrame from the function.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC **Level 1:** Write a `transform()` function that uppercases a column. Apply it with `.transform()`.
# MAGIC
# MAGIC **Level 2:** Write a parameterized transform that filters by a given column and value.
# MAGIC
# MAGIC **Level 3:** Chain 3 transform functions together on a sample DataFrame.
# MAGIC
# MAGIC **Level 4:** Use `build_pipeline()` (from examples) to compose 4 transforms into one function.
# MAGIC
# MAGIC **Level 5:** Use `mapInPandas()` to apply scipy z-score normalization to a numeric column.
# MAGIC
# MAGIC **Level 6:** Use `applyInPandas()` to fit a linear regression per group (store/customer).
# MAGIC
# MAGIC **Level 7:** Use `df.observe()` to collect row count and null count during a filter operation.
# MAGIC
# MAGIC **Level 8:** Build a transformation library with at least 5 functions for a data cleaning pipeline.
# MAGIC
# MAGIC **Level 9:** Compare `mapInPandas` vs `mapInArrow` performance on 1M rows. Which is faster and why?
# MAGIC
# MAGIC **Level 10:** Teach a teammate: Why is `df.transform()` better than nested function calls for maintainability?

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, upper, count, lit, when
from pyspark.sql import Observation
from functools import reduce

# Level 1: Simple transform
print("=== Level 1: Uppercase transform ===")
def make_upper(df, column="name"):
    return df.withColumn(column, upper(col(column)))  # Uppercase the column

df = spark.createDataFrame([("alice", 25), ("bob", 30)], ["name", "age"])
df.transform(make_upper).show()

# Level 2: Parameterized filter
print("=== Level 2: Parameterized filter ===")
def filter_by(df, column, value):
    return df.filter(col(column) == value)  # Filter by any column/value

df2 = spark.createDataFrame([
    ("alice", "eng"), ("bob", "mkt"), ("charlie", "eng")
], ["name", "dept"])
df2.transform(filter_by, column="dept", value="eng").show()

# Level 4: build_pipeline
print("=== Level 4: build_pipeline ===")
def build_pipeline(*transforms):
    def pipeline(df):
        return reduce(lambda d, f: f(d), transforms, df)
    return pipeline

def add_flag(df):
    return df.withColumn("flag", lit(True))  # Add a flag column

def double_age(df):
    return df.withColumn("age_x2", col("age") * 2)  # Double the age

pipeline = build_pipeline(make_upper, add_flag, double_age)
df.transform(pipeline).show()

# Level 7: observe with metrics
print("=== Level 7: observe ===")
df7 = spark.createDataFrame([
    ("a", 1), ("b", None), ("c", 3), (None, 4)
], ["name", "value"])

obs = Observation("homework_obs")
result = df7.observe(
    obs,
    count(lit(1)).alias("total"),
    count("name").alias("non_null_names"),  # count() skips nulls!
    count("value").alias("non_null_values")
)
result.filter(col("name").isNotNull()).show()  # Trigger action

metrics = obs.get
print(f"  Metrics: {metrics}")
print(f"  Null names: {metrics['total'] - metrics['non_null_names']}")
print(f"  Null values: {metrics['total'] - metrics['non_null_values']}")

print("\n\u2705 All homework solutions complete!")