# Databricks notebook source
# DBTITLE 1,NB_45 Header
# MAGIC %md
# MAGIC # NB_45 — Handling Nulls (Full Strategy)
# MAGIC
# MAGIC **Module 7: Data Cleaning & Quality** | Notebook 45 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * NULL semantics in Spark (3-valued logic)
# MAGIC * Detection: isNull(), isNotNull(), isnan()
# MAGIC * df.na API: drop(), fill(), replace()
# MAGIC * Column-specific fill strategies
# MAGIC * Conditional fill: coalesce(), when/otherwise
# MAGIC * Statistical imputation (mean, median, mode)
# MAGIC * Forward-fill and backward-fill patterns
# MAGIC * NULL-safe comparisons: eqNullSafe ()
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Foundation of clean data)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — NULL Semantics
# MAGIC %md
# MAGIC ## SECTION 1 — Understanding NULL in Spark (Real-World Analogy)
# MAGIC
# MAGIC ### 📦 The Missing Label
# MAGIC
# MAGIC NULL is like a package with NO label — you don't know what's inside:
# MAGIC
# MAGIC | Question | Answer | Why |
# MAGIC |---|---|---|
# MAGIC | Is NULL = NULL? | Unknown (not TRUE!) | Two missing labels could be anything |
# MAGIC | Is NULL > 5? | Unknown | Can't compare nothing to something |
# MAGIC | NULL + 10 = ? | NULL | Anything + unknown = unknown |
# MAGIC | COUNT(col) vs COUNT(*) | COUNT(col) skips NULLs | COUNT(*) counts all rows |
# MAGIC
# MAGIC ### Spark's 3-Valued Logic
# MAGIC ```
# MAGIC TRUE  AND NULL  = NULL     (not FALSE!)
# MAGIC FALSE AND NULL  = FALSE    (short-circuit)
# MAGIC TRUE  OR  NULL  = TRUE     (short-circuit)
# MAGIC FALSE OR  NULL  = NULL     (not FALSE!)
# MAGIC ```
# MAGIC
# MAGIC ### NULL Handling Strategies
# MAGIC 1. **Drop** — Remove rows/columns with NULLs (data loss risk)
# MAGIC 2. **Fill** — Replace with constant/default values
# MAGIC 3. **Impute** — Replace with statistical estimates (mean/median/mode)
# MAGIC 4. **Forward-fill** — Use previous row's value (time series)
# MAGIC 5. **Flag** — Keep NULL but add indicator column

# COMMAND ----------

# DBTITLE 1,SECTION 2 — The df.na API
# MAGIC %md
# MAGIC ## SECTION 2 — The df.na API Reference
# MAGIC
# MAGIC ### Dropping NULLs
# MAGIC ```python
# MAGIC df.na.drop()                    # Drop row if ANY column is null
# MAGIC df.na.drop("all")               # Drop row if ALL columns are null
# MAGIC df.na.drop("any")               # Drop row if ANY column is null (default)
# MAGIC df.na.drop(thresh=3)            # Keep row if at least 3 non-null values
# MAGIC df.na.drop(subset=["a","b"])    # Only check columns a and b
# MAGIC df.na.drop("any", subset=["a"]) # Drop if column a is null
# MAGIC ```
# MAGIC
# MAGIC ### Filling NULLs
# MAGIC ```python
# MAGIC df.na.fill(0)                   # Fill all numeric nulls with 0
# MAGIC df.na.fill("")                  # Fill all string nulls with ""
# MAGIC df.na.fill({"age": 0, "name": "Unknown"})  # Column-specific fills
# MAGIC df.fillna({"salary": 50000})    # Same as df.na.fill()
# MAGIC ```
# MAGIC
# MAGIC ### Replacing Values
# MAGIC ```python
# MAGIC df.na.replace(["old"], ["new"])          # Replace in all string cols
# MAGIC df.na.replace({"M": "Male"}, subset=["gender"])  # In specific column
# MAGIC df.na.replace([float("nan")], [None])   # Replace NaN with NULL
# MAGIC ```
# MAGIC
# MAGIC ### Key Insight: fill() type matching
# MAGIC `fill(0)` only fills NUMERIC columns. `fill("")` only fills STRING columns.
# MAGIC Use a dictionary for mixed types: `fill({"num_col": 0, "str_col": "N/A"})`

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: NULL detection
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: NULL Detection
# ============================================================
# Real-world: Profile NULLs before cleaning.

from pyspark.sql import SparkSession  # Import SparkSession.
from pyspark.sql.functions import (  # Import functions.
    col, isnan, isnull, when, count, sum as spark_sum, lit
)  # End imports.

spark = SparkSession.builder.getOrCreate()  # Get session.

# Sample data with various NULL patterns.
employees = spark.createDataFrame([
    (1, "Alice", 75000.0, "Engineering", "NYC", "2020-03-15"),
    (2, "Bob", None, "Sales", None, "2019-06-01"),          # Salary NULL, city NULL.
    (3, "Charlie", 82000.0, None, "Chicago", "2021-01-10"), # Dept NULL.
    (4, None, 65000.0, "Marketing", "Boston", None),         # Name NULL, date NULL.
    (5, "Eve", float("nan"), "Engineering", "NYC", "2022-07-20"),  # NaN (not NULL!).
    (6, "Frank", 90000.0, "Sales", "Seattle", "2020-11-05"),
    (7, "Grace", None, None, None, "2023-01-15"),            # Multiple NULLs.
], "id INT, name STRING, salary DOUBLE, dept STRING, city STRING, hire_date STRING")

print("=== Original Data ===")  # Print heading.
employees.show(truncate=False)  # Display.

# NULL profile: count NULLs per column.
print("=== NULL Profile Per Column ===")  # Print heading.
null_counts = employees.select([
    spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)  # Count NULLs.
    for c in employees.columns  # For each column.
])
null_counts.show()  # Display counts.

# Also detect NaN in numeric columns.
print("=== NaN Detection ===")  # Print heading.
nan_counts = employees.select(
    spark_sum(when(isnan(col("salary")), 1).otherwise(0)).alias("salary_nan"),  # NaN.
    spark_sum(when(col("salary").isNull(), 1).otherwise(0)).alias("salary_null"),  # NULL.
)
nan_counts.show()  # Display.
print("KEY: NaN and NULL are DIFFERENT! isNull() won't catch NaN.")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Dropping NULLs
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Dropping NULLs (df.na.drop)
# ============================================================
# Real-world: Remove incomplete records based on business rules.

from pyspark.sql.functions import col  # Import.

print("=== Method 1: Drop if ANY column is null ===")  # Heading.
any_null = employees.na.drop("any")  # Drop rows with any NULL.
print(f"Rows remaining: {any_null.count()} (from {employees.count()})")  # Count.
any_null.show(truncate=False)  # Display.

print("=== Method 2: Drop if ALL columns are null ===")  # Heading.
all_null = employees.na.drop("all")  # Drop only if ALL are NULL.
print(f"Rows remaining: {all_null.count()}")  # Count.

print("=== Method 3: Threshold-based (keep if >= 5 non-null values) ===")  # Heading.
thresh = employees.na.drop(thresh=5)  # At least 5 non-null values.
print(f"Rows remaining: {thresh.count()}")  # Count.
thresh.show(truncate=False)  # Display.

print("=== Method 4: Drop if critical columns are null ===")  # Heading.
critical = employees.na.drop(subset=["name", "salary"])  # Name and salary required.
print(f"Rows remaining: {critical.count()}")  # Count.
critical.show(truncate=False)  # Display.

print("=== Method 5: Combined (any null in subset) ===")  # Heading.
combined = employees.na.drop("any", subset=["name", "dept"])  # Name AND dept required.
print(f"Rows remaining: {combined.count()}")  # Count.
combined.show(truncate=False)  # Display.

print("\n⚠️ Remember: Dropping removes data permanently. Prefer fill/impute when possible.")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Filling NULLs
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Filling NULLs (df.na.fill)
# ============================================================
# Real-world: Replace NULLs with sensible defaults.

from pyspark.sql.functions import col, coalesce, lit, when, isnan  # Imports.

print("=== Method 1: Fill all numeric NULLs with 0 ===")  # Heading.
fill_zero = employees.na.fill(0)  # Fill numeric cols with 0.
fill_zero.show(truncate=False)  # Display.

print("=== Method 2: Fill all string NULLs with 'Unknown' ===")  # Heading.
fill_str = employees.na.fill("Unknown")  # Fill string cols.
fill_str.show(truncate=False)  # Display.

print("=== Method 3: Column-specific fills (BEST PRACTICE) ===")  # Heading.
filled = employees.na.fill({
    "name": "UNKNOWN",       # Default name.
    "salary": 0.0,           # Zero salary (will impute later).
    "dept": "Unassigned",    # Default department.
    "city": "Remote",        # Default city.
    "hire_date": "1900-01-01",  # Sentinel date.
})
filled.show(truncate=False)  # Display.

# Handle NaN separately (fill() doesn't catch NaN!).
print("=== Method 4: Handle NaN in numeric columns ===")  # Heading.
clean = employees.withColumn(
    "salary",
    when(isnan(col("salary")), None)  # Convert NaN to NULL first.
        .otherwise(col("salary"))  # Keep valid values.
).na.fill({"salary": 0.0})  # Then fill NULL.
clean.show(truncate=False)  # Display.

# coalesce: first non-null from multiple candidates.
print("=== Method 5: coalesce() for fallback values ===")  # Heading.
result = employees.withColumn(
    "display_name",
    coalesce(col("name"), col("dept"), lit("Employee"))  # First non-null.
)
result.select("id", "name", "dept", "display_name").show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Statistical imputation
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Statistical Imputation
# ============================================================
# Real-world: Replace NULLs with mean/median/mode.

from pyspark.sql.functions import (  # Import functions.
    col, mean, median, count, when, isnan, lit, first, desc
)  # End imports.
from pyspark.ml.feature import Imputer  # ML Imputer.

# Salary data with NULLs and NaN.
print("=== Statistical Imputation Strategies ===")  # Heading.

# First: clean NaN to NULL for consistent handling.
clean_employees = employees.withColumn(
    "salary",
    when(isnan(col("salary")), None).otherwise(col("salary"))  # NaN -> NULL.
)

# Strategy 1: Mean imputation.
print("=== Strategy 1: Mean Imputation ===")  # Heading.
mean_salary = clean_employees.select(mean("salary")).collect()[0][0]  # Compute mean.
print(f"Mean salary (non-null): {mean_salary:.2f}")  # Display mean.

mean_filled = clean_employees.na.fill({"salary": mean_salary})  # Fill with mean.
mean_filled.select("id", "name", "salary").show()  # Display.

# Strategy 2: Median imputation (more robust to outliers).
print("=== Strategy 2: Median Imputation ===")  # Heading.
median_salary = clean_employees.select(median("salary")).collect()[0][0]  # Compute median.
print(f"Median salary: {median_salary:.2f}")  # Display median.

median_filled = clean_employees.na.fill({"salary": median_salary})  # Fill with median.
median_filled.select("id", "name", "salary").show()  # Display.

# Strategy 3: Mode imputation (for categorical columns).
print("=== Strategy 3: Mode Imputation (Categorical) ===")  # Heading.
mode_dept = clean_employees.filter(col("dept").isNotNull()).groupBy("dept").count().orderBy(desc("count")).first()[0]  # Mode.
print(f"Mode department: {mode_dept}")  # Display.

mode_filled = clean_employees.na.fill({"dept": mode_dept})  # Fill with mode.
mode_filled.select("id", "name", "dept").show()  # Display.

# Strategy 4: Group-based imputation (mean salary per department).
print("=== Strategy 4: Group-Based Imputation ===")  # Heading.
dept_means = clean_employees.filter(col("salary").isNotNull()).groupBy("dept").agg(
    mean("salary").alias("dept_mean_salary"),  # Mean per dept.
)
dept_means.show()  # Display dept means.

# Join back and fill.
group_filled = clean_employees.join(dept_means, "dept", "left").withColumn(
    "salary",
    when(col("salary").isNull(), col("dept_mean_salary"))  # Fill from group mean.
        .otherwise(col("salary"))  # Keep existing.
).drop("dept_mean_salary")  # Cleanup.

group_filled.select("id", "name", "dept", "salary").show()  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Forward-fill and indicators
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Forward-Fill & NULL Indicators
# ============================================================
# Real-world: Time series with gaps — carry forward last known value.

from pyspark.sql.functions import (  # Import functions.
    col, when, last, first, lit, count, sum as spark_sum
)  # End imports.
from pyspark.sql.window import Window  # Window.

# Time series: sensor readings with gaps.
sensor = spark.createDataFrame([
    ("2024-01-01 10:00", "sensor_1", 22.5),
    ("2024-01-01 11:00", "sensor_1", None),    # Gap.
    ("2024-01-01 12:00", "sensor_1", None),    # Gap.
    ("2024-01-01 13:00", "sensor_1", 23.1),
    ("2024-01-01 14:00", "sensor_1", None),    # Gap.
    ("2024-01-01 10:00", "sensor_2", 18.0),
    ("2024-01-01 11:00", "sensor_2", 18.5),
    ("2024-01-01 12:00", "sensor_2", None),    # Gap.
    ("2024-01-01 13:00", "sensor_2", 19.0),
], ["timestamp", "sensor_id", "temperature"])  # Sensor data.

print("=== Original Sensor Data (with gaps) ===")  # Heading.
sensor.show(truncate=False)  # Display.

# Forward-fill: carry last known value forward.
print("=== Forward Fill (LOCF: Last Observation Carried Forward) ===")  # Heading.
w_forward = Window.partitionBy("sensor_id").orderBy("timestamp").rowsBetween(
    Window.unboundedPreceding, 0  # All rows up to current.
)

ffilled = sensor.withColumn(
    "temp_ffill",
    last(col("temperature"), ignorenulls=True).over(w_forward)  # Last non-null.
)
ffilled.show(truncate=False)  # Display.

# Backward-fill: use next known value.
print("=== Backward Fill (NOCB: Next Observation Carried Backward) ===")  # Heading.
w_backward = Window.partitionBy("sensor_id").orderBy("timestamp").rowsBetween(
    0, Window.unboundedFollowing  # Current to end.
)

bfilled = sensor.withColumn(
    "temp_bfill",
    first(col("temperature"), ignorenulls=True).over(w_backward)  # Next non-null.
)
bfilled.show(truncate=False)  # Display.

# NULL indicator column (useful for ML).
print("=== NULL Indicator Column ===")  # Heading.
with_indicator = ffilled.withColumn(
    "was_imputed",
    when(col("temperature").isNull(), True).otherwise(False)  # Flag.
).drop("temperature").withColumnRenamed("temp_ffill", "temperature")  # Clean up.

with_indicator.show(truncate=False)  # Display.
print("TIP: NULL indicators help ML models learn that imputed values may be unreliable.")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: NULL-safe operations
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: NULL-Safe Operations
# ============================================================
# Real-world: Comparisons and joins involving NULLs.

from pyspark.sql.functions import col, when, coalesce, lit  # Imports.

# NULL-safe equality (eqNullSafe).
print("=== NULL-Safe Equality ===")  # Heading.
null_demo = spark.createDataFrame([
    (1, "A", "A"),
    (2, "B", None),
    (3, None, None),
    (4, None, "D"),
], ["id", "col1", "col2"])  # Demo data.

result = null_demo.withColumn(
    "equals_unsafe", col("col1") == col("col2"),  # Regular: NULL == NULL = NULL (not True).
).withColumn(
    "equals_safe", col("col1").eqNullSafe(col("col2")),  # NULL-safe: NULL == NULL = True.
)
result.show()  # Display.
print("Regular ==: NULL == NULL returns NULL (Unknown)")
print("eqNullSafe: NULL == NULL returns True\n")

# NULL-safe JOIN (using eqNullSafe or coalesce).
print("=== NULL-Safe JOIN ===")  # Heading.
left = spark.createDataFrame([
    (1, "A"), (2, None), (3, "C")
], ["id", "key"])  # Left.

right = spark.createDataFrame([
    ("A", 100), (None, 200), ("C", 300)
], ["key", "value"])  # Right.

# Regular join: NULL keys won't match.
print("Regular join (misses NULL-NULL match):")
left.join(right, left["key"] == right["key"], "inner").show()  # NULLs don't match.

# NULL-safe join using eqNullSafe.
print("NULL-safe join (matches NULL-NULL):")
left.join(right, left["key"].eqNullSafe(right["key"]), "inner").show()  # NULLs match.

# Filtering: isNull vs == None (WRONG!).
print("=== Correct NULL Filtering ===")  # Heading.
print("WRONG: df.filter(col('x') == None) — always returns empty!")
print("RIGHT: df.filter(col('x').isNull())")
print("RIGHT: df.filter(col('x').isNotNull())")

null_demo.filter(col("col1").isNull()).show()  # Correct way.
print("\nNULL in aggregations:")
print("- COUNT(col) skips NULLs")
print("- COUNT(*) counts all rows")
print("- SUM/AVG/MIN/MAX all skip NULLs")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Production NULL handling
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Production NULL Handling Pipeline
# ============================================================
# Real-world: Complete NULL strategy with layered approach.

from pyspark.sql.functions import (  # Import functions.
    col, when, isnan, isnull, count, sum as spark_sum,
    mean, median, lit, coalesce, last, current_timestamp
)  # End imports.
from pyspark.sql.window import Window  # Window.
from pyspark.sql import DataFrame  # Type.

def null_handling_pipeline(df, config):
    """
    Production NULL handling with layered strategy.
    Config: dict with column strategies.
    Example: {"salary": "mean", "dept": "mode", "city": "constant:Unknown"}
    """
    result = df  # Start with original.
    report = {}  # Audit report.
    
    for col_name, strategy in config.items():  # Each column.
        null_count = result.filter(col(col_name).isNull() | isnan(col(col_name))).count()  # Count.
        report[col_name] = {"nulls": null_count, "strategy": strategy}  # Track.
        
        # First: normalize NaN to NULL.
        if dict(result.dtypes).get(col_name) in ["double", "float"]:  # Numeric.
            result = result.withColumn(
                col_name, when(isnan(col(col_name)), None).otherwise(col(col_name))
            )
        
        # Apply strategy.
        if strategy == "mean":  # Mean imputation.
            val = result.select(mean(col_name)).collect()[0][0]  # Compute.
            result = result.na.fill({col_name: val})  # Fill.
        elif strategy == "median":  # Median imputation.
            val = result.select(median(col_name)).collect()[0][0]  # Compute.
            result = result.na.fill({col_name: val})  # Fill.
        elif strategy == "mode":  # Mode imputation.
            from pyspark.sql.functions import desc  # Import.
            val = result.filter(col(col_name).isNotNull()).groupBy(col_name).count().orderBy(desc("count")).first()[0]
            result = result.na.fill({col_name: val})  # Fill.
        elif strategy.startswith("constant:"):  # Constant fill.
            val = strategy.split(":", 1)[1]  # Extract value.
            result = result.na.fill({col_name: val})  # Fill.
        elif strategy == "drop":  # Drop rows.
            result = result.filter(col(col_name).isNotNull())  # Remove.
    
    # Print report.
    print(f"\n{'='*55}")
    print(f"  NULL HANDLING AUDIT REPORT")
    print(f"{'='*55}")
    for c, info in report.items():  # Report.
        print(f"  {c:15s} | NULLs: {info['nulls']:3d} | Strategy: {info['strategy']}")
    print(f"{'='*55}")
    print(f"  Rows before: {df.count()} | Rows after: {result.count()}")
    print(f"{'='*55}\n")
    
    return result  # Return clean data.

# Apply pipeline.
print("=== Production NULL Handling ===")  # Heading.
clean = null_handling_pipeline(employees, {
    "salary": "mean",                # Mean for salary.
    "dept": "mode",                  # Most common for dept.
    "city": "constant:Unknown",      # Constant for city.
    "name": "constant:UNNAMED",      # Constant for name.
    "hire_date": "constant:1900-01-01",  # Sentinel date.
})
clean.show(truncate=False)  # Display clean data.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: ML-ready NULL handling
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: ML-Ready NULL Handling
# ============================================================
# Real-world: Prepare data for ML with proper imputation + indicators.

from pyspark.sql.functions import (  # Import functions.
    col, when, isnan, mean, lit
)  # End imports.
from pyspark.ml.feature import Imputer  # Spark ML Imputer.

# ML dataset with NULLs.
ml_data = spark.createDataFrame([
    (1, 25.0, 50000.0, 3.0, "YES"),
    (2, 30.0, None, 5.0, "NO"),
    (3, None, 60000.0, None, "YES"),
    (4, 35.0, 70000.0, 7.0, "NO"),
    (5, 28.0, None, 4.0, "YES"),
    (6, None, 55000.0, 2.0, "NO"),
    (7, 40.0, 80000.0, 10.0, "YES"),
    (8, 32.0, 65000.0, None, "NO"),
], "id INT, age DOUBLE, salary DOUBLE, experience DOUBLE, target STRING")

print("=== ML Data with NULLs ===")  # Heading.
ml_data.show()  # Display.

# Strategy 1: Spark ML Imputer (mean/median/mode).
print("=== Spark ML Imputer (median strategy) ===")  # Heading.
numeric_cols = ["age", "salary", "experience"]  # Numeric columns.

imputer = Imputer(
    inputCols=numeric_cols,  # Input columns.
    outputCols=[f"{c}_imputed" for c in numeric_cols],  # Output columns.
    strategy="median",  # Use median (robust to outliers).
)

model = imputer.fit(ml_data)  # Fit imputer.
imputed = model.transform(ml_data)  # Transform.
imputed.show()  # Display.

# Strategy 2: Add NULL indicator columns (missingness as feature).
print("=== NULL Indicators for ML ===")  # Heading.
ml_ready = ml_data  # Start.
for c in numeric_cols:  # Each numeric column.
    ml_ready = ml_ready.withColumn(
        f"{c}_missing",  # Indicator column.
        when(col(c).isNull() | isnan(col(c)), 1).otherwise(0)  # 1 if missing.
    )

# Fill NULLs with median.
for c in numeric_cols:  # Each column.
    med = ml_data.select(mean(c)).collect()[0][0]  # Mean as fallback.
    ml_ready = ml_ready.na.fill({c: med})  # Fill.

print("ML-ready data with imputation + indicators:")
ml_ready.show(truncate=False)  # Display.
print("TIP: Missing indicators let the model learn that imputed values may be unreliable.")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Comprehensive NULL analysis
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Comprehensive NULL Analysis
# ============================================================
# Real-world: Full NULL audit function for any DataFrame.

from pyspark.sql.functions import (  # Import functions.
    col, when, isnan, count, sum as spark_sum, round as spark_round,
    lit, avg, collect_list, array_distinct
)  # End imports.

def null_audit(df, name="DataFrame"):
    """Complete NULL audit: counts, percentages, patterns."""
    total = df.count()  # Total rows.
    print(f"\n{'='*65}")
    print(f"  NULL AUDIT: {name} ({total:,} rows, {len(df.columns)} columns)")
    print(f"{'='*65}")
    print(f"  {'Column':<15} {'Type':<10} {'Nulls':<8} {'%':<8} {'Non-Null':<10} {'Status'}")
    print(f"  {'-'*15} {'-'*10} {'-'*8} {'-'*8} {'-'*10} {'-'*10}")
    
    for c in df.columns:  # Each column.
        dtype = dict(df.dtypes)[c]  # Data type.
        null_count = df.filter(col(c).isNull()).count()  # NULL count.
        
        # Also check NaN for numeric.
        nan_count = 0  # Default.
        if dtype in ["double", "float"]:  # Numeric.
            nan_count = df.filter(isnan(col(c))).count()  # NaN count.
        
        total_missing = null_count + nan_count  # Total missing.
        pct = round(total_missing / total * 100, 1)  # Percentage.
        non_null = total - total_missing  # Non-null.
        
        # Status indicator.
        if pct == 0:  # Perfect.
            status = "✅ Complete"
        elif pct < 5:  # Minor.
            status = "🟡 Low NULLs"
        elif pct < 30:  # Moderate.
            status = "🟠 Moderate"
        else:  # High.
            status = "🔴 High NULLs"
        
        print(f"  {c:<15} {dtype:<10} {total_missing:<8} {pct:<8} {non_null:<10} {status}")
    
    # Rows with NO nulls vs some nulls.
    complete_rows = df.na.drop("any").count()  # Complete rows.
    print(f"\n  Complete rows (no NULLs): {complete_rows:,} ({round(complete_rows/total*100,1)}%)")
    print(f"  Rows with ≥1 NULL: {total - complete_rows:,} ({round((total-complete_rows)/total*100,1)}%)")
    print(f"{'='*65}\n")

# Run audit on our sample data.
null_audit(employees, "Employee Table")  # Audit.

# Test on a clean DataFrame.
print("\n--- After cleaning ---")
clean_emp = employees.na.fill({"name": "Unknown", "salary": 0.0, "dept": "N/A", "city": "N/A", "hire_date": "N/A"})  # Fill.
null_audit(clean_emp, "Employee Table (Cleaned)")  # Audit again.

print("✅ NULL handling mastery complete!")  # Done.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with NULL Handling
# MAGIC
# MAGIC ### Mistake 1: Using == None instead of isNull()
# MAGIC ```python
# MAGIC # WRONG — always returns empty DataFrame!
# MAGIC df.filter(col("x") == None)
# MAGIC
# MAGIC # CORRECT
# MAGIC df.filter(col("x").isNull())
# MAGIC df.filter(col("x").isNotNull())
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: fill() type mismatch
# MAGIC ```python
# MAGIC # fill(0) only fills NUMERIC columns, not strings!
# MAGIC df.na.fill(0)  # String NULLs remain!
# MAGIC
# MAGIC # Use dict for mixed types:
# MAGIC df.na.fill({"num_col": 0, "str_col": "Unknown"})
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Forgetting NaN ≠ NULL
# MAGIC ```python
# MAGIC # isNull() does NOT catch NaN!
# MAGIC df.filter(col("x").isNull())  # Misses NaN values!
# MAGIC
# MAGIC # Handle both:
# MAGIC df.filter(col("x").isNull() | isnan(col("x")))
# MAGIC # Or convert NaN to NULL first:
# MAGIC df.withColumn("x", when(isnan(col("x")), None).otherwise(col("x")))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Mean imputation biasing data
# MAGIC ```python
# MAGIC # Mean imputation reduces variance and biases correlations!
# MAGIC # Better alternatives:
# MAGIC # - Median (robust to outliers)
# MAGIC # - Group-based mean (per category)
# MAGIC # - Add missing indicator column for ML
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: NULL in JOIN keys
# MAGIC ```python
# MAGIC # Regular join: NULL keys NEVER match!
# MAGIC df1.join(df2, "key")  # NULLs dropped silently!
# MAGIC
# MAGIC # Use eqNullSafe for NULL-matching joins:
# MAGIC df1.join(df2, df1["key"].eqNullSafe(df2["key"]))
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of NULL Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Use `na.drop()`, `na.fill()`, `na.replace()` on sample data.
# MAGIC 2. Filter rows where specific columns are NULL.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Change from drop-any to threshold-based dropping.
# MAGIC 4. Change fill strategy from constant to mean.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Build pipeline: detect NULLs, report counts, fill with appropriate strategy.
# MAGIC 6. Add NULL indicator columns alongside imputed values.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Handle time-series data with forward-fill and backward-fill.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build a reusable `null_audit()` function that profiles any DataFrame.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design strategy: when to drop, fill, impute, or flag NULLs.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Compare imputation methods: mean vs median vs group-based vs ML Imputer.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Handle: NaN vs NULL, NULL in JOINs, NULL in aggregations, NULL in CASE.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build configurable NULL pipeline with audit trail and rollback.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create decision tree: "Which NULL strategy for which data type/scenario?"

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all.
from pyspark.sql.window import Window  # Window.

# --- Level 1: Basic NULL operations ---
print("=== Level 1: Basic NULL Operations ===")  # Heading.
test = spark.createDataFrame([
    (1, "A", 10.0), (2, None, None), (3, "C", 30.0), (4, None, 40.0)
], "id INT, name STRING, value DOUBLE")  # Test data.

test.na.drop().show()  # Drop any NULL.
test.na.fill({"name": "X", "value": 0.0}).show()  # Fill.
test.filter(col("name").isNull()).show()  # Filter NULLs.

# --- Level 5: Reusable audit ---
print("\n=== Level 5: Reusable Null Audit ===")  # Heading.
def quick_null_report(df):
    """Quick null count for all columns."""
    exprs = [sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
    return df.select(exprs)  # Return counts.

quick_null_report(employees).show()  # Apply.

# --- Level 7: Comparison of methods ---
print("\n=== Level 7: Imputation Comparison ===")  # Heading.
compare = spark.createDataFrame([
    (1, 10.0), (2, None), (3, 100.0), (4, None), (5, 20.0), (6, 1000.0)  # Outlier.
], "id INT, value DOUBLE")  # With outlier.

m = compare.select(mean("value")).collect()[0][0]  # Mean.
med = compare.select(median("value")).collect()[0][0]  # Median.
print(f"Mean: {m:.1f} (skewed by outlier 1000)")
print(f"Median: {med:.1f} (robust to outlier)")
print("Recommendation: Use median for skewed data, mean for normal distributions.")

# --- Level 8: NaN vs NULL ---
print("\n=== Level 8: NaN vs NULL ===")  # Heading.
nan_test = spark.createDataFrame([
    (1, float("nan")), (2, None), (3, 10.0)
], "id INT, val DOUBLE")  # Both NaN and NULL.

nan_test.select(
    col("id"),
    col("val"),
    col("val").isNull().alias("is_null"),  # NULL check.
    isnan(col("val")).alias("is_nan"),  # NaN check.
    (col("val").isNull() | isnan(col("val"))).alias("is_missing"),  # Either.
).show()

print("✅ All NULL homework solutions complete!")  # Done.