# Databricks notebook source
# DBTITLE 1,NB_39 Header
# MAGIC %md
# MAGIC # NB_39 — Null Handling Functions
# MAGIC
# MAGIC **Module 5: Built-in Functions** | Notebook 39 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Detection: isNull(), isNotNull(), isnan()
# MAGIC * Replacement: coalesce(), ifnull(), nullif(), nvl(), nvl2(), nanvl()
# MAGIC * DataFrame methods: na.fill(), na.drop(), na.replace()
# MAGIC * NULL in expressions: NULL arithmetic, NULL comparisons, NULL in aggregations
# MAGIC * Patterns: NULL-safe equality (<=>), NULL ordering in sort
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (NULLs cause more bugs than any other concept)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is NULL?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is NULL? (Real-World Analogy)
# MAGIC
# MAGIC ### ❓ The Empty Mailbox
# MAGIC
# MAGIC NULL means "unknown" or "not applicable" — NOT zero, NOT empty string:
# MAGIC
# MAGIC | Real World | Data Equivalent | Is NULL? |
# MAGIC |---|---|---|
# MAGIC | No mailbox exists | NULL | YES |
# MAGIC | Empty mailbox | `""` (empty string) | NO |
# MAGIC | Mailbox has "0" letters | `0` | NO |
# MAGIC | Unknown contents | NULL | YES |
# MAGIC
# MAGIC ### Three-Valued Logic
# MAGIC SQL/Spark uses THREE truth values: TRUE, FALSE, **UNKNOWN (NULL)**
# MAGIC * `NULL = NULL` → UNKNOWN (not TRUE!)
# MAGIC * `NULL != 5` → UNKNOWN (not TRUE!)
# MAGIC * `NULL AND TRUE` → UNKNOWN
# MAGIC * `NULL OR TRUE` → TRUE
# MAGIC
# MAGIC ### Why NULLs Matter
# MAGIC * NULLs propagate through arithmetic: `5 + NULL = NULL`
# MAGIC * NULLs are excluded from aggregations: `avg(1, 2, NULL)` = 1.5 (not 1.0)
# MAGIC * NULLs break equality: `col == NULL` never matches! (use `isNull()`)

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How NULL Handling Works
# MAGIC %md
# MAGIC ## SECTION 2 — How NULL Handling Works
# MAGIC
# MAGIC ### Function Reference
# MAGIC ```
# MAGIC ┌──────────────────┬──────────────────┬──────────────────┐
# MAGIC │ DETECTION        │ REPLACEMENT      │ DATAFRAME OPS    │
# MAGIC │ isNull()         │ coalesce(a,b,c)  │ df.na.fill()     │
# MAGIC │ isNotNull()      │ ifnull(a, b)     │ df.na.drop()     │
# MAGIC │ isnan()          │ nvl(a, b)        │ df.na.replace()  │
# MAGIC │ col.eqNullSafe() │ nvl2(a, b, c)    │                  │
# MAGIC │                  │ nullif(a, b)     │                  │
# MAGIC │                  │ nanvl(a, b)      │                  │
# MAGIC └──────────────────┴──────────────────┴──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Behaviors
# MAGIC ```
# MAGIC NULL + 5          = NULL     (arithmetic propagation)
# MAGIC NULL = NULL       = UNKNOWN  (not TRUE!)
# MAGIC NULL <=> NULL     = TRUE     (null-safe equality)
# MAGIC COUNT(*)          counts NULLs
# MAGIC COUNT(col)        skips NULLs
# MAGIC SUM/AVG/MAX/MIN   skip NULLs
# MAGIC GROUP BY          puts NULLs in their own group
# MAGIC ORDER BY          NULLs go first by default (NULLS LAST to change)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: NULL detection
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: NULL Detection
# ============================================================
# Real-world: Finding missing data in datasets.

from pyspark.sql import SparkSession  # Import SparkSession.
from pyspark.sql.functions import (  # Import NULL functions.
    col, isnan, isnull, when, lit, count, sum as spark_sum
)  # End imports.

spark = SparkSession.builder.getOrCreate()  # Get active session.

# Data with NULLs, NaN, empty strings, and zeros.
data = spark.createDataFrame([
    (1, "Alice", 30.0, "NYC"),
    (2, "Bob", None, "Chicago"),     # NULL age.
    (3, None, 25.0, "Seattle"),      # NULL name.
    (4, "Diana", float('nan'), None),  # NaN age, NULL city.
    (5, "", 0.0, ""),                # Empty strings and zero (NOT NULL!).
], "id INT, name STRING, age DOUBLE, city STRING")  # Mixed data.

# Detection.
print("=== NULL and NaN Detection ===")  # Print heading.
data.select(
    col("id"),  # Keep id.
    col("name"),  # Name value.
    col("age"),  # Age value.
    col("name").isNull().alias("name_is_null"),  # True if NULL.
    col("name").isNotNull().alias("name_not_null"),  # True if not NULL.
    col("age").isNull().alias("age_is_null"),  # True if NULL.
    isnan(col("age")).alias("age_is_nan"),  # True if NaN.
    # Combined: NULL OR NaN.
    (col("age").isNull() | isnan(col("age"))).alias("age_missing"),  # Either NULL or NaN.
).show(truncate=False)  # Display detection results.

# Count NULLs per column.
print("=== NULL Counts Per Column ===")  # Print heading.
data.select(
    count("*").alias("total_rows"),  # Total rows.
    spark_sum(col("name").isNull().cast("int")).alias("name_nulls"),  # NULL names.
    spark_sum(col("age").isNull().cast("int")).alias("age_nulls"),  # NULL ages.
    spark_sum(when(isnan(col("age")), 1).otherwise(0)).alias("age_nans"),  # NaN ages.
    spark_sum(col("city").isNull().cast("int")).alias("city_nulls"),  # NULL cities.
    # Empty strings (NOT null!).
    spark_sum(when(col("name") == "", 1).otherwise(0)).alias("name_empty"),  # Empty names.
).show(truncate=False)  # Display NULL counts.

print("""KEY INSIGHT:
- NULL: absence of value (isNull() = True)
- NaN: "Not a Number" from invalid math (isnan() = True)
- Empty string "": IS a value, NOT null!
- Zero 0: IS a value, NOT null!""")  # Key insight.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: coalesce, nvl, ifnull
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: coalesce, nvl, ifnull
# ============================================================
# Real-world: Replacing NULLs with default values.

from pyspark.sql.functions import (  # Import replacement functions.
    col, coalesce, lit, expr, when, nanvl
)  # End imports.

# Data with NULLs.
employees = spark.createDataFrame([
    (1, "Alice", 5000.0, 500.0, None),
    (2, "Bob", 4000.0, None, 200.0),
    (3, "Charlie", None, None, None),  # All compensation NULL.
    (4, "Diana", 6000.0, 600.0, 300.0),  # All present.
], "id INT, name STRING, salary DOUBLE, bonus DOUBLE, commission DOUBLE")  # Compensation data.

# coalesce: returns first non-NULL value.
print("=== coalesce() — First Non-NULL ===")  # Print heading.
employees.select(
    col("name"),  # Keep name.
    col("salary"), col("bonus"), col("commission"),  # Original.
    # Get first available compensation.
    coalesce(col("salary"), col("bonus"), col("commission"), lit(0.0)).alias("primary_comp"),
    # Total compensation (NULL-safe).
    (coalesce(col("salary"), lit(0.0)) +
     coalesce(col("bonus"), lit(0.0)) +
     coalesce(col("commission"), lit(0.0))).alias("total_comp"),
).show(truncate=False)  # Display coalesce results.

# nvl (SQL function): if first is NULL, return second.
print("=== nvl() — NULL Replacement (SQL) ===")  # Print heading.
employees.select(
    col("name"),  # Keep name.
    col("salary"),  # Original.
    expr("nvl(salary, 0)").alias("salary_nvl"),  # Replace NULL with 0.
    expr("nvl(bonus, salary * 0.1)").alias("bonus_or_10pct"),  # NULL → 10% of salary.
    expr("ifnull(commission, 0)").alias("comm_ifnull"),  # ifnull = same as nvl.
).show(truncate=False)  # Display nvl results.

# nvl2: if first is NOT NULL return second, else return third.
print("=== nvl2() — Conditional on NULL ===")  # Print heading.
employees.select(
    col("name"),  # Keep name.
    col("bonus"),  # Original.
    # nvl2(x, val_if_not_null, val_if_null).
    expr("nvl2(bonus, 'Has Bonus', 'No Bonus')").alias("bonus_status"),
    # nullif: returns NULL if two values are equal.
    expr("nullif(salary, 5000)").alias("nullif_5000"),  # 5000 becomes NULL.
).show(truncate=False)  # Display nvl2 results.

# nanvl: replace NaN with alternative.
print("=== nanvl() — NaN Replacement ===")  # Print heading.
nan_df = spark.createDataFrame([
    (1, 10.0), (2, float('nan')), (3, None)
], "id INT, value DOUBLE")  # With NaN.

nan_df.select(
    col("id"), col("value"),  # Original.
    nanvl(col("value"), lit(0.0)).alias("nanvl_0"),  # NaN→0, NULL stays NULL.
    coalesce(nanvl(col("value"), lit(None)), lit(0.0)).alias("both_handled"),  # Handle both.
).show()  # Display NaN handling.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: na.fill, na.drop, na.replace
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: DataFrame NA Methods
# ============================================================
# Real-world: Bulk NULL handling across entire DataFrames.

from pyspark.sql.functions import col  # Import col.

# Data with various NULLs.
df = spark.createDataFrame([
    (1, "Alice", 30, 5000.0, "Engineering"),
    (2, "Bob", None, 4000.0, None),
    (3, None, 25, None, "Marketing"),
    (4, "Diana", None, None, None),
    (5, "Eve", 28, 4500.0, "Engineering"),
], "id INT, name STRING, age INT, salary DOUBLE, dept STRING")  # Mixed NULLs.

print("=== Original Data ===")  # Print heading.
df.show()  # Display original.

# na.fill() — replace NULLs with default values.
print("=== na.fill() — Fill All Columns ===")  # Print heading.
# Fill all string columns with "Unknown", all numeric with 0.
df.na.fill("Unknown").na.fill(0).show()  # Fill strings then numbers.

# Column-specific fill.
print("=== na.fill() — Column-Specific ===")  # Print heading.
df.na.fill({
    "name": "Unknown",  # Fill name NULLs.
    "age": 0,  # Fill age NULLs.
    "salary": 3000.0,  # Fill salary NULLs with default.
    "dept": "Unassigned",  # Fill dept NULLs.
}).show()  # Display filled data.

# na.drop() — remove rows with NULLs.
print("=== na.drop() — Remove Rows ===")  # Print heading.
print(f"Original row count: {df.count()}")  # Before.
print(f"After drop(any): {df.na.drop('any').count()}")  # Drop if ANY NULL.
print(f"After drop(all): {df.na.drop('all').count()}")  # Drop only if ALL NULL.
print(f"After drop(any, subset=['name','dept']): {df.na.drop('any', subset=['name','dept']).count()}")  # Specific cols.

# na.drop with threshold.
print(f"After drop(thresh=4): {df.na.drop(thresh=4).count()}")  # Keep if >= 4 non-null.

# na.replace() — replace specific values.
print("\n=== na.replace() — Value Substitution ===")  # Print heading.
df.na.replace({"Engineering": "Eng", "Marketing": "Mkt"}, subset=["dept"]).show()  # Replace values.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: NULL in expressions
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: NULL in Expressions
# ============================================================
# Real-world: Understanding NULL propagation in calculations.

from pyspark.sql.functions import (  # Import functions.
    col, lit, when, coalesce, expr, count, avg, sum as spark_sum, max as spark_max
)  # End imports.

# Data for NULL arithmetic.
calc = spark.createDataFrame([
    (1, 10.0, 5.0),
    (2, 20.0, None),
    (3, None, 3.0),
    (4, None, None),
], "id INT, a DOUBLE, b DOUBLE")  # Calculation data.

# NULL arithmetic propagation.
print("=== NULL Propagation in Arithmetic ===")  # Print heading.
calc.select(
    col("id"), col("a"), col("b"),  # Keep values.
    (col("a") + col("b")).alias("a_plus_b"),  # NULL + 5 = NULL.
    (col("a") * col("b")).alias("a_times_b"),  # NULL * 5 = NULL.
    (col("a") + lit(10)).alias("a_plus_10"),  # NULL + 10 = NULL.
    # NULL-safe addition.
    (coalesce(col("a"), lit(0.0)) + coalesce(col("b"), lit(0.0))).alias("safe_sum"),
).show(truncate=False)  # Display propagation.

# NULL in comparisons.
print("=== NULL in Comparisons ===")  # Print heading.
calc.select(
    col("id"), col("a"),  # Keep.
    (col("a") == lit(None)).alias("a_eq_null"),  # ALWAYS NULL! Never True!
    (col("a") != lit(None)).alias("a_neq_null"),  # ALWAYS NULL!
    col("a").isNull().alias("correct_null_check"),  # CORRECT way.
    col("a").eqNullSafe(lit(None)).alias("null_safe_eq"),  # True when both NULL.
    (col("a") > lit(5)).alias("a_gt_5"),  # NULL if a is NULL.
).show(truncate=False)  # Display comparison results.

# NULL in aggregations.
print("=== NULL in Aggregations ===")  # Print heading.
agg_data = spark.createDataFrame([
    (10.0,), (20.0,), (None,), (30.0,), (None,)
], "value DOUBLE")  # With NULLs.

agg_data.select(
    count("*").alias("count_star"),  # 5 (counts ALL rows including NULL).
    count(col("value")).alias("count_value"),  # 3 (skips NULLs!).
    spark_sum(col("value")).alias("sum_value"),  # 60 (skips NULLs).
    avg(col("value")).alias("avg_value"),  # 20 (60/3, not 60/5!).
    spark_max(col("value")).alias("max_value"),  # 30 (skips NULLs).
).show()  # Display aggregation behavior.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: NULL-safe patterns
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: NULL-Safe Patterns
# ============================================================
# Real-world: Writing NULL-safe joins, filters, and comparisons.

from pyspark.sql.functions import col, when, coalesce, lit, expr  # Imports.

# NULL-safe equality (<=>).
print("=== NULL-Safe Equality (<=>) ===")  # Print heading.
left = spark.createDataFrame([
    (1, "A"), (2, None), (3, "C")
], "id INT, key STRING")  # Left table.

right = spark.createDataFrame([
    (1, "A"), (2, None), (3, "D")
], "id INT, key STRING")  # Right table.

# Regular equality: NULL = NULL → UNKNOWN (doesn't match!).
print("Regular join (NULL != NULL):")
left.alias("l").join(
    right.alias("r"),
    col("l.key") == col("r.key"),  # Regular equality.
    "inner"
).select("l.id", "l.key").show()  # Row 2 is MISSING!

# NULL-safe equality: NULL <=> NULL → TRUE.
print("NULL-safe join (NULL <=> NULL):")
left.alias("l").join(
    right.alias("r"),
    col("l.key").eqNullSafe(col("r.key")),  # NULL-safe.
    "inner"
).select("l.id", "l.key").show()  # Row 2 IS included!

# NULL-safe filter patterns.
print("=== NULL-Safe Filtering ===")  # Print heading.
data = spark.createDataFrame([
    (1, "active"), (2, "inactive"), (3, None), (4, "active")
], "id INT, status STRING")  # With NULL status.

# WRONG: col("status") != "inactive" EXCLUDES NULLs!
print("WRONG (excludes NULLs):")
data.filter(col("status") != "inactive").show()  # Row 3 gone!

# CORRECT: explicitly include NULLs.
print("CORRECT (includes NULLs):")
data.filter(
    (col("status") != "inactive") | col("status").isNull()  # Include NULLs.
).show()  # Row 3 preserved!

# NULL ordering in sort.
print("=== NULL Ordering in sort() ===")  # Print heading.
from pyspark.sql.functions import asc_nulls_last, desc_nulls_first  # Imports.
sort_data = spark.createDataFrame([
    (1, 30), (2, None), (3, 25), (4, None), (5, 35)
], "id INT, age INT")  # With NULL ages.

print("Default (NULLs first in ASC):")
sort_data.orderBy(col("age").asc()).show()  # NULLs first.
print("NULLs last:")
sort_data.orderBy(asc_nulls_last(col("age"))).show()  # NULLs last.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: NULL handling strategies
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: NULL Handling Strategies
# ============================================================
# Real-world: Choosing the right strategy for different scenarios.

from pyspark.sql.functions import (  # Import functions.
    col, coalesce, when, lit, avg, last, first, count,
    sum as spark_sum, lag, lead, expr
)  # End imports.
from pyspark.sql.window import Window  # Import Window.

# Time-series data with gaps.
ts_data = spark.createDataFrame([
    ("sensor-1", "2024-01-01", 23.5),
    ("sensor-1", "2024-01-02", None),  # Missing.
    ("sensor-1", "2024-01-03", None),  # Missing.
    ("sensor-1", "2024-01-04", 25.0),
    ("sensor-1", "2024-01-05", 24.8),
    ("sensor-1", "2024-01-06", None),  # Missing.
    ("sensor-1", "2024-01-07", 26.0),
], "sensor STRING, date STRING, reading DOUBLE")  # Time series.

# Strategy 1: Forward-fill (carry last known value).
print("=== Strategy 1: Forward Fill ===")  # Print heading.
w = Window.partitionBy("sensor").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

ts_data.select(
    col("sensor"), col("date"), col("reading"),  # Original.
    last(col("reading"), ignorenulls=True).over(w).alias("forward_fill"),  # Carry forward.
).show(truncate=False)  # Display forward fill.

# Strategy 2: Replace with column average.
print("=== Strategy 2: Mean Imputation ===")  # Print heading.
mean_val = ts_data.select(avg("reading")).first()[0]  # Compute mean.
ts_data.select(
    col("date"), col("reading"),  # Original.
    coalesce(col("reading"), lit(mean_val)).alias("mean_filled"),  # Replace NULL with mean.
).show()  # Display mean fill.

# Strategy 3: Interpolation (average of neighbors).
print("=== Strategy 3: Linear Interpolation ===")  # Print heading.
w_full = Window.partitionBy("sensor").orderBy("date")

ts_data.select(
    col("date"), col("reading"),  # Original.
    lag(col("reading"), 1).over(w_full).alias("prev"),  # Previous value.
    lead(col("reading"), 1).over(w_full).alias("next"),  # Next value.
    # Interpolate: average of prev and next when NULL.
    when(col("reading").isNull(),
         (coalesce(lag(col("reading"), 1).over(w_full), lit(0)) +
          coalesce(lead(col("reading"), 1).over(w_full), lit(0))) / 2
    ).otherwise(col("reading")).alias("interpolated"),  # Interpolated.
).show(truncate=False)  # Display interpolation.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: NULL profiling at scale
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: NULL Profiling at Scale
# ============================================================
# Real-world: Automated data quality report for NULL analysis.

from pyspark.sql.functions import (  # Import functions.
    col, count, sum as spark_sum, when, isnan, lit, round as spark_round,
    countDistinct
)  # End imports.

# Create realistic dataset with various NULL patterns.
import random  # Random for generation.
random.seed(42)  # Reproducible.

profile_data = spark.createDataFrame([
    (i,
     f"name_{i}" if random.random() > 0.1 else None,  # 10% NULL.
     random.randint(20, 60) if random.random() > 0.2 else None,  # 20% NULL.
     round(random.uniform(30000, 100000), 2) if random.random() > 0.05 else None,  # 5% NULL.
     ["Eng", "Mkt", "Sales", "HR", None][random.randint(0, 4)],  # 20% NULL.
     random.choice(["M", "F", None]) if random.random() > 0.3 else None,  # 30% NULL.
    ) for i in range(100)
], "id INT, name STRING, age INT, salary DOUBLE, dept STRING, gender STRING")  # 100 rows.

# Build NULL profile for all columns.
print("=== Automated NULL Profile ===")  # Print heading.
total = profile_data.count()  # Total rows.

# Profile each column.
null_profile = profile_data.select(
    lit(total).alias("total_rows"),  # Total.
    *[spark_sum(col(c).isNull().cast("int")).alias(f"{c}_nulls") for c in profile_data.columns],  # NULL counts.
)

null_profile.show(truncate=False)  # Display NULL counts.

# Percentage view.
print("=== NULL Percentages ===")  # Print heading.
pct_profile = profile_data.select(
    *[spark_round(spark_sum(col(c).isNull().cast("int")) * 100.0 / lit(total), 1).alias(f"{c}_%null")
      for c in profile_data.columns]  # Percentage NULL.
)
pct_profile.show(truncate=False)  # Display percentages.

# Data quality summary.
print("=== Column Quality Summary ===")  # Print heading.
for c in profile_data.columns:  # Iterate columns.
    null_count = profile_data.filter(col(c).isNull()).count()  # Count NULLs.
    null_pct = round(null_count / total * 100, 1)  # Percentage.
    distinct = profile_data.select(countDistinct(c)).first()[0]  # Distinct values.
    print(f"  {c:10s}: {null_pct:5.1f}% NULL, {distinct} distinct values")  # Print stats.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: NULL-aware data pipeline
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: NULL-Aware Data Pipeline
# ============================================================
# Real-world: Complete NULL handling in ETL pipelines.

from pyspark.sql.functions import (  # Import functions.
    col, coalesce, when, lit, isnan, nanvl, trim, lower,
    current_timestamp, count, avg, expr
)  # End imports.

# Raw data with all kinds of NULL issues.
raw = spark.createDataFrame([
    (1, "  Alice  ", 30.0, 5000.0, "Engineering", "active"),
    (2, "Bob", float('nan'), 4000.0, None, "active"),
    (3, "", None, None, "Marketing", None),  # Empty name!
    (4, None, 25.0, 3500.0, "  ", "inactive"),  # Whitespace dept!
    (5, "Eve", 28.0, float('nan'), "Engineering", "active"),
], "id INT, name STRING, age DOUBLE, salary DOUBLE, dept STRING, status STRING")  # Raw data.

print("=== Raw Data ===")  # Print heading.
raw.show(truncate=False)  # Display raw.

# Step 1: Normalize NULLs (treat empty/whitespace as NULL).
print("=== Step 1: Normalize NULLs ===")  # Print heading.
normalized = raw.select(
    col("id"),  # Keep id.
    # Treat empty/whitespace strings as NULL.
    when(trim(col("name")) == "", None).otherwise(trim(col("name"))).alias("name"),
    # NaN to NULL for numeric.
    when(isnan(col("age")), None).otherwise(col("age")).alias("age"),
    when(isnan(col("salary")), None).otherwise(col("salary")).alias("salary"),
    # Whitespace strings to NULL.
    when(trim(col("dept")) == "", None).otherwise(trim(col("dept"))).alias("dept"),
    col("status"),  # Keep as-is.
)
normalized.show(truncate=False)  # Display normalized.

# Step 2: Apply imputation rules.
print("=== Step 2: Imputation ===")  # Print heading.
avg_age = normalized.select(avg("age")).first()[0]  # Mean age.
avg_salary = normalized.select(avg("salary")).first()[0]  # Mean salary.

imputed = normalized.select(
    col("id"),  # Keep id.
    coalesce(col("name"), lit("Unknown")).alias("name"),  # Default name.
    coalesce(col("age"), lit(round(avg_age, 0))).alias("age"),  # Mean imputation.
    coalesce(col("salary"), lit(round(avg_salary, 0))).alias("salary"),  # Mean imputation.
    coalesce(col("dept"), lit("Unassigned")).alias("dept"),  # Default dept.
    coalesce(col("status"), lit("unknown")).alias("status"),  # Default status.
)

print("After imputation:")
imputed.show(truncate=False)  # Display imputed.

# Verify: no NULLs remain.
print("=== Verification: Zero NULLs ===")  # Print heading.
for c in imputed.columns:  # Check each column.
    null_count = imputed.filter(col(c).isNull()).count()  # Count.
    print(f"  {c}: {null_count} NULLs")  # Should all be 0.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production NULL utilities
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Production NULL Utilities
# ============================================================
# Real-world: Reusable NULL handling functions.

from pyspark.sql.functions import (  # Import functions.
    col, when, lit, coalesce, isnan, trim, count,
    sum as spark_sum, round as spark_round
)  # End imports.
from pyspark.sql import DataFrame, Column  # Types.
from functools import reduce  # For combining columns.

# === Utility: Normalize all NULL variants ===
def normalize_nulls(df, string_cols=None, numeric_cols=None):
    """Convert empty strings, whitespace, and NaN to proper NULLs."""
    result = df  # Start with original.
    for c in (string_cols or []):  # String columns.
        result = result.withColumn(c,
            when(trim(col(c)) == "", None).otherwise(trim(col(c)))  # Empty/whitespace → NULL.
        )
    for c in (numeric_cols or []):  # Numeric columns.
        result = result.withColumn(c,
            when(isnan(col(c)), None).otherwise(col(c))  # NaN → NULL.
        )
    return result  # Return normalized.

# === Utility: NULL completeness score ===
def completeness_score(df):
    """Compute percentage of non-null values per column."""
    total = df.count()  # Total rows.
    scores = {}  # Accumulator.
    for c in df.columns:  # Iterate.
        non_null = df.filter(col(c).isNotNull()).count()  # Count non-null.
        scores[c] = round(non_null / total * 100, 1)  # Percentage.
    return scores  # Return dict.

# === Utility: Row completeness ===
def add_row_completeness(df, cols=None):
    """Add column showing % of non-null fields per row."""
    check_cols = cols or df.columns  # Default: all.
    n = len(check_cols)  # Total fields.
    non_null_count = reduce(
        lambda a, b: a + b,
        [col(c).isNotNull().cast("int") for c in check_cols]  # Count non-nulls.
    )
    return df.withColumn(
        "_completeness_pct",
        spark_round(non_null_count * 100.0 / lit(n), 1)  # Percentage.
    )

# === Apply utilities ===
print("=== Production NULL Pipeline ===")  # Print heading.
test_df = spark.createDataFrame([
    (1, "Alice", 30.0, "NYC", "active"),
    (2, "", float('nan'), None, ""),
    (3, "  ", 25.0, "LA", None),
    (4, "Diana", None, "Chicago", "inactive"),
], "id INT, name STRING, age DOUBLE, city STRING, status STRING")  # Test data.

# Normalize.
cleaned = normalize_nulls(test_df, string_cols=["name", "city", "status"], numeric_cols=["age"])
print("After normalization:")
cleaned.show(truncate=False)  # Display.

# Add row completeness.
with_score = add_row_completeness(cleaned, ["name", "age", "city", "status"])
print("With completeness score:")
with_score.show(truncate=False)  # Display with score.

# Column completeness.
print("=== Column Completeness ===")  # Print heading.
scores = completeness_score(cleaned)
for col_name, pct in scores.items():  # Print scores.
    print(f"  {col_name}: {pct}% complete")

print("\n✅ NULL handling mastery complete!")  # Done.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with NULLs
# MAGIC
# MAGIC ### Mistake 1: Using == to check for NULL
# MAGIC ```python
# MAGIC # WRONG — col == NULL is ALWAYS UNKNOWN (never matches!).
# MAGIC df.filter(col("name") == None)  # Returns 0 rows!
# MAGIC df.filter(col("name") == lit(None))  # Still 0 rows!
# MAGIC
# MAGIC # CORRECT.
# MAGIC df.filter(col("name").isNull())  # Works!
# MAGIC df.filter(col("name").isNotNull())  # Works!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Forgetting NULLs in != filters
# MAGIC ```python
# MAGIC # WRONG — != excludes NULLs silently!
# MAGIC df.filter(col("status") != "inactive")  # NULLs are EXCLUDED!
# MAGIC
# MAGIC # CORRECT — explicitly include NULLs.
# MAGIC df.filter((col("status") != "inactive") | col("status").isNull())
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Assuming count(*) == count(column)
# MAGIC ```python
# MAGIC # count(*) = 100 (counts all rows, including NULLs)
# MAGIC # count(col) = 80 (skips NULLs!)
# MAGIC # This also affects avg(): avg = sum / count(col), NOT sum / count(*)
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Confusing NaN and NULL
# MAGIC ```python
# MAGIC # NaN is NOT NULL! They are different:
# MAGIC # isnan(NaN) = True,  isNull(NaN) = False
# MAGIC # isnan(NULL) = NULL, isNull(NULL) = True
# MAGIC # Handle both: (isnan(col) | isNull(col))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: NULL in concat_ws silently disappears
# MAGIC ```python
# MAGIC # concat_ws("|", "a", NULL, "c") = "a|c" (NULL skipped!)
# MAGIC # This means different data can produce same hash!
# MAGIC # Fix: coalesce(col, lit("__NULL__")) before hashing.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of NULL Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Use isNull(), isNotNull(), isnan() to detect missing values.
# MAGIC 2. Use coalesce() and nvl() to replace NULLs with defaults.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Change na.fill() to use column-specific defaults.
# MAGIC 4. Modify na.drop() threshold to keep rows with at least 3 non-null values.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Normalize: convert empty strings and NaN to NULL, then apply coalesce.
# MAGIC 6. Use NULL-safe equality (eqNullSafe) in a join.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Build a NULL report: for each column, show count, percentage, and data type.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Implement forward-fill and linear interpolation for time-series NULL gaps.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design a configurable imputation engine: mean for numeric, mode for categorical, forward-fill for time-series.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Compare: na.fill() vs withColumn+coalesce on 10M rows with 50% NULLs.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Test: NULL vs NaN vs empty string vs whitespace. NULL in GROUP BY, ORDER BY, DISTINCT.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build a data quality gate: fail pipeline if any column exceeds 20% NULL.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create a NULL behavior cheat sheet covering all operators and aggregations.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all.

# --- Level 1: Detection and replacement ---
print("=== Level 1: NULL Detection ===")  # Print heading.
test = spark.createDataFrame([
    (1, "A", 10.0), (2, None, None), (3, "", float('nan'))
], "id INT, name STRING, value DOUBLE")  # Test data.

test.select(
    col("id"),  # Keep.
    col("name").isNull().alias("name_null"),  # Detect.
    col("value").isNull().alias("val_null"),  # Detect.
    isnan(col("value")).alias("val_nan"),  # Detect NaN.
    coalesce(col("name"), lit("UNKNOWN")).alias("name_filled"),  # Replace.
    coalesce(col("value"), lit(0.0)).alias("val_filled"),  # Replace.
).show(truncate=False)  # Display.

# --- Level 6: NULL-safe join ---
print("=== Level 6: NULL-Safe Join ===")  # Print heading.
left = spark.createDataFrame([(1, "A"), (2, None), (3, "C")], "id INT, key STRING")
right = spark.createDataFrame([(1, "A"), (2, None), (3, "D")], "id INT, key STRING")

print("Regular join (misses NULL=NULL):")
left.join(right, left["key"] == right["key"]).select(left["id"]).show()

print("NULL-safe join (matches NULL=NULL):")
left.join(right, left["key"].eqNullSafe(right["key"])).select(left["id"]).show()

# --- Level 9: Quality gate ---
print("=== Level 9: Quality Gate ===")  # Print heading.
def quality_gate(df, max_null_pct=20.0):
    """Fail if any column exceeds max_null_pct% NULLs."""
    total = df.count()  # Total rows.
    failures = []  # Track failures.
    for c in df.columns:  # Check each.
        null_pct = df.filter(col(c).isNull()).count() / total * 100
        if null_pct > max_null_pct:  # Exceeds threshold.
            failures.append((c, round(null_pct, 1)))  # Record.
    if failures:  # Any failures?
        print(f"❌ QUALITY GATE FAILED! Columns exceeding {max_null_pct}% NULL:")
        for c, pct in failures:
            print(f"   {c}: {pct}%")
    else:
        print(f"✅ QUALITY GATE PASSED! All columns below {max_null_pct}% NULL.")
    return len(failures) == 0  # Return pass/fail.

# Test with high-NULL data.
high_null = spark.createDataFrame([
    (1, "A", None), (2, None, None), (3, "C", 10.0), (4, None, None)
], "id INT, name STRING, value DOUBLE")

quality_gate(high_null, max_null_pct=20.0)  # Should FAIL.

print("\n✅ All homework solutions complete!")  # Completion message.