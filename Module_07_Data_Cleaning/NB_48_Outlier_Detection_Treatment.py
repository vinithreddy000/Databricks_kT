# Databricks notebook source
# DBTITLE 1,NB_48 Header
# MAGIC %md
# MAGIC # NB_48 — Outlier Detection and Treatment
# MAGIC
# MAGIC **Module 7: Data Cleaning & Quality** | Notebook 48 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Statistical outlier methods: Z-score, Modified Z-score (MAD)
# MAGIC * IQR (Interquartile Range) method
# MAGIC * Domain-based constraints (business rules)
# MAGIC * Percentile-based capping/winsorization
# MAGIC * Isolation Forest (ML-based)
# MAGIC * Treatment strategies: remove, cap, flag, transform
# MAGIC * Multivariate outlier detection
# MAGIC * Production outlier pipeline with audit
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐⭐ (Requires statistical understanding)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are Outliers?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are Outliers? (Real-World Analogy)
# MAGIC
# MAGIC ### 📦 The Quality Inspector
# MAGIC
# MAGIC Outliers are like products that fall outside acceptable specs:
# MAGIC
# MAGIC | Analogy | Data Example | Action |
# MAGIC |---|---|---|
# MAGIC | Product too large | Salary = $10M (data entry error) | Remove/Cap |
# MAGIC | Product slightly off | Height = 6.8ft (tall but valid) | Keep (real outlier) |
# MAGIC | Wrong product on line | Age = -5 (impossible) | Remove (error) |
# MAGIC | Rare but valid | Order = $50K (big but legitimate) | Flag for review |
# MAGIC
# MAGIC ### Types of Outliers
# MAGIC 1. **Data errors:** Impossible values (negative age, future birth dates)
# MAGIC 2. **Measurement errors:** Sensor glitches, typos (salary 100000 vs 1000000)
# MAGIC 3. **Natural outliers:** Extreme but valid (CEO salary vs average employee)
# MAGIC 4. **Contextual outliers:** Normal in one context, outlier in another
# MAGIC
# MAGIC ### Detection Methods Summary
# MAGIC ```
# MAGIC ┌──────────────┬──────────────────┬────────────────────┐
# MAGIC │ Method       │ Best For           │ Assumption           │
# MAGIC ├──────────────┼──────────────────┼────────────────────┤
# MAGIC │ Z-Score      │ Normal data        │ Gaussian distribution│
# MAGIC │ IQR          │ Skewed data        │ None (robust)        │
# MAGIC │ MAD          │ Heavy-tailed data  │ None (very robust)   │
# MAGIC │ Percentile   │ Any distribution   │ None                 │
# MAGIC │ Domain Rules │ Known constraints  │ Business knowledge   │
# MAGIC └──────────────┴──────────────────┴────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 2 — Detection Methods Explained
# MAGIC %md
# MAGIC ## SECTION 2 — Detection Methods Explained
# MAGIC
# MAGIC ### Z-Score Method
# MAGIC ```
# MAGIC z = (value - mean) / std_dev
# MAGIC Outlier if |z| > 3 (99.7% rule for normal data)
# MAGIC ```
# MAGIC **Weakness:** Mean and std are themselves affected by outliers!
# MAGIC
# MAGIC ### IQR Method (Tukey's Fences)
# MAGIC ```
# MAGIC Q1 = 25th percentile
# MAGIC Q3 = 75th percentile
# MAGIC IQR = Q3 - Q1
# MAGIC Lower fence = Q1 - 1.5 * IQR
# MAGIC Upper fence = Q3 + 1.5 * IQR
# MAGIC Outlier if value < lower_fence OR value > upper_fence
# MAGIC ```
# MAGIC **Strength:** Robust — not affected by extreme values.
# MAGIC
# MAGIC ### Modified Z-Score (MAD)
# MAGIC ```
# MAGIC MAD = median(|value - median|)
# MAGIC Modified Z = 0.6745 * (value - median) / MAD
# MAGIC Outlier if |Modified Z| > 3.5
# MAGIC ```
# MAGIC **Strength:** Even more robust than IQR.
# MAGIC
# MAGIC ### Treatment Strategies
# MAGIC | Strategy | When to Use |
# MAGIC |---|---|
# MAGIC | **Remove** | Clearly erroneous data |
# MAGIC | **Cap/Winsorize** | Keep data, limit extremes |
# MAGIC | **Flag** | Need human review |
# MAGIC | **Transform** | Log/sqrt to reduce skew |
# MAGIC | **Impute** | Replace with boundary value |

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Z-Score method
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Z-Score Method
# ============================================================
# Real-world: Detect salary outliers using standard deviations.

from pyspark.sql import SparkSession  # Import.
from pyspark.sql.functions import (  # Import functions.
    col, mean, stddev, abs as spark_abs, when, count, lit, round as spark_round
)  # End imports.

spark = SparkSession.builder.getOrCreate()  # Session.

# Employee salary data with outliers.
salaries = spark.createDataFrame([
    (1, "Alice", 75000.0),
    (2, "Bob", 82000.0),
    (3, "Charlie", 78000.0),
    (4, "Diana", 90000.0),
    (5, "Eve", 85000.0),
    (6, "Frank", 72000.0),
    (7, "Grace", 88000.0),
    (8, "Hank", 500000.0),    # Outlier! (maybe CEO, maybe error).
    (9, "Ivy", 79000.0),
    (10, "Jack", 1000.0),     # Outlier! (likely error).
    (11, "Kate", 81000.0),
    (12, "Leo", 95000.0),
], ["id", "name", "salary"])  # Salary data.

print("=== Salary Data ===")  # Heading.
salaries.show()  # Display.

# Compute Z-scores.
print("=== Z-Score Outlier Detection ===")  # Heading.
stats = salaries.select(
    mean("salary").alias("mean_salary"),  # Mean.
    stddev("salary").alias("std_salary"),  # Std dev.
).collect()[0]  # Collect stats.

mean_val = stats["mean_salary"]  # Mean.
std_val = stats["std_salary"]  # Std.
print(f"Mean: ${mean_val:,.0f}")  # Print.
print(f"Std Dev: ${std_val:,.0f}")  # Print.
print(f"Threshold: |z| > 3 (values beyond 3 std deviations)\n")  # Rule.

# Add Z-score column.
z_scored = salaries.withColumn(
    "z_score",
    spark_round((col("salary") - lit(mean_val)) / lit(std_val), 2)  # Z-score.
).withColumn(
    "is_outlier",
    spark_abs(col("z_score")) > 3  # Flag outliers.
).withColumn(
    "outlier_type",
    when(col("z_score") > 3, "HIGH")
    .when(col("z_score") < -3, "LOW")
    .otherwise("NORMAL")  # Classify.
)

z_scored.show(truncate=False)  # Display.

# Summary.
print(f"Total rows: {z_scored.count()}")  # Total.
print(f"Outliers detected: {z_scored.filter(col('is_outlier')).count()}")  # Outliers.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: IQR method
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: IQR (Interquartile Range) Method
# ============================================================
# Real-world: Robust outlier detection that handles skewed data.

from pyspark.sql.functions import (  # Import functions.
    col, when, lit, round as spark_round, expr, percentile_approx
)  # End imports.

print("=== IQR Outlier Detection ===")  # Heading.

# Compute quartiles.
quantiles = salaries.select(
    percentile_approx("salary", 0.25).alias("Q1"),  # 25th percentile.
    percentile_approx("salary", 0.50).alias("Q2"),  # Median.
    percentile_approx("salary", 0.75).alias("Q3"),  # 75th percentile.
).collect()[0]  # Collect.

Q1 = quantiles["Q1"]  # Q1.
Q3 = quantiles["Q3"]  # Q3.
IQR = Q3 - Q1  # Interquartile range.
lower_fence = Q1 - 1.5 * IQR  # Lower bound.
upper_fence = Q3 + 1.5 * IQR  # Upper bound.

print(f"Q1 (25th): ${Q1:,.0f}")  # Q1.
print(f"Q3 (75th): ${Q3:,.0f}")  # Q3.
print(f"IQR: ${IQR:,.0f}")  # IQR.
print(f"Lower fence: ${lower_fence:,.0f}")  # Lower.
print(f"Upper fence: ${upper_fence:,.0f}")  # Upper.

# Detect outliers.
iqr_result = salaries.withColumn(
    "is_outlier",
    (col("salary") < lit(lower_fence)) | (col("salary") > lit(upper_fence))  # Outside fences.
).withColumn(
    "outlier_type",
    when(col("salary") < lit(lower_fence), "BELOW_LOWER")
    .when(col("salary") > lit(upper_fence), "ABOVE_UPPER")
    .otherwise("NORMAL")  # Classify.
).withColumn(
    "distance_from_fence",
    when(col("salary") < lit(lower_fence), col("salary") - lit(lower_fence))
    .when(col("salary") > lit(upper_fence), col("salary") - lit(upper_fence))
    .otherwise(lit(0))  # Distance.
)

print("\n=== Results ===")  # Heading.
iqr_result.show(truncate=False)  # Display.

# Compare Z-score vs IQR.
print("=== Comparison: Z-Score finds 0 outliers, IQR finds 2 ===")  # Heading.
print("IQR is MORE SENSITIVE — better for skewed data.")
print("Z-Score is influenced by extreme values (inflated std dev).")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Domain-based constraints
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Domain-Based Constraints
# ============================================================
# Real-world: Business rules define what's valid.

from pyspark.sql.functions import (
    col, when, lit, current_date, datediff, to_date
)  # Imports.

# Employee data with various issues.
employees = spark.createDataFrame([
    (1, "Alice", 28, 75000.0, "2020-03-15"),
    (2, "Bob", 150, 82000.0, "2019-06-01"),     # Age 150? Impossible!
    (3, "Charlie", -5, 60000.0, "2021-01-10"),  # Negative age?
    (4, "Diana", 35, -50000.0, "2022-07-20"),   # Negative salary?
    (5, "Eve", 25, 0.0, "2030-01-01"),          # Future hire date?
    (6, "Frank", 42, 5000000.0, "2020-11-05"),  # $5M salary?
    (7, "Grace", 55, 95000.0, "2018-04-10"),    # Normal.
    (8, "Hank", 22, 45000.0, "2024-01-15"),     # Normal.
], ["id", "name", "age", "salary", "hire_date"])  # Employees.

print("=== Domain-Based Outlier Detection ===")  # Heading.
employees.show(truncate=False)  # Display.

# Define business rules.
print("=== Business Rule Violations ===")  # Heading.
validated = employees.withColumn(
    "hire_dt", to_date(col("hire_date"))  # Parse date.
).withColumn(
    "age_valid",
    (col("age") >= 16) & (col("age") <= 100)  # Age: 16-100.
).withColumn(
    "salary_valid",
    (col("salary") > 0) & (col("salary") <= 1000000)  # Salary: $0-$1M.
).withColumn(
    "date_valid",
    col("hire_dt") <= current_date()  # Not in future.
).withColumn(
    "all_valid",
    col("age_valid") & col("salary_valid") & col("date_valid")  # All rules pass.
)

validated.select("id", "name", "age_valid", "salary_valid", "date_valid", "all_valid").show()

# Violation details.
print("=== Violation Details ===")  # Heading.
violations = validated.filter(~col("all_valid")).select(  # Only violations.
    col("id"), col("name"),
    when(~col("age_valid"), "Invalid age").alias("age_issue"),
    when(~col("salary_valid"), "Invalid salary").alias("salary_issue"),
    when(~col("date_valid"), "Future date").alias("date_issue"),
)
violations.show(truncate=False)  # Display.

# Keep only valid rows.
clean = validated.filter(col("all_valid")).select(employees.columns)  # Clean.
print(f"Valid rows: {clean.count()} / {employees.count()}")  # Summary.
clean.show()  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Winsorization (capping)
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Winsorization (Capping)
# ============================================================
# Real-world: Cap extreme values instead of removing them.

from pyspark.sql.functions import (
    col, when, lit, percentile_approx, mean, stddev, round as spark_round
)  # Imports.

print("=== Winsorization: Cap Extreme Values ===")  # Heading.
print("Strategy: Replace outliers with boundary values (preserve data count).\n")

# Method 1: Percentile capping (1st and 99th percentile).
print("--- Method 1: Percentile Capping (1st/99th) ---")
caps = salaries.select(
    percentile_approx("salary", 0.01).alias("p01"),  # 1st percentile.
    percentile_approx("salary", 0.99).alias("p99"),  # 99th percentile.
).collect()[0]

p01, p99 = caps["p01"], caps["p99"]  # Boundaries.
print(f"1st percentile: ${p01:,.0f}")
print(f"99th percentile: ${p99:,.0f}")

winsorized_pct = salaries.withColumn(
    "salary_capped",
    when(col("salary") < lit(p01), lit(p01))  # Cap low.
    .when(col("salary") > lit(p99), lit(p99))  # Cap high.
    .otherwise(col("salary"))  # Keep.
).withColumn(
    "was_capped",
    col("salary") != col("salary_capped")  # Track changes.
)
winsorized_pct.show(truncate=False)

# Method 2: IQR-based capping.
print("--- Method 2: IQR-Based Capping ---")
Q1 = salaries.select(percentile_approx("salary", 0.25)).collect()[0][0]  # Q1.
Q3 = salaries.select(percentile_approx("salary", 0.75)).collect()[0][0]  # Q3.
IQR = Q3 - Q1  # IQR.
lower_cap = Q1 - 1.5 * IQR  # Lower.
upper_cap = Q3 + 1.5 * IQR  # Upper.

winsorized_iqr = salaries.withColumn(
    "salary_capped",
    when(col("salary") < lit(lower_cap), lit(lower_cap))  # Cap low.
    .when(col("salary") > lit(upper_cap), lit(upper_cap))  # Cap high.
    .otherwise(col("salary"))  # Keep.
)

print(f"IQR caps: ${lower_cap:,.0f} to ${upper_cap:,.0f}")
winsorized_iqr.filter(col("salary") != col("salary_capped")).show(truncate=False)  # Show capped.

# Compare statistics before and after.
print("--- Impact on Statistics ---")
print("Before capping:")
salaries.select(
    spark_round(mean("salary"), 0).alias("mean"),
    spark_round(stddev("salary"), 0).alias("stddev"),
).show()
print("After capping:")
winsorized_iqr.select(
    spark_round(mean("salary_capped"), 0).alias("mean"),
    spark_round(stddev("salary_capped"), 0).alias("stddev"),
).show()

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: MAD method
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Modified Z-Score (MAD)
# ============================================================
# Real-world: Most robust method for heavy-tailed distributions.

from pyspark.sql.functions import (
    col, median, abs as spark_abs, lit, when, round as spark_round
)  # Imports.

print("=== Modified Z-Score (MAD Method) ===")  # Heading.
print("MAD = Median Absolute Deviation (much more robust than std dev)\n")

# Step 1: Compute median.
med = salaries.select(median("salary")).collect()[0][0]  # Median.
print(f"Median salary: ${med:,.0f}")

# Step 2: Compute MAD.
with_abs_dev = salaries.withColumn(
    "abs_deviation", spark_abs(col("salary") - lit(med))  # |value - median|.
)
mad = with_abs_dev.select(median("abs_deviation")).collect()[0][0]  # MAD.
print(f"MAD: ${mad:,.0f}")

# Step 3: Compute Modified Z-scores.
# Modified Z = 0.6745 * (value - median) / MAD
K = 0.6745  # Consistency constant for normal distribution.

mad_result = salaries.withColumn(
    "modified_z",
    spark_round(lit(K) * (col("salary") - lit(med)) / lit(mad), 2)  # Modified Z.
).withColumn(
    "is_outlier",
    spark_abs(col("modified_z")) > 3.5  # Standard threshold for MAD.
).withColumn(
    "severity",
    when(spark_abs(col("modified_z")) > 5, "EXTREME")
    .when(spark_abs(col("modified_z")) > 3.5, "MODERATE")
    .otherwise("NORMAL")  # Classify.
)

mad_result.show(truncate=False)  # Display.

# Comparison: Z-score vs IQR vs MAD.
print("=== Method Comparison ===")  # Heading.
print(f"Z-Score outliers (|z| > 3): ~0 (std inflated by outliers)")
print(f"IQR outliers: {salaries.filter((col('salary') < lit(Q1 - 1.5*IQR)) | (col('salary') > lit(Q3 + 1.5*IQR))).count()}")
print(f"MAD outliers (|mz| > 3.5): {mad_result.filter(col('is_outlier')).count()}")
print("\nMAD is BEST for: skewed data, small samples, heavy tails.")
print("It's not fooled by extreme values (unlike Z-score).")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Multi-column outlier analysis
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Multi-Column Outlier Analysis
# ============================================================
# Real-world: Detect outliers across multiple dimensions.

from pyspark.sql.functions import (
    col, mean, stddev, abs as spark_abs, lit, when,
    round as spark_round, sum as spark_sum, percentile_approx
)  # Imports.

# Multi-dimensional data.
orders = spark.createDataFrame([
    (1, 5, 150.0, 2.0),
    (2, 3, 89.99, 1.5),
    (3, 200, 50.0, 0.5),    # qty=200 outlier.
    (4, 4, 120.0, 3.0),
    (5, 2, 5000.0, 1.0),    # amount=5000 outlier.
    (6, 6, 200.0, 2.5),
    (7, 1, 45.0, 48.0),     # delivery_days=48 outlier.
    (8, 3, 110.0, 2.0),
    (9, 4, 175.0, 1.5),
    (10, 8, 250.0, 3.0),
], ["order_id", "quantity", "amount", "delivery_days"])  # Orders.

print("=== Multi-Column Outlier Detection ===")  # Heading.
orders.show()  # Display.

# IQR-based detection for each numeric column.
def iqr_outliers(df, columns):
    """Detect IQR outliers for multiple columns."""
    result = df  # Start.
    outlier_cols = []  # Track.
    
    for c in columns:  # Each column.
        stats = df.select(
            percentile_approx(c, 0.25).alias("q1"),
            percentile_approx(c, 0.75).alias("q3"),
        ).collect()[0]  # Stats.
        q1, q3 = stats["q1"], stats["q3"]  # Quartiles.
        iqr = q3 - q1  # IQR.
        lower = q1 - 1.5 * iqr  # Lower.
        upper = q3 + 1.5 * iqr  # Upper.
        
        col_flag = f"{c}_outlier"  # Flag name.
        outlier_cols.append(col_flag)  # Track.
        result = result.withColumn(
            col_flag,
            (col(c) < lit(lower)) | (col(c) > lit(upper))  # Flag.
        )
    
    # Total outlier score.
    result = result.withColumn(
        "outlier_score",
        sum([col(c).cast("int") for c in outlier_cols])  # Count flags.
    )
    return result  # Return.

numeric_cols = ["quantity", "amount", "delivery_days"]  # Check these.
multi_outliers = iqr_outliers(orders, numeric_cols)  # Detect.

print("=== Multi-Dimensional Outlier Flags ===")  # Heading.
multi_outliers.show(truncate=False)  # Display.

# Records that are outliers in multiple dimensions are more suspicious.
print("=== Outlier Summary ===")  # Heading.
multi_outliers.groupBy("outlier_score").count().orderBy("outlier_score").show()
print("Score 0: Normal | Score 1: Single outlier | Score 2+: Highly suspicious")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Production outlier pipeline
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Production Outlier Pipeline
# ============================================================
# Real-world: Complete outlier detection + treatment framework.

from pyspark.sql.functions import (
    col, mean, stddev, median, abs as spark_abs, when, lit,
    percentile_approx, count, sum as spark_sum, round as spark_round,
    log1p, sqrt
)  # Imports.
from pyspark.sql import DataFrame  # Type.

def outlier_pipeline(df, col_name, method="iqr", treatment="cap", threshold=1.5):
    """
    Production outlier detection and treatment.
    Methods: 'iqr', 'zscore', 'mad', 'percentile'
    Treatments: 'remove', 'cap', 'flag', 'null'
    """
    total = df.count()  # Total rows.
    
    # Detect based on method.
    if method == "iqr":  # IQR method.
        stats = df.select(
            percentile_approx(col_name, 0.25).alias("q1"),
            percentile_approx(col_name, 0.75).alias("q3"),
        ).collect()[0]
        q1, q3 = stats["q1"], stats["q3"]
        iqr = q3 - q1
        lower = q1 - threshold * iqr
        upper = q3 + threshold * iqr
    elif method == "zscore":  # Z-Score.
        stats = df.select(mean(col_name).alias("m"), stddev(col_name).alias("s")).collect()[0]
        lower = stats["m"] - threshold * stats["s"]
        upper = stats["m"] + threshold * stats["s"]
    elif method == "mad":  # MAD.
        med = df.select(median(col_name)).collect()[0][0]
        mad_val = df.withColumn("_d", spark_abs(col(col_name) - lit(med))).select(median("_d")).collect()[0][0]
        lower = med - threshold * mad_val / 0.6745
        upper = med + threshold * mad_val / 0.6745
    elif method == "percentile":  # Percentile.
        lower = df.select(percentile_approx(col_name, 0.01)).collect()[0][0]
        upper = df.select(percentile_approx(col_name, 0.99)).collect()[0][0]
    
    # Flag outliers.
    flagged = df.withColumn(
        f"{col_name}_is_outlier",
        (col(col_name) < lit(lower)) | (col(col_name) > lit(upper))
    )
    outlier_count = flagged.filter(col(f"{col_name}_is_outlier")).count()
    
    # Apply treatment.
    if treatment == "cap":  # Winsorize.
        result = flagged.withColumn(
            col_name,
            when(col(col_name) < lit(lower), lit(lower))
            .when(col(col_name) > lit(upper), lit(upper))
            .otherwise(col(col_name))
        )
    elif treatment == "remove":  # Drop outlier rows.
        result = flagged.filter(~col(f"{col_name}_is_outlier"))
    elif treatment == "null":  # Replace with NULL.
        result = flagged.withColumn(
            col_name,
            when(col(f"{col_name}_is_outlier"), None).otherwise(col(col_name))
        )
    else:  # Flag only.
        result = flagged
    
    # Audit.
    print(f"\n{'='*55}")
    print(f"  OUTLIER PIPELINE: {col_name}")
    print(f"{'='*55}")
    print(f"  Method: {method} (threshold={threshold})")
    print(f"  Bounds: [{lower:,.2f}, {upper:,.2f}]")
    print(f"  Outliers: {outlier_count}/{total} ({round(outlier_count/total*100,1)}%)")
    print(f"  Treatment: {treatment}")
    print(f"{'='*55}\n")
    
    return result  # Return treated data.

# Apply with different strategies.
print("=== IQR + Cap ===")
result1 = outlier_pipeline(salaries, "salary", method="iqr", treatment="cap")
result1.show(truncate=False)

print("=== MAD + Flag ===")
result2 = outlier_pipeline(salaries, "salary", method="mad", treatment="flag", threshold=3.5)
result2.filter(col("salary_is_outlier")).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Log transformation for skewed data
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Log Transform for Skewed Data
# ============================================================
# Real-world: Transform highly skewed data to reduce outlier impact.

from pyspark.sql.functions import (
    col, log1p, sqrt, mean, stddev, percentile_approx,
    round as spark_round, when, lit, exp, pow as spark_pow
)  # Imports.

# Highly skewed data (typical of revenue, page views, etc.).
import random  # Random.
random.seed(42)  # Seed.

skewed_data = [(i, max(1, int(random.lognormvariate(4, 2)))) for i in range(1, 101)]  # Log-normal.
skewed_data += [(101, 500000), (102, 1000000)]  # Add extreme outliers.

skewed_df = spark.createDataFrame(skewed_data, ["id", "revenue"])  # Create.

print("=== Highly Skewed Data (Revenue) ===")  # Heading.
skewed_df.select(
    spark_round(mean("revenue"), 0).alias("mean"),
    percentile_approx("revenue", 0.50).alias("median"),
    percentile_approx("revenue", 0.99).alias("p99"),
).show()  # Stats.
print("Note: Mean >> Median indicates strong right skew!\n")

# Transform options.
print("=== Transformation Comparison ===")  # Heading.
transformed = skewed_df.withColumn(
    "log_revenue", log1p(col("revenue"))  # log(1+x) handles zeros.
).withColumn(
    "sqrt_revenue", sqrt(col("revenue"))  # Square root.
)

# Compare stats after transform.
print("--- Original ---")
skewed_df.select(
    spark_round(mean("revenue"), 0).alias("mean"),
    spark_round(stddev("revenue"), 0).alias("std"),
).show()

print("--- Log(1+x) Transform ---")
transformed.select(
    spark_round(mean("log_revenue"), 2).alias("mean"),
    spark_round(stddev("log_revenue"), 2).alias("std"),
).show()

print("--- Sqrt Transform ---")
transformed.select(
    spark_round(mean("sqrt_revenue"), 2).alias("mean"),
    spark_round(stddev("sqrt_revenue"), 2).alias("std"),
).show()

# Detect outliers on transformed scale.
print("=== Outlier Detection on Log-Transformed Data ===")  # Heading.
log_stats = transformed.select(
    mean("log_revenue").alias("m"), stddev("log_revenue").alias("s")
).collect()[0]

log_outliers = transformed.withColumn(
    "z_log", (col("log_revenue") - lit(log_stats["m"])) / lit(log_stats["s"])
).withColumn(
    "is_outlier_log", col("z_log") > 3  # Only right-tail for revenue.
)

print(f"Outliers on log scale: {log_outliers.filter(col('is_outlier_log')).count()}")
log_outliers.filter(col("is_outlier_log")).select("id", "revenue", "log_revenue", "z_log").show()

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Contextual outlier detection
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Contextual Outlier Detection
# ============================================================
# Real-world: What's normal depends on context (group/segment).

from pyspark.sql.functions import (
    col, mean, stddev, abs as spark_abs, when, lit,
    round as spark_round, percentile_approx
)  # Imports.
from pyspark.sql.window import Window  # Window.

# Salaries vary wildly by department.
contextual = spark.createDataFrame([
    ("Engineering", "Alice", 150000.0),
    ("Engineering", "Bob", 145000.0),
    ("Engineering", "Charlie", 160000.0),
    ("Engineering", "Diana", 500000.0),   # Outlier FOR engineering.
    ("Support", "Eve", 45000.0),
    ("Support", "Frank", 48000.0),
    ("Support", "Grace", 50000.0),
    ("Support", "Hank", 150000.0),        # Outlier FOR support.
    ("Executive", "Ivy", 300000.0),
    ("Executive", "Jack", 350000.0),
    ("Executive", "Kate", 1000000.0),     # NOT outlier for exec (higher variance).
], ["dept", "name", "salary"])  # Contextual data.

print("=== Contextual Outliers (Per-Group Detection) ===")  # Heading.
contextual.show(truncate=False)  # Display.

# Global detection (WRONG approach).
print("--- Global Z-Score (ignores context) ---")
global_stats = contextual.select(
    mean("salary").alias("m"), stddev("salary").alias("s")
).collect()[0]

global_result = contextual.withColumn(
    "global_z",
    spark_round((col("salary") - lit(global_stats["m"])) / lit(global_stats["s"]), 2)
).withColumn(
    "global_outlier", spark_abs(col("global_z")) > 2  # Flag.
)
global_result.select("dept", "name", "salary", "global_z", "global_outlier").show(truncate=False)

# Contextual detection (CORRECT approach).
print("--- Contextual Z-Score (per department) ---")
w = Window.partitionBy("dept")  # Window by department.

contextual_result = contextual.withColumn(
    "dept_mean", mean("salary").over(w)  # Group mean.
).withColumn(
    "dept_std", stddev("salary").over(w)  # Group std.
).withColumn(
    "contextual_z",
    spark_round((col("salary") - col("dept_mean")) / col("dept_std"), 2)  # Group Z.
).withColumn(
    "contextual_outlier",
    spark_abs(col("contextual_z")) > 2  # Flag within group.
)

contextual_result.select(
    "dept", "name", "salary", "contextual_z", "contextual_outlier"
).show(truncate=False)

print("KEY INSIGHT: Kate's $1M salary is NOT an outlier for Executives,")
print("but Hank's $150K IS an outlier for Support (2x the group mean).")
print("✅ Outlier detection and treatment mastery complete!")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Outlier Handling
# MAGIC
# MAGIC ### Mistake 1: Using Z-Score on skewed data
# MAGIC ```python
# MAGIC # Z-Score assumes normal distribution!
# MAGIC # For skewed data (revenue, counts, durations), use IQR or MAD.
# MAGIC # Or log-transform first, THEN apply Z-score.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Removing all outliers blindly
# MAGIC ```python
# MAGIC # WRONG: Always removing outliers destroys valid information!
# MAGIC # Ask: Is this value POSSIBLE in the real world?
# MAGIC # - Age = 200: Impossible -> Remove
# MAGIC # - Salary = $500K: Possible (CEO) -> Flag, don't remove
# MAGIC # - Revenue spike: Could be Black Friday -> Contextual!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Ignoring context
# MAGIC ```python
# MAGIC # $150K salary: outlier for junior roles, normal for senior.
# MAGIC # ALWAYS detect within appropriate groups (department, region, role).
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: One threshold for all
# MAGIC ```python
# MAGIC # 1.5*IQR is standard but arbitrary.
# MAGIC # Strict (fewer false positives): 3*IQR
# MAGIC # Lenient (catch more): 1.0*IQR
# MAGIC # Choose based on cost of false positives vs missed outliers.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not documenting decisions
# MAGIC ```python
# MAGIC # ALWAYS record:
# MAGIC # - Method used and why
# MAGIC # - How many values affected
# MAGIC # - What treatment was applied
# MAGIC # - Business justification
# MAGIC # Without this, results are unreproducible!
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Outlier Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Apply Z-score and IQR outlier detection to a numeric column.
# MAGIC 2. Cap outliers using winsorization.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Change IQR multiplier from 1.5 to 3.0 (strict mode).
# MAGIC 4. Switch from capping to removal.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Build: detect + flag + report + treat in one pipeline.
# MAGIC 6. Add domain constraints alongside statistical detection.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Apply contextual detection (per-group) to time series data.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build configurable `outlier_pipeline()` function.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design outlier strategy for a financial dataset with multiple currencies.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Compare methods: Z-score vs IQR vs MAD vs Isolation Forest on same data.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Handle: all-same values (std=0), very small samples (n<10), NULLs in data.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build streaming outlier detector with adaptive thresholds.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create guide: "When to use which outlier method."

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all.
from pyspark.sql.window import Window  # Window.

# --- Level 1: Basic Z-score + IQR ---
print("=== Level 1: Z-Score Detection ===")  # Heading.
test = spark.createDataFrame([(i, float(v)) for i, v in enumerate(
    [10, 12, 11, 13, 12, 100, 11, 12, -50, 13])], ["id", "val"])  # With outliers.

stats = test.select(mean("val").alias("m"), stddev("val").alias("s")).collect()[0]
test.withColumn("z", round((col("val") - lit(stats["m"])) / lit(stats["s"]), 2)).withColumn(
    "outlier", abs(col("z")) > 2).show()  # Display.

# --- Level 3: Combined pipeline ---
print("\n=== Level 3: Detect + Flag + Report ===")  # Heading.
def simple_outlier_check(df, col_name, multiplier=1.5):
    """Quick IQR check with report."""
    q = df.select(percentile_approx(col_name, 0.25), percentile_approx(col_name, 0.75)).collect()[0]
    iqr = q[1] - q[0]
    lo, hi = q[0] - multiplier * iqr, q[1] + multiplier * iqr
    flagged = df.withColumn("outlier", (col(col_name) < lo) | (col(col_name) > hi))
    n_out = flagged.filter(col("outlier")).count()
    print(f"  {col_name}: bounds=[{lo:.1f}, {hi:.1f}], outliers={n_out}")
    return flagged

simple_outlier_check(test, "val", 1.5).show()

# --- Level 8: Edge case (std=0) ---
print("\n=== Level 8: Edge Case (constant values) ===")  # Heading.
constant = spark.createDataFrame([(i, 5.0) for i in range(10)], ["id", "val"])
stats2 = constant.select(mean("val").alias("m"), stddev("val").alias("s")).collect()[0]
print(f"Std = {stats2['s']}")
print("Z-score: division by zero! Use IQR instead (IQR=0 means no spread).")
print("Best practice: if std/IQR/MAD = 0, skip outlier detection for that column.")

print("\n✅ All outlier homework solutions complete!")  # Done.