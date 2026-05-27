# Databricks notebook source
# DBTITLE 1,NB_33 Header
# MAGIC %md
# MAGIC # NB_33 — Math Functions (Every One)
# MAGIC
# MAGIC **Module 5: Built-in Functions** | Notebook 33 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Rounding: round, bround, floor, ceil
# MAGIC * Absolute value: abs
# MAGIC * Power and roots: sqrt, cbrt, pow, exp, expm1
# MAGIC * Logarithms: log, log2, log10, log1p, ln
# MAGIC * Trigonometry: sin, cos, tan, asin, acos, atan, atan2, degrees, radians
# MAGIC * Hyperbolic: sinh, cosh, tanh
# MAGIC * Greatest and least: greatest, least
# MAGIC * Random: rand, randn
# MAGIC * Numeric utilities: signum, factorial, hex, unhex, conv
# MAGIC * Special: pi, e (Euler's number), NaN handling
# MAGIC
# MAGIC **Difficulty:** ⭐⭐ (Reference Catalog)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are Math Functions?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are Math Functions? (Real-World Analogy)
# MAGIC
# MAGIC ### 🏭 The Engineering Calculator
# MAGIC
# MAGIC Imagine a scientific calculator that can process millions of numbers in parallel:
# MAGIC
# MAGIC | Calculator Button | PySpark Function | Use Case |
# MAGIC |---|---|---|
# MAGIC | Round button | `round()`, `floor()`, `ceil()` | Round prices to 2 decimals |
# MAGIC | Absolute value | `abs()` | Compute unsigned distance |
# MAGIC | Square root | `sqrt()` | Standard deviation, distance |
# MAGIC | Power key | `pow()` | Compound interest |
# MAGIC | Log key | `log()`, `log10()` | Normalize skewed data |
# MAGIC | Trig keys | `sin()`, `cos()` | Geospatial, signal processing |
# MAGIC | Random | `rand()` | Sampling, A/B test assignment |
# MAGIC | Max/Min | `greatest()`, `least()` | Row-level max across columns |
# MAGIC
# MAGIC ### Why Math Functions Matter
# MAGIC * **Finance:** compound interest with `pow()`, rounding with `round()`
# MAGIC * **Data Science:** log-transform skewed features with `log()`
# MAGIC * **Geospatial:** distance with `sin()`, `cos()`, `atan2()`
# MAGIC * **Randomization:** A/B test buckets with `rand()`
# MAGIC * **Data Quality:** clamping with `greatest()` and `least()`

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Math Functions Work
# MAGIC %md
# MAGIC ## SECTION 2 — How Math Functions Work (Internal Mechanics)
# MAGIC
# MAGIC ### Function Categories
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────┬──────────────────┬──────────────────┐
# MAGIC │  ROUNDING        │  POWER / ROOTS   │  LOGARITHMS      │
# MAGIC │  round(x, d)     │  sqrt(x)         │  log(base, x)    │
# MAGIC │  bround(x, d)    │  cbrt(x)         │  log2(x)         │
# MAGIC │  floor(x)        │  pow(x, y)       │  log10(x)        │
# MAGIC │  ceil(x)         │  exp(x)          │  log1p(x)        │
# MAGIC │  abs(x)          │  expm1(x)        │  ln(x)           │
# MAGIC ├──────────────────┼──────────────────┼──────────────────┤
# MAGIC │  TRIGONOMETRY    │  COMPARISON      │  RANDOM          │
# MAGIC │  sin, cos, tan   │  greatest(a,b,c) │  rand(seed)      │
# MAGIC │  asin, acos, atan│  least(a,b,c)    │  randn(seed)     │
# MAGIC │  atan2(y, x)     │                  │                  │
# MAGIC │  degrees, radians│                  │                  │
# MAGIC ├──────────────────┼──────────────────┼──────────────────┤
# MAGIC │  SPECIAL         │  CONVERSION      │  CONSTANTS       │
# MAGIC │  signum(x)       │  hex(x)          │  lit(math.pi)    │
# MAGIC │  factorial(x)    │  unhex(x)        │  lit(math.e)     │
# MAGIC │  isnan(x)        │  conv(x,from,to) │                  │
# MAGIC └──────────────────┴──────────────────┴──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Rules
# MAGIC 1. All math functions return NULL if input is NULL
# MAGIC 2. `round()` uses HALF_UP; `bround()` uses HALF_EVEN (banker's rounding)
# MAGIC 3. `floor()` rounds toward negative infinity; `ceil()` toward positive infinity
# MAGIC 4. `rand()` generates uniform [0,1); `randn()` generates standard normal
# MAGIC 5. `greatest()`/`least()` compare across columns per row (not across rows)
# MAGIC 6. Trigonometric functions work in radians (use `radians()` to convert degrees)

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Rounding Functions
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Rounding Functions
# ============================================================
# Real-world: Rounding prices, percentages, and financial values.

from pyspark.sql import SparkSession  # Import SparkSession.
from pyspark.sql.functions import (  # Import rounding functions.
    col, round as spark_round, bround, floor, ceil, abs as spark_abs, lit
)  # End imports.

spark = SparkSession.builder.getOrCreate()  # Get active session.

# Create sample data with decimal values.
data = [
    (1, 3.14159, -7.5, 2.5),
    (2, 99.995, -0.3, 3.5),
    (3, 1234.5678, -100.7, 4.5),
    (4, 0.004, 0.0, 5.5),
]

df = spark.createDataFrame(data, ["id", "value", "negative", "half_val"])  # Create DataFrame.

# Apply rounding functions.
result = df.select(
    col("id"),  # Keep id.
    col("value"),  # Original value.
    spark_round(col("value"), 2).alias("round_2dp"),  # Round to 2 decimal places.
    spark_round(col("value"), 0).alias("round_0dp"),  # Round to nearest integer.
    floor(col("value")).alias("floor"),  # Round down (toward -infinity).
    ceil(col("value")).alias("ceil"),  # Round up (toward +infinity).
    spark_abs(col("negative")).alias("abs_neg"),  # Absolute value.
    # Banker's rounding: HALF_EVEN rounds 0.5 to nearest even number.
    bround(col("half_val"), 0).alias("bround"),  # 2.5→2, 3.5→4, 4.5→4, 5.5→6
    spark_round(col("half_val"), 0).alias("round"),  # Standard: 2.5→3, 3.5→4
)

print("=== Rounding Functions ===")  # Print heading.
result.show(truncate=False)  # Display results.

# Practical: Round prices to 2 decimal places.
prices = spark.createDataFrame([(9.999,), (14.501,), (100.005,)], ["raw_price"])  # Create prices.
prices.select(
    col("raw_price"),  # Original.
    spark_round(col("raw_price"), 2).alias("display_price"),  # Customer-facing price.
    floor(col("raw_price")).alias("floor_price"),  # Truncated price.
    ceil(col("raw_price")).alias("ceil_price"),  # Rounded-up price.
).show()  # Display prices.

# Expected Output:
# +---+-----------+--------+--------+-----+----+-------+------+-----+
# |id |value      |round_2dp|round_0dp|floor|ceil|abs_neg|bround|round|
# +---+-----------+--------+--------+-----+----+-------+------+-----+
# |1  |3.14159    |3.14    |3.0     |3    |4   |7.5    |2.0   |3.0  |
# |2  |99.995     |100.0   |100.0   |99   |100 |0.3    |4.0   |4.0  |
# +---+-----------+--------+--------+-----+----+-------+------+-----+

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Power, Roots, and Logarithms
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Power, Roots, and Logarithms
# ============================================================
# Real-world: Compound interest, feature scaling, signal processing.

from pyspark.sql.functions import (  # Import power and log functions.
    sqrt, cbrt, pow as spark_pow, exp, expm1, log, log2, log10, log1p, ln
)  # End imports.

# Create numeric data.
num_df = spark.createDataFrame([
    (1, 4.0, 27.0, 100.0, 2.0),
    (2, 9.0, 64.0, 1000.0, 3.0),
    (3, 16.0, 125.0, 10000.0, 0.5),
    (4, 25.0, 8.0, 1.0, 10.0),
], ["id", "a", "b", "c", "x"])  # Create DataFrame.

# Power and root functions.
print("=== Power and Root Functions ===")  # Print heading.
num_df.select(
    col("id"),  # Keep id.
    col("a"),  # Original value.
    sqrt(col("a")).alias("sqrt_a"),  # Square root: sqrt(4)=2, sqrt(9)=3.
    cbrt(col("b")).alias("cbrt_b"),  # Cube root: cbrt(27)=3, cbrt(64)=4.
    spark_pow(col("x"), lit(3)).alias("x_cubed"),  # x^3: 2^3=8, 3^3=27.
    spark_pow(lit(2), col("x")).alias("two_to_x"),  # 2^x: 2^2=4, 2^3=8.
    exp(col("x")).alias("e_to_x"),  # e^x: Euler's number raised to x.
).show(truncate=False)  # Display results.

# Logarithm functions.
print("=== Logarithm Functions ===")  # Print heading.
num_df.select(
    col("id"),  # Keep id.
    col("c"),  # Original value.
    ln(col("c")).alias("ln_c"),  # Natural log: ln(100)≈4.6
    log2(col("c")).alias("log2_c"),  # Log base 2: log2(100)≈6.6
    log10(col("c")).alias("log10_c"),  # Log base 10: log10(100)=2, log10(1000)=3.
    log(lit(2), col("c")).alias("log_base2"),  # General log(base, x).
    log1p(col("x")).alias("log1p_x"),  # log(1+x): safer for small x.
).show(truncate=False)  # Display results.

# Practical: Compound interest calculation.
print("=== Compound Interest: A = P * (1 + r)^n ===")  # Print heading.
investment = spark.createDataFrame([
    ("Alice", 10000.0, 0.05, 10),  # $10K at 5% for 10 years.
    ("Bob", 5000.0, 0.08, 5),  # $5K at 8% for 5 years.
    ("Charlie", 25000.0, 0.03, 20),  # $25K at 3% for 20 years.
], ["name", "principal", "rate", "years"])  # Investment data.

investment.select(
    col("name"),  # Keep name.
    col("principal"),  # Original amount.
    col("rate"),  # Interest rate.
    col("years"),  # Duration.
    spark_round(col("principal") * spark_pow(lit(1) + col("rate"), col("years")), 2).alias("future_value"),  # A = P*(1+r)^n.
).show(truncate=False)  # Display future values.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Greatest, Least, and Random
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Greatest, Least, and Random
# ============================================================
# Real-world: Row-level max/min across columns, random sampling/bucketing.

from pyspark.sql.functions import greatest, least, rand, randn, lit  # Import comparison and random functions.

# Create multi-column scores.
scores_df = spark.createDataFrame([
    ("Alice", 85, 92, 78),  # Three test scores.
    ("Bob", 90, 70, 95),
    ("Charlie", 65, 88, 72),
    ("Diana", 92, 92, 92),  # All same.
], ["student", "test1", "test2", "test3"])  # Column names.

# greatest() and least() — compare across columns PER ROW.
print("=== greatest() and least() — Row-Level Comparison ===")  # Print heading.
scores_df.select(
    col("student"),  # Keep student.
    col("test1"), col("test2"), col("test3"),  # Keep scores.
    greatest(col("test1"), col("test2"), col("test3")).alias("best_score"),  # Highest of 3.
    least(col("test1"), col("test2"), col("test3")).alias("worst_score"),  # Lowest of 3.
    (greatest(col("test1"), col("test2"), col("test3")) -
     least(col("test1"), col("test2"), col("test3"))).alias("score_range"),  # Range.
).show(truncate=False)  # Display results.

# Practical: Clamping values to a range [0, 100].
print("=== Clamping with greatest/least ===")  # Print heading.
raw_data = spark.createDataFrame([(-5,), (42,), (105,), (0,), (100,)], ["raw_value"])  # Values outside range.
raw_data.select(
    col("raw_value"),  # Original.
    greatest(least(col("raw_value"), lit(100)), lit(0)).alias("clamped"),  # Clamp to [0, 100].
).show()  # Display clamped values.

# Random number generation.
print("=== rand() and randn() ===")  # Print heading.
print("rand():  Uniform [0, 1)")
print("randn(): Standard Normal (mean=0, stddev=1)")

random_df = spark.range(5).select(  # 5 rows.
    col("id"),  # Keep id.
    spark_round(rand(seed=42), 4).alias("uniform"),  # Uniform random with seed.
    spark_round(randn(seed=42), 4).alias("normal"),  # Normal random with seed.
    # A/B test bucket assignment: 50/50 split.
    (rand(seed=123) < 0.5).cast("int").alias("group_A"),  # 1=Group A, 0=Group B.
)
random_df.show(truncate=False)  # Display random values.

# Expected Output:
# +-------+-----+-----+-----+----------+-----------+-----------+
# |student|test1|test2|test3|best_score|worst_score|score_range|
# +-------+-----+-----+-----+----------+-----------+-----------+
# |Alice  |85   |92   |78   |92        |78         |14         |
# |Bob    |90   |70   |95   |95        |70         |25         |
# +-------+-----+-----+-----+----------+-----------+-----------+

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Trigonometry and Geospatial
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Trigonometry and Geospatial
# ============================================================
# Real-world: Computing distances between coordinates using Haversine formula.

import math  # Python math for constants.
from pyspark.sql.functions import (  # Import trig functions.
    sin, cos, tan, asin, acos, atan, atan2, degrees, radians,
    sqrt, pow as spark_pow, lit, col, round as spark_round
)  # End imports.

# Trigonometric basics.
print("=== Basic Trigonometry (input in RADIANS) ===")  # Print heading.
trig_df = spark.createDataFrame([
    (0.0,), (math.pi / 6,), (math.pi / 4,), (math.pi / 3,), (math.pi / 2,)
], ["angle_rad"])  # Angles in radians.

trig_df.select(
    spark_round(col("angle_rad"), 4).alias("radians"),  # Original angle.
    spark_round(degrees(col("angle_rad")), 1).alias("degrees"),  # Convert to degrees.
    spark_round(sin(col("angle_rad")), 4).alias("sin"),  # Sine.
    spark_round(cos(col("angle_rad")), 4).alias("cos"),  # Cosine.
    spark_round(tan(col("angle_rad")), 4).alias("tan"),  # Tangent.
).show(truncate=False)  # Display trig results.

# Haversine distance formula between two GPS coordinates.
print("=== Haversine Distance Between Cities ===")  # Print heading.
cities = spark.createDataFrame([
    ("NYC", 40.7128, -74.0060, "London", 51.5074, -0.1278),  # NYC to London.
    ("NYC", 40.7128, -74.0060, "LA", 34.0522, -118.2437),  # NYC to LA.
    ("Berlin", 52.5200, 13.4050, "Paris", 48.8566, 2.3522),  # Berlin to Paris.
], ["city1", "lat1", "lon1", "city2", "lat2", "lon2"])  # Coordinates.

# Haversine formula: d = 2r * arcsin(sqrt(sin²(Δlat/2) + cos(lat1)*cos(lat2)*sin²(Δlon/2)))
R = 6371.0  # Earth radius in km.

haversine = cities.select(
    col("city1"), col("city2"),  # City names.
    spark_round(
        lit(2 * R) * asin(sqrt(
            spark_pow(sin((radians(col("lat2")) - radians(col("lat1"))) / 2), lit(2)) +
            cos(radians(col("lat1"))) * cos(radians(col("lat2"))) *
            spark_pow(sin((radians(col("lon2")) - radians(col("lon1"))) / 2), lit(2))
        )),
        1
    ).alias("distance_km"),  # Haversine distance.
)

haversine.show(truncate=False)  # Display distances.

# Expected: NYC-London ≈ 5570 km, NYC-LA ≈ 3944 km, Berlin-Paris ≈ 878 km.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Signum, Factorial, Hex, Conv
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Signum, Factorial, Hex, Conv
# ============================================================
# Real-world: Sign detection, base conversion, data encoding.

from pyspark.sql.functions import (  # Import special math functions.
    signum, factorial, hex as spark_hex, unhex, conv, col, lit
)  # End imports.

# signum — returns -1, 0, or +1 based on sign.
print("=== signum() — Sign Detection ===")  # Print heading.
sign_df = spark.createDataFrame([
    (-50.0,), (-0.001,), (0.0,), (0.001,), (42.0,)
], ["value"])  # Values with different signs.

sign_df.select(
    col("value"),  # Original.
    signum(col("value")).alias("sign"),  # -1.0, 0.0, or 1.0.
).show()  # Display sign results.

# factorial — n! for integer values.
print("=== factorial() ===")  # Print heading.
fact_df = spark.createDataFrame([(0,), (1,), (5,), (10,), (20,)], ["n"])  # Integers.
fact_df.select(
    col("n"),  # Original.
    factorial(col("n")).alias("n_factorial"),  # n!
).show()  # Display factorials.

# hex/unhex — convert to/from hexadecimal.
print("=== hex() and unhex() ===")  # Print heading.
hex_df = spark.createDataFrame([
    (255,), (16,), (0,), (1024,), (65535,)
], ["decimal_val"])  # Decimal numbers.

hex_df.select(
    col("decimal_val"),  # Original decimal.
    spark_hex(col("decimal_val")).alias("hex_str"),  # Convert to hex string: 255 -> "FF".
).show()  # Display hex conversions.

# conv() — convert number strings between bases.
print("=== conv() — Base Conversion ===")  # Print heading.
base_df = spark.createDataFrame([
    ("FF",), ("1010",), ("777",), ("10",)
], ["num_str"])  # Number strings in various bases.

base_df.select(
    col("num_str"),  # Original.
    conv(col("num_str"), 16, 10).alias("hex_to_dec"),  # Hex to decimal: FF -> 255.
    conv(col("num_str"), 2, 10).alias("bin_to_dec"),  # Binary to decimal: 1010 -> 10.
    conv(col("num_str"), 8, 10).alias("oct_to_dec"),  # Octal to decimal: 777 -> 511.
    conv(lit("255"), 10, 2).alias("dec_to_bin"),  # Decimal to binary: 255 -> 11111111.
).show(truncate=False)  # Display base conversions.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: NaN handling and numeric safety
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: NaN Handling and Numeric Safety
# ============================================================
# Real-world: Cleaning sensor data with infinity and NaN values.

from pyspark.sql.functions import (  # Import NaN and safety functions.
    isnan, isnull, when, col, lit, nanvl, log as spark_log,
    sqrt, greatest, least, round as spark_round
)  # End imports.

# Create data with edge cases.
edge_df = spark.createDataFrame([
    (1, 100.0),
    (2, 0.0),       # Division by zero candidate.
    (3, -1.0),     # Negative sqrt candidate.
    (4, float('nan')),  # Explicit NaN.
    (5, None),     # Null value.
], ["id", "value"])  # Data with edge cases.

print("=== NaN Detection and Handling ===")  # Print heading.
edge_df.select(
    col("id"),  # Keep id.
    col("value"),  # Original value.
    isnan(col("value")).alias("is_nan"),  # Detect NaN (True/False).
    isnull(col("value")).alias("is_null"),  # Detect NULL (True/False).
    # nanvl: replace NaN with a substitute value.
    nanvl(col("value"), lit(0.0)).alias("nanvl_0"),  # Replace NaN with 0.
    # Safe log: protect against log(0) and log(negative).
    when(col("value") > 0, spark_round(spark_log(col("value")), 4))
        .otherwise(lit(None)).alias("safe_log"),  # NULL for invalid inputs.
    # Safe sqrt: protect against sqrt(negative).
    when(col("value") >= 0, spark_round(sqrt(col("value")), 4))
        .otherwise(lit(None)).alias("safe_sqrt"),  # NULL for negative inputs.
).show(truncate=False)  # Display results.

# Practical: Safe division function.
print("=== Safe Division (avoid divide-by-zero) ===")  # Print heading.
div_df = spark.createDataFrame([
    (100.0, 3.0), (50.0, 0.0), (75.0, None), (200.0, 4.0)
], ["numerator", "denominator"])  # Division data.

div_df.select(
    col("numerator"),  # Keep numerator.
    col("denominator"),  # Keep denominator.
    # Safe: returns NULL when denominator is 0 or NULL.
    when((col("denominator").isNull()) | (col("denominator") == 0), lit(None))
        .otherwise(spark_round(col("numerator") / col("denominator"), 4))
        .alias("safe_divide"),  # Protected division.
).show(truncate=False)  # Display safe division results.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Statistical math for feature engineering
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Statistical Math for Feature Engineering
# ============================================================
# Real-world: Z-score normalization, log-transform, and clipping for ML.

from pyspark.sql.functions import (  # Import statistical helpers.
    avg, stddev, col, lit, round as spark_round, ln, exp,
    greatest, least, when, count, sqrt, pow as spark_pow
)  # End imports.
from pyspark.sql.window import Window  # Import Window for global stats.

# Create feature data (skewed distribution).
import random  # Python random for data generation.
random.seed(42)  # Reproducible.
feature_data = [(i, round(random.lognormvariate(3, 1), 2)) for i in range(50)]  # Log-normal data.
feat_df = spark.createDataFrame(feature_data, ["id", "raw_feature"])  # Create DataFrame.

print("=== Raw Feature Statistics ===")  # Print heading.
feat_df.describe("raw_feature").show()  # Show distribution.

# Compute global mean and stddev for z-score.
global_stats = feat_df.select(
    avg("raw_feature").alias("mean"),  # Global mean.
    stddev("raw_feature").alias("std"),  # Global stddev.
).first()  # Collect as Row.

mean_val = global_stats["mean"]  # Extract mean.
std_val = global_stats["std"]  # Extract stddev.

# Apply transformations.
transformed = feat_df.select(
    col("id"),  # Keep id.
    col("raw_feature"),  # Original.
    # Z-score normalization: (x - mean) / std.
    spark_round((col("raw_feature") - lit(mean_val)) / lit(std_val), 4).alias("z_score"),
    # Log transform (common for right-skewed data).
    spark_round(ln(col("raw_feature") + lit(1)), 4).alias("log_transform"),
    # Min-max clamping to [1st percentile, 99th percentile] range.
    spark_round(
        greatest(least(col("raw_feature"), lit(100.0)), lit(1.0)),  # Clamp [1, 100].
        2
    ).alias("clipped"),
    # Square root transform (milder than log).
    spark_round(sqrt(col("raw_feature")), 4).alias("sqrt_transform"),
)

print("=== Transformed Features (first 10) ===")  # Print heading.
transformed.show(10, truncate=False)  # Display first 10 rows.

# Verify z-score has mean≈0 and std≈1.
print("=== Z-Score Verification ===")  # Print heading.
transformed.select(
    spark_round(avg("z_score"), 6).alias("z_mean"),  # Should be ~0.
    spark_round(stddev("z_score"), 6).alias("z_std"),  # Should be ~1.
).show()  # Display verification.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Financial calculations
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Financial Calculations
# ============================================================
# Real-world: Loan amortization, depreciation, and growth rates.

from pyspark.sql.functions import (  # Import financial helpers.
    col, lit, pow as spark_pow, round as spark_round, log as spark_log, ln,
    when, greatest, sequence, explode, expr
)  # End imports.

# Loan amortization: Monthly payment = P * [r(1+r)^n] / [(1+r)^n - 1]
print("=== Loan Amortization Calculator ===")  # Print heading.
loans = spark.createDataFrame([
    ("Home Mortgage", 300000.0, 0.065, 30),  # $300K at 6.5% for 30 years.
    ("Car Loan", 35000.0, 0.049, 5),  # $35K at 4.9% for 5 years.
    ("Student Loan", 50000.0, 0.055, 10),  # $50K at 5.5% for 10 years.
], ["loan_type", "principal", "annual_rate", "years"])  # Loan data.

loan_calc = loans.select(
    col("loan_type"),  # Keep loan type.
    col("principal"),  # Loan amount.
    col("annual_rate"),  # Annual interest rate.
    col("years"),  # Loan term.
    # Monthly rate.
    (col("annual_rate") / 12).alias("monthly_rate"),
    # Number of payments.
    (col("years") * 12).alias("num_payments"),
)

# Monthly payment formula: PMT = P * [r(1+r)^n] / [(1+r)^n - 1]
loan_result = loan_calc.select(
    col("loan_type"),  # Keep type.
    spark_round(col("principal"), 0).alias("principal"),  # Loan amount.
    spark_round(
        col("principal") *
        (col("monthly_rate") * spark_pow(lit(1) + col("monthly_rate"), col("num_payments"))) /
        (spark_pow(lit(1) + col("monthly_rate"), col("num_payments")) - lit(1)),
        2
    ).alias("monthly_payment"),  # Calculated payment.
    spark_round(
        col("principal") *
        (col("monthly_rate") * spark_pow(lit(1) + col("monthly_rate"), col("num_payments"))) /
        (spark_pow(lit(1) + col("monthly_rate"), col("num_payments")) - lit(1)) *
        col("num_payments"),
        2
    ).alias("total_paid"),  # Total over life of loan.
)

loan_result = loan_result.withColumn(
    "total_interest",
    spark_round(col("total_paid") - col("principal"), 2)  # Interest portion.
)

loan_result.show(truncate=False)  # Display loan calculations.

# CAGR (Compound Annual Growth Rate): ((end/start)^(1/years)) - 1
print("=== CAGR Calculator ===")  # Print heading.
growth = spark.createDataFrame([
    ("Stock A", 1000.0, 2500.0, 5),  # $1K to $2.5K in 5 years.
    ("Stock B", 5000.0, 8000.0, 3),  # $5K to $8K in 3 years.
    ("Revenue", 1000000.0, 5000000.0, 10),  # $1M to $5M in 10 years.
], ["asset", "start_val", "end_val", "years"])  # Growth data.

growth.select(
    col("asset"),  # Keep asset name.
    col("start_val"), col("end_val"), col("years"),  # Keep values.
    spark_round(
        (spark_pow(col("end_val") / col("start_val"), lit(1.0) / col("years")) - lit(1)) * 100,
        2
    ).alias("cagr_pct"),  # CAGR as percentage.
).show(truncate=False)  # Display CAGR results.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production numeric pipeline
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Production Numeric Pipeline
# ============================================================
# Real-world: Building reusable numeric transformation functions.

from pyspark.sql.functions import (  # Import production helpers.
    col, when, lit, isnan, isnull, greatest, least,
    round as spark_round, sqrt, ln, abs as spark_abs, signum,
    avg, stddev, percentile_approx
)  # End imports.
from pyspark.sql import Column  # Type hint.

# === Reusable Numeric Functions ===
def safe_divide(num: Column, denom: Column, default=None) -> Column:
    """Divide safely: returns default (or NULL) when denominator is 0 or NULL."""
    safe = when((denom.isNull()) | (denom == 0) | isnan(denom), lit(default)).otherwise(num / denom)
    return safe  # Return safe division result.

def clamp(c: Column, lower: float, upper: float) -> Column:
    """Clamp column values to [lower, upper] range."""
    return greatest(least(c, lit(upper)), lit(lower))  # Clip to bounds.

def z_score(c: Column, mean_val: float, std_val: float) -> Column:
    """Compute z-score given pre-computed mean and std."""
    return (c - lit(mean_val)) / lit(std_val)  # Standardize.

def log_safe(c: Column) -> Column:
    """Log transform with protection for zero/negative."""
    return when(c > 0, ln(c)).otherwise(lit(None))  # NULL for invalid.

def winsorize(c: Column, lower_pct: float, upper_pct: float, lower_val: float, upper_val: float) -> Column:
    """Winsorize: clip extreme values to percentile bounds."""
    return greatest(least(c, lit(upper_val)), lit(lower_val))  # Clip to bounds.

# === Apply Pipeline ===
print("=== Production Numeric Pipeline ===")  # Print heading.

# Generate test data.
import random  # Python random.
random.seed(99)  # Seed.
pipeline_data = [
    (i, round(random.gauss(50, 20), 2), round(random.uniform(0, 100), 2))
    for i in range(20)
]  # 20 rows.
pipeline_data.append((20, float('nan'), -5.0))  # Add NaN row.
pipeline_data.append((21, None, 0.0))  # Add NULL row.

pipe_df = spark.createDataFrame(pipeline_data, ["id", "metric_a", "metric_b"])  # Create DataFrame.

# Compute stats for z-score.
stats = pipe_df.filter(~isnan(col("metric_a")) & col("metric_a").isNotNull()).select(
    avg("metric_a").alias("mean_a"),  # Mean.
    stddev("metric_a").alias("std_a"),  # Stddev.
).first()  # Collect stats.

# Apply pipeline.
result = pipe_df.select(
    col("id"),  # Keep id.
    col("metric_a"),  # Original.
    col("metric_b"),  # Original.
    # Clean NaN/NULL.
    when(isnan(col("metric_a")) | col("metric_a").isNull(), lit(None))
        .otherwise(col("metric_a")).alias("clean_a"),  # Cleaned.
    # Z-score.
    spark_round(z_score(col("metric_a"), stats["mean_a"], stats["std_a"]), 4).alias("z_a"),
    # Clamped.
    clamp(col("metric_b"), 10.0, 90.0).alias("clamped_b"),  # Clip to [10, 90].
    # Safe divide.
    spark_round(safe_divide(col("metric_a"), col("metric_b")), 4).alias("a_div_b"),
    # Log transform.
    spark_round(log_safe(col("metric_b")), 4).alias("log_b"),
)

result.show(truncate=False)  # Display pipeline output.
print("✅ Production numeric pipeline complete!")  # Done message.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Math Functions
# MAGIC
# MAGIC ### Mistake 1: Confusing round() with floor()/ceil()
# MAGIC ```python
# MAGIC # round(2.5) = 3.0 (rounds to nearest, HALF_UP)
# MAGIC # floor(2.5) = 2   (always rounds toward -infinity)
# MAGIC # ceil(2.5)  = 3   (always rounds toward +infinity)
# MAGIC # round(-2.5) = -3.0 (rounds away from zero)
# MAGIC # floor(-2.5) = -3   (toward -infinity)
# MAGIC # ceil(-2.5)  = -2   (toward +infinity)
# MAGIC ```
# MAGIC **Fix:** Use `floor()` for always-down, `ceil()` for always-up, `round()` for nearest.
# MAGIC
# MAGIC ### Mistake 2: Using greatest/least for column-wise min/max
# MAGIC ```python
# MAGIC # WRONG — greatest() compares ACROSS COLUMNS in the same row!
# MAGIC df.select(greatest("salary"))  # Error or meaningless!
# MAGIC
# MAGIC # CORRECT for row-level max across columns:
# MAGIC df.select(greatest(col("test1"), col("test2"), col("test3")))  # Max of 3 columns per row.
# MAGIC
# MAGIC # CORRECT for column-level max across rows:
# MAGIC df.select(max("salary"))  # Aggregate max across all rows.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Forgetting radians for trig functions
# MAGIC ```python
# MAGIC # WRONG — sin expects radians, not degrees!
# MAGIC df.select(sin(lit(90)))  # sin(90 radians) ≈ 0.894, NOT 1.0!
# MAGIC
# MAGIC # CORRECT — convert degrees to radians first.
# MAGIC df.select(sin(radians(lit(90))))  # sin(90°) = 1.0
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Not handling NaN in math operations
# MAGIC ```python
# MAGIC # NaN propagates: NaN + 5 = NaN, NaN * 0 = NaN
# MAGIC # NULL is different: NULL + 5 = NULL
# MAGIC # Check with: isnan(col) for NaN, isnull(col) for NULL.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Using rand() without seed for reproducibility
# MAGIC ```python
# MAGIC # WRONG — different results every run!
# MAGIC df.withColumn("random", rand())  # Non-reproducible.
# MAGIC
# MAGIC # CORRECT — use seed for reproducibility.
# MAGIC df.withColumn("random", rand(seed=42))  # Same values every run.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Math Function Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Round a column to 0, 1, and 2 decimal places. Compare `round` vs `bround` on 2.5.
# MAGIC 2. Compute `sqrt`, `cbrt`, and `pow(x, 3)` on a set of numbers.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Change the compound interest formula to compute monthly compounding instead of annual.
# MAGIC 4. Modify the Haversine example to compute distance in miles (multiply by 0.621371).
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Use `log10` + `floor` + `pow` to extract the number of digits in an integer.
# MAGIC 6. Combine `greatest`, `least`, and `abs` to compute the maximum absolute deviation from the mean per row.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Build a BMI calculator: BMI = weight_kg / height_m². Categorize into underweight/normal/overweight/obese.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build a complete loan comparison tool: given multiple loan offers (varying rates, terms), compute monthly payment, total interest, and rank by total cost.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design a `NumericProfiler` that computes: min, max, mean, median, std, skewness, kurtosis, % NaN, % NULL, % zero for any numeric column.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Compare performance of `log` vs UDF-based log on 10M rows.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Test: `sqrt(-1)`, `log(0)`, `1/0`, `pow(0,0)`, `factorial(21)` (overflow?), `round(2.5)` vs `bround(2.5)`.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build a feature engineering pipeline that applies z-score, log-transform, clipping, and min-max scaling to multiple numeric columns in a configurable way.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create a visual reference: "Which math function for which task?" covering rounding, scaling, distance, growth, and randomization.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all for solutions.
import math  # Python math.

# --- Level 1: Rounding comparison ---
print("=== Level 1: round vs bround ===")  # Print heading.
round_df = spark.createDataFrame([(2.5,), (3.5,), (4.5,), (5.5,)], ["val"])  # Half values.
round_df.select(
    col("val"),  # Original.
    round(col("val"), 0).alias("round"),  # HALF_UP.
    bround(col("val"), 0).alias("bround"),  # HALF_EVEN (banker's).
).show()  # Display comparison.

# --- Level 4: BMI Calculator ---
print("=== Level 4: BMI Calculator ===")  # Print heading.
bmi_df = spark.createDataFrame([
    ("Alice", 60.0, 1.65),  # Normal.
    ("Bob", 95.0, 1.80),  # Overweight.
    ("Charlie", 50.0, 1.75),  # Underweight.
    ("Diana", 110.0, 1.60),  # Obese.
], ["name", "weight_kg", "height_m"])  # BMI data.

bmi_df.select(
    col("name"),  # Keep name.
    col("weight_kg"), col("height_m"),  # Keep measurements.
    round(col("weight_kg") / pow(col("height_m"), lit(2)), 1).alias("bmi"),  # BMI formula.
    when(col("weight_kg") / pow(col("height_m"), lit(2)) < 18.5, "Underweight")
        .when(col("weight_kg") / pow(col("height_m"), lit(2)) < 25.0, "Normal")
        .when(col("weight_kg") / pow(col("height_m"), lit(2)) < 30.0, "Overweight")
        .otherwise("Obese").alias("category"),  # BMI category.
).show(truncate=False)  # Display BMI results.

# --- Level 5: Digit counter using log10 ---
print("=== Level 5: Digit Counter ===")  # Print heading.
digit_df = spark.createDataFrame([(1,), (9,), (10,), (99,), (100,), (999,), (1000,)], ["num"])  # Numbers.
digit_df.select(
    col("num"),  # Original.
    (floor(log10(col("num").cast("double"))) + 1).alias("num_digits"),  # Digit count.
).show()  # Display digit counts.

# --- Level 8: Edge Cases ---
print("=== Level 8: Edge Cases ===")  # Print heading.
edge = spark.createDataFrame([(1,)], ["x"])  # Single row.
edge.select(
    sqrt(lit(-1)).alias("sqrt_neg1"),  # NaN.
    log(lit(0.0)).alias("log_zero"),  # -Infinity.
    (lit(1.0) / lit(0.0)).alias("div_by_zero"),  # Infinity.
    pow(lit(0), lit(0)).alias("zero_to_zero"),  # 1.0 (mathematical convention).
    round(lit(2.5), 0).alias("round_2_5"),  # 3.0.
    bround(lit(2.5), 0).alias("bround_2_5"),  # 2.0 (banker's).
).show(truncate=False)  # Display edge case results.

print("✅ All homework solutions complete!")  # Completion message.