# Databricks notebook source
# DBTITLE 1,NB_23 Header
# MAGIC %md
# MAGIC # NB_23 — Aggregations: groupBy, agg, All Functions
# MAGIC
# MAGIC **Module 4: DataFrame Operations** | Notebook 23 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC - groupBy() basics and multiple columns
# MAGIC - agg() with multiple aggregate functions
# MAGIC - count, sum, avg, min, max, mean
# MAGIC - approx_count_distinct, countDistinct
# MAGIC - collect_list, collect_set
# MAGIC - first, last (with ignoreNulls)
# MAGIC - Statistical: skewness, kurtosis, stddev, variance, corr, covar
# MAGIC - rollup() and cube() for subtotals
# MAGIC - pivot() for reshaping
# MAGIC - grouping() and grouping_id()
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Core Analytics)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are Aggregations?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are Aggregations? (Real-World Analogy)
# MAGIC
# MAGIC ### 🏭 The Census Bureau
# MAGIC
# MAGIC Imagine the national census collecting data from millions of people:
# MAGIC
# MAGIC | Census Question | PySpark Aggregation | Result |
# MAGIC |---|---|---|
# MAGIC | "How many people in each state?" | `groupBy("state").count()` | State-level counts |
# MAGIC | "Average income per city?" | `groupBy("city").avg("income")` | City averages |
# MAGIC | "Total sales by region + year?" | `groupBy("region","year").sum("sales")` | Multi-dim totals |
# MAGIC | "Unique products sold per store?" | `groupBy("store").agg(countDistinct("product"))` | Distinct counts |
# MAGIC | "List all items bought per customer?" | `groupBy("customer").agg(collect_list("item"))` | Aggregated lists |
# MAGIC
# MAGIC ### Aggregation = Many Rows → Fewer Rows
# MAGIC - Input: 1 million individual sales records
# MAGIC - Output: 50 regional summaries (one per region)
# MAGIC - Key idea: **Collapse groups into single summary rows**
# MAGIC
# MAGIC ### The Three Parts
# MAGIC 1. **groupBy()** — Define the groups (which columns to group on)
# MAGIC 2. **agg()** — Define what to compute for each group
# MAGIC 3. **Result** — One row per unique group combination

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Aggregations Work
# MAGIC %md
# MAGIC ## SECTION 2 — How Aggregations Work (Internal Mechanics)
# MAGIC
# MAGIC ### Execution Plan
# MAGIC
# MAGIC ```
# MAGIC df.groupBy("dept").agg(sum("salary"), avg("age"))
# MAGIC
# MAGIC Phase 1: Partial Aggregation (per partition, no shuffle)
# MAGIC   Partition 1: {Eng: (sum=200K, count=2), Mkt: (sum=72K, count=1)}
# MAGIC   Partition 2: {Eng: (sum=95K, count=1), Sales: (sum=65K, count=1)}
# MAGIC
# MAGIC Phase 2: Shuffle (group same keys together)
# MAGIC   Shuffle to: {Eng: both partials, Mkt: its partial, Sales: its partial}
# MAGIC
# MAGIC Phase 3: Final Aggregation (merge partials)
# MAGIC   {Eng: sum=295K/count=3=avg:98.3K, Mkt: sum=72K/1=72K, Sales: sum=65K/1=65K}
# MAGIC ```
# MAGIC
# MAGIC ### All Aggregate Functions
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────┬────────────────────┬─────────────────────┐
# MAGIC │  BASIC           │  COLLECTION         │  STATISTICAL          │
# MAGIC │  count()         │  collect_list()     │  stddev() / stddev_pop │
# MAGIC │  sum()           │  collect_set()      │  variance() / var_pop  │
# MAGIC │  avg() / mean()  │  first()            │  skewness()            │
# MAGIC │  min() / max()   │  last()             │  kurtosis()            │
# MAGIC │  countDistinct() │                    │  corr()               │
# MAGIC │  approx_count    │                    │  covar_pop/samp()     │
# MAGIC ├──────────────────┼────────────────────┼─────────────────────┤
# MAGIC │  MULTI-DIM       │  CONDITIONAL        │  PERCENTILE           │
# MAGIC │  rollup()        │  sum(when(...))     │  percentile_approx()  │
# MAGIC │  cube()          │  count(when(...))   │  approxQuantile()     │
# MAGIC │  pivot()         │  filter(having)     │                       │
# MAGIC │  grouping_id()   │                    │                       │
# MAGIC └──────────────────┴────────────────────┴─────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Shuffle Cost
# MAGIC - groupBy ALWAYS causes a shuffle (data redistribution)
# MAGIC - Exception: If data is already partitioned by groupBy key
# MAGIC - Optimization: Use `approx_count_distinct` instead of `countDistinct` for large data (3-5x faster)

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: groupBy basics
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: groupBy Basics
# ============================================================
# Real-world: Summarizing sales data by department

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, min as _min, max as _max

spark = SparkSession.builder.getOrCreate()  # Get session

# Sales data
sales_data = [
    ("Electronics", "Laptop", 999, 3),
    ("Electronics", "Phone", 699, 7),
    ("Electronics", "Tablet", 449, 5),
    ("Clothing", "Shirt", 49, 20),
    ("Clothing", "Pants", 89, 12),
    ("Clothing", "Jacket", 199, 4),
    ("Food", "Coffee", 15, 100),
    ("Food", "Snacks", 8, 150),
    ("Food", "Lunch", 12, 80),
    ("Electronics", "Headphones", 299, 8),
]

df = spark.createDataFrame(sales_data, ["department", "product", "price", "qty_sold"])
print("=== Original Sales Data ===")
df.show()  # Show all 10 rows

# --- Basic groupBy with single aggregation ---
print("\n=== groupBy + count() ===")
df.groupBy("department").count().show()  # Count rows per department

# --- groupBy with sum ---
print("\n=== groupBy + sum() ===")
df.groupBy("department").sum("qty_sold").show()  # Total qty per department

# --- groupBy with avg ---
print("\n=== groupBy + avg(price) ===")
df.groupBy("department").avg("price").show()  # Average price per department

# --- groupBy with min and max ---
print("\n=== groupBy + min/max ===")
df.groupBy("department").agg(
    _min("price").alias("cheapest"),   # Cheapest item per dept
    _max("price").alias("priciest"),   # Most expensive per dept
).show()

# --- Multiple columns in groupBy ---
print("\n=== groupBy multiple columns ===")
# This doesn't make sense here but shows syntax:
df.withColumn("revenue", col("price") * col("qty_sold")) \
    .groupBy("department") \
    .agg(
        count("*").alias("num_products"),          # Count products
        _sum(col("price") * col("qty_sold")).alias("total_revenue"),  # Revenue
        avg("price").alias("avg_price"),            # Average price
    ).show()

# Expected Output:
# +------------+------------+-------------+---------+
# |department  |num_products|total_revenue|avg_price|
# +------------+------------+-------------+---------+
# |Electronics |4           |13159        |611.5    |
# |Clothing    |3           |2804         |112.33   |
# |Food        |3           |3510         |11.67    |
# +------------+------------+-------------+---------+

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: agg with multiple functions
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: agg() with Multiple Functions
# ============================================================
# Real-world: Complete department performance dashboard

from pyspark.sql.functions import (
    col, count, sum as _sum, avg, min as _min, max as _max,
    round as _round, countDistinct, mean
)

# Employee data for aggregation
emp_data = [
    ("Engineering", "Alice", 110000, 28),
    ("Engineering", "Bob", 95000, 35),
    ("Engineering", "Charlie", 88000, 42),
    ("Engineering", "Diana", 105000, 31),
    ("Marketing", "Eve", 72000, 29),
    ("Marketing", "Frank", 68000, 33),
    ("Marketing", "Grace", 75000, 27),
    ("Sales", "Henry", 65000, 38),
    ("Sales", "Ivy", 62000, 25),
    ("Sales", "Jack", 70000, 45),
    ("Sales", "Kate", 58000, 23),
]

emp_df = spark.createDataFrame(emp_data, ["dept", "name", "salary", "age"])

# Multiple aggregations in one agg() call
print("=== Department Performance Summary ===")
dept_summary = emp_df.groupBy("dept").agg(
    count("*").alias("headcount"),                      # Total employees
    _round(avg("salary"), 2).alias("avg_salary"),       # Average salary
    _round(avg("age"), 1).alias("avg_age"),             # Average age
    _min("salary").alias("min_salary"),                 # Lowest salary
    _max("salary").alias("max_salary"),                 # Highest salary
    _sum("salary").alias("total_payroll"),              # Total payroll
    (_max("salary") - _min("salary")).alias("salary_range"),  # Salary spread
)

dept_summary.show(truncate=False)  # Show department stats

# countDistinct vs count
print("\n=== countDistinct vs count ===")
from pyspark.sql.functions import approx_count_distinct

# Add some duplicate departments for demo
print(f"count('dept'): counts non-null values = {emp_df.select(count('dept')).first()[0]}")
print(f"countDistinct('dept'): unique values = {emp_df.select(countDistinct('dept')).first()[0]}")
print(f"approx_count_distinct('dept'): approximate = {emp_df.select(approx_count_distinct('dept')).first()[0]}")

# mean() is an alias for avg()
print("\n=== mean() == avg() ===")
emp_df.groupBy("dept").agg(
    avg("salary").alias("avg_salary"),   # avg()
    mean("salary").alias("mean_salary"), # mean() — identical!
).show()

# Expected Output:
# +----------+---------+----------+-------+----------+----------+------------+------------+
# |dept      |headcount|avg_salary|avg_age|min_salary|max_salary|total_payroll|salary_range|
# +----------+---------+----------+-------+----------+----------+------------+------------+
# |Engineering|4       |99500.0   |34.0   |88000     |110000    |398000      |22000       |
# |Marketing |3       |71666.67  |29.7   |68000     |75000     |215000      |7000        |
# |Sales     |4       |63750.0   |32.8   |58000     |70000     |255000      |12000       |
# +----------+---------+----------+-------+----------+----------+------------+------------+

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: collect_list, collect_set, first, last
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: collect_list, collect_set, first, last
# ============================================================
# Real-world: Aggregating into arrays (all products per category)

from pyspark.sql.functions import (
    col, collect_list, collect_set, first, last,
    sort_array, size, array_distinct
)

# Order data — multiple orders per customer
order_data = [
    ("Alice", "Laptop", "Electronics", "2024-01-05"),
    ("Alice", "Phone", "Electronics", "2024-01-12"),
    ("Alice", "Coffee", "Food", "2024-01-15"),
    ("Alice", "Laptop", "Electronics", "2024-02-01"),  # Repeat purchase!
    ("Bob", "Shirt", "Clothing", "2024-01-08"),
    ("Bob", "Pants", "Clothing", "2024-01-20"),
    ("Bob", "Phone", "Electronics", "2024-02-05"),
    ("Charlie", "Coffee", "Food", "2024-01-03"),
    ("Charlie", "Coffee", "Food", "2024-01-10"),  # Same item again
    ("Charlie", "Snacks", "Food", "2024-01-18"),
]

df = spark.createDataFrame(order_data, ["customer", "product", "category", "order_date"])

# collect_list — gather all values into array (keeps duplicates!)
print("=== collect_list() — All Values (with duplicates) ===")
df.groupBy("customer").agg(
    collect_list("product").alias("all_products"),       # All products including repeats
    collect_list("category").alias("all_categories"),   # All categories
    size(collect_list("product")).alias("total_orders"), # Count of orders
).show(truncate=False)

# collect_set — gather UNIQUE values into array (no duplicates)
print("\n=== collect_set() — Unique Values Only ===")
df.groupBy("customer").agg(
    collect_set("product").alias("unique_products"),    # Unique products only
    collect_set("category").alias("unique_categories"), # Unique categories
    size(collect_set("product")).alias("distinct_products"),  # Count unique
).show(truncate=False)

# first() and last() — get first/last value in group
print("\n=== first() and last() ===")
print("NOTE: first/last are NON-DETERMINISTIC without orderBy!")
df.groupBy("customer").agg(
    first("product").alias("first_product"),            # First seen product
    last("product").alias("last_product"),              # Last seen product
    first("order_date").alias("first_order_date"),     # First order date
    last("order_date").alias("last_order_date"),       # Last order date
).show(truncate=False)

# Sort collected arrays
print("\n=== Sorted collect_list ===")
df.groupBy("customer").agg(
    sort_array(collect_set("product")).alias("products_sorted"),  # Alphabetical
    sort_array(collect_list("order_date")).alias("dates_sorted"),  # Chronological
).show(truncate=False)

# Expected Output (collect_list):
# +--------+----------------------------------------+---------------------------+
# |customer|all_products                            |total_orders               |
# +--------+----------------------------------------+---------------------------+
# |Alice   |[Laptop, Phone, Coffee, Laptop]         |4                          |
# |Bob     |[Shirt, Pants, Phone]                   |3                          |
# |Charlie |[Coffee, Coffee, Snacks]                |3                          |
# +--------+----------------------------------------+---------------------------+

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Statistical Functions
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Statistical Aggregates
# ============================================================
# Real-world: Analyzing distribution of metrics per segment

from pyspark.sql.functions import (
    col, stddev, stddev_pop, stddev_samp,
    variance, var_pop, var_samp,
    skewness, kurtosis, corr, covar_pop, covar_samp,
    round as _round, count, avg, percentile_approx
)

# Generate more realistic data for statistics
import random
random.seed(42)

stat_data = []
for dept in ["Engineering", "Marketing", "Sales"]:
    base_salary = {"Engineering": 95000, "Marketing": 70000, "Sales": 60000}[dept]
    base_age = {"Engineering": 32, "Marketing": 29, "Sales": 35}[dept]
    for i in range(20):  # 20 employees per dept
        salary = base_salary + random.gauss(0, 15000)
        age = base_age + random.randint(-8, 8)
        years_exp = max(0, age - 22 + random.randint(-3, 3))
        stat_data.append((dept, round(salary, 2), age, years_exp))

stat_df = spark.createDataFrame(stat_data, ["dept", "salary", "age", "years_exp"])

# Standard deviation and variance
print("=== Standard Deviation & Variance ===")
stat_df.groupBy("dept").agg(
    count("*").alias("n"),
    _round(avg("salary"), 0).alias("mean_salary"),
    _round(stddev("salary"), 0).alias("stddev_salary"),       # Sample std (n-1)
    _round(stddev_pop("salary"), 0).alias("stddev_pop_salary"), # Population std (n)
    _round(variance("salary"), 0).alias("variance_salary"),   # Sample variance
).show(truncate=False)

# Skewness and Kurtosis
print("\n=== Skewness & Kurtosis ===")
print("Skewness: 0=symmetric, >0=right-tail, <0=left-tail")
print("Kurtosis: 0=normal, >0=heavy tails, <0=light tails")
stat_df.groupBy("dept").agg(
    _round(skewness("salary"), 3).alias("salary_skew"),    # Distribution shape
    _round(kurtosis("salary"), 3).alias("salary_kurt"),    # Tail heaviness
    _round(skewness("age"), 3).alias("age_skew"),
).show(truncate=False)

# Correlation and Covariance
print("\n=== Correlation & Covariance ===")
print("Correlation: -1 to +1 (linear relationship strength)")
stat_df.groupBy("dept").agg(
    _round(corr("salary", "years_exp"), 3).alias("corr_salary_exp"),      # Salary vs experience
    _round(corr("salary", "age"), 3).alias("corr_salary_age"),           # Salary vs age
    _round(covar_samp("salary", "years_exp"), 0).alias("cov_salary_exp"), # Covariance
).show(truncate=False)

# Percentiles (approximate)
print("\n=== Percentile Approximation ===")
stat_df.groupBy("dept").agg(
    percentile_approx("salary", 0.25).alias("p25_salary"),    # 25th percentile
    percentile_approx("salary", 0.50).alias("p50_salary"),    # Median
    percentile_approx("salary", 0.75).alias("p75_salary"),    # 75th percentile
    percentile_approx("salary", 0.90).alias("p90_salary"),    # 90th percentile
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Rollup and Cube
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: rollup() and cube()
# ============================================================
# Real-world: Building reports with subtotals and grand totals

from pyspark.sql.functions import (
    col, sum as _sum, count, avg, round as _round,
    grouping, grouping_id, coalesce, lit
)

# Regional sales data
regional_data = [
    ("North", "Electronics", "Q1", 150000),
    ("North", "Electronics", "Q2", 180000),
    ("North", "Clothing", "Q1", 90000),
    ("North", "Clothing", "Q2", 110000),
    ("South", "Electronics", "Q1", 120000),
    ("South", "Electronics", "Q2", 140000),
    ("South", "Clothing", "Q1", 75000),
    ("South", "Clothing", "Q2", 95000),
]

df = spark.createDataFrame(regional_data, ["region", "category", "quarter", "revenue"])

# === rollup() — Hierarchical subtotals ===
print("=== rollup() — Hierarchical Subtotals ===")
print("Produces: group totals + subtotals up to grand total")
print("NULL in a column = subtotal for that level")
print()

# rollup(A, B) gives: (A,B), (A,null), (null,null)
df.rollup("region", "category").agg(
    _sum("revenue").alias("total_revenue"),  # Sum revenue at each level
    count("*").alias("num_records"),         # Count records
).orderBy(col("region").asc_nulls_last(), col("category").asc_nulls_last()) \
 .show(truncate=False)

print("\n=== Interpret rollup results ===")
print("region=North, category=Electronics: subtotal for that combo")
print("region=North, category=NULL: subtotal for ALL of North")
print("region=NULL, category=NULL: GRAND TOTAL")

# === cube() — ALL dimension combinations ===
print("\n=== cube() — All Dimension Combinations ===")
print("Produces subtotals for EVERY combination of grouping columns")
print("cube(A, B) gives: (A,B), (A,null), (null,B), (null,null)")
print()

df.cube("region", "category").agg(
    _sum("revenue").alias("total_revenue"),
    count("*").alias("num_records"),
).orderBy(col("region").asc_nulls_last(), col("category").asc_nulls_last()) \
 .show(truncate=False)

# === grouping() and grouping_id() — identify subtotal rows ===
print("\n=== grouping() — Distinguish Subtotals from Real NULLs ===")
df.cube("region", "category").agg(
    _sum("revenue").alias("total_revenue"),
    grouping("region").alias("is_region_subtotal"),     # 1 if this is a subtotal level
    grouping("category").alias("is_category_subtotal"), # 1 if this is a subtotal level
    grouping_id("region", "category").alias("group_level"),  # Binary encoding of level
).orderBy("group_level", "region", "category") \
 .show(truncate=False)

print("grouping_id values:")
print("  0 = both columns are real values (detail row)")
print("  1 = category is subtotal (region subtotal)")
print("  2 = region is subtotal (category subtotal)")
print("  3 = both are subtotals (grand total)")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: pivot
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: pivot()
# ============================================================
# Real-world: Transforming rows into columns (cross-tab reports)

from pyspark.sql.functions import (
    col, sum as _sum, avg, count, first, round as _round, coalesce, lit
)

# Monthly revenue data
monthly_data = [
    ("Alice", "Jan", 15000), ("Alice", "Feb", 18000), ("Alice", "Mar", 22000),
    ("Bob", "Jan", 12000), ("Bob", "Feb", 11000), ("Bob", "Mar", 14000),
    ("Charlie", "Jan", 20000), ("Charlie", "Feb", 25000), ("Charlie", "Mar", 19000),
    ("Alice", "Jan", 5000),  # Alice has 2 records in Jan!
]

df = spark.createDataFrame(monthly_data, ["salesperson", "month", "revenue"])

# === Basic pivot — rows to columns ===
print("=== pivot() — Rows to Columns ===")
print("Before: Each row is (salesperson, month, revenue)")
print("After: Each row is (salesperson, Jan_rev, Feb_rev, Mar_rev)")
print()

pivoted = df.groupBy("salesperson").pivot("month").sum("revenue")
pivoted.show(truncate=False)  # Cross-tab view

# === pivot with explicit values (FASTER! Avoids extra scan) ===
print("\n=== pivot with explicit values (performance best practice) ===")
pivoted_fast = df.groupBy("salesperson") \
    .pivot("month", ["Jan", "Feb", "Mar"]) \
    .sum("revenue")

pivoted_fast.show(truncate=False)  # Same result, faster

# === pivot with multiple aggregations ===
print("\n=== pivot with avg ===")
df.groupBy("salesperson") \
    .pivot("month", ["Jan", "Feb", "Mar"]) \
    .avg("revenue") \
    .show(truncate=False)  # Average (divides Alice's Jan by 2)

# === Fill NULLs in pivoted result ===
print("\n=== Handling NULLs after pivot ===")
# Add a person with missing months
extra = spark.createDataFrame([("Diana", "Jan", 8000)], ["salesperson", "month", "revenue"])
df_with_gaps = df.union(extra)

df_with_gaps.groupBy("salesperson") \
    .pivot("month", ["Jan", "Feb", "Mar"]) \
    .sum("revenue") \
    .fillna(0) \
    .show(truncate=False)  # 0 instead of NULL for missing months

# === Un-pivot (reverse: columns to rows) using stack ===
print("\n=== Un-pivot with stack() ===")
from pyspark.sql.functions import expr

unpivoted = pivoted_fast.select(
    col("salesperson"),
    expr("stack(3, 'Jan', Jan, 'Feb', Feb, 'Mar', Mar) as (month, revenue)")
).filter(col("revenue").isNotNull())  # Remove NULLs from missing combos

unpivoted.show(truncate=False)  # Back to original format

# Expected Output (pivoted):
# +-----------+-----+-----+-----+
# |salesperson|Jan  |Feb  |Mar  |
# +-----------+-----+-----+-----+
# |Alice      |20000|18000|22000|
# |Bob        |12000|11000|14000|
# |Charlie    |20000|25000|19000|
# +-----------+-----+-----+-----+
# Note: Alice Jan = 15000 + 5000 = 20000 (aggregated!)

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Conditional Aggregation
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Conditional Aggregation
# ============================================================
# Real-world: Building complex KPI dashboards with conditional logic

from pyspark.sql.functions import (
    col, count, sum as _sum, avg, when, lit,
    round as _round, countDistinct, expr
)

# E-commerce order data
ecom_data = [
    ("Alice", "Electronics", 500, "completed", "2024-01-05"),
    ("Alice", "Clothing", 80, "completed", "2024-01-10"),
    ("Alice", "Electronics", 1200, "completed", "2024-02-01"),
    ("Alice", "Food", 30, "cancelled", "2024-02-05"),
    ("Bob", "Electronics", 800, "completed", "2024-01-12"),
    ("Bob", "Clothing", 150, "returned", "2024-01-20"),
    ("Bob", "Food", 45, "completed", "2024-02-08"),
    ("Charlie", "Electronics", 2000, "completed", "2024-01-15"),
    ("Charlie", "Clothing", 300, "completed", "2024-01-22"),
    ("Charlie", "Electronics", 150, "cancelled", "2024-02-10"),
    ("Charlie", "Food", 60, "completed", "2024-02-15"),
]

df = spark.createDataFrame(ecom_data, ["customer", "category", "amount", "status", "date"])

# === Conditional counts (count where...) ===
print("=== Conditional Aggregation — Customer KPI Dashboard ===")

kpis = df.groupBy("customer").agg(
    # Total orders
    count("*").alias("total_orders"),
    
    # Conditional counts
    count(when(col("status") == "completed", 1)).alias("completed_orders"),
    count(when(col("status") == "cancelled", 1)).alias("cancelled_orders"),
    count(when(col("status") == "returned", 1)).alias("returned_orders"),
    
    # Conditional sums
    _sum(when(col("status") == "completed", col("amount")).otherwise(0)).alias("revenue"),
    _sum(when(col("status") == "cancelled", col("amount")).otherwise(0)).alias("lost_revenue"),
    
    # Conditional averages
    _round(avg(when(col("status") == "completed", col("amount"))), 2).alias("avg_order_value"),
    
    # Category-specific sums (mini pivot without pivot!)
    _sum(when(col("category") == "Electronics", col("amount")).otherwise(0)).alias("electronics_spend"),
    _sum(when(col("category") == "Clothing", col("amount")).otherwise(0)).alias("clothing_spend"),
    _sum(when(col("category") == "Food", col("amount")).otherwise(0)).alias("food_spend"),
    
    # Distinct categories
    countDistinct("category").alias("categories_shopped"),
)

kpis.show(truncate=False)  # Show customer dashboard

# Calculate derived metrics
print("\n=== Derived Metrics ===")
kpis.select(
    col("customer"),
    col("revenue"),
    _round(col("completed_orders") / col("total_orders") * 100, 1).alias("completion_rate_%"),
    _round(col("lost_revenue") / (col("revenue") + col("lost_revenue")) * 100, 1).alias("cancellation_impact_%"),
).show(truncate=False)

# Expected: Rich KPI per customer, all computed in a single pass

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Multi-level Aggregation Pipeline
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Multi-Level Aggregation Pipeline
# ============================================================
# Real-world: Building aggregation at multiple granularities

from pyspark.sql.functions import (
    col, count, sum as _sum, avg, min as _min, max as _max,
    round as _round, collect_list, collect_set, size,
    expr, lit, when, datediff, current_date
)

# Transaction data
txn_data = [
    ("Store_A", "North", "Electronics", 500, "2024-01-05"),
    ("Store_A", "North", "Electronics", 800, "2024-01-12"),
    ("Store_A", "North", "Clothing", 200, "2024-01-08"),
    ("Store_B", "North", "Electronics", 600, "2024-01-10"),
    ("Store_B", "North", "Food", 100, "2024-01-15"),
    ("Store_C", "South", "Electronics", 900, "2024-01-03"),
    ("Store_C", "South", "Clothing", 350, "2024-01-20"),
    ("Store_C", "South", "Clothing", 150, "2024-01-25"),
    ("Store_D", "South", "Food", 80, "2024-01-07"),
    ("Store_D", "South", "Electronics", 1200, "2024-01-18"),
]

txn_df = spark.createDataFrame(txn_data, ["store", "region", "category", "amount", "date"])

# Level 1: Store-level summary
print("=== Level 1: Store Summary ===")
store_summary = txn_df.groupBy("store", "region").agg(
    count("*").alias("num_transactions"),
    _sum("amount").alias("total_revenue"),
    _round(avg("amount"), 2).alias("avg_transaction"),
    _min("date").alias("first_transaction"),
    _max("date").alias("last_transaction"),
    size(collect_set("category")).alias("category_diversity"),
    collect_set("category").alias("categories"),
)
store_summary.show(truncate=False)

# Level 2: Region-level summary (aggregate the aggregates)
print("\n=== Level 2: Region Summary ===")
region_summary = txn_df.groupBy("region").agg(
    countDistinct("store").alias("num_stores"),
    count("*").alias("total_transactions"),
    _sum("amount").alias("region_revenue"),
    _round(avg("amount"), 2).alias("avg_transaction"),
    collect_set("category").alias("all_categories"),
)
region_summary.show(truncate=False)

# Level 3: Category-level summary
print("\n=== Level 3: Category Summary ===")
cat_summary = txn_df.groupBy("category").agg(
    count("*").alias("num_sales"),
    _sum("amount").alias("total_revenue"),
    _round(avg("amount"), 2).alias("avg_sale"),
    countDistinct("store").alias("stores_selling"),
    countDistinct("region").alias("regions_present"),
)
cat_summary.show(truncate=False)

# Level 4: Grand total
print("\n=== Level 4: Grand Total ===")
txn_df.agg(
    count("*").alias("total_transactions"),
    _sum("amount").alias("grand_total_revenue"),
    _round(avg("amount"), 2).alias("global_avg"),
    countDistinct("store").alias("total_stores"),
    countDistinct("category").alias("total_categories"),
).show(truncate=False)

print("✅ Multi-level aggregation complete!")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Aggregation Performance Patterns
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Performance Optimization Patterns
# ============================================================
# Real-world: Optimizing aggregations on large datasets

import time
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, countDistinct,
    approx_count_distinct, round as _round, expr, lit
)

# Generate larger dataset for performance testing
big_df = spark.range(1000000).select(
    col("id"),
    expr("CASE WHEN id % 100 < 50 THEN 'A' WHEN id % 100 < 80 THEN 'B' ELSE 'C' END").alias("group"),
    expr("concat('cat_', cast(id % 10000 as string))").alias("category"),  # 10K unique
    expr("rand() * 10000").alias("amount"),
)

print(f"=== Performance Patterns ({big_df.count():,} rows) ===")

# Pattern 1: approx_count_distinct vs countDistinct
print("\n--- Pattern 1: Approximate vs Exact Distinct Count ---")

start = time.time()
exact = big_df.groupBy("group").agg(countDistinct("category").alias("exact")).collect()
t_exact = time.time() - start

start = time.time()
approx = big_df.groupBy("group").agg(approx_count_distinct("category", 0.05).alias("approx")).collect()
t_approx = time.time() - start

print(f"countDistinct:        {t_exact:.3f}s")
print(f"approx_count_distinct: {t_approx:.3f}s (5% error tolerance)")
print(f"Speedup: {t_exact/max(t_approx,0.001):.1f}x")

# Pattern 2: Pre-filter before aggregation
print("\n--- Pattern 2: Filter BEFORE groupBy ---")

start = time.time()
# BAD: Aggregate all, then filter result
result1 = big_df.groupBy("group").agg(_sum("amount").alias("total")).filter(col("group") == "A").collect()
t_after = time.time() - start

start = time.time()
# GOOD: Filter first, then aggregate (less data to shuffle)
result2 = big_df.filter(col("group") == "A").groupBy("group").agg(_sum("amount").alias("total")).collect()
t_before = time.time() - start

print(f"groupBy then filter: {t_after:.3f}s")
print(f"filter then groupBy: {t_before:.3f}s")
print(f"Speedup: {t_after/max(t_before,0.001):.1f}x")

# Pattern 3: Avoid collect_list on high-cardinality groups
print("\n--- Pattern 3: collect_list size awareness ---")
print("⚠️  collect_list() stores ALL values in memory per group!")
print("   If a group has 1M values, that's 1M items in one array.")
print("   Alternative: Use approx functions or limit with slice()")

# Pattern 4: Repartition by groupBy key before aggregation
print("\n--- Pattern 4: Repartition hint ---")
print("If you do MULTIPLE groupBy operations on the same key:")
print("  df_by_group = df.repartition('group')  # One shuffle")
print("  result1 = df_by_group.groupBy('group').sum(...)  # No shuffle!")
print("  result2 = df_by_group.groupBy('group').avg(...)  # No shuffle!")

print("\n✅ Key takeaways:")
print("  1. Use approx_count_distinct for large distinct counts")
print("  2. Filter BEFORE groupBy when possible")
print("  3. Be careful with collect_list on large groups")
print("  4. Repartition by key if doing multiple aggregations")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Aggregations
# MAGIC
# MAGIC ### ❌ Mistake 1: Selecting non-aggregated columns after groupBy
# MAGIC ```python
# MAGIC # WRONG — "name" is not in groupBy or an aggregate!
# MAGIC df.groupBy("dept").agg(sum("salary"), col("name"))  # AnalysisException!
# MAGIC
# MAGIC # CORRECT — Every column must be grouped or aggregated
# MAGIC df.groupBy("dept").agg(sum("salary"), first("name"))  # first() is an aggregate
# MAGIC df.groupBy("dept").agg(sum("salary"), collect_list("name"))  # collect all names
# MAGIC ```
# MAGIC **Why:** After groupBy, each group becomes ONE row. Which "name" should it pick? You must tell Spark explicitly.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 2: Using Python sum/min/max instead of PySpark functions
# MAGIC ```python
# MAGIC # WRONG — This is Python's built-in sum, NOT PySpark's!
# MAGIC df.groupBy("dept").agg(sum(col("salary")))  # TypeError or wrong result!
# MAGIC
# MAGIC # CORRECT — Import PySpark's sum with alias
# MAGIC from pyspark.sql.functions import sum as _sum
# MAGIC df.groupBy("dept").agg(_sum("salary"))
# MAGIC ```
# MAGIC **Why:** Python's `sum()`, `min()`, `max()` work on Python iterables, not Spark Columns. Always alias the imports.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 3: pivot() without specifying values
# MAGIC ```python
# MAGIC # SLOW — Spark must scan data first to find all pivot values
# MAGIC df.groupBy("name").pivot("month").sum("revenue")  # Extra scan!
# MAGIC
# MAGIC # FAST — Provide values explicitly
# MAGIC df.groupBy("name").pivot("month", ["Jan","Feb","Mar"]).sum("revenue")
# MAGIC ```
# MAGIC **Why:** Without explicit values, Spark runs an extra aggregation to discover all unique pivot values. Always provide them if known.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 4: Assuming first()/last() are deterministic
# MAGIC ```python
# MAGIC # WRONG assumption — first() picks any arbitrary row from the group!
# MAGIC df.groupBy("dept").agg(first("name"))  # Which name? Non-deterministic!
# MAGIC
# MAGIC # CORRECT — Sort first, then use first()
# MAGIC from pyspark.sql.window import Window
# MAGIC w = Window.partitionBy("dept").orderBy("salary")
# MAGIC df.withColumn("rn", row_number().over(w)).filter(col("rn")==1)
# MAGIC ```
# MAGIC **Why:** Without ordering, `first()`/`last()` depend on data distribution across partitions. Results vary between runs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 5: Confusing rollup NULLs with real NULLs
# MAGIC ```python
# MAGIC # PROBLEM — How to tell if NULL means "subtotal" or "actual NULL value"?
# MAGIC df.rollup("region", "city").sum("sales")  # NULL region = grand total? Or missing?
# MAGIC
# MAGIC # SOLUTION — Use grouping() to distinguish
# MAGIC df.rollup("region", "city").agg(
# MAGIC     sum("sales"),
# MAGIC     grouping("region").alias("is_region_total"),  # 1 = subtotal, 0 = real value
# MAGIC     grouping("city").alias("is_city_total"),
# MAGIC )
# MAGIC ```
# MAGIC **Why:** `rollup`/`cube` use NULL to indicate subtotal rows. If your data already has NULLs, use `grouping()` to disambiguate.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Aggregation Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Create a sales DataFrame. Use `groupBy("category").count()` and `groupBy("category").sum("revenue")`.
# MAGIC 2. Use `agg()` to compute count, sum, avg, min, max in one call.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Add `countDistinct("product")` to see how many unique products per category.
# MAGIC 4. Replace `sum` with `collect_list` to see all products per category.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Use groupBy + agg + when to create conditional counts (completed vs cancelled orders per customer).
# MAGIC 6. Use pivot + fillna to create a cross-tab of sales by region × month.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Given web traffic data, compute per-page: total visits, unique visitors, avg session duration, bounce rate (sessions with 1 pageview).
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build a complete sales dashboard:
# MAGIC    - Overall KPIs (total revenue, order count, avg order value)
# MAGIC    - By category, by region, by month
# MAGIC    - Rollup with subtotals
# MAGIC    - Top-5 products by revenue
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design a reusable `aggregate_at_levels()` function that takes a DataFrame, a list of dimension columns, and a list of metrics, and returns aggregations at every level (detail, subtotals, grand total).
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. With 10M rows and 100K distinct categories:
# MAGIC     - Compare `countDistinct` vs `approx_count_distinct` (speed & accuracy)
# MAGIC     - Measure impact of filter-before-groupBy vs filter-after
# MAGIC     - Test repartition by groupBy key for repeated aggregations
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Test:
# MAGIC     - groupBy on a column with NULLs (NULL becomes its own group)
# MAGIC     - collect_list with NULLs (NULLs are included!)
# MAGIC     - stddev/variance with only 1 row per group (returns NULL)
# MAGIC     - pivot with > 10,000 unique values (performance impact)
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build an aggregation framework:
# MAGIC     - Configurable dimensions and metrics via YAML/dict
# MAGIC     - Automatic rollup generation
# MAGIC     - Data quality checks (no negative counts, sums match)
# MAGIC     - Incremental aggregation (only reprocess new data)
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create a comparison guide: "groupBy vs rollup vs cube vs pivot — When to use which?"

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *

# --- Level 1: Basic Aggregation ---
print("=== Level 1: Basic groupBy ===")
sales = spark.createDataFrame([
    ("Electronics", "Laptop", 999, 3), ("Electronics", "Phone", 699, 5),
    ("Clothing", "Shirt", 49, 20), ("Clothing", "Pants", 89, 12),
    ("Food", "Coffee", 15, 100), ("Food", "Snacks", 8, 50),
], ["category", "product", "price", "qty"])

sales.groupBy("category").count().show()  # Count per category
sales.groupBy("category").agg(
    count("*").alias("num_products"),
    sum(col("price") * col("qty")).alias("total_revenue"),
    round(avg("price"), 2).alias("avg_price"),
    min("price").alias("min_price"),
    max("price").alias("max_price"),
).show()

# --- Level 3: Conditional + Pivot ---
print("\n=== Level 3: Conditional Aggregation ===")
orders = spark.createDataFrame([
    ("Alice", 100, "completed"), ("Alice", 200, "cancelled"),
    ("Alice", 150, "completed"), ("Bob", 300, "completed"),
    ("Bob", 50, "returned"), ("Bob", 120, "completed"),
], ["customer", "amount", "status"])

orders.groupBy("customer").agg(
    count(when(col("status") == "completed", 1)).alias("completed"),
    count(when(col("status") == "cancelled", 1)).alias("cancelled"),
    count(when(col("status") == "returned", 1)).alias("returned"),
    sum(when(col("status") == "completed", col("amount")).otherwise(0)).alias("revenue"),
).show()

# --- Level 5: Sales Dashboard ---
print("\n=== Level 5: Sales Dashboard ===")
dash = spark.createDataFrame([
    ("North", "Electronics", "Jan", 500), ("North", "Electronics", "Feb", 600),
    ("North", "Clothing", "Jan", 200), ("South", "Electronics", "Jan", 800),
    ("South", "Clothing", "Feb", 300), ("South", "Food", "Jan", 100),
], ["region", "category", "month", "revenue"])

# Overall KPIs
print("--- Overall ---")
dash.agg(
    sum("revenue").alias("total_revenue"),
    count("*").alias("total_transactions"),
    round(avg("revenue"), 2).alias("avg_transaction"),
).show()

# By region with rollup
print("--- Region Rollup ---")
dash.rollup("region", "category").agg(
    sum("revenue").alias("total"),
    count("*").alias("count"),
).orderBy(col("region").asc_nulls_last(), col("category").asc_nulls_last()).show()

# Pivot by month
print("--- Pivot by Month ---")
dash.groupBy("region").pivot("month", ["Jan", "Feb"]).sum("revenue").fillna(0).show()

print("\n✅ All homework solutions complete!")