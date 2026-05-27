# Databricks notebook source
# DBTITLE 1,NB_37 Header
# MAGIC %md
# MAGIC # NB_37 — Higher-Order Functions
# MAGIC
# MAGIC **Module 5: Built-in Functions** | Notebook 37 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Array HOFs: transform(), filter(), aggregate(), exists(), forall()
# MAGIC * Array combining: zip_with(), array_sort() with comparator
# MAGIC * Map HOFs: map_filter(), map_zip_with(), transform_keys(), transform_values()
# MAGIC * Lambda syntax in SQL expressions
# MAGIC * Performance: HOFs vs explode+groupBy patterns
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐⭐ (Functional programming in Spark)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are Higher-Order Functions?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are Higher-Order Functions? (Real-World Analogy)
# MAGIC
# MAGIC ### 🏭 The Assembly Line with Custom Tools
# MAGIC
# MAGIC Higher-Order Functions (HOFs) let you apply custom logic to EACH element of an array/map WITHOUT exploding:
# MAGIC
# MAGIC | Assembly Line Step | HOF | What It Does |
# MAGIC |---|---|---|
# MAGIC | Paint each item | `transform(arr, x -> ...)` | Apply function to every element |
# MAGIC | Quality check | `filter(arr, x -> ...)` | Keep elements matching condition |
# MAGIC | Melt all into one | `aggregate(arr, init, merge)` | Reduce to single value |
# MAGIC | Check if any defective | `exists(arr, x -> ...)` | True if any matches |
# MAGIC | Verify ALL pass QA | `forall(arr, x -> ...)` | True if all match |
# MAGIC | Pair from two lines | `zip_with(a, b, (x,y) -> ...)` | Combine element-by-element |
# MAGIC
# MAGIC ### Why HOFs Over Explode+GroupBy?
# MAGIC * **No shuffle:** Process within the row
# MAGIC * **Preserves structure:** No re-aggregation needed
# MAGIC * **Cleaner code:** One expression instead of multi-step
# MAGIC * **Better performance:** Avoids expensive groupBy

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Higher-Order Functions Work
# MAGIC %md
# MAGIC ## SECTION 2 — How Higher-Order Functions Work
# MAGIC
# MAGIC ### Lambda Syntax (used with expr())
# MAGIC ```
# MAGIC transform(array, x -> x + 1)                          -- unary
# MAGIC transform(array, (x, i) -> i)                          -- with index
# MAGIC aggregate(array, 0, (acc, x) -> acc + x)               -- accumulator
# MAGIC aggregate(array, 0, (acc, x) -> acc + x, acc -> acc/n) -- with finish
# MAGIC ```
# MAGIC
# MAGIC ### Execution: No Shuffle
# MAGIC ```
# MAGIC Row: [1, 2, 3, 4, 5]  →  transform(arr, x -> x * 2)  →  [2, 4, 6, 8, 10]
# MAGIC Processed entirely WITHIN each row/task. No data movement.
# MAGIC ```
# MAGIC
# MAGIC ### Function Reference
# MAGIC ```
# MAGIC ARRAY → ARRAY:   transform(), filter(), zip_with()
# MAGIC ARRAY → SCALAR:  aggregate()
# MAGIC ARRAY → BOOL:    exists(), forall()
# MAGIC MAP FUNCTIONS:   map_filter(), transform_keys(), transform_values(), map_zip_with()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: transform()
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: transform()
# ============================================================
# Real-world: Apply transformations to every element in an array.

from pyspark.sql import SparkSession  # Import SparkSession.
from pyspark.sql.functions import col, expr, array, lit  # Imports.

spark = SparkSession.builder.getOrCreate()  # Get active session.

# Student scores.
students = spark.createDataFrame([
    (1, "Alice", [85, 92, 78, 90, 88]),
    (2, "Bob", [70, 65, 80, 72, 68]),
    (3, "Charlie", [95, 98, 92, 97, 100]),
], "id INT, name STRING, scores ARRAY<INT>")  # Score arrays.

print("=== transform() — Apply to Each Element ===")  # Print heading.
students.select(
    col("name"),  # Keep name.
    col("scores"),  # Original.
    expr("transform(scores, x -> x + 5)").alias("with_bonus"),  # Add 5 bonus.
    expr("transform(scores, x -> x * 2)").alias("doubled"),  # Double.
    expr("transform(scores, x -> CASE WHEN x > 100 THEN 100 ELSE x END)").alias("capped"),  # Cap at 100.
).show(truncate=False)  # Display.

# transform with index.
print("=== transform() with Index ===")  # Print heading.
students.select(
    col("name"),  # Keep name.
    expr("transform(scores, (x, i) -> x * (5 - i))").alias("weighted"),  # Position weight.
    expr("transform(scores, (x, i) -> concat('T', cast(i+1 as string), ':', cast(x as string)))").alias("labeled"),  # Labeled.
).show(truncate=False)  # Display indexed transforms.

# Normalize.
print("=== Normalize Array Values ===")  # Print heading.
prices_df = spark.createDataFrame([([100.0, 200.0, 50.0, 300.0],)], "prices ARRAY<DOUBLE>")  # Prices.
prices_df.select(
    col("prices"),  # Original.
    expr("transform(prices, x -> round(x / array_max(prices), 2))").alias("normalized"),  # [0,1].
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: filter() and exists()/forall()
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: filter(), exists(), forall()
# ============================================================
# Real-world: Conditional array operations without explode.

from pyspark.sql.functions import col, expr, size  # Imports.

orders = spark.createDataFrame([
    (1, "Alice", [29.99, 149.99, 9.99, 299.99, 4.99]),
    (2, "Bob", [5.99, 3.99, 7.99, 2.99]),
    (3, "Charlie", [999.99, 499.99, 199.99]),
], "id INT, customer STRING, prices ARRAY<DOUBLE>")  # Prices.

print("=== filter() ===")  # Print heading.
orders.select(
    col("customer"),  # Keep.
    col("prices"),  # Original.
    expr("filter(prices, x -> x > 10.0)").alias("above_10"),  # Keep > $10.
    expr("filter(prices, x -> x >= 10.0 AND x <= 200.0)").alias("mid_range"),  # Range.
).show(truncate=False)  # Display.

print("=== Filter + Size = Conditional Count ===")  # Print heading.
orders.select(
    col("customer"),  # Keep.
    size(col("prices")).alias("total"),  # Total.
    size(expr("filter(prices, x -> x > 50.0)")).alias("expensive"),  # Count > $50.
    size(expr("filter(prices, x -> x < 10.0)")).alias("cheap"),  # Count < $10.
).show(truncate=False)  # Display.

print("=== exists() and forall() ===")  # Print heading.
orders.select(
    col("customer"),  # Keep.
    expr("exists(prices, x -> x > 500)").alias("has_expensive"),  # Any > $500?
    expr("forall(prices, x -> x > 0)").alias("all_positive"),  # All > 0?
    expr("forall(prices, x -> x >= 10)").alias("all_above_10"),  # All >= $10?
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: aggregate()
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: aggregate()
# ============================================================
# Real-world: Reduce array to single value (sum, product, custom).

from pyspark.sql.functions import col, expr  # Imports.

devices = spark.createDataFrame([
    ("sensor-01", [23.5, 24.1, 22.8, 25.0, 23.2]),
    ("sensor-02", [18.0, 19.5, 17.8, 20.1]),
], "device STRING, readings ARRAY<DOUBLE>")  # Readings.

print("=== aggregate() — Reduce to Single Value ===")  # Print heading.
devices.select(
    col("device"),  # Keep.
    col("readings"),  # Original.
    expr("aggregate(readings, 0D, (acc, x) -> acc + x)").alias("total"),  # Sum.
    expr("aggregate(readings, 0D, (acc, x) -> acc + x, acc -> round(acc / size(readings), 2))").alias("average"),  # Avg.
    expr("aggregate(readings, cast(0 as double), (acc, x) -> CASE WHEN x > acc THEN x ELSE acc END)").alias("max_val"),  # Max.
    expr("aggregate(readings, 0, (acc, x) -> acc + CASE WHEN x > 23 THEN 1 ELSE 0 END)").alias("above_23"),  # Count.
).show(truncate=False)  # Display.

# Product.
print("=== Product of Array ===")  # Print heading.
nums = spark.createDataFrame([([2, 3, 4, 5],), ([1, 1, 1, 1],)], "numbers ARRAY<INT>")  # Numbers.
nums.select(
    col("numbers"),  # Original.
    expr("aggregate(numbers, 1, (acc, x) -> acc * x)").alias("product"),  # Multiply all.
).show(truncate=False)  # Display.

# Running total.
print("=== Running Total ===")  # Print heading.
devices.select(
    col("device"),  # Keep.
    col("readings"),  # Original.
    expr("transform(sequence(1, size(readings)), i -> aggregate(slice(readings, 1, i), 0D, (acc, x) -> acc + x))").alias("running_total"),  # Cumulative.
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: zip_with()
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: zip_with()
# ============================================================
# Real-world: Element-wise operations between parallel arrays.

from pyspark.sql.functions import col, expr  # Imports.

student_scores = spark.createDataFrame([
    (1, "Alice", ["Math", "Science", "English"], [85, 92, 78], [70, 80, 90]),
    (2, "Bob", ["Math", "Science", "English"], [70, 65, 80], [60, 75, 85]),
], "id INT, name STRING, subjects ARRAY<STRING>, midterm ARRAY<INT>, final ARRAY<INT>")  # Data.

print("=== zip_with() — Combine Arrays ===")  # Print heading.
student_scores.select(
    col("name"),  # Keep.
    expr("zip_with(midterm, final, (m, f) -> (m + f) / 2)").alias("averages"),  # Avg.
    expr("zip_with(midterm, final, (m, f) -> f - m)").alias("improvement"),  # Change.
    expr("zip_with(midterm, final, (m, f) -> round(m * 0.4 + f * 0.6, 1))").alias("weighted"),  # Weighted.
).show(truncate=False)  # Display.

# Chained pipeline.
print("=== Complex Pipeline: filter + transform + aggregate ===")  # Print heading.
student_scores.select(
    col("name"),  # Keep.
    col("final"),  # Scores.
    expr("filter(final, x -> x >= 75)").alias("passing"),  # Filter.
    expr("aggregate(transform(filter(final, x -> x >= 75), x -> x + 5), 0, (acc, x) -> acc + x)").alias("bonus_sum"),  # Chain.
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Map HOFs
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Map Higher-Order Functions
# ============================================================
# Real-world: Dynamic map transformations.

from pyspark.sql.functions import col, expr  # Imports.

servers = spark.createDataFrame([
    ("web-01", {"cpu_pct": 85.0, "mem_pct": 72.0, "disk_pct": 45.0, "net_mbps": 150.0}),
    ("web-02", {"cpu_pct": 92.0, "mem_pct": 88.0, "disk_pct": 95.0, "net_mbps": 30.0}),
], "server STRING, metrics MAP<STRING, DOUBLE>")  # Metrics.

print("=== map_filter() ===")  # Print heading.
servers.select(
    col("server"),  # Keep.
    expr("map_filter(metrics, (k, v) -> k LIKE '%pct%')").alias("pct_only"),  # Filter by key.
    expr("map_filter(metrics, (k, v) -> v > 80)").alias("high_usage"),  # Filter by value.
).show(truncate=False)  # Display.

print("=== transform_keys() and transform_values() ===")  # Print heading.
servers.select(
    col("server"),  # Keep.
    expr("transform_keys(metrics, (k, v) -> replace(k, '_pct', ''))").alias("clean_keys"),  # Clean keys.
    expr("transform_values(metrics, (k, v) -> CASE WHEN k LIKE '%pct%' THEN v / 100.0 ELSE v END)").alias("fractions"),  # To fractions.
).show(truncate=False)  # Display.

# map_zip_with.
print("=== map_zip_with() ===")  # Print heading.
compare = spark.createDataFrame([
    ("web-01", {"cpu": 85.0, "mem": 72.0}, {"cpu": 78.0, "mem": 68.0}),
], "server STRING, current MAP<STRING,DOUBLE>, previous MAP<STRING,DOUBLE>")  # Compare.

compare.select(
    col("server"),  # Keep.
    expr("map_zip_with(current, previous, (k, v1, v2) -> round(v1 - v2, 1))").alias("change"),  # Diff.
    expr("map_zip_with(current, previous, (k, v1, v2) -> round((v1-v2)/v2*100, 1))").alias("pct_change"),  # % change.
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: HOF vs Explode
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: HOF vs Explode Comparison
# ============================================================
# Same result, different approaches.

from pyspark.sql.functions import col, expr, explode, collect_list, avg, count  # Imports.

users = spark.createDataFrame([
    (1, "Alice", [85, 92, 78, 90, 65, 88]),
    (2, "Bob", [70, 65, 80, 72, 68, 55]),
    (3, "Charlie", [95, 98, 92, 97, 100, 91]),
], "id INT, name STRING, scores ARRAY<INT>")  # Scores.

# HOF approach (no shuffle).
print("=== HOF Approach (No Shuffle) ===")  # Print heading.
users.select(
    col("name"),  # Keep.
    expr("filter(scores, x -> x >= 80)").alias("passing"),  # Filter.
    expr("size(filter(scores, x -> x >= 80))").alias("count"),  # Count.
    expr("aggregate(filter(scores, x -> x >= 80), 0D, (a,x) -> a+x, a -> round(a/size(filter(scores, x -> x >= 80)),1))").alias("avg"),  # Avg.
).show(truncate=False)  # Display.

# Explode approach (causes shuffle).
print("=== Explode Approach (Shuffle) ===")  # Print heading.
users.select(col("id"), col("name"), explode(col("scores")).alias("score")).filter(
    col("score") >= 80
).groupBy("id", "name").agg(
    collect_list("score").alias("passing"),
    count("*").alias("count"),
    expr("round(avg(score), 1)").alias("avg"),
).show(truncate=False)  # Display.

print("""COMPARISON:
- HOF: No shuffle, preserves structure, single expression.
- Explode: Shuffle required, multiplies rows, more flexible for cross-row logic.
- Use HOF for within-row transforms; Explode for joins/cross-row analysis.""")  # Summary.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Complex chained HOF pipelines
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Complex Chained HOF Pipelines
# ============================================================
# Real-world: Multi-step processing within arrays.

from pyspark.sql.functions import col, expr, size  # Imports.

orders = spark.createDataFrame([
    (1, "Alice", ["Laptop", "Mouse", "Keyboard"], [999.99, 29.99, 59.99], [1, 2, 1]),
    (2, "Bob", ["Book", "Pen", "Notebook", "Eraser"], [15.99, 2.99, 8.99, 1.49], [3, 10, 5, 2]),
], "id INT, customer STRING, items ARRAY<STRING>, prices ARRAY<DOUBLE>, quantities ARRAY<INT>")  # Orders.

print("=== Multi-Step Pipeline ===")  # Print heading.
orders.select(
    col("customer"),  # Keep.
    expr("zip_with(prices, quantities, (p, q) -> round(p * q, 2))").alias("line_totals"),  # Line totals.
    expr("aggregate(zip_with(prices, quantities, (p, q) -> p * q), 0D, (a,x) -> a+x, a -> round(a,2))").alias("order_total"),  # Sum.
    expr("size(filter(quantities, q -> q > 2))").alias("bulk_items"),  # Bulk count.
).show(truncate=False)  # Display.

# Discount tiers.
print("=== Quantity-Based Discounts ===")  # Print heading.
orders.select(
    col("customer"),  # Keep.
    expr("""
        aggregate(
            zip_with(prices, quantities, (p, q) -> 
                p * q * CASE WHEN q >= 5 THEN 0.80 WHEN q >= 3 THEN 0.90 ELSE 1.0 END
            ), 0D, (a, x) -> a + x, a -> round(a, 2)
        )
    """).alias("discounted_total"),  # With discounts.
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Custom sorting and Top-N
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Custom Sorting and Top-N
# ============================================================
# Real-world: Rank within arrays using custom comparators.

from pyspark.sql.functions import col, expr  # Imports.

nums = spark.createDataFrame([([5, 2, 8, 1, 9, 3],)], "arr ARRAY<INT>")  # Numbers.

print("=== array_sort() with Comparator ===")  # Print heading.
nums.select(
    col("arr"),  # Original.
    expr("array_sort(arr, (a, b) -> CASE WHEN a < b THEN -1 WHEN a > b THEN 1 ELSE 0 END)").alias("asc"),  # Ascending.
    expr("array_sort(arr, (a, b) -> CASE WHEN a > b THEN -1 WHEN a < b THEN 1 ELSE 0 END)").alias("desc"),  # Descending.
).show(truncate=False)  # Display.

# Sort strings by length.
print("=== Sort by Length ===")  # Print heading.
words = spark.createDataFrame([(["cat", "elephant", "dog", "hippopotamus", "ant"],)], "words ARRAY<STRING>")  # Words.
words.select(
    expr("array_sort(words, (a, b) -> CASE WHEN length(a) < length(b) THEN -1 WHEN length(a) > length(b) THEN 1 ELSE 0 END)").alias("by_length"),  # By length.
).show(truncate=False)  # Display.

# Top-N from array.
print("=== Top-N from Array ===")  # Print heading.
scores = spark.createDataFrame([
    ("Alice", [85, 92, 78, 90, 65, 88, 95, 70]),
    ("Bob", [70, 65, 80, 72, 68, 55, 60, 75]),
], "name STRING, all_scores ARRAY<INT>")  # Scores.

scores.select(
    col("name"),  # Keep.
    expr("slice(array_sort(all_scores, (a,b) -> CASE WHEN a > b THEN -1 ELSE 1 END), 1, 3)").alias("top_3"),  # Top 3.
    expr("slice(array_sort(all_scores, (a,b) -> CASE WHEN a < b THEN -1 ELSE 1 END), 1, 3)").alias("bottom_3"),  # Bottom 3.
    expr("aggregate(slice(array_sort(all_scores, (a,b) -> CASE WHEN a > b THEN -1 ELSE 1 END), 1, 3), 0D, (a,x) -> a+x, a -> round(a/3,1))").alias("avg_top3"),  # Avg top 3.
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production HOF patterns
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Production HOF Patterns
# ============================================================
# Real-world: Reusable patterns for data pipelines.

from pyspark.sql.functions import col, expr, size  # Imports.

# Pattern 1: Array statistics without explode.
print("=== Pattern 1: Array Stats ===")  # Print heading.
data = spark.createDataFrame([
    ("sensor-A", [23.5, 24.1, 22.8, 25.0, 23.2, 100.0]),
    ("sensor-B", [18.0, 19.5, 17.8, 20.1, 18.5]),
], "device STRING, vals ARRAY<DOUBLE>")  # Sensor data.

data.select(
    col("device"),  # Keep.
    size(col("vals")).alias("count"),  # Count.
    expr("aggregate(vals, 0D, (a,x) -> a+x, a -> round(a/size(vals),2))").alias("mean"),  # Mean.
    expr("array_min(vals)").alias("min"),  # Min.
    expr("array_max(vals)").alias("max"),  # Max.
    expr("size(filter(vals, x -> x > 2 * aggregate(vals, 0D, (a,v)->a+v, a->a/size(vals))))").alias("outliers"),  # Outlier count.
).show(truncate=False)  # Display.

# Pattern 2: Data quality.
print("=== Pattern 2: Array Quality ===")  # Print heading.
quality = spark.createDataFrame([
    (1, ["valid@email.com", "also@test.org", "invalid"]),
    (2, ["good@mail.com"]),
    (3, ["bad", "nope", "wrong"]),
], "id INT, emails ARRAY<STRING>")  # Emails.

quality.select(
    col("id"),  # Keep.
    expr("filter(emails, e -> e LIKE '%@%')").alias("valid"),  # Valid.
    expr("filter(emails, e -> NOT e LIKE '%@%')").alias("invalid"),  # Invalid.
    expr("round(size(filter(emails, e -> e LIKE '%@%')) * 100.0 / size(emails), 1)").alias("quality_pct"),  # Score.
    expr("forall(emails, e -> e LIKE '%@%')").alias("all_valid"),  # All valid?
).show(truncate=False)  # Display.

# Pattern 3: Dynamic alert tags.
print("=== Pattern 3: Dynamic Alerts ===")  # Print heading.
metrics = spark.createDataFrame([
    ("srv-1", 92.0, 88.0, 95.0),
    ("srv-2", 45.0, 30.0, 20.0),
    ("srv-3", 98.0, 50.0, 85.0),
], ["server", "cpu", "memory", "disk"])  # Metrics.

metrics.select(
    col("server"),  # Keep.
    expr("filter(array(CASE WHEN cpu > 90 THEN 'HIGH_CPU' END, CASE WHEN memory > 80 THEN 'HIGH_MEM' END, CASE WHEN disk > 90 THEN 'HIGH_DISK' END), x -> x IS NOT NULL)").alias("alerts"),  # Alerts.
).show(truncate=False)  # Display.

print("✅ Higher-Order Functions mastery complete!")  # Done.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Higher-Order Functions
# MAGIC
# MAGIC ### Mistake 1: Python lambdas in PySpark HOFs
# MAGIC ```python
# MAGIC # WRONG — Can't use Python lambda!
# MAGIC df.select(transform(col("arr"), lambda x: x + 1))  # Error!
# MAGIC
# MAGIC # CORRECT — Use expr() with SQL lambda.
# MAGIC df.select(expr("transform(arr, x -> x + 1)"))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Wrong initial type in aggregate
# MAGIC ```python
# MAGIC # WRONG — int initial with double array!
# MAGIC expr("aggregate(doubles, 0, (a,x) -> a+x)")  # Type mismatch!
# MAGIC
# MAGIC # CORRECT — Use 0D for double.
# MAGIC expr("aggregate(doubles, 0D, (a,x) -> a+x)")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Missing finish function
# MAGIC ```python
# MAGIC # Sum only: expr("aggregate(arr, 0, (a,x) -> a+x)")
# MAGIC # Average needs finish: 
# MAGIC # expr("aggregate(arr, 0D, (a,x) -> a+x, a -> a/size(arr))")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: filter() HOF vs df.filter()
# MAGIC ```python
# MAGIC # expr("filter(array_col, x -> ...)")  -- filters array ELEMENTS
# MAGIC # df.filter(col("x") > 5)             -- filters DataFrame ROWS
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: zip_with with unequal arrays
# MAGIC ```python
# MAGIC # Shorter array is padded with NULLs!
# MAGIC # zip_with([1,2,3], [10,20], (a,b) -> a+b) → [11, 22, NULL]
# MAGIC # Handle: CASE WHEN b IS NULL THEN a ELSE a+b END
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of HOF Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Use `transform()` to double each element. Use `filter()` to keep only evens.
# MAGIC 2. Use `aggregate()` to sum an array and compute its average.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Modify to apply percentage increase. Add finish function for rounding.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Chain: filter > 50, transform * 1.1, aggregate to sum.
# MAGIC 6. Dot product with `zip_with` + `aggregate`.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Weighted average calculator from parallel score and weight arrays.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Order pricing engine with bulk discounts using chained HOFs.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Array data quality scorer with validation rules.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Benchmark HOF vs explode+groupBy on 1M rows.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Empty arrays, NULLs inside arrays, mismatched zip_with lengths.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Time-series anomaly detector: flag values > 3 stddev from mean.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Decision matrix: which HOF for which task?

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all.

# --- Level 1 ---
print("=== Level 1: transform + filter + aggregate ===")  # Print heading.
basic = spark.createDataFrame([([10, -5, 20, -3, 15, 0],)], "arr ARRAY<INT>")  # Sample.
basic.select(
    col("arr"),  # Original.
    expr("transform(arr, x -> x * 2)").alias("doubled"),  # Double.
    expr("filter(arr, x -> x % 2 = 0)").alias("evens"),  # Evens.
    expr("aggregate(arr, 0, (a,x) -> a + x)").alias("sum"),  # Sum.
    expr("aggregate(arr, 0D, (a,x) -> a+x, a -> round(a/size(arr),2))").alias("avg"),  # Avg.
).show(truncate=False)  # Display.

# --- Level 3: Dot product ---
print("=== Level 3: Dot Product ===")  # Print heading.
v = spark.createDataFrame([([1, 2, 3], [4, 5, 6])], "v1 ARRAY<INT>, v2 ARRAY<INT>")  # Vectors.
v.select(
    expr("aggregate(zip_with(v1, v2, (a,b) -> a*b), 0, (acc,x) -> acc+x)").alias("dot_product"),  # 32.
).show()  # Display.

# --- Level 8: Edge cases ---
print("=== Level 8: Edge Cases ===")  # Print heading.
edge = spark.createDataFrame([
    ([],), ([None, 1, None, 2],), ([42],)
], "arr ARRAY<INT>")  # Edges.

edge.select(
    col("arr"),  # Original.
    expr("transform(arr, x -> x + 1)").alias("transform"),  # NULL+1=NULL.
    expr("filter(arr, x -> x IS NOT NULL)").alias("no_nulls"),  # Remove NULLs.
    expr("exists(arr, x -> x IS NULL)").alias("has_null"),  # Any NULL?
    size(col("arr")).alias("size"),  # Size.
).show(truncate=False)  # Display.

print("✅ All homework solutions complete!")  # Completion message.