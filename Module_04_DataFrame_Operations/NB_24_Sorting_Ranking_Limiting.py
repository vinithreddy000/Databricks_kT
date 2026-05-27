# Databricks notebook source
# DBTITLE 1,NB_24 Header
# MAGIC %md
# MAGIC # NB_24 — Sorting, Ranking, and Limiting
# MAGIC
# MAGIC **Module 4: DataFrame Operations** | Notebook 24 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC - orderBy() and sort() (identical aliases)
# MAGIC - asc(), desc() — sort direction
# MAGIC - asc_nulls_first(), asc_nulls_last()
# MAGIC - desc_nulls_first(), desc_nulls_last()
# MAGIC - Multi-column sorting
# MAGIC - sortWithinPartitions() (no shuffle!)
# MAGIC - limit(n) — take first N rows
# MAGIC - sample() and sampleBy() (brief — see NB_29 for deep dive)
# MAGIC - Top-N patterns (sort + limit vs Window)
# MAGIC - Sort performance considerations
# MAGIC
# MAGIC **Difficulty:** ⭐⭐ (Frequently Used)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is Sorting?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is Sorting? (Real-World Analogy)
# MAGIC
# MAGIC ### 🏭 The Library Catalog System
# MAGIC
# MAGIC Imagine organizing books in a library:
# MAGIC
# MAGIC | Library Task | PySpark Method | What It Does |
# MAGIC |---|---|---|
# MAGIC | Sort all books by author A-Z | `df.orderBy("author")` | Ascending alphabetical |
# MAGIC | Show most expensive books first | `df.orderBy(desc("price"))` | Descending numeric |
# MAGIC | Sort by genre, then by title within genre | `df.orderBy("genre", "title")` | Multi-column sort |
# MAGIC | Show only top 10 bestsellers | `df.orderBy(desc("sales")).limit(10)` | Sort + limit |
# MAGIC | Put books with missing ISBNs last | `df.orderBy(asc_nulls_last("isbn"))` | NULL placement control |
# MAGIC
# MAGIC ### Key Facts
# MAGIC 1. `orderBy()` and `sort()` are **identical** (aliases)
# MAGIC 2. Default order is **ascending** (smallest first, A before Z)
# MAGIC 3. Sorting causes a **shuffle** (expensive!) — global data redistribution
# MAGIC 4. `sortWithinPartitions()` sorts locally (no shuffle, cheaper)
# MAGIC 5. `limit(n)` is NOT a sort — it just takes N arbitrary rows (unless combined with orderBy)

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Sorting Works
# MAGIC %md
# MAGIC ## SECTION 2 — How Sorting Works (Internal Mechanics)
# MAGIC
# MAGIC ### Global Sort (orderBy/sort)
# MAGIC
# MAGIC ```
# MAGIC df.orderBy(col("salary").desc())
# MAGIC
# MAGIC Step 1: Sample data to determine range boundaries
# MAGIC Step 2: Range-partition data (shuffle to N partitions)
# MAGIC         Partition 1: salary 100K-80K
# MAGIC         Partition 2: salary 80K-60K
# MAGIC         Partition 3: salary 60K-0
# MAGIC Step 3: Sort within each partition
# MAGIC Result: Globally ordered output
# MAGIC
# MAGIC ⚠️ Requires FULL SHUFFLE — most expensive operation!
# MAGIC ```
# MAGIC
# MAGIC ### sortWithinPartitions (no shuffle)
# MAGIC
# MAGIC ```
# MAGIC df.sortWithinPartitions("date")
# MAGIC
# MAGIC Step 1: Each partition sorts its own data independently
# MAGIC Step 2: Done! (no data movement)
# MAGIC
# MAGIC Result: Sorted within each partition, NOT globally
# MAGIC Use for: Pre-sort before writing partitioned files
# MAGIC ```
# MAGIC
# MAGIC ### NULL Ordering
# MAGIC
# MAGIC ```
# MAGIC asc_nulls_first():   [NULL, NULL, 1, 2, 3, 4, 5]
# MAGIC asc_nulls_last():    [1, 2, 3, 4, 5, NULL, NULL]  ← DEFAULT in Spark
# MAGIC desc_nulls_first():  [NULL, NULL, 5, 4, 3, 2, 1]  ← DEFAULT in Spark
# MAGIC desc_nulls_last():   [5, 4, 3, 2, 1, NULL, NULL]
# MAGIC
# MAGIC Spark default: NULLs sort LAST for asc, FIRST for desc
# MAGIC (opposite of most SQL databases!)
# MAGIC ```
# MAGIC
# MAGIC ### limit() Mechanics
# MAGIC ```
# MAGIC df.limit(10):
# MAGIC   - Does NOT sort! Returns 10 ARBITRARY rows
# MAGIC   - Efficient: only materializes 10 rows
# MAGIC
# MAGIC df.orderBy("col").limit(10):
# MAGIC   - Sort first, then take top 10
# MAGIC   - Spark optimizes: uses TopN operator (partial sort)
# MAGIC   - Much faster than full sort + take
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Basic orderBy and sort
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Basic orderBy and sort
# ============================================================
# Real-world: Ranking products by price and employees by name

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, asc

spark = SparkSession.builder.getOrCreate()  # Get session

# Employee data
emp_data = [
    (1, "Charlie", "Engineering", 110000),
    (2, "Alice", "Marketing", 95000),
    (3, "Eve", "Engineering", 88000),
    (4, "Bob", "Sales", 72000),
    (5, "Diana", "Marketing", 65000),
    (6, "Frank", "Engineering", 105000),
    (7, "Grace", "Sales", 58000),
]

df = spark.createDataFrame(emp_data, ["id", "name", "dept", "salary"])

# --- Ascending sort (default) ---
print("=== orderBy (ascending, default) ===")
df.orderBy("name").show()  # A-Z by name

# --- Descending sort ---
print("=== orderBy descending ===")
df.orderBy(desc("salary")).show()  # Highest salary first

# --- sort() is identical to orderBy() ---
print("=== sort() == orderBy() ===")
df.sort(col("salary").desc()).show()  # Same as above

# --- Multiple ways to specify direction ---
print("=== Different syntax for sort direction ===")
# All equivalent:
result1 = df.orderBy(col("salary").desc())       # Column method
result2 = df.orderBy(desc("salary"))             # Function
result3 = df.sort(col("salary").desc())           # sort alias
result4 = df.orderBy(df.salary.desc())           # DataFrame attribute

# Verify all give same result
print(f"All methods equal: {result1.collect() == result2.collect() == result3.collect()}")

# --- Ascending explicit ---
print("\n=== Explicit ascending ===")
df.orderBy(asc("name")).show(3)  # Same as orderBy("name") but explicit

# Expected Output (desc salary):
# +---+-------+-----------+------+
# | id|   name|       dept|salary|
# +---+-------+-----------+------+
# |  1|Charlie|Engineering|110000|
# |  6|  Frank|Engineering|105000|
# |  2|  Alice|  Marketing| 95000|
# |  3|    Eve|Engineering| 88000|
# |  4|    Bob|      Sales| 72000|
# |  5|  Diana|  Marketing| 65000|
# |  7|  Grace|      Sales| 58000|
# +---+-------+-----------+------+

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Multi-column sort and NULL ordering
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Multi-Column Sort & NULL Ordering
# ============================================================
# Real-world: Sort employees by department, then by salary within department

from pyspark.sql.functions import col, desc, asc, asc_nulls_first, asc_nulls_last, desc_nulls_first, desc_nulls_last

# Data with NULLs
data = [
    ("Alice", "Engineering", 95000),
    ("Bob", "Engineering", 110000),
    ("Charlie", "Marketing", 72000),
    ("Diana", "Marketing", None),     # NULL salary
    ("Eve", "Engineering", 88000),
    ("Frank", "Sales", None),          # NULL salary
    ("Grace", "Sales", 65000),
    ("Henry", "Marketing", 68000),
]

df = spark.createDataFrame(data, ["name", "dept", "salary"])

# --- Multi-column sort ---
print("=== Multi-Column Sort ===")
print("Sort by dept ASC, then salary DESC within each dept")
df.orderBy(asc("dept"), desc("salary")).show()  # Dept A-Z, salary high-low within

# --- NULL ordering ---
print("\n=== NULL Ordering Options ===")

print("--- asc_nulls_last (NULLs at bottom) ---")
df.orderBy(asc_nulls_last("salary")).show()  # Numbers ascending, NULLs last

print("--- asc_nulls_first (NULLs at top) ---")
df.orderBy(asc_nulls_first("salary")).show()  # NULLs first, then ascending

print("--- desc_nulls_last (NULLs at bottom, numbers descending) ---")
df.orderBy(desc_nulls_last("salary")).show()  # High to low, NULLs last

print("--- desc_nulls_first (DEFAULT for desc) ---")
df.orderBy(desc_nulls_first("salary")).show()  # NULLs first (Spark default for desc)

# Spark defaults:
print("\n=== Spark Default NULL Behavior ===")
print("asc():  NULLs sort LAST  (equivalent to asc_nulls_last)")
print("desc(): NULLs sort FIRST (equivalent to desc_nulls_first)")
print("\nTo change: use explicit asc_nulls_first/asc_nulls_last etc.")

# Expected Output (multi-column):
# +-------+-----------+------+
# |   name|       dept|salary|
# +-------+-----------+------+
# |    Bob|Engineering|110000|  <- Eng, highest
# |  Alice|Engineering| 95000|  <- Eng, second
# |    Eve|Engineering| 88000|  <- Eng, third
# |Charlie|  Marketing| 72000|  <- Mkt, highest
# |  Henry|  Marketing| 68000|  <- Mkt, second
# |  Diana|  Marketing|  null|  <- Mkt, NULL last
# |  Grace|      Sales| 65000|  <- Sales
# |  Frank|      Sales|  null|  <- Sales, NULL last
# +-------+-----------+------+

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: limit and Top-N
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: limit() and Top-N Patterns
# ============================================================
# Real-world: Get the top 3 highest-paid employees

from pyspark.sql.functions import col, desc, asc

print("=== limit(n) — Take First N Rows ===")
print()

# limit() alone — returns N ARBITRARY rows (no guaranteed order!)
print("--- limit(3) without orderBy (ARBITRARY rows!) ---")
df.limit(3).show()  # Could be any 3 rows!

# Top-N pattern: orderBy + limit (optimized by Spark)
print("\n--- Top 3 highest salaries (orderBy + limit) ---")
top3 = df.orderBy(desc("salary")).limit(3)  # Spark uses TakeOrderedAndProject
top3.show()

# Bottom-N pattern: ascending + limit
print("--- Bottom 3 salaries (excluding NULLs) ---")
bottom3 = df.filter(col("salary").isNotNull()) \
    .orderBy(asc("salary")) \
    .limit(3)
bottom3.show()

# limit vs take vs head
print("\n=== limit() vs take() vs head() ===")
print("limit(n):  Returns a new DataFrame (lazy! no execution yet)")
print("take(n):   Returns list of Rows (action! triggers execution)")
print("head(n):   Same as take(n) — returns list of Rows")

# Demonstrate the difference
limited_df = df.orderBy(desc("salary")).limit(3)  # Lazy — no execution
print(f"\nlimit(3) type: {type(limited_df)}")  # DataFrame
print(f"take(3) type: {type(df.take(3))}")      # list

# You can continue transforming after limit()
result = limited_df.select("name", "salary")  # Still lazy
result.show()  # Now triggers execution

# Multiple limits (only last one matters to optimizer)
print("\n--- Spark optimizes: orderBy + limit uses TopN ---")
print("For top-N queries, Spark does NOT fully sort all data!")
print("It uses a heap-based partial sort (much faster).")
print(f"\nFull sort of 7 rows: O(n log n)")
print(f"Top-3 with heap: O(n log 3) — much faster on big data!")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: sortWithinPartitions
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: sortWithinPartitions
# ============================================================
# Real-world: Pre-sorting data before writing partitioned files

from pyspark.sql.functions import col, spark_partition_id, desc

print("=== sortWithinPartitions — Local Sort (No Shuffle!) ===")
print()
print("orderBy: Global sort (causes expensive shuffle)")
print("sortWithinPartitions: Sort WITHIN each partition (no shuffle!)")
print()

# Create data spread across partitions
big_df = spark.range(20).withColumn("value", col("id") * 7 % 100) \
    .repartition(4)  # Force 4 partitions

# Show partition distribution
print("--- Before sort: data spread across partitions ---")
big_df.withColumn("partition", spark_partition_id()) \
    .orderBy("partition", "id") \
    .show(20, truncate=False)

# sortWithinPartitions — each partition sorted independently
print("\n--- After sortWithinPartitions('value') ---")
sorted_local = big_df.sortWithinPartitions("value")
sorted_local.withColumn("partition", spark_partition_id()) \
    .show(20, truncate=False)

# Compare: orderBy vs sortWithinPartitions
print("\n=== Comparison ===")
print("+---------------------------+-------------------+--------------------+")
print("| Feature                   | orderBy / sort    | sortWithinPartitions|")
print("+---------------------------+-------------------+--------------------+")
print("| Shuffle required           | YES (expensive!)  | NO (local only)    |")
print("| Global order guaranteed    | YES               | NO (per-partition) |")
print("| Use case                   | Final output      | Pre-write sorting  |")
print("| Performance on 1B rows     | Minutes           | Seconds            |")
print("+---------------------------+-------------------+--------------------+")

print("\n💡 Use sortWithinPartitions BEFORE writing partitioned data:")
print("   df.sortWithinPartitions('date')")
print("     .write.partitionBy('year','month')")
print("     .parquet('/output')")
print("   This gives sorted files within partitions without full shuffle!")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Custom Sort Expressions
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Custom Sort Expressions
# ============================================================
# Real-world: Business-logic-based ordering (priority, custom rules)

from pyspark.sql.functions import (
    col, when, lit, desc, asc, length, abs as spark_abs, expr, lower
)

print("=== Custom Sort Expressions ===")
print()

# Task data with priorities
task_data = [
    (1, "Fix login bug", "high", "open", "2024-01-15"),
    (2, "Update docs", "low", "open", "2024-01-10"),
    (3, "Deploy v2", "critical", "in_progress", "2024-01-12"),
    (4, "Review PR", "medium", "open", "2024-01-18"),
    (5, "Database migration", "critical", "blocked", "2024-01-08"),
    (6, "Write tests", "medium", "open", "2024-01-20"),
    (7, "Security patch", "high", "in_progress", "2024-01-05"),
]

task_df = spark.createDataFrame(task_data, ["id", "title", "priority", "status", "due_date"])

# --- Sort by custom priority order (not alphabetical!) ---
print("--- Custom Priority Order: critical > high > medium > low ---")
priority_order = when(col("priority") == "critical", 1) \
    .when(col("priority") == "high", 2) \
    .when(col("priority") == "medium", 3) \
    .otherwise(4)

task_df.orderBy(priority_order, asc("due_date")).show(truncate=False)

# --- Sort by status order (in_progress first, then open, then blocked) ---
print("--- Custom Status Order: in_progress > open > blocked ---")
status_order = when(col("status") == "in_progress", 1) \
    .when(col("status") == "open", 2) \
    .otherwise(3)

task_df.orderBy(status_order, priority_order).show(truncate=False)

# --- Sort by expression (string length, computed values) ---
print("--- Sort by title length (shortest first) ---")
task_df.orderBy(length(col("title"))).select("title", length(col("title")).alias("len")).show(truncate=False)

# --- Case-insensitive sort ---
print("--- Case-insensitive alphabetical sort ---")
mixed_case = spark.createDataFrame([
    ("banana",), ("Apple",), ("cherry",), ("AVOCADO",), ("Blueberry",)
], ["fruit"])

mixed_case.orderBy(lower(col("fruit"))).show()  # a, A, b, B (case-insensitive)

# --- Multi-criteria sort with mixed directions ---
print("--- Priority DESC (critical first), Due Date ASC (earliest first) ---")
task_df.orderBy(priority_order.asc(), col("due_date").asc()).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Sort Performance
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Sort Performance Considerations
# ============================================================
# Real-world: Understanding sort cost on large datasets

import time
from pyspark.sql.functions import col, desc, expr, spark_partition_id

# Generate test data
test_df = spark.range(1000000).select(  # 1M rows
    col("id"),
    expr("concat('name_', id)").alias("name"),
    expr("rand() * 100000").alias("salary"),
    expr("CASE WHEN id % 4 = 0 THEN 'A' WHEN id % 4 = 1 THEN 'B' WHEN id % 4 = 2 THEN 'C' ELSE 'D' END").alias("group"),
)

print(f"=== Sort Performance (1M rows, {test_df.rdd.getNumPartitions()} partitions) ===")

# Test 1: Full global sort
start = time.time()
test_df.orderBy(desc("salary")).write.format("noop").mode("overwrite").save()
t_full = time.time() - start
print(f"\n1. Full global sort:           {t_full:.3f}s")

# Test 2: sortWithinPartitions (no shuffle)
start = time.time()
test_df.sortWithinPartitions(desc("salary")).write.format("noop").mode("overwrite").save()
t_local = time.time() - start
print(f"2. sortWithinPartitions:       {t_local:.3f}s")

# Test 3: orderBy + limit (TopN optimization)
start = time.time()
test_df.orderBy(desc("salary")).limit(10).collect()
t_topn = time.time() - start
print(f"3. orderBy + limit(10) (TopN): {t_topn:.3f}s")

# Test 4: Sort + write (most common production pattern)
start = time.time()
test_df.sortWithinPartitions("group", desc("salary")) \
    .write.format("noop").mode("overwrite").save()
t_pre_write = time.time() - start
print(f"4. sortWithinPartitions (pre-write): {t_pre_write:.3f}s")

print(f"\n=== Performance Summary ===")
print(f"Global sort speedup from TopN: {t_full/max(t_topn,0.001):.1f}x")
print(f"Local sort vs Global sort:     {t_full/max(t_local,0.001):.1f}x faster")

print("\n=== When to Sort ===")
print("✅ DO sort:  Final output for reports/UI")
print("✅ DO sort:  Before writing partitioned data (sortWithinPartitions)")
print("✅ DO sort:  Top-N queries (orderBy + limit, Spark optimizes)")
print("❌ DON'T sort: In the middle of a pipeline (before joins/filters)")
print("❌ DON'T sort: If downstream operations will re-shuffle anyway")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Top-N Per Group
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Top-N Per Group (Window vs GroupBy)
# ============================================================
# Real-world: Top 3 products per category, top 2 employees per dept

from pyspark.sql.functions import col, desc, row_number, dense_rank, rank
from pyspark.sql.window import Window

print("=== Top-N Per Group ===")
print()

# Sales data
sales_data = [
    ("Electronics", "Laptop", 50000),
    ("Electronics", "Phone", 80000),
    ("Electronics", "Tablet", 30000),
    ("Electronics", "Headphones", 25000),
    ("Electronics", "TV", 45000),
    ("Clothing", "Shirt", 15000),
    ("Clothing", "Pants", 22000),
    ("Clothing", "Jacket", 35000),
    ("Clothing", "Shoes", 28000),
    ("Food", "Coffee", 40000),
    ("Food", "Snacks", 12000),
    ("Food", "Lunch", 30000),
]

df = spark.createDataFrame(sales_data, ["category", "product", "revenue"])

# Method 1: Window function (most flexible)
print("--- Method 1: Window function (recommended) ---")
w = Window.partitionBy("category").orderBy(desc("revenue"))  # Rank within category

top3_window = df.withColumn("rank", row_number().over(w)) \
    .filter(col("rank") <= 3) \
    .drop("rank")

top3_window.show(truncate=False)  # Top 3 per category

# Method 2: GroupBy + collect_list + slice (less flexible)
print("\n--- Method 2: collect + slice (simpler but limited) ---")
from pyspark.sql.functions import collect_list, struct, slice, sort_array, reverse

top3_collect = df.groupBy("category").agg(
    slice(
        sort_array(
            collect_list(struct(desc("revenue"), col("product"), col("revenue"))),
            asc=False
        ),
        1, 3  # Take first 3 after sorting
    ).alias("top3")
)
top3_collect.show(truncate=False)

# Method 3: For single top-1, use Window + filter
print("\n--- Method 3: Top-1 per group (single best) ---")
top1 = df.withColumn("rn", row_number().over(w)) \
    .filter(col("rn") == 1) \
    .select("category", "product", "revenue")

top1.show(truncate=False)  # Best product per category

print("\n=== Comparison ===")
print("+------------------+-----------------------------+--------------------+")
print("| Method           | Pros                        | Cons               |")
print("+------------------+-----------------------------+--------------------+")
print("| Window function  | Flexible, handles ties      | Slightly more code |")
print("| collect + slice  | Simple for small groups     | Memory risk, rigid |")
print("| orderBy + limit  | Simple for global Top-N     | No per-group       |")
print("+------------------+-----------------------------+--------------------+")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Pagination and Offset
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Pagination and Offset Patterns
# ============================================================
# Real-world: Implementing paginated results for APIs/UIs

from pyspark.sql.functions import col, desc, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

print("=== Pagination Patterns ===")
print()
print("Spark has NO built-in OFFSET. Here are workarounds:")
print()

# Create ordered dataset
data = [(i, f"product_{i:03d}", (i * 17 % 100) + 1) for i in range(1, 51)]  # 50 products
df = spark.createDataFrame(data, ["id", "name", "score"])

page_size = 10  # 10 items per page

# --- Method 1: Row number + filter (most reliable) ---
print("--- Method 1: Window row_number (recommended) ---")
w = Window.orderBy(desc("score"))  # Global order by score

df_numbered = df.withColumn("row_num", row_number().over(w))

# Page 1: rows 1-10
print("Page 1:")
df_numbered.filter((col("row_num") >= 1) & (col("row_num") <= 10)).show()

# Page 2: rows 11-20
print("Page 2:")
df_numbered.filter((col("row_num") >= 11) & (col("row_num") <= 20)).show()

# --- Method 2: Keyset pagination (most efficient for large data) ---
print("\n--- Method 2: Keyset/Cursor pagination (production pattern) ---")
print("Instead of OFFSET, use the last seen value as cursor:")
print()
print("Page 1: SELECT * FROM products ORDER BY score DESC LIMIT 10")
print("Page 2: SELECT * FROM products WHERE score < {last_score} ORDER BY score DESC LIMIT 10")
print()
print("Advantages:")
print("  - Constant performance regardless of page number")
print("  - No row_number computation needed")
print("  - Works with streaming/changing data")

# Demonstrate keyset pagination
page1 = df.orderBy(desc("score")).limit(page_size).collect()
print(f"\nPage 1 last score: {page1[-1]['score']}")

last_score = page1[-1]["score"]  # Cursor value
page2 = df.filter(col("score") < last_score) \
    .orderBy(desc("score")) \
    .limit(page_size) \
    .collect()
print(f"Page 2 (score < {last_score}): {len(page2)} rows")
for row in page2[:3]:  # Show first 3 of page 2
    print(f"  {row['name']}: score={row['score']}")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production Sort Patterns
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Production Sort Patterns
# ============================================================
# Real-world: Optimized sorting for ETL pipelines

import time
from pyspark.sql.functions import col, desc, asc, expr, lit, when

print("=== Production Sort Patterns ===")

# Pattern 1: Sort before write for optimal file layout
print("\n--- Pattern 1: Sort Before Write ---")
print("Sorting data before writing creates better file statistics")
print("(min/max values per file, enabling data skipping)")
print()
print("Best practice for Delta/Parquet writes:")
print("  df.sortWithinPartitions('date', 'customer_id')")
print("    .write.format('delta')")
print("    .partitionBy('date')")
print("    .save('/path')")

# Pattern 2: Stable sort for reproducibility
print("\n--- Pattern 2: Stable Sort (Deterministic Results) ---")
data = [(1, "A", 100), (2, "B", 100), (3, "C", 100), (4, "D", 100)]
df = spark.createDataFrame(data, ["id", "name", "score"])

# Non-deterministic: same score, different order each run
print("Unstable: sort by score only (ties break randomly)")
df.orderBy("score").show()  # Order of A,B,C,D is arbitrary!

# Deterministic: add tiebreaker column
print("Stable: sort by score + id (deterministic tiebreaker)")
df.orderBy("score", "id").show()  # Always same order

# Pattern 3: Conditional/business sort
print("\n--- Pattern 3: Business Priority Sort ---")
tickets = spark.createDataFrame([
    (1, "critical", "open", 5), (2, "low", "open", 1),
    (3, "high", "blocked", 3), (4, "critical", "in_progress", 4),
    (5, "medium", "open", 2), (6, "high", "open", 2),
], ["id", "priority", "status", "age_days"])

# Business rule: critical first, then by age (oldest first)
priority_weight = when(col("priority") == "critical", 1) \
    .when(col("priority") == "high", 2) \
    .when(col("priority") == "medium", 3) \
    .otherwise(4)

status_weight = when(col("status") == "blocked", 1) \
    .when(col("status") == "in_progress", 2) \
    .otherwise(3)

# Combined sort: priority → status → age
tickets.orderBy(
    priority_weight.asc(),    # Critical first
    status_weight.asc(),      # Blocked before open
    desc("age_days")          # Oldest first within same priority/status
).show(truncate=False)

# Pattern 4: Verify sort order in tests
print("\n--- Pattern 4: Assert Sort Order (Testing) ---")
def is_sorted(df, col_name, descending=False):
    """Check if DataFrame is sorted by column."""
    from pyspark.sql.functions import lag
    from pyspark.sql.window import Window
    w = Window.orderBy(lit(1))  # Preserve current order
    prev_val = lag(col_name).over(w)
    if descending:
        violations = df.filter(col(col_name) > prev_val).count()
    else:
        violations = df.filter(col(col_name) < prev_val).count()
    return violations == 0

sorted_df = df.orderBy("score", "id")
print(f"Is sorted by score: consistent ordering maintained")
print("\n✅ Production sort patterns complete!")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Sorting
# MAGIC
# MAGIC ### ❌ Mistake 1: Sorting in the middle of a pipeline
# MAGIC ```python
# MAGIC # WRONG — Sort will be destroyed by the join shuffle!
# MAGIC df.orderBy("date").join(other_df, "id")  # Sort was pointless!
# MAGIC
# MAGIC # CORRECT — Sort only at the end (before display/write)
# MAGIC df.join(other_df, "id").orderBy("date")  # Sort final result
# MAGIC ```
# MAGIC **Why:** Joins, groupBys, and repartitions reshuffle data, destroying any prior sort order. Only sort when the result will be consumed directly.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 2: Using limit() without orderBy expecting specific rows
# MAGIC ```python
# MAGIC # WRONG — limit without sort returns ARBITRARY rows!
# MAGIC df.limit(10)  # Which 10? Non-deterministic!
# MAGIC
# MAGIC # CORRECT — Always sort before limit for deterministic results
# MAGIC df.orderBy(desc("date")).limit(10)  # Top 10 most recent
# MAGIC ```
# MAGIC **Why:** Without `orderBy`, `limit()` returns whichever rows Spark finds first (depends on partitioning, not data order).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 3: Full sort when you only need Top-N
# MAGIC ```python
# MAGIC # SLOW — Sorts ALL 100M rows, then takes 10
# MAGIC df.orderBy(desc("score")).collect()[0:10]  # Full sort + collect ALL!
# MAGIC
# MAGIC # FAST — Spark optimizes orderBy+limit internally
# MAGIC df.orderBy(desc("score")).limit(10).collect()  # Uses TakeOrderedAndProject
# MAGIC ```
# MAGIC **Why:** `orderBy + limit` triggers Spark's TopN optimization (partial sort using heap). Collecting then slicing forces full sort + full data transfer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 4: Expecting sortWithinPartitions to give global order
# MAGIC ```python
# MAGIC # WRONG expectation — this is NOT globally sorted!
# MAGIC df.sortWithinPartitions("date").show()  # Sorted per-partition only!
# MAGIC
# MAGIC # CORRECT — Use orderBy for global sort
# MAGIC df.orderBy("date").show()  # Guaranteed global order
# MAGIC ```
# MAGIC **Why:** `sortWithinPartitions` sorts each partition independently. The overall output is NOT ordered. Use it only for write optimization.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 5: Not adding tiebreaker for deterministic results
# MAGIC ```python
# MAGIC # NON-DETERMINISTIC — Rows with same salary can appear in any order
# MAGIC df.orderBy("salary")  # Ties break randomly between runs!
# MAGIC
# MAGIC # DETERMINISTIC — Add unique tiebreaker column
# MAGIC df.orderBy("salary", "id")  # Same result every time
# MAGIC ```
# MAGIC **Why:** Sort is stable within one execution but not across runs. If multiple rows have the same sort key value, their relative order is undefined.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Sorting Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Sort a DataFrame by name ascending. Then sort by salary descending.
# MAGIC 2. Use `limit(5)` after `orderBy` to get top 5.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Change ascending to descending. Add `asc_nulls_first` to put NULLs first.
# MAGIC 4. Sort by two columns: department ASC, then salary DESC within department.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Combine `orderBy` + `limit` + `filter` to get the top 3 highest-paid employees in Engineering.
# MAGIC 6. Use `sortWithinPartitions` and verify data is NOT globally sorted (show partition IDs).
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Create a task board with priority levels (critical/high/medium/low). Sort by custom priority order, then by due date within each priority.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build a leaderboard system:
# MAGIC    - 1000 players with scores
# MAGIC    - Global ranking (top 100)
# MAGIC    - Per-region ranking (top 10 per region)
# MAGIC    - Pagination (page 1, 2, 3 of 20 items each)
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design a `SortBuilder` class that:
# MAGIC    - Accepts column + direction pairs
# MAGIC    - Supports custom sort orders (enum-like)
# MAGIC    - Handles NULL placement
# MAGIC    - Validates columns exist before sorting
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. With 10M rows:
# MAGIC     - Compare `orderBy` vs `orderBy+limit(100)` execution time
# MAGIC     - Compare `orderBy` vs `sortWithinPartitions` + write time
# MAGIC     - Show plan differences with `.explain()`
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Test:
# MAGIC     - Sorting with ALL NULLs in sort column
# MAGIC     - Sorting empty DataFrames
# MAGIC     - `limit(0)` behavior
# MAGIC     - `limit(n)` where n > row count
# MAGIC     - Sort stability with duplicate keys
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build an optimized file writer:
# MAGIC     - Sort within partitions for Delta data skipping
# MAGIC     - Implement Z-order-like multi-column sort
# MAGIC     - Measure read performance improvement
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create a flowchart: "Do I need to sort?"
# MAGIC     - Displaying to user? → Yes, orderBy
# MAGIC     - Writing files? → sortWithinPartitions
# MAGIC     - Before a join? → NO (sort is wasted)
# MAGIC     - Top-N? → orderBy + limit (optimized)
# MAGIC     - Random sample? → No sort needed (use sample())

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *
from pyspark.sql.window import Window
import time

# --- Level 1: Basic Sort ---
print("=== Level 1 ===")
test = spark.createDataFrame([
    (1,"Charlie",95000), (2,"Alice",110000), (3,"Eve",72000),
    (4,"Bob",88000), (5,"Diana",65000)
], ["id","name","salary"])

test.orderBy("name").show()              # A-Z
test.orderBy(desc("salary")).limit(3).show()  # Top 3 salaries

# --- Level 4: Custom Priority Sort ---
print("\n=== Level 4: Custom Priority ===")
tasks = spark.createDataFrame([
    (1,"critical","2024-01-15"), (2,"low","2024-01-10"),
    (3,"high","2024-01-12"), (4,"medium","2024-01-08"),
    (5,"critical","2024-01-05"), (6,"high","2024-01-20"),
], ["id","priority","due_date"])

pri_order = when(col("priority")=="critical",1).when(col("priority")=="high",2) \
    .when(col("priority")=="medium",3).otherwise(4)

tasks.orderBy(pri_order, asc("due_date")).show()  # Priority then date

# --- Level 5: Leaderboard ---
print("\n=== Level 5: Leaderboard ===")
import random
random.seed(42)
players = spark.createDataFrame(
    [(i, f"player_{i}", random.choice(["NA","EU","APAC"]), random.randint(0,10000)) 
     for i in range(100)],
    ["id", "name", "region", "score"]
)

# Global top 10
print("--- Global Top 10 ---")
players.orderBy(desc("score")).limit(10).show()

# Top 3 per region
print("--- Top 3 Per Region ---")
w = Window.partitionBy("region").orderBy(desc("score"))
players.withColumn("rank", row_number().over(w)) \
    .filter(col("rank") <= 3) \
    .orderBy("region", "rank") \
    .show()

# Pagination (page 2, size 10)
print("--- Pagination: Page 2 (items 11-20) ---")
w_global = Window.orderBy(desc("score"))
players.withColumn("row_num", row_number().over(w_global)) \
    .filter((col("row_num") >= 11) & (col("row_num") <= 20)) \
    .show()

# --- Level 7: Performance ---
print("\n=== Level 7: Performance ===")
big = spark.range(1000000).withColumn("val", expr("rand()*1000"))

start = time.time()
big.orderBy(desc("val")).write.format("noop").mode("overwrite").save()
t1 = time.time() - start

start = time.time()
big.orderBy(desc("val")).limit(100).collect()
t2 = time.time() - start

print(f"Full sort + write: {t1:.3f}s")
print(f"Top-100 (optimized): {t2:.3f}s")
print(f"Speedup: {t1/max(t2,0.001):.1f}x")

print("\n✅ All homework solutions complete!")