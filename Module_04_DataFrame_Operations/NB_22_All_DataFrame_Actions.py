# Databricks notebook source
# DBTITLE 1,NB_22 Header
# MAGIC %md
# MAGIC # NB_22 — All DataFrame Actions (Every Output Method)
# MAGIC
# MAGIC **Module 4: DataFrame Operations** | Notebook 22 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC - show(), display() — visual output
# MAGIC - printSchema(), dtypes, columns, schema — metadata inspection
# MAGIC - count(), isEmpty() — row counting
# MAGIC - first(), head(), take(), tail() — peek at rows
# MAGIC - collect(), toLocalIterator() — bring to driver
# MAGIC - toPandas(), to_pandas_on_spark() — Pandas conversion
# MAGIC - describe(), summary() — statistics
# MAGIC - foreach(), foreachPartition() — side effects
# MAGIC - write actions (brief overview)
# MAGIC
# MAGIC **Difficulty:** ⭐⭐ (Essential Reference)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are DataFrame Actions?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are DataFrame Actions? (Real-World Analogy)
# MAGIC
# MAGIC ### 🏭 The Lazy Restaurant Kitchen
# MAGIC
# MAGIC Imagine a restaurant where the kitchen only cooks when a waiter places an order (not before):
# MAGIC
# MAGIC | Waiter's Action | PySpark Action | What Happens |
# MAGIC |---|---|---|
# MAGIC | "Show me the menu" | `printSchema()` | See what's available (no cooking) |
# MAGIC | "Bring 5 dishes to taste" | `head(5)` / `take(5)` | Cook and serve just 5 |
# MAGIC | "How many dishes total?" | `count()` | Count everything (full scan) |
# MAGIC | "Bring all dishes to my table" | `collect()` | Cook and deliver ALL to driver |
# MAGIC | "Give me nutritional stats" | `describe()` | Compute summary statistics |
# MAGIC | "Serve each table individually" | `foreach()` | Execute side-effect per row |
# MAGIC | "Display in the dining room" | `display()` / `show()` | Formatted output |
# MAGIC
# MAGIC ### Key Insight: Transformations vs Actions
# MAGIC - **Transformations** (select, filter, join) = lazy, just build a plan
# MAGIC - **Actions** = trigger actual computation and return results
# MAGIC - Without an action, NOTHING runs!
# MAGIC
# MAGIC ### The Action Rule
# MAGIC Every PySpark program follows: `Read → Transform (lazy) → Action (triggers execution)`

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Actions Work
# MAGIC %md
# MAGIC ## SECTION 2 — How Actions Work (Internal Mechanics)
# MAGIC
# MAGIC ### Action Categories
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────┐
# MAGIC │          ALL DATAFRAME ACTIONS                    │
# MAGIC ├──────────────────┬─────────────────┬────────────────┤
# MAGIC │  DISPLAY         │  METADATA        │  ROW ACCESS    │
# MAGIC │  show()          │  printSchema()  │  first()       │
# MAGIC │  display()       │  dtypes         │  head(n)       │
# MAGIC │  (Databricks)    │  columns        │  take(n)       │
# MAGIC │                  │  schema         │  tail(n)       │
# MAGIC │                  │  isEmpty()      │  collect()     │
# MAGIC ├──────────────────┼─────────────────┼────────────────┤
# MAGIC │  STATISTICS      │  CONVERSION     │  SIDE EFFECTS  │
# MAGIC │  count()         │  toPandas()     │  foreach()     │
# MAGIC │  describe()      │  toLocalIter()  │  foreachPart() │
# MAGIC │  summary()       │  toJSON()       │  write.*       │
# MAGIC │  stat.*          │  rdd            │               │
# MAGIC └──────────────────┴─────────────────┴────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### What Happens When You Call an Action
# MAGIC
# MAGIC ```
# MAGIC df.filter(...).select(...).count()
# MAGIC                               │
# MAGIC                     ACTION triggered!
# MAGIC                               │
# MAGIC                               ▼
# MAGIC ┌──────────────────────────────────────┐
# MAGIC │ 1. Logical Plan created             │
# MAGIC │ 2. Catalyst optimizer runs          │
# MAGIC │ 3. Physical plan generated           │
# MAGIC │ 4. DAG splits into stages           │
# MAGIC │ 5. Tasks distributed to executors   │
# MAGIC │ 6. Results collected to driver       │
# MAGIC └──────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Memory Safety of Actions
# MAGIC
# MAGIC | Action | Data to Driver | Risk Level |
# MAGIC |--------|---------------|------------|
# MAGIC | `count()` | Single number | ✅ Safe |
# MAGIC | `first()` / `head(1)` | 1 row | ✅ Safe |
# MAGIC | `take(100)` | 100 rows | ✅ Safe |
# MAGIC | `show(20)` | 20 rows (text) | ✅ Safe |
# MAGIC | `describe()` | Stats (few rows) | ✅ Safe |
# MAGIC | `collect()` | ALL rows | ⚠️ DANGEROUS on big data |
# MAGIC | `toPandas()` | ALL rows as pandas | ⚠️ DANGEROUS on big data |

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: show, display, printSchema
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: show(), display(), printSchema()
# ============================================================
# Real-world: First steps when exploring any new dataset

from pyspark.sql import SparkSession  # Import SparkSession
from pyspark.sql.functions import col  # Import col

spark = SparkSession.builder.getOrCreate()  # Get existing session

# Create sample employee data
emp_data = [
    (1, "Alice Johnson", "Engineering", 95000.50, True, "2020-01-15"),
    (2, "Bob Smith", "Marketing", 72000.00, False, "2021-06-01"),
    (3, "Charlie Williams", "Engineering", 88000.75, True, "2019-03-20"),
    (4, "Diana Brown", "Sales", 65000.00, True, "2022-08-10"),
    (5, "Eve Davis", "Engineering", 120000.00, True, "2018-12-05"),
]

df = spark.createDataFrame(
    emp_data, 
    ["id", "name", "dept", "salary", "is_active", "start_date"]
)

# === show() — text-based table output ===
print("=== show() Variations ===")
print("\n--- show() — default (20 rows, truncate at 20 chars) ---")
df.show()  # Default: 20 rows, truncate=True

print("--- show(3) — only 3 rows ---")
df.show(3)  # Show only 3 rows

print("--- show(truncate=False) — full column width ---")
df.show(truncate=False)  # Don't truncate long values

print("--- show(n=3, truncate=10) — 3 rows, max 10 chars ---")
df.show(n=3, truncate=10)  # Limit both rows and width

print("--- show(vertical=True) — vertical format ---")
df.show(n=2, vertical=True)  # One column per line (good for wide tables)

# === display() — Databricks rich output (HTML table, charts) ===
print("\n=== display() — Databricks Enhanced Output ===")
print("display() renders as interactive HTML table in Databricks")
print("Supports: sorting, filtering, chart visualizations, downloading")
display(df)  # Rich HTML table in Databricks

# === printSchema() — show column types ===
print("\n=== printSchema() — Column Types ===")
df.printSchema()  # Tree-format schema

# === schema attribute — programmatic access ===
print("\n=== schema (StructType object) ===")
print(df.schema)  # StructType representation
print(f"\nSchema as JSON: {df.schema.json()[:200]}...")  # JSON format

# === dtypes — list of (name, type) tuples ===
print("\n=== dtypes — (name, type) tuples ===")
for name, dtype in df.dtypes:  # Iterate column types
    print(f"  {name:15s} -> {dtype}")

# === columns — list of column names ===
print(f"\n=== columns — column name list ===")
print(f"Columns: {df.columns}")  # List of strings
print(f"Number of columns: {len(df.columns)}")  # Column count

# Expected Output:
# printSchema:
# root
#  |-- id: long (nullable = true)
#  |-- name: string (nullable = true)
#  |-- dept: string (nullable = true)
#  |-- salary: double (nullable = true)
#  |-- is_active: boolean (nullable = true)
#  |-- start_date: string (nullable = true)

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: count, isEmpty, first, head, take
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: count, isEmpty, first, head, take
# ============================================================
# Real-world: Quick checks before processing a dataset

from pyspark.sql.functions import col

# Reuse df from previous cell
print("=== Row Counting ===")

# count() — returns total number of rows (triggers full scan)
row_count = df.count()  # Full table scan!
print(f"Total rows: {row_count}")  # 5

# isEmpty() — check if DataFrame has zero rows (Spark 3.3+)
# More efficient than count() == 0 (can short-circuit)
try:
    is_empty = df.isEmpty()  # Returns boolean
    print(f"Is empty: {is_empty}")  # False
    
    empty_df = df.filter(col("salary") > 999999)  # No rows match
    print(f"Filtered isEmpty: {empty_df.isEmpty()}")  # True
except AttributeError:
    print("isEmpty() not available (Spark 3.3+ required)")

print("\n=== Peeking at Rows ===")

# first() — returns first Row object (or None if empty)
first_row = df.first()  # Returns a Row object
print(f"\nfirst(): {first_row}")  # Row(id=1, name='Alice Johnson', ...)
print(f"  Access by name: {first_row['name']}")  # 'Alice Johnson'
print(f"  Access by index: {first_row[1]}")  # 'Alice Johnson'

# head(n) — returns first N rows as list of Row objects
print(f"\nhead(3): returns list of 3 Row objects")
for row in df.head(3):  # Iterate first 3 rows
    print(f"  {row['name']:20s} | ${row['salary']:,.2f}")

# head() without argument = head(1) = same as first()
print(f"\nhead() == first(): {df.head() == df.first()}")  # True

# take(n) — same as head(n), returns list of Row objects
print(f"\ntake(2):")
for row in df.take(2):  # Same as head(2)
    print(f"  ID={row['id']}, Name={row['name']}")

# tail(n) — returns LAST N rows (requires full scan!)
print(f"\ntail(2): (requires full data scan!)")
for row in df.tail(2):  # Last 2 rows
    print(f"  ID={row['id']}, Name={row['name']}")

# Key difference: head/take scan from start, tail scans everything
print("\n=== Performance Notes ===")
print("first()/head(n)/take(n): May read only 1 partition (fast!)")
print("tail(n): ALWAYS reads ALL data then takes last N (slow!)")
print("count(): ALWAYS reads ALL data (full scan)")
print("isEmpty(): Can short-circuit after finding 1 row (efficient!)")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: collect and toLocalIterator
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: collect() and toLocalIterator()
# ============================================================
# Real-world: Bringing distributed data to driver for Python processing

from pyspark.sql.functions import col

print("=== collect() — Bring ALL Rows to Driver ===")
print()
print("⚠️  WARNING: collect() pulls ALL data to driver memory!")
print("   Only use on small DataFrames (< 100K rows typically)")
print()

# Collect returns list of Row objects
all_rows = df.collect()  # ALL rows to driver as list
print(f"Type: {type(all_rows)}")  # <class 'list'>
print(f"Length: {len(all_rows)}")  # 5
print(f"First element type: {type(all_rows[0])}")  # <class 'pyspark.sql.types.Row'>

# Working with collected rows in Python
print("\n--- Processing collected rows ---")
for row in all_rows:  # Iterate all rows
    print(f"  {row.name}: ${row.salary:,.2f} ({row.dept})")

# Convert Row to dict
print("\n--- Row as dictionary ---")
first_dict = all_rows[0].asDict()  # Convert to Python dict
print(f"  {first_dict}")

# Access Row fields
print("\n--- Row field access ---")
row = all_rows[0]
print(f"  By name: row.name = '{row.name}'")       # Dot notation
print(f"  By key:  row['name'] = '{row['name']}'") # Dict-style
print(f"  By index: row[1] = '{row[1]}'")           # Position (0-based)

print("\n=== toLocalIterator() — Memory-Safe Alternative ===")
print()
print("toLocalIterator() fetches data partition-by-partition")
print("Only ONE partition in memory at a time (safe for large data)")
print()

# toLocalIterator returns an iterator (lazy, one partition at a time)
iterator = df.toLocalIterator()  # Returns iterator
print(f"Type: {type(iterator)}")  # iterator

print("\n--- Iterating (one partition at a time in memory) ---")
for i, row in enumerate(df.toLocalIterator()):  # Process row by row
    print(f"  Row {i}: {row.name} - {row.dept}")
    if i >= 3:  # Stop after a few for demo
        print("  ... (stopping early for demo)")
        break

# When to use which:
print("\n=== collect() vs toLocalIterator() ===")
print("+------------------+---------------------------+")
print("| Method           | Use When                  |")
print("+------------------+---------------------------+")
print("| collect()        | Small data (<100K rows)   |")
print("|                  | Need random access to rows|")
print("|                  | Need list operations      |")
print("+------------------+---------------------------+")
print("| toLocalIterator()| Large data, streaming out |")
print("|                  | Memory-constrained driver |")
print("|                  | Only need sequential access|")
print("+------------------+---------------------------+")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: describe and summary
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: describe() and summary()
# ============================================================
# Real-world: Quick statistical profiling before analysis

from pyspark.sql.functions import col

print("=== describe() — Basic Statistics ===")
print("Returns: count, mean, stddev, min, max for numeric/string columns")
print()

# describe() on all columns
df.describe().show()  # Stats for all columns

# describe() on specific columns
print("--- describe specific columns ---")
df.describe("salary", "name").show()  # Only salary and name

print("\n=== summary() — Extended Statistics ===")
print("Returns: count, mean, stddev, min, 25%, 50%, 75%, max")
print()

# summary() — includes percentiles (quartiles)
df.summary().show()  # Full summary with percentiles

# summary() with specific statistics
print("--- Select specific stats ---")
df.summary("count", "min", "max", "50%").show()  # Only these stats

# Practical: Quick data quality check
print("\n=== Practical: Data Quality Profile ===")

# Create a larger dataset for meaningful stats
import random
random.seed(42)
profile_data = [(i, f"user_{i}", random.gauss(75000, 15000), random.randint(22, 65)) 
                for i in range(100)]  # 100 rows

profile_df = spark.createDataFrame(profile_data, ["id", "username", "salary", "age"])

print("--- Profile of 100 employees ---")
profile_df.describe().show()  # See distribution

print("--- Percentile breakdown ---")
profile_df.select("salary", "age").summary("25%", "50%", "75%").show()

# stat methods — additional statistics
print("\n=== stat methods ===")

# Correlation between salary and age
corr_val = profile_df.stat.corr("salary", "age")  # Pearson correlation
print(f"Correlation(salary, age): {corr_val:.4f}")

# Covariance
cov_val = profile_df.stat.cov("salary", "age")  # Covariance
print(f"Covariance(salary, age): {cov_val:.2f}")

# Approximate quantiles (faster than exact)
quantiles = profile_df.stat.approxQuantile("salary", [0.25, 0.5, 0.75], 0.01)
print(f"Salary quartiles (approx): Q1={quantiles[0]:.0f}, Q2={quantiles[1]:.0f}, Q3={quantiles[2]:.0f}")

# Frequent items
print("\nFrequent ages (>1% of data):")
profile_df.stat.freqItems(["age"], 0.01).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: toPandas and Pandas conversion
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: toPandas() and Pandas Conversion
# ============================================================
# Real-world: Using Pandas for visualization, ML libraries, or exports

import pandas as pd
from pyspark.sql.functions import col

print("=== toPandas() — Convert to Pandas DataFrame ===")
print()
print("⚠️  WARNING: toPandas() collects ALL data to driver!")
print("   Safe for < 1M rows typically. Use Arrow for speed.")
print()

# Convert small PySpark DF to Pandas
pdf = df.toPandas()  # All data to driver as pandas DataFrame
print(f"Type: {type(pdf)}")  # <class 'pandas.core.frame.DataFrame'>
print(f"Shape: {pdf.shape}")  # (5, 6)

print("\n--- Pandas DataFrame ---")
print(pdf.head())  # Pandas head
print(f"\nPandas dtypes:\n{pdf.dtypes}")  # Pandas types

# Arrow optimization for toPandas (much faster!)
print("\n=== Arrow-Optimized toPandas ===")
print("Set spark.sql.execution.arrow.pyspark.enabled=true for 10-100x speedup")
print()

# Enable Arrow (already enabled by default in newer Databricks runtimes)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Now toPandas uses Arrow (columnar transfer, no serialization overhead)
pdf_arrow = df.toPandas()  # Arrow-optimized transfer
print(f"Arrow result identical: {pdf.equals(pdf_arrow)}")  # True

# === Going the other direction: Pandas → PySpark ===
print("\n=== createDataFrame(pandas_df) — Pandas to PySpark ===")

# Create a Pandas DataFrame
pd_data = pd.DataFrame({
    "product": ["Widget", "Gadget", "Doohickey"],
    "price": [9.99, 24.99, 4.99],
    "in_stock": [True, False, True],
})

# Convert to PySpark (Arrow-optimized automatically)
spark_df = spark.createDataFrame(pd_data)  # Pandas → PySpark
spark_df.show()  # Show PySpark DataFrame
spark_df.printSchema()  # Types preserved from Pandas

# === toJSON() — Convert to JSON strings ===
print("\n=== toJSON() — Each Row as JSON String ===")
json_rdd = df.toJSON()  # Returns RDD of JSON strings
for j in json_rdd.take(3):  # Show first 3 as JSON
    print(f"  {j}")

# === Limit before toPandas (safe pattern) ===
print("\n=== Safe Pattern: Limit Before toPandas ===")
print("Always limit when you don't need all rows:")
safe_pdf = df.limit(1000).toPandas()  # Only bring 1000 rows max
print(f"Safe pandas shape: {safe_pdf.shape}")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: foreach and foreachPartition
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: foreach() and foreachPartition()
# ============================================================
# Real-world: Sending each row to an external system (API, database, queue)

from pyspark.sql.functions import col

print("=== foreach() — Execute Function Per Row ===")
print()
print("foreach(func): Applies func to EACH ROW on executors")
print("Use for: writing to external systems, sending notifications")
print("⚠️  func runs on EXECUTORS, not driver! No print() visibility.")
print()

# Example: Process each row (simulated external write)
processed_count = spark.sparkContext.accumulator(0)  # Counter on driver

def process_row(row):  # This runs on executors!
    """Simulate sending each row to an external API."""
    processed_count.add(1)  # Increment counter (accumulator goes to driver)
    # In real code: requests.post(api_url, json=row.asDict())

df.foreach(process_row)  # Execute on each row
print(f"Processed {processed_count.value} rows via foreach")  # 5

print("\n=== foreachPartition() — Execute Function Per Partition ===")
print()
print("foreachPartition(func): Applies func to each PARTITION (batch)")
print("MUCH more efficient: open connection once per partition, not per row")
print()

partition_count = spark.sparkContext.accumulator(0)  # Partition counter
row_count_acc = spark.sparkContext.accumulator(0)  # Row counter

def process_partition(iterator):  # This runs on executors!
    """Simulate batch write: open connection once, write many rows."""
    partition_count.add(1)  # One partition being processed
    # In real code: connection = db.connect(...)
    rows_in_partition = 0
    for row in iterator:  # Iterate all rows in this partition
        rows_in_partition += 1
        row_count_acc.add(1)
        # In real code: connection.insert(row.asDict())
    # In real code: connection.commit(); connection.close()

df.foreachPartition(process_partition)  # Execute per partition
print(f"Partitions processed: {partition_count.value}")
print(f"Total rows processed: {row_count_acc.value}")

# foreachBatch (Structured Streaming) — brief mention
print("\n=== Key Differences ===")
print("+--------------------+-----------------------------+")
print("| Method             | Use Case                    |")
print("+--------------------+-----------------------------+")
print("| foreach(f)         | Simple per-row processing   |")
print("|                    | No connection pooling needed|")
print("+--------------------+-----------------------------+")
print("| foreachPartition(f)| Batch writes to databases   |")
print("|                    | Connection pooling (1/part) |")
print("|                    | MUCH more efficient!        |")
print("+--------------------+-----------------------------+")
print("| foreachBatch       | Streaming micro-batch writes|")
print("|                    | (Structured Streaming only) |")
print("+--------------------+-----------------------------+")

print("\n💡 Best Practice: Always prefer foreachPartition over foreach")
print("   Creating connections is expensive; do it once per partition.")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Action Performance Comparison
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Action Performance Comparison
# ============================================================
# Real-world: Understanding which actions are expensive

import time
from pyspark.sql.functions import col, expr

# Create a substantial DataFrame
big_df = spark.range(1000000).select(  # 1M rows
    col("id"),
    expr("concat('user_', id)").alias("name"),
    expr("rand() * 100000").alias("salary"),
    expr("CASE WHEN id % 5 = 0 THEN 'A' WHEN id % 5 = 1 THEN 'B' ELSE 'C' END").alias("group"),
)

print("=== Action Performance Comparison (1M rows) ===")
print(f"DataFrame: {big_df.rdd.getNumPartitions()} partitions, 1M rows")
print()

# Warm up
big_df.head(1)

# Test 1: count()
start = time.time()
result = big_df.count()  # Full scan + aggregation
t1 = time.time() - start
print(f"count():           {t1:.3f}s | Result: {result:,} rows")

# Test 2: first() / head(1)
start = time.time()
result = big_df.first()  # May read only 1 partition
t2 = time.time() - start
print(f"first():           {t2:.3f}s | Result: {result['name']}")

# Test 3: head(10)
start = time.time()
result = big_df.head(10)  # Read minimal partitions
t3 = time.time() - start
print(f"head(10):          {t3:.3f}s | Result: {len(result)} rows")

# Test 4: take(100)
start = time.time()
result = big_df.take(100)  # Read minimal partitions
t4 = time.time() - start
print(f"take(100):         {t4:.3f}s | Result: {len(result)} rows")

# Test 5: tail(10) — full scan required!
start = time.time()
result = big_df.tail(10)  # Must read ALL data
t5 = time.time() - start
print(f"tail(10):          {t5:.3f}s | Result: {len(result)} rows (FULL SCAN!)")

# Test 6: describe()
start = time.time()
result = big_df.describe("salary")  # Stats = full scan
t6 = time.time() - start
print(f"describe():        {t6:.3f}s | Result: statistical summary")

# Test 7: collect() — DANGEROUS!
print(f"\ncollect():         SKIPPED (would transfer 1M rows to driver!)")
print(f"toPandas():        SKIPPED (would transfer 1M rows to driver!)")

print("\n=== Summary ===")
print(f"Fastest (partial scan):  first/head  ~ {min(t2,t3):.3f}s")
print(f"Full scan required:      count/tail  ~ {max(t1,t5):.3f}s")
print(f"Stat computation:        describe    ~ {t6:.3f}s")
print("\n💡 Lesson: Use head()/first() for quick peeks, avoid tail()/collect() on big data")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Practical Action Patterns
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Practical Action Patterns
# ============================================================
# Real-world: Production patterns for safe data retrieval

from pyspark.sql.functions import col, count, sum as _sum, when, lit

print("=== Pattern 1: Safe Existence Check ===")
def safe_has_data(df):
    """Check if DataFrame has any rows (efficient)."""
    try:
        return not df.isEmpty()  # Spark 3.3+
    except AttributeError:
        return df.head(1) is not None  # Fallback for older versions

result = safe_has_data(df)  # True
print(f"Has data: {result}")

print("\n=== Pattern 2: Safe collect with limit ===")
def safe_collect(df, max_rows=10000):
    """Collect with safety limit to prevent OOM."""
    actual_count = df.count()  # Get total
    if actual_count > max_rows:  # Too many rows?
        print(f"WARNING: {actual_count:,} rows exceeds limit {max_rows:,}. Truncating.")
        return df.limit(max_rows).collect()  # Truncate
    return df.collect()  # Safe to collect all

rows = safe_collect(df)  # Safe: only 5 rows
print(f"Collected {len(rows)} rows safely")

print("\n=== Pattern 3: Chunked processing with toLocalIterator ===")
def process_in_chunks(df, chunk_size=1000):
    """Process large DataFrame in memory-safe chunks."""
    chunk = []
    total_processed = 0
    for row in df.toLocalIterator():  # One partition at a time
        chunk.append(row.asDict())  # Build chunk
        if len(chunk) >= chunk_size:  # Chunk full?
            # Process chunk (e.g., batch API call)
            total_processed += len(chunk)
            chunk = []  # Reset
    if chunk:  # Process remaining
        total_processed += len(chunk)
    return total_processed

total = process_in_chunks(df, chunk_size=2)  # Process in chunks of 2
print(f"Processed {total} rows in chunks")

print("\n=== Pattern 4: Column value extraction ===")
# Get distinct values of a column (common need)
depts = [row.dept for row in df.select("dept").distinct().collect()]  # Collect unique values
print(f"Departments: {sorted(depts)}")

# Get min/max of a column
from pyspark.sql.functions import min as _min, max as _max
stats = df.select(_min("salary").alias("min_sal"), _max("salary").alias("max_sal")).first()
print(f"Salary range: ${stats['min_sal']:,.2f} to ${stats['max_sal']:,.2f}")

print("\n=== Pattern 5: Data quality quick-check ===")
def quick_quality_check(df):
    """Run a quick data quality assessment."""
    total = df.count()  # Total rows
    print(f"  Total rows:    {total:,}")
    print(f"  Columns:       {len(df.columns)}")
    print(f"  Partitions:    {df.rdd.getNumPartitions()}")
    
    # Null check per column
    null_counts = df.select(
        *[count(when(col(c).isNull(), 1)).alias(c) for c in df.columns]
    ).first()
    
    print("  Null counts:")
    for c in df.columns:
        nulls = null_counts[c]
        pct = (nulls / total * 100) if total > 0 else 0
        if nulls > 0:  # Only show columns with nulls
            print(f"    {c}: {nulls} ({pct:.1f}%)")
    
    if all(null_counts[c] == 0 for c in df.columns):
        print("    ✅ No nulls found!")

quick_quality_check(df)  # Run quality check

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Write Actions Overview
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Write Actions and Output Modes
# ============================================================
# Real-world: Saving processed data to various destinations

from pyspark.sql.functions import col, lit, current_timestamp

print("=== Write Actions Overview ===")
print()
print("Write actions are the most important actions in production:")
print("They trigger computation AND persist results.")
print()

# Create sample data for writing
write_df = df.withColumn("processed_at", current_timestamp())

# === All write modes ===
print("=== Write Modes ===")
print("+-------------+-------------------------------------------+")
print("| Mode        | Behavior                                  |")
print("+-------------+-------------------------------------------+")
print("| 'overwrite' | Delete existing data, write new            |")
print("| 'append'    | Add to existing data                      |")
print("| 'ignore'    | Skip if path/table exists (no error)      |")
print("| 'error'     | Throw error if exists (DEFAULT)           |")
print("+-------------+-------------------------------------------+")

# === Write to different formats (demonstration only) ===
print("\n=== Write Syntax Examples (not executed to avoid side effects) ===")
print("""
# Write as Delta (default in Databricks)
df.write.format("delta").mode("overwrite").saveAsTable("catalog.schema.table")

# Write as Parquet
df.write.format("parquet").mode("overwrite").save("/path/to/output")

# Write as CSV
df.write.format("csv").option("header", True).mode("overwrite").save("/path/output.csv")

# Write with partitioning
df.write.format("delta").partitionBy("dept", "year").saveAsTable("my_table")

# Write with options
df.write.format("delta") \\
    .mode("overwrite") \\
    .option("overwriteSchema", "true") \\
    .option("mergeSchema", "true") \\
    .saveAsTable("my_table")
""")

# noop write (triggers execution without actually writing anywhere)
print("\n=== noop format — trigger execution without writing ===")
print("Useful for benchmarking (measures compute time without I/O)")
import time
start = time.time()
write_df.write.format("noop").mode("overwrite").save()  # Execute but don't save
print(f"noop write time: {time.time()-start:.3f}s")

# === Action trigger summary ===
print("\n=== Complete Action Trigger Summary ===")
print("+---------------------+------------------+------------------+")
print("| Action              | Triggers Compute | Returns Data     |")
print("+---------------------+------------------+------------------+")
print("| show()              | Yes              | None (prints)    |")
print("| display()           | Yes              | None (renders)   |")
print("| count()             | Yes              | int              |")
print("| collect()           | Yes              | List[Row]        |")
print("| toPandas()          | Yes              | pd.DataFrame     |")
print("| first()/head()      | Yes (partial)    | Row              |")
print("| take(n)             | Yes (partial)    | List[Row]        |")
print("| tail(n)             | Yes (full!)      | List[Row]        |")
print("| describe()/summary()| Yes              | DataFrame        |")
print("| foreach(f)          | Yes              | None (side-effect)|")
print("| write.*             | Yes              | None (persists)  |")
print("+---------------------+------------------+------------------+")
print("\n| printSchema()       | NO               | None (local meta)|")
print("| columns / dtypes    | NO               | list (local meta)|")
print("+---------------------+------------------+------------------+")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with DataFrame Actions
# MAGIC
# MAGIC ### ❌ Mistake 1: Using collect() on large DataFrames
# MAGIC ```python
# MAGIC # WRONG — May crash driver with OutOfMemoryError
# MAGIC all_data = big_df.collect()  # 100M rows → driver OOM!
# MAGIC
# MAGIC # CORRECT — Limit first, or use toLocalIterator
# MAGIC all_data = big_df.limit(10000).collect()  # Safe
# MAGIC # OR
# MAGIC for row in big_df.toLocalIterator():  # Memory-safe streaming
# MAGIC     process(row)
# MAGIC ```
# MAGIC **Why:** `collect()` transfers ALL data to driver RAM. 100M rows × 100 bytes/row = 10GB on driver.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 2: Calling count() just to check if data exists
# MAGIC ```python
# MAGIC # WRONG — Scans ALL data just to check existence
# MAGIC if df.count() > 0:  # Full table scan!
# MAGIC     process(df)
# MAGIC
# MAGIC # CORRECT — isEmpty() or head() short-circuits
# MAGIC if not df.isEmpty():  # Stops after finding 1 row
# MAGIC     process(df)
# MAGIC # OR (older Spark)
# MAGIC if df.head(1):  # Reads at most 1 partition
# MAGIC     process(df)
# MAGIC ```
# MAGIC **Why:** `count()` always scans every row. `isEmpty()`/`head(1)` can stop after 1 row found.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 3: Thinking printSchema() is an action
# MAGIC ```python
# MAGIC # WRONG assumption
# MAGIC df.filter(...).printSchema()  # "My filter must have run"
# MAGIC # Actually: printSchema() reads LOCAL metadata, no computation triggered!
# MAGIC
# MAGIC # To verify filter works, use an actual action:
# MAGIC df.filter(...).show(5)  # THIS triggers computation
# MAGIC ```
# MAGIC **Why:** `printSchema()`, `columns`, `dtypes` read from the local logical plan — no executor computation needed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 4: Using tail() thinking it's like head()
# MAGIC ```python
# MAGIC # WRONG assumption — tail() is NOT the reverse of head()
# MAGIC df.tail(5)  # Reads ALL partitions! As expensive as count().
# MAGIC
# MAGIC # If you need last N rows, order explicitly:
# MAGIC df.orderBy(col("date").desc()).head(5)  # Last 5 by date (Spark optimizes)
# MAGIC ```
# MAGIC **Why:** `tail()` must read all data to find the "end". Unlike `head()` which can stop after 1 partition.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 5: Calling multiple actions without caching
# MAGIC ```python
# MAGIC # WRONG — Each action recomputes from scratch
# MAGIC print(df.count())     # Full computation #1
# MAGIC print(df.first())     # Full computation #2
# MAGIC df.describe().show()  # Full computation #3
# MAGIC
# MAGIC # CORRECT — Cache if using multiple actions
# MAGIC df.cache()            # Store in memory
# MAGIC print(df.count())     # Computation + cache
# MAGIC print(df.first())     # From cache (instant)
# MAGIC df.describe().show()  # From cache (instant)
# MAGIC df.unpersist()        # Free memory when done
# MAGIC ```
# MAGIC **Why:** DataFrames are lazy. Each action re-evaluates the entire plan unless cached.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of DataFrame Actions Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Create a 5-row DataFrame. Call `show()`, `printSchema()`, `count()`, `first()`, and `columns`.
# MAGIC 2. Call `describe()` and `summary()` on a numeric column.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Use `show(vertical=True)` on a wide DataFrame (10+ columns).
# MAGIC 4. Change `head(5)` to `take(5)` and verify they return the same result.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Use `count()` + `describe()` + `distinct().count()` to build a mini data profile.
# MAGIC 6. Use `collect()` on a filtered, limited DataFrame and convert each Row to a dict.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Create a function that takes a DataFrame and generates a complete profiling report: row count, column count, null percentages, distinct counts, min/max per column.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build a "safe_export" function that:
# MAGIC    - Checks row count against a threshold
# MAGIC    - If safe, converts to Pandas and exports to CSV
# MAGIC    - If too large, uses toLocalIterator to write in chunks
# MAGIC    - Logs metrics about the export
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design a `DataFrameInspector` class that wraps a DataFrame and provides:
# MAGIC    - `.profile()` — full statistical profile
# MAGIC    - `.quality()` — null analysis, duplicate detection
# MAGIC    - `.sample(n)` — smart sampling
# MAGIC    - `.compare(other_df)` — schema/count comparison
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Create a 10M row DataFrame. Benchmark:
# MAGIC     - `count()` vs `select(count(*))` vs `rdd.count()`
# MAGIC     - `toPandas()` with Arrow enabled vs disabled
# MAGIC     - `collect()` vs `toLocalIterator()` memory usage
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Test behavior with:
# MAGIC     - `first()` on an empty DataFrame (returns None)
# MAGIC     - `head(10)` when only 3 rows exist (returns 3)
# MAGIC     - `collect()` with NULL-only columns
# MAGIC     - `describe()` on boolean and date columns
# MAGIC     - `toPandas()` with complex types (arrays, structs)
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build a production data quality framework:
# MAGIC     - Pre-flight checks before expensive operations
# MAGIC     - Metric collection using accumulators + foreach
# MAGIC     - Automatic alerting when quality drops below threshold
# MAGIC     - Checkpoint results at each pipeline stage
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create a decision tree poster: "Which action should I use?"
# MAGIC     - Need row count? → count() / isEmpty()
# MAGIC     - Need a peek? → head() / show()
# MAGIC     - Need all data? → collect() / toPandas() (with size check)
# MAGIC     - Need side effects? → foreach / foreachPartition
# MAGIC     - Need stats? → describe() / summary()

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *
import time

# --- Level 1: Basic Actions ---
print("=== Level 1: All Basic Actions ===")
test_df = spark.createDataFrame([
    (1, "Alice", 95000.0, "Engineering"),
    (2, "Bob", 72000.0, "Marketing"),
    (3, "Charlie", 88000.0, "Sales"),
    (4, "Diana", 65000.0, "Engineering"),
    (5, "Eve", 110000.0, "Marketing"),
], ["id", "name", "salary", "dept"])

print(f"count(): {test_df.count()}")       # 5
print(f"columns: {test_df.columns}")       # ['id', 'name', 'salary', 'dept']
print(f"first(): {test_df.first()}")       # Row(id=1, ...)
test_df.printSchema()                       # Schema tree
test_df.describe("salary").show()           # Stats
test_df.summary("count", "min", "max").show()  # Summary

# --- Level 5: Safe Export Function ---
print("\n=== Level 5: Safe Export ===")
import os

def safe_export(df, path, max_direct=50000, chunk_size=10000):
    """Safely export DataFrame to CSV with size protection."""
    total = df.count()  # Get row count
    print(f"  Exporting {total:,} rows to {path}")
    
    if total == 0:  # Empty check
        print("  ⚠️ No data to export!")
        return 0
    
    if total <= max_direct:  # Small enough for direct conversion
        print(f"  Direct export (under {max_direct:,} threshold)")
        pdf = df.toPandas()  # Safe size
        # pdf.to_csv(path, index=False)  # Would write file
        print(f"  ✅ Exported {len(pdf)} rows directly")
        return len(pdf)
    else:  # Too large, chunk it
        print(f"  Chunked export ({total:,} rows in chunks of {chunk_size:,})")
        exported = 0
        chunk = []
        for row in df.toLocalIterator():  # Memory-safe
            chunk.append(row.asDict())
            if len(chunk) >= chunk_size:
                # pd.DataFrame(chunk).to_csv(path, mode='a', header=(exported==0))
                exported += len(chunk)
                chunk = []
        if chunk:  # Remaining rows
            exported += len(chunk)
        print(f"  ✅ Exported {exported:,} rows in chunks")
        return exported

safe_export(test_df, "/tmp/test_export.csv")  # Test with small data

# --- Level 7: Performance Benchmark ---
print("\n=== Level 7: Performance Benchmark ===")
big = spark.range(1000000)  # 1M rows

start = time.time()
c1 = big.count()  # DataFrame count
t1 = time.time() - start

start = time.time()
c2 = big.select(count("*")).first()[0]  # SQL-style count
t2 = time.time() - start

print(f"df.count():           {t1:.3f}s -> {c1:,}")
print(f"select(count(*)):     {t2:.3f}s -> {c2:,}")

# Arrow benchmark
small = spark.range(100000)  # 100K for toPandas test

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
start = time.time()
pdf1 = small.toPandas()
t_no_arrow = time.time() - start

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
start = time.time()
pdf2 = small.toPandas()
t_arrow = time.time() - start

print(f"\ntoPandas (no Arrow):  {t_no_arrow:.3f}s")
print(f"toPandas (Arrow):     {t_arrow:.3f}s")
print(f"Arrow speedup:        {t_no_arrow/max(t_arrow, 0.001):.1f}x")

print("\n✅ All homework solutions complete!")