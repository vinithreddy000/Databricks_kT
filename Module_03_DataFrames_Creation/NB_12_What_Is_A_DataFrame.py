# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 12: What Is A DataFrame?
# MAGIC # Module: DataFrames — Creation & Basics
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 35 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: A Spreadsheet on Steroids
# MAGIC
# MAGIC A **DataFrame** is like an Excel spreadsheet:
# MAGIC - It has **rows** (records) and **columns** (fields)
# MAGIC - Each column has a **name** and a **data type** (like formatting in Excel)
# MAGIC - You can filter, sort, group, and aggregate
# MAGIC
# MAGIC But unlike Excel:
# MAGIC - A DataFrame can hold **billions** of rows across hundreds of machines
# MAGIC - Operations are **optimized automatically** by Spark's Catalyst optimizer
# MAGIC - It's **immutable** — operations create new DataFrames, never modify the original
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### DataFrame vs RDD
# MAGIC
# MAGIC | Feature | RDD | DataFrame |
# MAGIC |---------|-----|----------|
# MAGIC | Structure | Unstructured (any Python object) | Tabular (rows + named columns) |
# MAGIC | Optimization | None (you control everything) | Catalyst optimizer (automatic!) |
# MAGIC | Speed | Slower (Python serialization) | 10-100x faster (Tungsten engine) |
# MAGIC | API | Functional (map, filter, reduce) | SQL-like (select, where, groupBy) |
# MAGIC | Schema | No schema | Has schema (column names + types) |
# MAGIC | Use case | Low-level control, unstructured | 99% of data tasks |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use DataFrames (Almost Always!)
# MAGIC
# MAGIC 1. **Reading files** (CSV, JSON, Parquet, Delta) → DataFrame
# MAGIC 2. **SQL queries** → DataFrame
# MAGIC 3. **Joins, aggregations, window functions** → DataFrame
# MAGIC 4. **Machine Learning** (MLlib) → DataFrame
# MAGIC 5. **Writing output** → DataFrame
# MAGIC
# MAGIC ### When to Use RDDs (Rare)
# MAGIC 1. Need fine-grained control over partitioning
# MAGIC 2. Working with unstructured data (binary blobs)
# MAGIC 3. Need custom serialization
# MAGIC 4. Legacy code that hasn't been migrated

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### DataFrame Architecture
# MAGIC
# MAGIC ```
# MAGIC   User writes:  df.select("name").filter(col("age") > 25).groupBy("city")
# MAGIC        │
# MAGIC        ▼
# MAGIC   Catalyst Optimizer:
# MAGIC        1. Parses your operations into a Logical Plan
# MAGIC        2. Optimizes (predicate pushdown, column pruning, join reorder)
# MAGIC        3. Generates a Physical Plan (how to actually execute)
# MAGIC        4. Hands off to Tungsten Engine for execution
# MAGIC        │
# MAGIC        ▼
# MAGIC   Tungsten Engine:
# MAGIC        - Uses whole-stage code generation (creates Java bytecode!)
# MAGIC        - Manages memory manually (off-heap, binary format)
# MAGIC        - 10-100x faster than Python RDD operations
# MAGIC ```
# MAGIC
# MAGIC ### Why DataFrames Are Faster Than RDDs
# MAGIC
# MAGIC ```
# MAGIC   RDD path:                     DataFrame path:
# MAGIC   ─────────                     ──────────────
# MAGIC   Python object                 Binary format (Tungsten)
# MAGIC   → Serialize to Java           → No serialization needed!
# MAGIC   → Process in JVM              → Direct binary processing
# MAGIC   → Serialize back to Python    → No deserialization!
# MAGIC   
# MAGIC   Result: DataFrame = 10-100x faster for structured data
# MAGIC ```
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC 1. **Schema** = column names + data types (like a CREATE TABLE statement)
# MAGIC 2. **Rows** = individual records (like SQL rows)
# MAGIC 3. **Columns** = named fields with types (like SQL columns)
# MAGIC 4. **Lazy evaluation** = nothing executes until an action is called
# MAGIC 5. **Immutable** = every operation creates a NEW DataFrame

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Creating Your First DataFrame
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: Your First DataFrame
# ═══════════════════════════════════════════════════════

print("=== Your First DataFrame ===")
print()

# Create a DataFrame from a list of tuples
# spark.createDataFrame(data, columns)
data = [
    ("Alice", 30, "Engineering"),   # Row 1: name, age, department
    ("Bob", 25, "Marketing"),       # Row 2
    ("Charlie", 35, "Engineering"), # Row 3
    ("Diana", 28, "Sales"),         # Row 4
    ("Eve", 32, "Marketing"),       # Row 5
]

# Create the DataFrame with column names
df = spark.createDataFrame(data, ["name", "age", "department"])  # Specify column names

# show() — displays the DataFrame as a table
print("1. show() — display the table:")
df.show()  # Default: first 20 rows, truncate at 20 chars

# printSchema() — shows column names and types
print("2. printSchema() — the structure:")
df.printSchema()  # Shows: name(string), age(long), department(string)

# count() — number of rows
print(f"3. count(): {df.count()} rows")  # 5

# columns — list of column names
print(f"4. columns: {df.columns}")  # ['name', 'age', 'department']

# dtypes — columns with their types
print(f"5. dtypes: {df.dtypes}")  # [('name', 'string'), ('age', 'bigint'), ...]

# Expected Output:
# +-------+---+-----------+
# |   name|age| department|
# +-------+---+-----------+
# |  Alice| 30|Engineering|
# |    Bob| 25|  Marketing|
# |Charlie| 35|Engineering|
# |  Diana| 28|      Sales|
# |    Eve| 32|  Marketing|
# +-------+---+-----------+

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Basic DataFrame Operations
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: Basic Operations
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col  # Import col for column references

print("=== Basic DataFrame Operations ===")
print()

# Recreate the DataFrame
data = [
    ("Alice", 30, "Engineering", 95000),
    ("Bob", 25, "Marketing", 72000),
    ("Charlie", 35, "Engineering", 110000),
    ("Diana", 28, "Sales", 68000),
    ("Eve", 32, "Marketing", 85000),
    ("Frank", 40, "Engineering", 125000),
]
df = spark.createDataFrame(data, ["name", "age", "department", "salary"])

# select() — choose specific columns (like SQL SELECT)
print("1. select() — pick columns:")
df.select("name", "salary").show()  # Only name and salary

# filter() / where() — keep rows matching condition (like SQL WHERE)
print("2. filter() — keep matching rows:")
df.filter(col("age") > 30).show()  # Only people over 30

# orderBy() / sort() — sort the data
print("3. orderBy() — sort data:")
df.orderBy(col("salary").desc()).show()  # Highest salary first

# withColumn() — add or modify a column
print("4. withColumn() — add new column:")
df_with_bonus = df.withColumn("bonus", col("salary") * 0.10)  # 10% bonus
df_with_bonus.show()  # Shows original columns + bonus

# drop() — remove a column
print("5. drop() — remove column:")
df.drop("department").show()  # Removes department column

print("--- Key: Each operation returns a NEW DataFrame (immutable!) ---")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: DataFrame vs RDD Side by Side
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: DataFrame vs RDD Comparison
# ═══════════════════════════════════════════════════════

import time  # For timing comparison

sc = spark.sparkContext  # Get SparkContext for RDD

print("=== Same Task: RDD vs DataFrame ===")
print()

# Task: Average salary per department
data = [
    ("Alice", "Eng", 95000), ("Bob", "Mkt", 72000),
    ("Charlie", "Eng", 110000), ("Diana", "Sales", 68000),
    ("Eve", "Mkt", 85000), ("Frank", "Eng", 125000),
]

# --- RDD approach (verbose, no optimization) ---
print("--- RDD Approach (manual, verbose) ---")
start = time.time()
rdd = sc.parallelize(data)  # Create RDD
# Step 1: Map to (dept, (salary, 1))
pair_rdd = rdd.map(lambda x: (x[1], (x[2], 1)))  # (dept, (salary, count))
# Step 2: Reduce to sum salaries and counts per dept
agg_rdd = pair_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))  # Sum
# Step 3: Compute average
avg_rdd = agg_rdd.mapValues(lambda x: x[0] / x[1])  # Divide
rdd_result = avg_rdd.collect()  # Trigger
rdd_time = time.time() - start
print(f"  Result: {sorted(rdd_result)}")
print(f"  Time: {rdd_time:.4f}s")
print(f"  Lines of code: 5")

# --- DataFrame approach (concise, optimized) ---
print("\n--- DataFrame Approach (concise, optimized) ---")
start = time.time()
df = spark.createDataFrame(data, ["name", "dept", "salary"])  # Create DF
df_result = df.groupBy("dept").avg("salary")  # ONE line does everything!
df_result.show()  # Display
df_time = time.time() - start
print(f"  Time: {df_time:.4f}s")
print(f"  Lines of code: 2")

# --- Comparison ---
print("\nComparison:")
print(f"  RDD: 5 lines, manual optimization, {rdd_time:.4f}s")
print(f"  DataFrame: 2 lines, auto-optimized, {df_time:.4f}s")
print("\n  DataFrame wins because:")
print("  1. Catalyst optimizer chooses best execution plan")
print("  2. Tungsten engine processes data in binary format")
print("  3. Predicate pushdown, column pruning are automatic")
print("  4. Code is 60% shorter and more readable")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: DataFrame from Different Sources
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 1: Multiple Creation Methods
# ═══════════════════════════════════════════════════════

from pyspark.sql import Row  # Import Row class
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

print("=== Multiple Ways to Create DataFrames ===")
print()

# Method 1: From list of tuples (most common for testing)
print("--- Method 1: List of tuples + column names ---")
df1 = spark.createDataFrame(
    [("Alice", 30), ("Bob", 25)],  # Data
    ["name", "age"]  # Column names
)
df1.show()

# Method 2: From list of Row objects (more explicit)
print("--- Method 2: Row objects ---")
df2 = spark.createDataFrame([
    Row(name="Alice", age=30, city="NYC"),  # Named fields
    Row(name="Bob", age=25, city="LA"),
    Row(name="Charlie", age=35, city="NYC"),
])
df2.show()

# Method 3: From list of dicts (convenient, Pandas-like)
print("--- Method 3: List of dictionaries ---")
df3 = spark.createDataFrame([
    {"product": "Widget", "price": 9.99, "qty": 100},
    {"product": "Gadget", "price": 24.99, "qty": 50},
    {"product": "Tool", "price": 14.99, "qty": 75},
])
df3.show()

# Method 4: From explicit schema (production-grade, type-safe)
print("--- Method 4: Explicit StructType schema ---")
schema = StructType([
    StructField("emp_id", IntegerType(), False),     # Not nullable
    StructField("name", StringType(), True),         # Nullable
    StructField("salary", DoubleType(), True),       # Nullable
])
df4 = spark.createDataFrame(
    [(1, "Alice", 95000.0), (2, "Bob", 72000.0), (3, None, 88000.0)],
    schema=schema  # Pass the schema explicitly
)
df4.show()
df4.printSchema()  # Shows types match our schema exactly

# Method 5: From RDD (bridge from old code)
print("--- Method 5: From existing RDD ---")
rdd = sc.parallelize([("X", 1), ("Y", 2), ("Z", 3)])  # Existing RDD
df5 = rdd.toDF(["letter", "number"])  # Convert RDD to DataFrame
df5.show()

print("--- Summary: Use explicit schema (Method 4) in production! ---")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Schema Inspection and Metadata
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 2: Understanding Schema
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType

print("=== Schema Deep Dive ===")
print()

# Create a DataFrame with complex types
complex_data = [
    (1, "Alice", ["Python", "SQL"], {"city": "NYC", "state": "NY"}),
    (2, "Bob", ["Java", "Scala", "Python"], {"city": "LA", "state": "CA"}),
    (3, "Charlie", ["SQL"], {"city": "Chicago", "state": "IL"}),
]

# Define schema with complex types
complex_schema = StructType([
    StructField("id", IntegerType(), False),                          # Simple: integer
    StructField("name", StringType(), True),                         # Simple: string
    StructField("skills", ArrayType(StringType()), True),            # Array of strings
    StructField("address", MapType(StringType(), StringType()), True) # Map<string, string>
])

df = spark.createDataFrame(complex_data, complex_schema)

# Inspect schema in different ways
print("1. printSchema() — human-readable:")
df.printSchema()

print("2. schema — StructType object:")
print(f"   {df.schema}")

print("\n3. schema.json() — JSON representation:")
import json
schema_json = json.loads(df.schema.json())  # Parse to dict
print(f"   Fields: {[f['name'] for f in schema_json['fields']]}")

print("\n4. Individual field access:")
for field in df.schema.fields:  # Iterate over fields
    print(f"   {field.name}: type={field.dataType.simpleString()}, nullable={field.nullable}")

print("\n5. describe() — statistical summary:")
df_nums = spark.createDataFrame([
    (1, 30, 95000.0), (2, 25, 72000.0), (3, 35, 110000.0),
    (4, 28, 68000.0), (5, 32, 85000.0)
], ["id", "age", "salary"])
df_nums.describe().show()  # count, mean, stddev, min, max

print("--- Key: Schema defines the 'contract' of your data ---")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Lazy Evaluation and Explain
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 3: Lazy Evaluation & Explain
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, upper, when
import time

print("=== Lazy Evaluation: Nothing Runs Until You Ask ===")
print()

# Create a DataFrame
df = spark.createDataFrame([
    ("Alice", 30, 95000), ("Bob", 25, 72000),
    ("Charlie", 35, 110000), ("Diana", 28, 68000),
], ["name", "age", "salary"])

# These are ALL LAZY — nothing executes!
print("Building transformations (lazy — no execution):")
start = time.time()
step1 = df.filter(col("age") > 25)                    # Lazy
step2 = step1.withColumn("name_upper", upper(col("name")))  # Lazy
step3 = step2.withColumn("tax_bracket",                # Lazy
    when(col("salary") > 100000, "High")
    .when(col("salary") > 75000, "Medium")
    .otherwise("Low")
)
step4 = step3.select("name_upper", "age", "salary", "tax_bracket")  # Lazy
lazy_time = time.time() - start
print(f"  Time to build 4 transformations: {lazy_time:.6f}s (instant!)")
print("  Nothing computed yet — just building the plan.")

# explain() — shows what Spark WILL do (without executing)
print("\nexplain() — the execution plan:")
step4.explain()  # Shows physical plan

# explain(True) — shows all plan levels
print("\nexplain(True) — full plan (parsed → analyzed → optimized → physical):")
step4.explain(True)  # Logical + Physical plans

# NOW trigger execution with an action
print("\nTriggering execution with show():")
start = time.time()
step4.show()  # ACTION! Now everything executes
exec_time = time.time() - start
print(f"  Execution time: {exec_time:.3f}s")

print("\n--- Transformations (lazy): select, filter, withColumn, join, groupBy ---")
print("--- Actions (trigger): show, count, collect, write, toPandas ---")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Catalyst Optimizer in Action
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 1: Catalyst Optimizer
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, lit

print("=== Catalyst Optimizer: Automatic Query Optimization ===")
print()

# Create two DataFrames for join
employees = spark.createDataFrame([
    (1, "Alice", 1), (2, "Bob", 2), (3, "Charlie", 1),
    (4, "Diana", 3), (5, "Eve", 2), (6, "Frank", 1),
], ["emp_id", "name", "dept_id"])

departments = spark.createDataFrame([
    (1, "Engineering", "Building A"),
    (2, "Marketing", "Building B"),
    (3, "Sales", "Building C"),
], ["dept_id", "dept_name", "location"])

# Write a SUBOPTIMAL query (filter AFTER join)
print("--- Suboptimal query (filter after join) ---")
result_bad = (
    employees
    .join(departments, "dept_id")           # Join first (all rows)
    .filter(col("dept_name") == "Engineering")  # Then filter
    .select("name", "dept_name", "location")
)
print("Your code says: join ALL, then filter")
print("Catalyst optimizes to: filter first, then join fewer rows!")
result_bad.explain()  # Shows predicate pushdown!

# Write an OPTIMAL query (filter BEFORE join)
print("\n--- Optimal query (filter before join) ---")
result_good = (
    employees
    .join(
        departments.filter(col("dept_name") == "Engineering"),  # Filter first
        "dept_id"
    )
    .select("name", "dept_name", "location")
)
result_good.explain()  # Same physical plan!

print("\n--- Key Insight ---")
print("Both queries produce the SAME physical plan!")
print("Catalyst automatically pushes filters down.")
print("\nOptimizations Catalyst does automatically:")
print("  1. Predicate Pushdown: moves filters before joins/scans")
print("  2. Column Pruning: only reads needed columns")
print("  3. Constant Folding: pre-computes constant expressions")
print("  4. Join Reordering: picks optimal join order")
print("  5. Broadcast Join: auto-broadcasts small tables")

result_bad.show()  # Both give same result

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: DataFrame vs RDD Performance Benchmark
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 2: Performance Benchmark
# ═══════════════════════════════════════════════════════

import time
from pyspark.sql.functions import col, sum as spark_sum, avg as spark_avg

print("=== Performance: DataFrame vs RDD at Scale ===")
print()

# Generate a larger dataset for meaningful benchmark
num_rows = 1000000  # 1 million rows

# Create as DataFrame
df_large = spark.range(num_rows).withColumn(  # range() is fast
    "dept", (col("id") % 100).cast("string")  # 100 departments
).withColumn(
    "salary", (col("id") % 50000 + 50000).cast("double")  # Salary 50K-100K
)
df_large.cache()  # Cache for fair comparison
df_large.count()  # Trigger cache

# Convert to RDD for comparison
rdd_large = df_large.rdd  # Get underlying RDD
rdd_large.cache()  # Cache RDD too
rdd_large.count()  # Trigger cache

print(f"Dataset: {num_rows:,} rows, 100 departments")
print()

# Task: Average salary per department
# --- DataFrame approach ---
print("--- DataFrame: groupBy + avg ---")
start = time.time()
df_result = df_large.groupBy("dept").agg(spark_avg("salary")).count()
df_time = time.time() - start
print(f"  Time: {df_time:.3f}s")

# --- RDD approach ---
print("\n--- RDD: map + reduceByKey ---")
start = time.time()
rdd_result = (
    rdd_large
    .map(lambda r: (r.dept, (r.salary, 1)))  # (dept, (salary, 1))
    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))  # Sum
    .mapValues(lambda x: x[0]/x[1])  # Average
    .count()
)
rdd_time = time.time() - start
print(f"  Time: {rdd_time:.3f}s")

# Comparison
print(f"\n{'=' * 40}")
speedup = rdd_time / df_time if df_time > 0 else float('inf')
print(f"  DataFrame: {df_time:.3f}s")
print(f"  RDD:       {rdd_time:.3f}s")
print(f"  DataFrame is {speedup:.1f}x faster!")
print(f"\nWhy:")
print(f"  • DataFrame uses Tungsten (binary processing)")
print(f"  • DataFrame avoids Python serialization")
print(f"  • Catalyst optimizes the execution plan")
print(f"  • Whole-stage code generation (compiled to JVM bytecode)")

df_large.unpersist()  # Cleanup
rdd_large.unpersist()  # Cleanup

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Converting Between DataFrame and RDD
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 3: DF ↔ RDD Conversions
# ═══════════════════════════════════════════════════════

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

print("=== Converting Between DataFrame and RDD ===")
print()

# Start with a DataFrame
df = spark.createDataFrame([
    ("Alice", 30, "Engineering"),
    ("Bob", 25, "Marketing"),
    ("Charlie", 35, "Sales"),
], ["name", "age", "department"])

# --- DataFrame to RDD ---
print("--- DataFrame → RDD ---")
rdd_from_df = df.rdd  # Each element is a Row object
print(f"1. df.rdd type: {type(rdd_from_df)}")
print(f"   First element: {rdd_from_df.first()}")  # Row(name='Alice', age=30, ...)
print(f"   Element type: {type(rdd_from_df.first())}")  # <class 'pyspark.sql.types.Row'>

# Access Row fields
first_row = rdd_from_df.first()  # Get first Row
print(f"\n2. Accessing Row fields:")
print(f"   By name: {first_row.name}")      # Alice
print(f"   By index: {first_row[0]}")        # Alice
print(f"   As dict: {first_row.asDict()}")   # {'name': 'Alice', 'age': 30, ...}

# --- RDD to DataFrame (multiple methods) ---
print("\n--- RDD → DataFrame ---")

# Method 1: toDF() with column names
rdd = sc.parallelize([("X", 1), ("Y", 2), ("Z", 3)])
df1 = rdd.toDF(["letter", "number"])  # Specify columns
print("Method 1: rdd.toDF(columns)")
df1.show()

# Method 2: RDD of Row objects
rdd_rows = sc.parallelize([
    Row(product="Widget", price=9.99),
    Row(product="Gadget", price=24.99),
])
df2 = spark.createDataFrame(rdd_rows)  # Infers schema from Row
print("Method 2: RDD of Row objects")
df2.show()

# Method 3: RDD + explicit schema (most robust)
rdd_tuples = sc.parallelize([(1, "active"), (2, "inactive"), (3, "active")])
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("status", StringType(), True),
])
df3 = spark.createDataFrame(rdd_tuples, schema)  # Explicit types
print("Method 3: RDD + StructType schema")
df3.show()
df3.printSchema()

print("--- Key: Prefer DataFrame. Only convert to RDD for custom logic. ---")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Using RDD When DataFrame Would Be Better
# MAGIC **Issue:** Writing complex map/reduce chains for simple aggregations.  
# MAGIC **Fix:** Use DataFrame API (groupBy, agg) — it's faster AND simpler.
# MAGIC
# MAGIC ### Mistake #2: Forgetting DataFrames Are Immutable
# MAGIC **Issue:** `df.filter(...)` doesn't modify `df` — it returns a NEW DataFrame.  
# MAGIC **Fix:** Always assign the result: `df_filtered = df.filter(...)`
# MAGIC
# MAGIC ### Mistake #3: Using collect() on Large DataFrames
# MAGIC **Issue:** `df.collect()` brings ALL data to the driver — causes OOM on large data.  
# MAGIC **Fix:** Use `show(n)`, `take(n)`, or `limit(n).toPandas()` for previews.
# MAGIC
# MAGIC ### Mistake #4: Not Checking Schema Early
# MAGIC **Issue:** Column type mismatches cause runtime errors deep in the pipeline.  
# MAGIC **Fix:** Always `printSchema()` after loading data. Use explicit schemas in production.
# MAGIC
# MAGIC ### Mistake #5: Treating DataFrames Like Pandas
# MAGIC **Issue:** Expecting row-by-row iteration or in-place modification (both are anti-patterns in Spark).  
# MAGIC **Fix:** Think in terms of column operations and transformations, not loops.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC ### Level 1: Create a DataFrame with 5 rows and 3 columns. Call show() and printSchema().
# MAGIC ### Level 2: Use select() to pick 2 columns. Use filter() to keep rows where age > 30.
# MAGIC ### Level 3: Create a DataFrame using Row objects AND using explicit StructType schema.
# MAGIC ### Level 4: Chain 3 operations (select, filter, orderBy) and explain the lazy behavior.
# MAGIC ### Level 5: Convert a DataFrame to RDD, manipulate it, convert back to DataFrame.
# MAGIC ### Level 6: Compare timing of the SAME aggregation using RDD vs DataFrame on 1M rows.
# MAGIC ### Level 7: Use explain(True) to see how Catalyst optimizes a filter-then-join query.
# MAGIC ### Level 8: Create a DataFrame with complex types (Array, Map) and access nested fields.
# MAGIC ### Level 9: Write a function that creates a DataFrame from any Python dictionary.
# MAGIC ### Level 10: Explain to a colleague why DataFrames are faster than RDDs (use explain, timing, and analogies).

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql.functions import col, explode, map_keys
import time

# Level 1: Basic DataFrame
print("=== Level 1 ===")
df1 = spark.createDataFrame([
    ("Tokyo", "Japan", 14000000),
    ("London", "UK", 9000000),
    ("Paris", "France", 2200000),
    ("NYC", "USA", 8300000),
    ("Mumbai", "India", 20700000),
], ["city", "country", "population"])
df1.show()  # Display table
df1.printSchema()  # Show structure

# Level 2: select + filter
print("\n=== Level 2 ===")
df1.select("city", "population").filter(col("population") > 9000000).show()

# Level 3: Row objects + StructType
print("\n=== Level 3 ===")
# Method A: Row objects
df_row = spark.createDataFrame([
    Row(name="Alice", score=95),
    Row(name="Bob", score=87),
])
df_row.show()
# Method B: StructType
schema = StructType([StructField("name", StringType()), StructField("score", IntegerType())])
df_schema = spark.createDataFrame([("Charlie", 72)], schema)
df_schema.show()

# Level 5: DataFrame ↔ RDD conversion
print("\n=== Level 5 ===")
df5 = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "val"])
rdd5 = df5.rdd.map(lambda r: (r.id * 10, r.val.upper()))  # Transform in RDD
df5_back = rdd5.toDF(["id_x10", "val_upper"])  # Back to DataFrame
df5_back.show()

# Level 6: Performance comparison
print("\n=== Level 6 ===")
df_big = spark.range(1000000).withColumn("dept", (col("id") % 50).cast("string"))
df_big.cache(); df_big.count()  # Warm up
start = time.time()
df_big.groupBy("dept").count().count()  # DataFrame aggregation
df_time = time.time() - start
rdd_big = df_big.rdd; rdd_big.cache(); rdd_big.count()  # Warm up
start = time.time()
rdd_big.map(lambda r: (r.dept, 1)).reduceByKey(lambda a,b: a+b).count()  # RDD
rdd_time = time.time() - start
print(f"DataFrame: {df_time:.3f}s | RDD: {rdd_time:.3f}s | Speedup: {rdd_time/df_time:.1f}x")
df_big.unpersist(); rdd_big.unpersist()

# Level 8: Complex types
print("\n=== Level 8 ===")
df8 = spark.createDataFrame([
    (1, ["Python", "SQL"], {"city": "NYC"}),
    (2, ["Java"], {"city": "LA", "zip": "90001"}),
], ["id", "skills", "address"])
df8.show(truncate=False)  # Show full content
df8.select("id", explode("skills").alias("skill")).show()  # Explode array

print("\n\u2705 All homework complete!")