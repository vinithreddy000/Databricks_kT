# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 03: SparkContext vs SparkSession
# MAGIC # Module: PySpark Foundation & SparkSession
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 35 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: Old Phone vs Smartphone
# MAGIC
# MAGIC Imagine the evolution of phones:
# MAGIC - **Old days (Spark 1.x):** You had a **separate device** for each task:
# MAGIC   - A landline phone (SparkContext → for basic calls/RDDs)
# MAGIC   - A calculator (SQLContext → for math/SQL)
# MAGIC   - A camera (HiveContext → for photos/Hive)
# MAGIC   - A radio (StreamingContext → for music/streaming)
# MAGIC
# MAGIC - **Today (Spark 2.0+):** You have a **smartphone** (SparkSession) that does EVERYTHING in one device.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### SparkContext: The Original Engine (Spark 1.x)
# MAGIC
# MAGIC | Feature | SparkContext |
# MAGIC |---------|-------------|
# MAGIC | Introduced | Spark 1.0 (2014) |
# MAGIC | Main purpose | Connect to the cluster, create RDDs |
# MAGIC | Variable name | `sc` |
# MAGIC | Created by | `SparkConf` + `SparkContext(conf)` |
# MAGIC | Limitation | Can only work with RDDs (no SQL, no DataFrames) |
# MAGIC
# MAGIC ### SparkSession: The Modern Way (Spark 2.0+)
# MAGIC
# MAGIC | Feature | SparkSession |
# MAGIC |---------|-------------|
# MAGIC | Introduced | Spark 2.0 (2016) |
# MAGIC | Main purpose | Unified entry point for ALL Spark operations |
# MAGIC | Variable name | `spark` |
# MAGIC | Created by | `SparkSession.builder.getOrCreate()` |
# MAGIC | Contains | SparkContext + SQLContext + HiveContext + more |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When You Still Use SparkContext Directly
# MAGIC
# MAGIC Even though SparkSession is the modern way, you still need SparkContext for:
# MAGIC 1. **Creating RDDs** — `sc.parallelize()`, `sc.textFile()`
# MAGIC 2. **Broadcast variables** — `sc.broadcast(large_lookup)`
# MAGIC 3. **Accumulators** — `sc.accumulator(0)`
# MAGIC 4. **Checkpointing** — `sc.setCheckpointDir(path)`
# MAGIC 5. **Adding files/JARs** — `sc.addFile()`, `sc.addPyFile()`
# MAGIC
# MAGIC For everything else, use SparkSession!

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### The Evolution Timeline
# MAGIC
# MAGIC ```
# MAGIC Spark 1.0 (2014)                    Spark 2.0 (2016)
# MAGIC ────────────────────────────────────────────────────
# MAGIC
# MAGIC OLD WAY (Multiple Objects):         NEW WAY (One Object):
# MAGIC ┌───────────────────┐                ┌───────────────────┐
# MAGIC │  SparkContext   │  ────────────▶ │                   │
# MAGIC │  (sc)           │                │   SparkSession   │
# MAGIC └───────────────────┘                │   (spark)         │
# MAGIC ┌───────────────────┐                │                   │
# MAGIC │  SQLContext     │  ────────────▶ │  Contains:        │
# MAGIC │  (sqlContext)   │                │  - SparkContext  │
# MAGIC └───────────────────┘                │  - SQL Engine    │
# MAGIC ┌───────────────────┐                │  - Hive Support  │
# MAGIC │  HiveContext    │  ────────────▶ │  - Catalog       │
# MAGIC │  (hiveContext)  │                │  - Config        │
# MAGIC └───────────────────┘                │  - Streaming     │
# MAGIC ┌───────────────────┐                │  - UDF Registry  │
# MAGIC │StreamingContext │  ────────────▶ │                   │
# MAGIC │  (ssc)          │                └───────────────────┘
# MAGIC └───────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Relationship Between Them
# MAGIC
# MAGIC ```
# MAGIC SparkSession
# MAGIC     │
# MAGIC     ├─── .sparkContext (sc)     → RDDs, broadcasts, accumulators
# MAGIC     ├─── .conf                  → Read/set configuration
# MAGIC     ├─── .catalog               → Tables, databases, metadata
# MAGIC     ├─── .read / .readStream     → Read data (batch/streaming)
# MAGIC     ├─── .sql("...")            → Run SQL queries
# MAGIC     ├─── .createDataFrame(...)  → Create DataFrames
# MAGIC     └─── .udf                   → Register custom functions
# MAGIC ```
# MAGIC
# MAGIC ### In Databricks Notebooks
# MAGIC
# MAGIC - `spark` → The SparkSession (pre-created, always available)
# MAGIC - `sc` → The SparkContext (also pre-created as `spark.sparkContext`)
# MAGIC - Both are ready to use without any setup!

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Accessing Both Objects
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 1: Accessing SparkContext and SparkSession
# ═══════════════════════════════════════════════════════

# Both 'spark' and 'sc' are pre-created in Databricks
print("=== SparkSession (spark) ===")
print(f"Type: {type(spark)}")  # pyspark.sql.session.SparkSession
print(f"Version: {spark.version}")  # Spark version

print("\n=== SparkContext (sc) ===")
# Method 1: Use the pre-existing 'sc' shortcut
sc = spark.sparkContext  # Access SparkContext from SparkSession
print(f"Type: {type(sc)}")  # pyspark.context.SparkContext
print(f"App Name: {sc.appName}")  # Application name
print(f"Master: {sc.master}")  # Cluster manager URL
print(f"Parallelism: {sc.defaultParallelism}")  # Number of parallel tasks

# Prove they're connected
print("\n=== Are They Connected? ===")
print(f"spark.sparkContext is sc: {spark.sparkContext is sc}")  # Should be True
print(f"SparkContext inside SparkSession: {spark.sparkContext}")  # Shows the same object

# The key relationship
print("\n--- Key Relationship ---")
print("SparkSession CONTAINS SparkContext")
print("You access it via: spark.sparkContext or just 'sc' in Databricks")

# Expected Output:
# === SparkSession (spark) ===
# Type: <class 'pyspark.sql.session.SparkSession'>
# Version: 3.5.0
#
# === SparkContext (sc) ===
# Type: <class 'pyspark.context.SparkContext'>
# App Name: Databricks Shell
# Master: local[*]
# Parallelism: 8
#
# === Are They Connected? ===
# spark.sparkContext is sc: True

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: What SparkContext Can Do
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 2: Things ONLY SparkContext Can Do
# ═══════════════════════════════════════════════════════

print("=== SparkContext-Only Operations ===")
print()

# 1. Create an RDD from a Python list
print("1. Create RDD with sc.parallelize():")
fruits_rdd = sc.parallelize(["apple", "banana", "cherry", "date", "elderberry"])  # Create RDD
print(f"   RDD created with {fruits_rdd.count()} elements")  # Count elements
print(f"   First fruit: {fruits_rdd.first()}")  # Get first element

# 2. Create a broadcast variable
print("\n2. Create Broadcast Variable with sc.broadcast():")
country_codes = {"US": "United States", "UK": "United Kingdom", "DE": "Germany"}  # Lookup dict
broadcast_codes = sc.broadcast(country_codes)  # Broadcast to all workers
print(f"   Broadcast value: {broadcast_codes.value}")  # Access the value
print(f"   Lookup 'UK': {broadcast_codes.value['UK']}")  # Use the lookup

# 3. Create an accumulator
print("\n3. Create Accumulator with sc.accumulator():")
counter = sc.accumulator(0)  # Initialize accumulator at 0
# Use the accumulator in an RDD operation
numbers_rdd = sc.parallelize(range(1, 101))  # Numbers 1-100
numbers_rdd.foreach(lambda x: counter.add(1))  # Count each element
print(f"   Accumulator value (should be 100): {counter.value}")  # Read the final count

# 4. Check Spark version and Python version
print("\n4. Other SparkContext info:")
print(f"   Spark version: {sc.version}")  # Spark version
print(f"   Python version: {sc.pythonVer}")  # Python version on the cluster
print(f"   Spark user: {sc.sparkUser()}")  # Who is running this

# Cleanup
broadcast_codes.unpersist()  # Release broadcast memory

# Expected Output:
# 1. Create RDD: RDD created with 5 elements, First fruit: apple
# 2. Broadcast: Shows the dictionary, Lookup 'UK': United Kingdom
# 3. Accumulator value: 100
# 4. Spark/Python versions

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: What SparkSession Can Do
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 3: Things ONLY SparkSession Can Do
# ═══════════════════════════════════════════════════════

print("=== SparkSession-Only Operations ===")
print()

# 1. Create a DataFrame directly
print("1. Create DataFrame with spark.createDataFrame():")
cars_df = spark.createDataFrame([  # Create DataFrame directly (no RDD needed!)
    ("Toyota", "Camry", 2024, 28000),
    ("Honda", "Civic", 2023, 25000),
    ("Tesla", "Model 3", 2024, 42000)
], ["make", "model", "year", "price"])  # Column names
display(cars_df)  # Show in Databricks table format

# 2. Run SQL queries
print("\n2. Run SQL with spark.sql():")
cars_df.createOrReplaceTempView("cars")  # Register as temp view
expensive = spark.sql("SELECT make, model, price FROM cars WHERE price > 26000")  # SQL query
display(expensive)  # Show filtered results

# 3. Access the catalog
print("\n3. Explore catalog with spark.catalog:")
print(f"   Tables: {[t.name for t in spark.catalog.listTables()]}")  # List all tables
print(f"   'cars' exists: {spark.catalog.tableExists('cars')}")  # Check if table exists

# 4. Generate a sequence with spark.range()
print("\n4. Generate data with spark.range():")
ids_df = spark.range(1, 6)  # Generate numbers 1-5
print(f"   Generated {ids_df.count()} rows")  # Count rows
display(ids_df)  # Show the generated numbers

# 5. Read/set configuration
print("\n5. Configuration with spark.conf:")
print(f"   Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")  # Read config

print("\n--- Summary ---")
print("SparkSession = DataFrames + SQL + Catalog + Config + Streaming")
print("SparkContext = RDDs + Broadcasts + Accumulators (low-level)")

# Expected Output:
# Shows cars table, filtered SQL results, catalog info, and generated data

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Decision Guide
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# Example 1: Decision Guide — When to Use Which
# ═══════════════════════════════════════════════════════

# Let's show the SAME task done both ways to understand when each is better

print("=== Same Task, Two Ways ===")
print("Task: Count words in a list of sentences")
print()

sentences = [  # Sample data
    "Spark is fast and powerful",
    "Spark processes big data efficiently",
    "Data engineering with Spark is fun"
]

# --- Way 1: Using SparkContext (RDD approach) ---
print("--- Way 1: SparkContext + RDDs ---")
sentences_rdd = sc.parallelize(sentences)  # Create RDD from list
word_counts_rdd = (  # Chain transformations
    sentences_rdd
    .flatMap(lambda line: line.lower().split(" "))  # Split into words
    .map(lambda word: (word, 1))  # Map each word to (word, 1)
    .reduceByKey(lambda a, b: a + b)  # Sum counts per word
    .sortBy(lambda x: -x[1])  # Sort by count descending
)
print("Top 5 words (RDD approach):")
for word, count in word_counts_rdd.take(5):  # Get top 5
    print(f"  '{word}': {count}")  # Print each word and count

# --- Way 2: Using SparkSession (DataFrame approach) ---
print("\n--- Way 2: SparkSession + DataFrames ---")
from pyspark.sql.functions import explode, split, lower, col, count as spark_count  # Import functions

sentences_df = spark.createDataFrame(  # Create DataFrame
    [(s,) for s in sentences], ["sentence"]  # Wrap each sentence in a tuple
)
word_counts_df = (  # Chain DataFrame operations
    sentences_df
    .select(explode(split(lower(col("sentence")), " ")).alias("word"))  # Split and explode
    .groupBy("word")  # Group by word
    .agg(spark_count("*").alias("count"))  # Count occurrences
    .orderBy(col("count").desc())  # Sort descending
)
print("Top 5 words (DataFrame approach):")
display(word_counts_df.limit(5))  # Show top 5

# --- Verdict ---
print("\n--- When to Use Which ---")
print("Use SparkSession (DataFrames) when:")
print("  - Working with structured/tabular data (99% of the time)")
print("  - You want SQL support")
print("  - You want the Catalyst optimizer to make your code faster")
print("\nUse SparkContext (RDDs) when:")
print("  - Working with unstructured text/binary data")
print("  - You need broadcast variables or accumulators")
print("  - You're doing low-level custom partitioning")
print("  - You're maintaining legacy code")

# Expected Output:
# Top 5 words shown in both formats (should match)
# 'spark': 3, 'is': 3, 'data': 2, etc.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: SparkConf
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# Example 2: SparkConf — Setting Configs Programmatically
# ═══════════════════════════════════════════════════════

from pyspark import SparkConf  # Import SparkConf class

# SparkConf is how you SET configurations before creating a SparkContext
# In Databricks, the session is already created, but let's understand how it works

print("=== Understanding SparkConf ===")
print()

# In a standalone script (NOT Databricks), you'd do this:
# conf = SparkConf()
# conf.setAppName("My Data Pipeline")
# conf.setMaster("local[4]")
# conf.set("spark.executor.memory", "4g")
# conf.set("spark.sql.shuffle.partitions", "50")
# sc = SparkContext(conf=conf)

# In Databricks, we can READ the current conf from SparkContext:
current_conf = sc.getConf()  # Get the SparkConf from the running context
print("Current SparkConf settings (first 10):")
all_settings = current_conf.getAll()  # Get all key-value pairs
for key, value in sorted(all_settings)[:10]:  # Show first 10 sorted
    print(f"  {key} = {value}")  # Print each setting

print(f"\nTotal configurations: {len(all_settings)}")  # How many configs are set

# The 3 levels of configuration (priority order, highest first):
print("\n=== Configuration Priority (Highest to Lowest) ===")
print("1. spark.conf.set() in code       → Overrides everything (session-level)")
print("2. Cluster Spark Config settings   → Persists across notebooks")
print("3. Spark defaults                  → Built-in defaults")

# Demonstrate reading vs setting
print("\n=== Demo: Read, Set, Verify ===")
original = spark.conf.get("spark.sql.shuffle.partitions")  # Read current value
print(f"Original: {original}")
spark.conf.set("spark.sql.shuffle.partitions", "75")  # Change it
print(f"Changed to: {spark.conf.get('spark.sql.shuffle.partitions')}")  # Verify
spark.conf.set("spark.sql.shuffle.partitions", original)  # Reset back
print(f"Reset to: {spark.conf.get('spark.sql.shuffle.partitions')}")  # Confirm reset

# Expected Output:
# Lists current configuration settings
# Shows the read-set-verify-reset pattern

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Converting Between RDD and DF
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# Example 3: Converting Between RDDs and DataFrames
# ═══════════════════════════════════════════════════════

from pyspark.sql import Row  # Import Row for creating structured data from RDDs
from pyspark.sql.types import StructType, StructField, StringType, IntegerType  # Schema types

print("=== Converting Between RDDs and DataFrames ===")
print()

# --- Direction 1: RDD → DataFrame ---
print("--- RDD → DataFrame ---")

# Method A: Using .toDF() with column names
rdd_tuples = sc.parallelize([("Alice", 30), ("Bob", 25), ("Charlie", 35)])  # RDD of tuples
df_from_rdd_a = rdd_tuples.toDF(["name", "age"])  # Convert with column names
print("Method A: rdd.toDF([columns])")
display(df_from_rdd_a)  # Show result

# Method B: Using Row objects
rdd_rows = sc.parallelize([  # RDD of Row objects
    Row(city="London", population=9000000),  # Row with named fields
    Row(city="Tokyo", population=14000000),
    Row(city="Paris", population=2200000)
])
df_from_rdd_b = spark.createDataFrame(rdd_rows)  # Spark infers schema from Row
print("\nMethod B: spark.createDataFrame(rdd_of_rows)")
display(df_from_rdd_b)  # Show result

# Method C: With explicit schema
rdd_data = sc.parallelize([("Widget", 9.99), ("Gadget", 19.99)])  # RDD of tuples
schema = StructType([  # Define exact schema
    StructField("product", StringType(), True),  # String column
    StructField("price", IntegerType(), True)  # Note: price will be cast to int
])
df_from_rdd_c = spark.createDataFrame(rdd_data, schema)  # Create with explicit schema
print("\nMethod C: spark.createDataFrame(rdd, schema)")
df_from_rdd_c.printSchema()  # Show the enforced schema

# --- Direction 2: DataFrame → RDD ---
print("\n--- DataFrame → RDD ---")
people_df = spark.createDataFrame([("Eve", 28), ("Frank", 33)], ["name", "age"])  # Create DF
people_rdd = people_df.rdd  # Access the underlying RDD (each element is a Row)
print(f"Type: {type(people_rdd)}")  # Should be RDD
print(f"First element: {people_rdd.first()}")  # Shows a Row object
print(f"First name: {people_rdd.first()['name']}")  # Access by column name
print(f"All data: {people_rdd.collect()}")  # Collect all rows

print("\n--- Key Insight ---")
print("RDD → DataFrame: Use .toDF() or spark.createDataFrame()")
print("DataFrame → RDD: Use .rdd property (each row becomes a Row object)")

# Expected Output:
# Shows DataFrames created from RDDs in 3 different ways
# Shows RDD created from DataFrame with Row objects

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Performance Comparison
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Examples
# Example 1: Why DataFrames are Faster than RDDs
# ═══════════════════════════════════════════════════════

import time  # For timing operations
from pyspark.sql.functions import col, sum as spark_sum  # Import DF functions

print("=== Performance: DataFrame vs RDD ===")
print("Task: Sum of all even numbers from 1 to 2 million")
print()

# Generate data
data_size = 2000000  # 2 million numbers

# --- RDD Approach (no Catalyst optimizer) ---
start = time.time()  # Start timer
rdd = sc.parallelize(range(1, data_size + 1), 8)  # Create RDD with 8 partitions
rdd_result = rdd.filter(lambda x: x % 2 == 0).sum()  # Filter even, then sum
rdd_time = time.time() - start  # Measure time
print(f"RDD Result: {int(rdd_result):,}")  # Print with commas
print(f"RDD Time: {rdd_time:.3f} seconds")

# --- DataFrame Approach (WITH Catalyst optimizer) ---
start = time.time()  # Start timer
df = spark.range(1, data_size + 1)  # Create DataFrame
df_result = df.filter(col("id") % 2 == 0).agg(spark_sum("id")).collect()[0][0]  # Filter + sum
df_time = time.time() - start  # Measure time
print(f"\nDataFrame Result: {int(df_result):,}")  # Print with commas
print(f"DataFrame Time: {df_time:.3f} seconds")

# --- Why is DataFrame faster? ---
print(f"\nSpeedup: {rdd_time/df_time:.1f}x faster with DataFrames")
print("\n--- WHY DataFrames Are Faster ---")
print("1. Catalyst Optimizer: Spark rewrites your query for efficiency")
print("2. Tungsten Engine: Uses memory more efficiently (off-heap, binary format)")
print("3. Whole-stage Codegen: Compiles your query to optimized Java bytecode")
print("4. Predicate Pushdown: Filters data as early as possible")
print("5. No Python serialization: RDD lambdas require Python↔Java data transfer")

# Expected Output:
# RDD Result: 1,000,001,000,000
# DataFrame Result: 1,000,001,000,000 (same!)
# DataFrame is typically 2-10x faster for this type of operation

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Using RDDs When DataFrames Would Be Better
# MAGIC
# MAGIC **What happens:** You write complex lambda functions with RDDs for tabular data.  
# MAGIC **Why it's bad:** You lose the Catalyst optimizer, making your code 2-10x slower.  
# MAGIC **The fix:** Use DataFrames for ANY structured/tabular data. Only use RDDs for truly unstructured data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #2: Creating a New SparkContext
# MAGIC
# MAGIC **What happens:** You try `SparkContext()` and get an error: "Only one SparkContext may be running".  
# MAGIC **Why it fails:** There can only be ONE SparkContext per JVM. It's already created in Databricks.  
# MAGIC **The fix:** Use the existing `sc` (which is `spark.sparkContext`).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #3: Confusing `sc` and `spark`
# MAGIC
# MAGIC **What happens:** You try `sc.createDataFrame()` and get an error.  
# MAGIC **Why it fails:** SparkContext (`sc`) doesn't have `createDataFrame()`. That's on SparkSession (`spark`).  
# MAGIC **The fix:** Remember: `spark` for DataFrames/SQL, `sc` for RDDs/broadcasts/accumulators.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #4: Not Knowing the `sc` Shortcut Exists
# MAGIC
# MAGIC **What happens:** You always write `spark.sparkContext.parallelize(...)` which is verbose.  
# MAGIC **The fix:** In Databricks, `sc` is already defined as `spark.sparkContext`. Just use `sc.parallelize(...)`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #5: Using Old API Patterns from Spark 1.x Tutorials
# MAGIC
# MAGIC **What happens:** You follow an old tutorial that uses `SQLContext` or `HiveContext`.  
# MAGIC **Why it's wrong:** These are deprecated. SparkSession replaces all of them.  
# MAGIC **The fix:** If you see `SQLContext` or `HiveContext` in any tutorial, mentally replace with `spark` (SparkSession).

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC ### Level 1 (Just Read and Run)
# MAGIC Run the Beginner Example 1 cell. Note the types of `spark` and `sc`.
# MAGIC
# MAGIC ### Level 2 (Tiny Change)
# MAGIC Access `sc.pythonVer` and `sc.version`. Print both.
# MAGIC
# MAGIC ### Level 3 (Combine Two Things)
# MAGIC Create an RDD of 5 numbers with `sc.parallelize()`, then convert it to a DataFrame with `.toDF()`. Display both.
# MAGIC
# MAGIC ### Level 4 (New Scenario)
# MAGIC Create a broadcast variable containing a dictionary of department codes. Then create an RDD and use the broadcast to look up full department names.
# MAGIC
# MAGIC ### Level 5 (Intermediate Project)
# MAGIC Do the SAME word-count task using BOTH the RDD approach (sc) and the DataFrame approach (spark). Compare results.
# MAGIC
# MAGIC ### Level 6 (Design First)
# MAGIC Design a function that takes any RDD of tuples and converts it to a DataFrame with proper column names. Handle edge cases.
# MAGIC
# MAGIC ### Level 7 (Optimize It)
# MAGIC Take a piece of code that uses RDDs for filtering and aggregating numbers. Rewrite it using DataFrames and compare performance.
# MAGIC
# MAGIC ### Level 8 (Edge Cases)
# MAGIC What happens when you try to access sc.parallelize after calling spark.stop()? (Describe conceptually — don't actually stop!)
# MAGIC
# MAGIC ### Level 9 (Production-Grade)
# MAGIC Write a helper function `get_spark_info()` that returns a dictionary with version, app_name, master, parallelism, python_version.
# MAGIC
# MAGIC ### Level 10 (Teach It)
# MAGIC Explain to a colleague the difference between SparkContext and SparkSession. When would you use each? Give 2 examples for each.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS — All 10 Levels
# ═══════════════════════════════════════════════════════

from pyspark.sql import Row  # Import Row
from pyspark.sql.functions import col, explode, split, lower, count as spark_count  # Functions

# ---- LEVEL 1 ----
print("=== Level 1: Types ===")
print(f"spark type: {type(spark)}")  # SparkSession
print(f"sc type: {type(sc)}")  # SparkContext

# ---- LEVEL 2 ----
print("\n=== Level 2: Versions ===")
print(f"Python version: {sc.pythonVer}")  # Python version
print(f"Spark version: {sc.version}")  # Spark version

# ---- LEVEL 3 ----
print("\n=== Level 3: RDD to DataFrame ===")
nums_rdd = sc.parallelize([10, 20, 30, 40, 50])  # Create RDD
nums_df = nums_rdd.map(lambda x: (x,)).toDF(["number"])  # Convert to DataFrame
display(nums_df)  # Display the DataFrame

# ---- LEVEL 4 ----
print("\n=== Level 4: Broadcast Lookup ===")
dept_lookup = {"ENG": "Engineering", "MKT": "Marketing", "SAL": "Sales"}  # Lookup dictionary
bc_dept = sc.broadcast(dept_lookup)  # Broadcast it
employee_rdd = sc.parallelize([("Alice", "ENG"), ("Bob", "MKT"), ("Charlie", "SAL")])  # Data
resolved_rdd = employee_rdd.map(lambda x: (x[0], bc_dept.value.get(x[1], "Unknown")))  # Lookup
print(f"Resolved: {resolved_rdd.collect()}")  # Show results
bc_dept.unpersist()  # Cleanup

# ---- LEVEL 5 ----
print("\n=== Level 5: Word Count Both Ways ===")
text = ["hello world hello", "world hello spark", "spark spark hello"]
# RDD way
rdd_wc = sc.parallelize(text).flatMap(lambda l: l.split(" ")).map(lambda w: (w,1)).reduceByKey(lambda a,b: a+b)
print(f"RDD result: {sorted(rdd_wc.collect())}")
# DF way
df_wc = spark.createDataFrame([(t,) for t in text], ["text"]).select(explode(split(col("text"), " ")).alias("word")).groupBy("word").agg(spark_count("*").alias("cnt"))
print("DF result:")
display(df_wc.orderBy("word"))

# ---- LEVEL 6 ----
print("\n=== Level 6: Generic RDD-to-DF Converter ===")
def rdd_to_df(rdd, columns):
    """Safely converts an RDD of tuples to a DataFrame."""
    if rdd.isEmpty():  # Handle empty RDD
        print("Warning: RDD is empty, returning empty DataFrame")
        return spark.createDataFrame([], columns) if isinstance(columns, type(None)) else spark.createDataFrame([], columns)
    return rdd.toDF(columns)  # Convert with column names

test_rdd = sc.parallelize([(1, "a"), (2, "b")])  # Test data
test_df = rdd_to_df(test_rdd, ["id", "letter"])  # Convert
display(test_df)  # Show

# ---- LEVEL 9 ----
print("\n=== Level 9: Production Helper ===")
def get_spark_info():
    """Returns a dictionary with all key Spark environment info."""
    return {
        "spark_version": spark.version,
        "app_name": sc.appName,
        "master": sc.master,
        "parallelism": sc.defaultParallelism,
        "python_version": sc.pythonVer,
        "user": sc.sparkUser()
    }

info = get_spark_info()  # Call the function
for key, val in info.items():  # Print each item
    print(f"  {key}: {val}")

print("\n\u2705 All homework levels complete!")
# WHY each level: builds progressively from basic access to production patterns