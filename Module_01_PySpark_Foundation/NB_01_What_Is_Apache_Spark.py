# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 01: What is Apache Spark? Why Does It Exist?
# MAGIC # Module: PySpark Foundation & SparkSession
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 45 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### The Big Data Problem (Plain English)
# MAGIC
# MAGIC Imagine you work in a **giant warehouse** with **10 million boxes**. Each box has a label, and your boss says:  
# MAGIC *"Count how many boxes are labeled 'FRAGILE'."*
# MAGIC
# MAGIC **Option A: One Worker (Traditional Computing)**  
# MAGIC One person walks through every aisle, checks every box, one at a time.  
# MAGIC This takes **10 days**.
# MAGIC
# MAGIC **Option B: 1,000 Workers (Apache Spark)**  
# MAGIC You hire 1,000 workers. Each one takes a section of the warehouse.  
# MAGIC They all count at the same time. Then they report their totals to a **manager** who adds them up.  
# MAGIC This takes **15 minutes**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### So What IS Apache Spark?
# MAGIC
# MAGIC **Apache Spark is a tool that lets you process huge amounts of data by splitting the work across many computers at the same time.**
# MAGIC
# MAGIC Think of it like this:
# MAGIC - **One computer** = one worker = slow for big data
# MAGIC - **Spark** = a manager that coordinates thousands of workers (computers) = fast for big data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Why Does Spark Exist?
# MAGIC
# MAGIC Before Spark, we had **Hadoop MapReduce**. It worked, but it was painfully slow because:
# MAGIC 1. It wrote every intermediate result to **disk** (like writing notes on paper between every step)
# MAGIC 2. It required a lot of **boilerplate code** for simple tasks
# MAGIC 3. It had **no interactive mode** — you couldn't explore data quickly
# MAGIC
# MAGIC **Spark fixed all of this:**
# MAGIC - Keeps data in **memory (RAM)** — 100x faster than disk
# MAGIC - Simple API — do in 5 lines what took 50 in MapReduce
# MAGIC - Interactive notebooks — explore data like you're having a conversation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Vocabulary (Plain English)
# MAGIC
# MAGIC | Term | Plain English Meaning |
# MAGIC |------|----------------------|
# MAGIC | Distributed Computing | Splitting work across many computers |
# MAGIC | Cluster | A group of computers working together |
# MAGIC | Driver | The "manager" computer that plans the work |
# MAGIC | Executor | A "worker" computer that does the actual processing |
# MAGIC | Partition | One slice/chunk of your data given to one worker |
# MAGIC | Lazy Evaluation | Spark plans but doesn't act until you ask for a result |

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Spark Architecture (Text Diagram)
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                      YOUR NOTEBOOK                           │
# MAGIC │              (where you write your code)                     │
# MAGIC └──────────────────────────┬──────────────────────────────────┘
# MAGIC                            │
# MAGIC                            ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    DRIVER PROGRAM                            │
# MAGIC │         (The Manager — plans the work, collects results)    │
# MAGIC │                                                             │
# MAGIC │   ┌─────────────┐                                           │
# MAGIC │   │ SparkSession│  ← Your gateway to everything in Spark    │
# MAGIC │   └─────────────┘                                           │
# MAGIC └──────────────────────────┬──────────────────────────────────┘
# MAGIC                            │
# MAGIC                            ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                   CLUSTER MANAGER                            │
# MAGIC │        (Assigns workers and resources — like HR)            │
# MAGIC │        Options: Standalone, YARN, Mesos, Kubernetes         │
# MAGIC └────────┬────────────────┬───────────────────┬───────────────┘
# MAGIC          │                │                   │
# MAGIC          ▼                ▼                   ▼
# MAGIC ┌──────────────┐  ┌──────────────┐   ┌──────────────┐
# MAGIC │  EXECUTOR 1  │  │  EXECUTOR 2  │   │  EXECUTOR 3  │
# MAGIC │  (Worker 1)  │  │  (Worker 2)  │   │  (Worker 3)  │
# MAGIC │              │  │              │   │              │
# MAGIC │  Task  Task  │  │  Task  Task  │   │  Task  Task  │
# MAGIC │  ┌──┐  ┌──┐ │  │  ┌──┐  ┌──┐ │   │  ┌──┐  ┌──┐ │
# MAGIC │  │T1│  │T2│ │  │  │T3│  │T4│ │   │  │T5│  │T6│ │
# MAGIC │  └──┘  └──┘ │  │  └──┘  └──┘ │   │  └──┘  └──┘ │
# MAGIC └──────────────┘  └──────────────┘   └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Spark vs Hadoop MapReduce
# MAGIC
# MAGIC | Feature | Hadoop MapReduce | Apache Spark |
# MAGIC |---------|-----------------|---------------|
# MAGIC | Speed | Slow (disk-based) | 100x faster (memory-based) |
# MAGIC | Ease of Use | Complex Java code | Simple Python/Scala/SQL |
# MAGIC | Processing | Batch only | Batch + Streaming + ML + Graph |
# MAGIC | Interactive | No | Yes (notebooks!) |
# MAGIC | Fault Tolerance | Yes (disk) | Yes (lineage) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Spark's 5 Main Components
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────┐
# MAGIC │                  APACHE SPARK                         │
# MAGIC ├─────────┬─────────┬──────────┬────────┬─────────────┤
# MAGIC │  Spark  │  Spark  │ Spark    │ Spark  │   GraphX    │
# MAGIC │  SQL    │Streaming│   MLlib  │  Core  │  (Graphs)   │
# MAGIC │         │         │  (ML)    │        │             │
# MAGIC ├─────────┴─────────┴──────────┴────────┴─────────────┤
# MAGIC │              Spark Core Engine (RDDs)                 │
# MAGIC └─────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC 1. **Spark Core** — The foundation. Handles scheduling, memory, fault recovery. Uses RDDs.
# MAGIC 2. **Spark SQL** — Work with structured data using SQL queries and DataFrames.
# MAGIC 3. **Spark Streaming** — Process real-time data streams (like live sensor data).
# MAGIC 4. **MLlib (Machine Learning)** — Train ML models on big data (classification, regression, clustering).
# MAGIC 5. **GraphX** — Process graph data (social networks, recommendation engines).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### How Spark Processes Your Code (Step by Step)
# MAGIC
# MAGIC 1. You write code in your notebook
# MAGIC 2. Spark creates a **plan** (like a recipe) — this is called the DAG
# MAGIC 3. Spark does NOTHING yet (lazy evaluation!)
# MAGIC 4. When you ask for a result (an "action"), Spark executes the plan
# MAGIC 5. Work is split into **tasks** and sent to **executors**
# MAGIC 6. Results come back to the **driver** (your notebook)

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Check Spark Version
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 1: Check Spark Version and Session Info
# ═══════════════════════════════════════════════════════

# The 'spark' variable is automatically available in every Databricks notebook
# It is your SparkSession — the entry point to all Spark functionality
print("Spark Version:", spark.version)  # Print the version of Spark running on this cluster

# Access the SparkContext (the lower-level engine) from the SparkSession
sc = spark.sparkContext  # sc is the SparkContext — manages the connection to the cluster

# Print basic information about our Spark setup
print("App Name:", sc.appName)  # The name of this Spark application
print("Master:", sc.master)  # Where Spark is running (local, yarn, etc.)
print("Default Parallelism:", sc.defaultParallelism)  # How many tasks can run at once

# Check how much memory and CPUs are available
print("\n--- Spark is ready! ---")  # Confirmation message
print("Think of this as: The manager (Driver) is online and ready to coordinate workers.")

# Expected Output:
# Spark Version: 3.5.0 (or your cluster's version)
# App Name: Databricks Shell
# Master: local[*] (or spark://... for a real cluster)
# Default Parallelism: 8 (depends on your cluster size)
# --- Spark is ready! ---
# Think of this as: The manager (Driver) is online and ready to coordinate workers.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Create RDD and Count
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 2: Create a Simple RDD and Count Elements
# ═══════════════════════════════════════════════════════

# An RDD (Resilient Distributed Dataset) is the most basic data structure in Spark
# Think of it as a list that is automatically split across multiple computers

# Create a list of numbers in regular Python
my_numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]  # A simple Python list

# Convert the Python list into a Spark RDD using sc.parallelize()
# This distributes the data across the cluster's workers
my_rdd = sc.parallelize(my_numbers)  # Now the data lives on multiple computers!

# Count how many elements are in the RDD
# .count() is an ACTION — it triggers Spark to actually do the work
total_count = my_rdd.count()  # Count all elements in the distributed dataset
print(f"Total elements in the RDD: {total_count}")  # Print the result

# Get the first element
first_element = my_rdd.first()  # Get just the first item
print(f"First element: {first_element}")  # Print it

# Get the first 3 elements
first_three = my_rdd.take(3)  # Get the first 3 items back to the driver
print(f"First 3 elements: {first_three}")  # Print them

# Get the sum of all elements
total_sum = my_rdd.sum()  # Add up all the numbers
print(f"Sum of all elements: {total_sum}")  # Print the sum

# Expected Output:
# Total elements in the RDD: 10
# First element: 1
# First 3 elements: [1, 2, 3]
# Sum of all elements: 55

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Create DataFrame
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 3: Create a Simple DataFrame from a List
# ═══════════════════════════════════════════════════════

# A DataFrame is like an Excel spreadsheet — it has rows and named columns
# It is the MOST COMMON way to work with data in Spark (much better than RDDs)

# Create sample data as a list of tuples
# Each tuple is one row: (name, age, city)
people_data = [  # Our sample data — think of each tuple as one row in a spreadsheet
    ("Alice", 30, "London"),      # Row 1
    ("Bob", 25, "New York"),      # Row 2
    ("Charlie", 35, "Paris"),     # Row 3
    ("Diana", 28, "Tokyo"),       # Row 4
    ("Eve", 32, "Berlin")         # Row 5
]

# Define the column names for our DataFrame
column_names = ["name", "age", "city"]  # These become the column headers

# Create the DataFrame using spark.createDataFrame()
# This distributes the data across the cluster automatically
people_df = spark.createDataFrame(people_data, column_names)  # Create the DataFrame

# Display the DataFrame using Databricks' rich display() function
# display() shows a nice formatted table (much better than .show())
display(people_df)  # Show the data in a beautiful table format

# Print the schema (structure) of the DataFrame
# This shows you the column names and their data types
print("\nSchema (the blueprint of our data):")  # Label for clarity
people_df.printSchema()  # Shows: column name, data type, nullable

# Count the rows
row_count = people_df.count()  # Count all rows in the DataFrame
print(f"\nTotal rows: {row_count}")  # Print the count

# Expected Output:
# +-------+---+--------+
# |   name|age|    city|
# +-------+---+--------+
# |  Alice| 30|  London|
# |    Bob| 25|New York|
# |Charlie| 35|   Paris|
# |  Diana| 28|   Tokyo|
# |    Eve| 32|  Berlin|
# +-------+---+--------+
#
# Schema (the blueprint of our data):
# root
#  |-- name: string (nullable = true)
#  |-- age: long (nullable = true)
#  |-- city: string (nullable = true)
#
# Total rows: 5

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Parallel Processing
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# Example 1: Demonstrate Parallel Processing
# ═══════════════════════════════════════════════════════

import time  # Import the time module to measure how long things take

# Create a large dataset — 1 million numbers
large_data = list(range(1, 1000001))  # Python list with 1 million numbers

# --- Method 1: Regular Python (single computer) ---
start_time = time.time()  # Record the start time
python_sum = sum(large_data)  # Sum using regular Python (single-threaded)
python_time = time.time() - start_time  # Calculate how long it took
print(f"Python sum: {python_sum:,}")  # Print the result with commas for readability
print(f"Python time: {python_time:.4f} seconds")  # Print time taken

# --- Method 2: Spark (distributed across many computers) ---
start_time = time.time()  # Record the start time
spark_rdd = sc.parallelize(large_data, 8)  # Distribute data into 8 partitions
spark_sum = spark_rdd.sum()  # Sum using Spark (can use multiple CPUs/machines)
spark_time = time.time() - start_time  # Calculate how long it took
print(f"\nSpark sum: {int(spark_sum):,}")  # Print the result
print(f"Spark time: {spark_time:.4f} seconds")  # Print time taken

# Show how the data is partitioned (split across workers)
num_partitions = spark_rdd.getNumPartitions()  # Check how many partitions were created
print(f"\nNumber of partitions: {num_partitions}")  # Each partition is processed by one task
print("Think of it as: The million numbers are split into 8 piles, one per worker.")

# Note: For small data, Python may be faster due to Spark's overhead
# Spark shines when data is HUGE (gigabytes/terabytes)

# Expected Output:
# Python sum: 500,000,500,000
# Python time: 0.0XXX seconds
#
# Spark sum: 500,000,500,000
# Spark time: 0.XXXX seconds
#
# Number of partitions: 8
# Think of it as: The million numbers are split into 8 piles, one per worker.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Lazy Evaluation
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# Example 2: Spark's Lazy Evaluation
# ═══════════════════════════════════════════════════════

# LAZY EVALUATION means: Spark plans what to do, but doesn't do it
# until you specifically ask for a result.
#
# Analogy: Writing a grocery list vs. actually going to the store
# - Writing the list = TRANSFORMATION (lazy, no work done)
# - Going to the store = ACTION (work actually happens)

print("=== Demonstrating Lazy Evaluation ===")
print()

# Step 1: Create an RDD (no real work yet)
numbers_rdd = sc.parallelize(range(1, 101))  # Create RDD with numbers 1-100
print("Step 1: Created RDD — Spark has NOT processed anything yet")
print(f"   Type: {type(numbers_rdd)}")  # It's just an RDD object, no computation

# Step 2: Apply a TRANSFORMATION (still no work!)
# Transformations are LAZY — they just add to the plan
doubled_rdd = numbers_rdd.map(lambda x: x * 2)  # Plan: multiply each number by 2
print("\nStep 2: Applied map(x * 2) — Spark STILL has NOT done anything!")
print("   It just added 'multiply by 2' to the recipe.")

# Step 3: Apply another TRANSFORMATION (still no work!)
filtered_rdd = doubled_rdd.filter(lambda x: x > 100)  # Plan: keep only numbers > 100
print("\nStep 3: Applied filter(x > 100) — STILL nothing has run!")
print("   The recipe now says: 'multiply by 2, then keep only numbers > 100'")

# Step 4: Call an ACTION — NOW Spark actually does the work!
print("\nStep 4: Calling .count() — THIS is an ACTION, Spark executes NOW!")
result = filtered_rdd.count()  # ACTION! Spark runs the entire plan now
print(f"   Result: {result} numbers are greater than 100 after doubling")

# Step 5: Another ACTION — re-executes the plan from scratch
first_five = filtered_rdd.take(5)  # ACTION! Get first 5 results
print(f"\nStep 5: First 5 results: {first_five}")  # These are numbers > 100

print("\n--- Key Takeaway ---")
print("TRANSFORMATIONS (map, filter, flatMap) = lazy = just planning")
print("ACTIONS (count, collect, take, sum, first) = trigger execution")

# Expected Output:
# === Demonstrating Lazy Evaluation ===
#
# Step 1: Created RDD — Spark has NOT processed anything yet
#    Type: <class 'pyspark.rdd.PipelinedRDD'>
#
# Step 2: Applied map(x * 2) — Spark STILL has NOT done anything!
#    It just added 'multiply by 2' to the recipe.
#
# Step 3: Applied filter(x > 100) — STILL nothing has run!
#    The recipe now says: 'multiply by 2, then keep only numbers > 100'
#
# Step 4: Calling .count() — THIS is an ACTION, Spark executes NOW!
#    Result: 50 numbers are greater than 100 after doubling
#
# Step 5: First 5 results: [102, 104, 106, 108, 110]
#
# --- Key Takeaway ---
# TRANSFORMATIONS (map, filter, flatMap) = lazy = just planning
# ACTIONS (count, collect, take, sum, first) = trigger execution

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Spark Components
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# Example 3: Accessing Spark's 5 Main Components
# ═══════════════════════════════════════════════════════

print("=== Spark's 5 Main Components Demo ===")
print()

# ---- Component 1: SPARK CORE (RDDs) ----
# The foundation of everything in Spark
core_rdd = sc.parallelize(["Spark", "Core", "is", "the", "foundation"])  # Create an RDD
word_count = core_rdd.count()  # Use an RDD action
print(f"1. SPARK CORE (RDDs): Created an RDD with {word_count} words")
print(f"   Words: {core_rdd.collect()}")  # Collect all words back to the driver

# ---- Component 2: SPARK SQL (DataFrames) ----
# Work with structured data using SQL or DataFrames
sales_data = [  # Sample sales data
    ("Product A", 100, "Electronics"),  # (product, price, category)
    ("Product B", 200, "Electronics"),
    ("Product C", 50, "Books")
]
sales_df = spark.createDataFrame(sales_data, ["product", "price", "category"])  # Create DF
sales_df.createOrReplaceTempView("sales")  # Register as SQL table
sql_result = spark.sql("SELECT category, SUM(price) as total FROM sales GROUP BY category")  # Run SQL!
print("\n2. SPARK SQL: Ran a SQL query on a DataFrame")
sql_result.show()  # Show the SQL query result

# ---- Component 3: SPARK STREAMING ----
# Process real-time data (we'll just show it's available)
print("3. SPARK STREAMING: Available for real-time data processing")
print("   Used with: spark.readStream and spark.writeStream")
print("   Example sources: Kafka, Event Hubs, Auto Loader")

# ---- Component 4: MLLIB (Machine Learning) ----
# Train ML models on big data
from pyspark.ml.feature import VectorAssembler  # Import an ML component
print("\n4. MLLIB (Machine Learning): Available for training models")
print(f"   Imported: VectorAssembler from pyspark.ml.feature")
print("   Can do: Classification, Regression, Clustering, Recommendation")

# ---- Component 5: GRAPHX (Graph Processing) ----
# Process graph/network data (limited in PySpark, more in Scala)
print("\n5. GRAPHX (Graph Processing): Available via GraphFrames library")
print("   Used for: Social networks, recommendation engines, fraud detection")
print("   In PySpark, use the 'graphframes' library")

print("\n" + "="*50)
print("All 5 components share the same Spark Core engine underneath!")
print("="*50)

# Expected Output:
# === Spark's 5 Main Components Demo ===
#
# 1. SPARK CORE (RDDs): Created an RDD with 5 words
#    Words: ['Spark', 'Core', 'is', 'the', 'foundation']
#
# 2. SPARK SQL: Ran a SQL query on a DataFrame
# +-----------+-----+
# |   category|total|
# +-----------+-----+
# |Electronics|  300|
# |      Books|   50|
# +-----------+-----+
#
# 3. SPARK STREAMING: Available for real-time data processing
#    ...
# 4. MLLIB (Machine Learning): Available for training models
#    ...
# 5. GRAPHX (Graph Processing): Available via GraphFrames library
#    ...

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Python vs Spark
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Examples
# Example 1: Python vs Spark — Processing at Scale
# ═══════════════════════════════════════════════════════

import time  # For measuring execution time
from pyspark.sql.functions import col, sum as spark_sum, avg, count  # Import Spark functions

print("=== Performance Comparison: Python vs Spark ===")
print("Task: Generate 5 million rows, filter, and aggregate")
print()

# ---- SPARK APPROACH (distributed) ----
start_time = time.time()  # Start timer

# Create a large DataFrame with 5 million rows using spark.range()
# This is much more efficient than creating from a Python list
large_df = spark.range(0, 5000000)  # Creates a DataFrame with column 'id' (0 to 4,999,999)

# Add computed columns to simulate real data
large_df = large_df.withColumn("value", col("id") % 100)  # Simulate a value (0-99)
large_df = large_df.withColumn("category", (col("id") % 5).cast("string"))  # 5 categories

# Filter: keep only rows where value > 50
filtered_df = large_df.filter(col("value") > 50)  # Filter rows

# Aggregate: sum and average by category
result_df = filtered_df.groupBy("category").agg(  # Group by category
    spark_sum("value").alias("total_value"),  # Sum of values per category
    avg("value").alias("avg_value"),  # Average value per category
    count("*").alias("row_count")  # Count of rows per category
)

# Force execution by collecting results
result_df.collect()  # ACTION: triggers the full computation
spark_time = time.time() - start_time  # Calculate elapsed time

print(f"Spark processed 5 million rows in: {spark_time:.2f} seconds")
print("\nResults by category:")
display(result_df.orderBy("category"))  # Show results sorted by category

# ---- PYTHON APPROACH (single-threaded, for comparison concept) ----
start_time = time.time()  # Start timer
python_data = [(i, i % 100, str(i % 5)) for i in range(5000000)]  # Generate data
python_filtered = [(i, v, c) for i, v, c in python_data if v > 50]  # Filter
# Group by category and compute aggregates
from collections import defaultdict  # Import for grouping
groups = defaultdict(list)  # Create a dictionary of lists
for i, v, c in python_filtered:  # Loop through each row
    groups[c].append(v)  # Add value to the appropriate category
python_result = {k: (sum(v), sum(v)/len(v), len(v)) for k, v in groups.items()}  # Aggregate
python_time = time.time() - start_time  # Calculate elapsed time

print(f"\nPython processed 5 million rows in: {python_time:.2f} seconds")
print(f"\nSpeedup ratio: {python_time/spark_time:.1f}x" if spark_time > 0 else "")
print("\nNote: Spark's advantage grows DRAMATICALLY with data size (GB/TB)!")

# Expected Output:
# Spark processed 5 million rows in: ~1-3 seconds
# Python processed 5 million rows in: ~5-15 seconds
# Speedup ratio: ~2-10x (varies, but Spark wins big on larger data)

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Execution Plan (explain)
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Examples
# Example 2: Reading the Spark Execution Plan
# ═══════════════════════════════════════════════════════

# The "execution plan" is Spark's blueprint for HOW it will do the work
# Think of it as reading the architect's blueprint before building a house

from pyspark.sql.functions import col, upper, length  # Import functions we'll use

# Create a sample DataFrame
employees_data = [  # Employee data
    ("Alice", "Engineering", 95000),    # (name, department, salary)
    ("Bob", "Marketing", 72000),
    ("Charlie", "Engineering", 105000),
    ("Diana", "Sales", 68000),
    ("Eve", "Engineering", 88000),
    ("Frank", "Marketing", 91000)
]
# Create the DataFrame with column names
emp_df = spark.createDataFrame(employees_data, ["name", "department", "salary"])

# Build a query with multiple operations
# This is like writing a recipe with several steps
result = (
    emp_df  # Start with the employee DataFrame
    .filter(col("salary") > 70000)  # Step 1: Keep only salaries > 70k
    .withColumn("name_upper", upper(col("name")))  # Step 2: Add uppercase name
    .groupBy("department")  # Step 3: Group by department
    .avg("salary")  # Step 4: Calculate average salary per department
)

# Show the PHYSICAL execution plan
# This tells you exactly what Spark will do
print("=== Physical Execution Plan ===")
print("(Read from bottom to top — bottom is first, top is last)")
print()
result.explain()  # Show the physical plan

print("\n" + "="*60)
print("\n=== Extended Plan (all 4 phases) ===")
print("Phases: Parsed → Analyzed → Optimized → Physical")
print()
result.explain(True)  # Show all 4 plan phases

# Show the actual results too
print("\n=== Actual Results ===")
display(result)  # Display the aggregated results

# Expected Output:
# === Physical Execution Plan ===
# == Physical Plan ==
# *(2) HashAggregate(keys=[department], functions=[avg(salary)])
# +- Exchange hashpartitioning(department, 200)
#    +- *(1) HashAggregate(keys=[department], functions=[partial_avg(salary)])
#       +- *(1) Project [department, salary]
#          +- *(1) Filter (salary > 70000)
#             +- *(1) Scan ExistingRDD [name, department, salary]

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Fault Tolerance and Lineage
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Examples
# Example 3: Fault Tolerance — RDD Lineage (the Safety Net)
# ═══════════════════════════════════════════════════════

# FAULT TOLERANCE means: If a computer crashes, Spark can recover without losing data
#
# How? Through LINEAGE — Spark remembers every step of the recipe.
# If a worker fails, Spark replays the recipe for just that worker's portion.
#
# Analogy: If a baker drops a cake, they don't need the original cake —
# they just follow the recipe again from scratch for that one cake.

print("=== Fault Tolerance Through Lineage ===")
print()

# Create an RDD and apply several transformations
# Each transformation adds to the "recipe" (lineage)
base_rdd = sc.parallelize(range(1, 101), 4)  # Step 1: 100 numbers in 4 partitions
print("Step 1: Created base RDD with 100 numbers in 4 partitions")

# Apply transformation chain
step2_rdd = base_rdd.map(lambda x: x * 2)  # Step 2: Double each number
print("Step 2: Mapped x * 2")

step3_rdd = step2_rdd.filter(lambda x: x > 50)  # Step 3: Keep only numbers > 50
print("Step 3: Filtered to keep only > 50")

step4_rdd = step3_rdd.map(lambda x: (x % 10, x))  # Step 4: Create key-value pairs
print("Step 4: Created (key, value) pairs")

step5_rdd = step4_rdd.reduceByKey(lambda a, b: a + b)  # Step 5: Sum by key
print("Step 5: Reduced by key (sum)")

# Now let's look at the LINEAGE (the recipe Spark saved)
print("\n" + "="*60)
print("RDD LINEAGE (the recipe Spark uses to rebuild if a failure occurs):")
print("="*60)
print(step5_rdd.toDebugString().decode('utf-8'))  # Show the full lineage graph

# Explanation of the lineage output
print("\n--- How to Read the Lineage ---")
print("Each line shows one step in the recipe.")
print("The number in parentheses (e.g., (4)) is the number of partitions.")
print("If any partition is lost, Spark re-runs ONLY the steps for that partition.")
print("This is WHY Spark is fault-tolerant without writing everything to disk!")

# Verify the result is correct
print("\n--- Final Result ---")
final_result = step5_rdd.collect()  # Trigger execution and collect results
print(f"Aggregated by key: {sorted(final_result)}")  # Sort for readability
print(f"Number of unique keys: {len(final_result)}")

# Expected Output:
# === Fault Tolerance Through Lineage ===
# Step 1: Created base RDD with 100 numbers in 4 partitions
# Step 2: Mapped x * 2
# Step 3: Filtered to keep only > 50
# Step 4: Created (key, value) pairs
# Step 5: Reduced by key (sum)
#
# RDD LINEAGE:
# (4) PythonRDD[X] at RDD at PythonRDD.scala:XX []
#  |  MapPartitionsRDD[X] ...
#  |  ShuffledRDD[X] ...
#  |  PairwiseRDD[X] ...
#  |  PythonRDD[X] ...
#  |  ParallelCollectionRDD[X] ...
#
# --- How to Read the Lineage ---
# Each line shows one step in the recipe.
# ...
# --- Final Result ---
# Aggregated by key: [(0, 750), (2, 780), (4, 810), (6, 840), (8, 870)]
# Number of unique keys: 5