# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 05: What is an RDD? The DNA of Spark
# MAGIC # Module: RDDs (Resilient Distributed Datasets)
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 45 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: A Recipe Card Collection
# MAGIC
# MAGIC Imagine you have a **deck of recipe cards** (your data), and you need to organize them:  
# MAGIC - You split the deck into **5 piles** and give one pile to each of your **5 friends** (distributed)  
# MAGIC - If one friend drops their pile in a puddle, you can **reprint those cards** from the original cookbook (resilient)  
# MAGIC - Nobody can CHANGE the original cards — they can only create NEW copies with modifications (immutable)  
# MAGIC - Your friends don't start cooking until you say "GO" (lazy)
# MAGIC
# MAGIC **An RDD is exactly this:**  
# MAGIC A collection of data split across many computers, that can be rebuilt if lost, never modified in place, and only processed when you ask for a result.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### RDD = Resilient Distributed Dataset
# MAGIC
# MAGIC | Word | Meaning |
# MAGIC |------|--------|
# MAGIC | **Resilient** | If a piece is lost (computer crashes), Spark can rebuild it from the recipe (lineage) |
# MAGIC | **Distributed** | The data is split across multiple computers (partitions) |
# MAGIC | **Dataset** | It's a collection of items (numbers, strings, objects, anything) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5 Key Properties of an RDD
# MAGIC
# MAGIC 1. **Immutable** — Once created, you cannot change it. You can only create a NEW RDD from it.
# MAGIC 2. **Distributed** — Data lives across many machines in partitions.
# MAGIC 3. **Lazy** — Transformations don't execute until you call an action.
# MAGIC 4. **Type-safe** — In Scala (less so in Python, but the concept applies).
# MAGIC 5. **Fault-tolerant** — Lineage (the recipe) allows Spark to recompute lost partitions.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use RDDs vs DataFrames
# MAGIC
# MAGIC | Use RDDs When... | Use DataFrames When... |
# MAGIC |------------------|------------------------|
# MAGIC | Working with unstructured data (text, binary) | Working with structured/tabular data (99% of the time) |
# MAGIC | You need fine-grained control over partitioning | You want Spark to optimize for you |
# MAGIC | You need broadcast variables or accumulators | You want SQL support |
# MAGIC | Maintaining legacy Spark 1.x code | Starting any new project |
# MAGIC
# MAGIC **Bottom line:** DataFrames are almost always better. But understanding RDDs helps you understand HOW Spark works internally.

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Creating RDDs — 4 Ways
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                   Ways to Create an RDD                       │
# MAGIC ├─────────────────────────────────────────────────────────────┤
# MAGIC │                                                             │
# MAGIC │  1. sc.parallelize(list)    → From a Python list/collection │
# MAGIC │  2. sc.textFile(path)       → From a text file (one line    │
# MAGIC │                                per element)                  │
# MAGIC │  3. sc.wholeTextFiles(path) → Each file = one element       │
# MAGIC │  4. sc.range(start, end)    → Generate numbers              │
# MAGIC │                                                             │
# MAGIC │  Also: df.rdd               → From a DataFrame              │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### RDD Lineage — The Safety Net
# MAGIC
# MAGIC ```
# MAGIC    ┌───────────┐     map(x*2)      ┌───────────┐    filter(>10)    ┌───────────┐
# MAGIC    │ Original  │ ──────────────────▶│ Doubled   │ ────────────────▶│ Filtered  │
# MAGIC    │ RDD       │                    │ RDD       │                  │ RDD       │
# MAGIC    │ [1,2,3..] │                    │ [2,4,6..] │                  │ [12,14..] │
# MAGIC    └───────────┘                    └───────────┘                  └───────────┘
# MAGIC         ▲                                                               │
# MAGIC         │                                                               │
# MAGIC         └──── If this partition is lost, Spark replays: ────────────────┘
# MAGIC               "Start from Original, apply map, then filter"
# MAGIC ```
# MAGIC
# MAGIC ### Partitions — How Data Is Split
# MAGIC
# MAGIC ```
# MAGIC    RDD with 12 elements, 4 partitions:
# MAGIC    
# MAGIC    Partition 0: [1, 2, 3]     → Worker A processes this
# MAGIC    Partition 1: [4, 5, 6]     → Worker B processes this
# MAGIC    Partition 2: [7, 8, 9]     → Worker C processes this
# MAGIC    Partition 3: [10, 11, 12]  → Worker D processes this
# MAGIC ```
# MAGIC
# MAGIC ### Two Types of Operations
# MAGIC
# MAGIC ```
# MAGIC    TRANSFORMATIONS (Lazy)          ACTIONS (Trigger Execution)
# MAGIC    ─────────────────────           ─────────────────────────
# MAGIC    map()                           collect()
# MAGIC    filter()                        count()
# MAGIC    flatMap()                       first()
# MAGIC    union()                         take(n)
# MAGIC    distinct()                      sum()
# MAGIC    groupByKey()                    reduce()
# MAGIC    reduceByKey()                   saveAsTextFile()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Creating RDDs
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 1: Creating RDDs in 4 Different Ways
# ═══════════════════════════════════════════════════════

# Get SparkContext (needed for RDD operations)
sc = spark.sparkContext  # Access SparkContext from SparkSession

print("=== 4 Ways to Create an RDD ===")
print()

# Way 1: sc.parallelize() — from a Python list
print("Way 1: sc.parallelize(list)")
numbers_rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])  # Create from list
print(f"  Created RDD with {numbers_rdd.count()} elements")  # Count elements
print(f"  Data: {numbers_rdd.collect()}")  # Bring all data back to driver

# Way 2: sc.parallelize() with custom partitions
print("\nWay 2: sc.parallelize(list, numPartitions)")
cities_rdd = sc.parallelize(["London", "Tokyo", "Paris", "Berlin", "NYC", "Mumbai"], 3)  # 3 partitions
print(f"  Created RDD with {cities_rdd.getNumPartitions()} partitions")  # Check partitions
print(f"  Data: {cities_rdd.collect()}")  # Show all data

# Way 3: sc.range() — generate a sequence of numbers
print("\nWay 3: sc.range(start, end, step)")
range_rdd = sc.range(0, 100, 10)  # Numbers 0, 10, 20, ..., 90
print(f"  Created RDD: {range_rdd.collect()}")  # Show the sequence

# Way 4: From a DataFrame (converting back to RDD)
print("\nWay 4: DataFrame.rdd")
df = spark.createDataFrame([("Alice", 30), ("Bob", 25)], ["name", "age"])  # Create DF
df_rdd = df.rdd  # Convert DataFrame to RDD (each element is a Row)
print(f"  From DataFrame: {df_rdd.collect()}")  # Shows Row objects
print(f"  First row name: {df_rdd.first()['name']}")  # Access by column name

# Expected Output:
# Way 1: Created RDD with 10 elements, Data: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
# Way 2: Created RDD with 3 partitions, Data: [London, Tokyo, ...]
# Way 3: Created RDD: [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
# Way 4: From DataFrame: [Row(name='Alice', age=30), Row(name='Bob', age=25)]

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: RDD Properties
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 2: Exploring RDD Properties
# ═══════════════════════════════════════════════════════

print("=== 5 Properties of an RDD ===")
print()

# Create a sample RDD
sample_rdd = sc.parallelize([10, 20, 30, 40, 50, 60, 70, 80, 90, 100], 4)  # 10 items, 4 partitions

# Property 1: DISTRIBUTED — data lives in partitions
print("1. DISTRIBUTED (split across partitions):")
num_partitions = sample_rdd.getNumPartitions()  # Get number of partitions
print(f"   Number of partitions: {num_partitions}")  # Should be 4
# See what's in each partition using glom()
partition_data = sample_rdd.glom().collect()  # glom() groups each partition's data into a list
for i, partition in enumerate(partition_data):  # Loop through partitions
    print(f"   Partition {i}: {partition}")  # Show contents of each partition

# Property 2: IMMUTABLE — you can't change it, only create new ones
print("\n2. IMMUTABLE (can't modify, only create new):")
new_rdd = sample_rdd.map(lambda x: x * 2)  # Creates a NEW RDD (doesn't modify original)
print(f"   Original: {sample_rdd.collect()}")  # Original unchanged!
print(f"   New (doubled): {new_rdd.collect()}")  # New RDD has doubled values

# Property 3: LAZY — transformations don't execute immediately
print("\n3. LAZY (nothing happens until an action):")
import time  # For timing
start = time.time()  # Start timer
lazy_rdd = sample_rdd.map(lambda x: x * 100).filter(lambda x: x > 5000)  # Transformations
print(f"   Time for transformations (should be ~0): {time.time() - start:.4f}s")  # Almost instant!
start = time.time()  # Reset timer
result = lazy_rdd.collect()  # ACTION! Now it actually runs
print(f"   Time for action (collect): {time.time() - start:.4f}s")  # Actual computation
print(f"   Result: {result}")  # The final data

# Property 4: RESILIENT — has lineage for recovery
print("\n4. RESILIENT (has lineage for fault tolerance):")
print(f"   Lineage of new_rdd:")
print(f"   {new_rdd.toDebugString().decode('utf-8')}")  # Shows the recovery recipe

# Property 5: TYPED — each element can be any Python object
print("\n5. TYPED (elements can be anything):")
mixed_rdd = sc.parallelize([1, "hello", 3.14, [1,2,3], {"key": "value"}])  # Mixed types!
print(f"   Mixed types: {mixed_rdd.collect()}")  # Works! (Python is flexible)

# Expected Output:
# Shows 4 partitions with data distributed across them
# Shows original unchanged after map (immutability)
# Shows transformations are instant, actions take time
# Shows lineage debug string
# Shows mixed types in an RDD

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Basic RDD Operations
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 3: Basic RDD Operations (Transformations + Actions)
# ═══════════════════════════════════════════════════════

print("=== Basic RDD Operations ===")
print()

# Create a simple RDD
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])  # Numbers 1-10

# --- TRANSFORMATIONS (lazy, return new RDDs) ---
print("--- Transformations (create new RDDs) ---")

# map() — apply a function to each element
doubled = numbers.map(lambda x: x * 2)  # Double each number
print(f"map(x*2): {doubled.collect()}")  # [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]

# filter() — keep only elements that match a condition
evens = numbers.filter(lambda x: x % 2 == 0)  # Keep only even numbers
print(f"filter(even): {evens.collect()}")  # [2, 4, 6, 8, 10]

# map to create tuples (key-value pairs)
labeled = numbers.map(lambda x: ("even" if x % 2 == 0 else "odd", x))  # Label each number
print(f"map(label): {labeled.collect()}")  # [('odd', 1), ('even', 2), ...]

# --- ACTIONS (trigger execution, return results) ---
print("\n--- Actions (trigger execution) ---")

# count() — how many elements
print(f"count(): {numbers.count()}")  # 10

# sum() — add all elements
print(f"sum(): {numbers.sum()}")  # 55

# first() — get the first element
print(f"first(): {numbers.first()}")  # 1

# take(n) — get first n elements
print(f"take(3): {numbers.take(3)}")  # [1, 2, 3]

# reduce() — combine all elements with a function
total = numbers.reduce(lambda a, b: a + b)  # Sum all numbers
print(f"reduce(+): {total}")  # 55

# max() and min()
print(f"max(): {numbers.max()}")  # 10
print(f"min(): {numbers.min()}")  # 1

# mean()
print(f"mean(): {numbers.mean()}")  # 5.5

print("\n--- Key Rule ---")
print("Transformations are LAZY (just planning).")
print("Actions TRIGGER execution (return results to driver).")

# Expected Output:
# map(x*2): [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
# filter(even): [2, 4, 6, 8, 10]
# count(): 10, sum(): 55, first(): 1, etc.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Chaining Transformations
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# Example 1: Chaining Transformations (Pipeline Pattern)
# ═══════════════════════════════════════════════════════

# Real-world scenario: Process a list of product prices
# Apply discount, filter expensive items, calculate tax

print("=== Chaining RDD Transformations ===")
print("Scenario: Process product prices")
print()

# Original prices in dollars
prices = sc.parallelize([10.99, 25.50, 8.75, 49.99, 99.99, 5.00, 75.00, 150.00, 3.50, 42.00])  # 10 prices

# Chain multiple transformations (ALL lazy until an action is called)
result = (
    prices  # Start with prices
    .map(lambda p: p * 0.9)  # Step 1: Apply 10% discount
    .filter(lambda p: p > 20.0)  # Step 2: Keep only items over $20
    .map(lambda p: round(p * 1.08, 2))  # Step 3: Add 8% tax
    .map(lambda p: ("premium" if p > 50 else "standard", p))  # Step 4: Categorize
)

# Nothing has executed yet! Let's trigger it with an action
print("Pipeline: discount → filter(>$20) → tax → categorize")
print(f"Results: {result.collect()}")  # NOW everything executes
print(f"Count of qualifying items: {result.count()}")  # Another action

# Show the pipeline step by step for clarity
print("\n--- Step by step ---")
step1 = prices.map(lambda p: round(p * 0.9, 2))  # 10% discount
print(f"After 10% discount: {step1.collect()}")  # Show intermediate

step2 = step1.filter(lambda p: p > 20.0)  # Filter
print(f"After filter >$20: {step2.collect()}")  # Show intermediate

step3 = step2.map(lambda p: round(p * 1.08, 2))  # Add tax
print(f"After 8% tax: {step3.collect()}")  # Show final prices

# Expected Output:
# Shows the chained pipeline results
# Then shows step-by-step intermediate results

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: flatMap and Text Processing
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# Example 2: flatMap vs map — Text Processing
# ═══════════════════════════════════════════════════════

# map() vs flatMap():
# map(f) — applies f to each element, returns one result per input
# flatMap(f) — applies f to each element, FLATTENS the results (one-to-many)

print("=== map() vs flatMap() ===")
print()

# Sample text data (like lines from a file)
lines_rdd = sc.parallelize([  # 3 sentences
    "Apache Spark is fast",
    "Spark processes big data",
    "Data engineering is fun"
])

# Using map() — splits each line, but result is a list OF lists
map_result = lines_rdd.map(lambda line: line.split(" "))  # Each line becomes a list
print("map(split): Returns a list of LISTS (nested):")
print(f"  {map_result.collect()}")  # [[word, word], [word, word], ...]
print(f"  Count: {map_result.count()} (one per input line)")

# Using flatMap() — splits AND flattens into individual words
flatmap_result = lines_rdd.flatMap(lambda line: line.split(" "))  # Each word is its own element
print("\nflatMap(split): Returns INDIVIDUAL words (flattened):")
print(f"  {flatmap_result.collect()}")  # [word, word, word, ...]
print(f"  Count: {flatmap_result.count()} (one per word)")

# Real-world use case: Word Count!
print("\n=== Classic Word Count ===")
word_counts = (
    lines_rdd
    .flatMap(lambda line: line.lower().split(" "))  # Split into lowercase words
    .map(lambda word: (word, 1))  # Create (word, 1) pairs
    .reduceByKey(lambda a, b: a + b)  # Sum counts per word
    .sortBy(lambda x: -x[1])  # Sort by count descending
)
print("Word frequencies:")
for word, count in word_counts.collect():  # Print each word and its count
    print(f"  '{word}': {count}")

# Key difference:
# map:     1 input → 1 output (always)
# flatMap: 1 input → 0 or more outputs (flattens)

# Expected Output:
# map gives list of lists, flatMap gives flat list
# Word count shows 'spark': 2, 'is': 2, etc.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: distinct, union, intersection
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# Example 3: Set Operations (distinct, union, intersection, subtract)
# ═══════════════════════════════════════════════════════

print("=== RDD Set Operations ===")
print()

# Create two RDDs with some overlapping elements
team_a = sc.parallelize(["Alice", "Bob", "Charlie", "Diana", "Bob"])  # Note: Bob appears twice!
team_b = sc.parallelize(["Charlie", "Diana", "Eve", "Frank"])

# distinct() — remove duplicates
print("--- distinct() ---")
print(f"Team A (with dups): {team_a.collect()}")  # Has 'Bob' twice
print(f"Team A (distinct): {team_a.distinct().collect()}")  # Removes duplicate Bob

# union() — combine two RDDs (keeps duplicates!)
print("\n--- union() ---")
combined = team_a.union(team_b)  # Merge both teams
print(f"Union: {combined.collect()}")  # Has duplicates from both
print(f"Union distinct: {combined.distinct().collect()}")  # Remove all duplicates

# intersection() — elements in BOTH RDDs
print("\n--- intersection() ---")
common = team_a.intersection(team_b)  # People in both teams
print(f"In both teams: {common.collect()}")  # Charlie, Diana

# subtract() — elements in first but NOT in second
print("\n--- subtract() ---")
only_a = team_a.distinct().subtract(team_b)  # In A but not B
only_b = team_b.subtract(team_a.distinct())  # In B but not A
print(f"Only in Team A: {only_a.collect()}")  # Alice, Bob
print(f"Only in Team B: {only_b.collect()}")  # Eve, Frank

# Practical example: finding new customers
print("\n=== Practical: Finding New Customers ===")
last_month = sc.parallelize([101, 102, 103, 104, 105])  # Customer IDs last month
this_month = sc.parallelize([103, 104, 105, 106, 107, 108])  # Customer IDs this month
new_customers = this_month.subtract(last_month)  # Customers who are new this month
lost_customers = last_month.subtract(this_month)  # Customers who didn't return
print(f"New customers: {new_customers.collect()}")  # [106, 107, 108]
print(f"Lost customers: {lost_customers.collect()}")  # [101, 102]

# Expected Output:
# distinct removes duplicates
# union combines (with dups)
# intersection finds common elements
# subtract finds differences

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: RDD Lineage Deep Dive
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Examples
# Example 1: RDD Lineage (toDebugString) Deep Dive
# ═══════════════════════════════════════════════════════

# Lineage is the "recipe" Spark stores to rebuild any RDD if a partition is lost
# Think of it as a chain of instructions Spark can replay

print("=== RDD Lineage Deep Dive ===")
print()

# Build a complex chain of transformations
base = sc.parallelize(range(1, 21), 4)  # 20 numbers in 4 partitions
print(f"Step 0 (base): {base.collect()[:5]}... ({base.count()} elements)")

step1 = base.map(lambda x: x * 3)  # Multiply by 3
print(f"Step 1 (x*3): {step1.collect()[:5]}...")

step2 = step1.filter(lambda x: x % 2 == 0)  # Keep evens
print(f"Step 2 (evens): {step2.collect()[:5]}...")

step3 = step2.map(lambda x: (x % 10, x))  # Create key-value pairs
print(f"Step 3 (pairs): {step3.collect()[:5]}...")

step4 = step3.reduceByKey(lambda a, b: a + b)  # Sum by key (causes SHUFFLE!)
print(f"Step 4 (reduce): {step4.collect()}")

# Now examine the LINEAGE of step4
print("\n=== Full Lineage of step4 ===")
lineage = step4.toDebugString().decode('utf-8')  # Get the lineage graph
print(lineage)

# Explain what the lineage means
print("\n--- Reading the Lineage ---")
print("The indentation shows dependencies (bottom = first, top = last)")
print("(4) means 4 partitions")
print("'ShuffledRDD' means data was REDISTRIBUTED across the network")
print("'PythonRDD' means a Python function was applied")
print("'ParallelCollectionRDD' is the original sc.parallelize()")
print("\nIf Partition 2 of step4 is lost, Spark will:")
print("  1. Go back to the base RDD")
print("  2. Re-apply x*3")
print("  3. Re-apply filter(evens)")
print("  4. Re-apply key-value creation")
print("  5. Re-do the shuffle for ONLY that partition")
print("  This is WHY Spark is fault-tolerant without writing to disk!")

# Expected Output:
# Shows each step's data
# Shows the full lineage graph (debug string)
# Explains how to read it

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Using `collect()` on Large RDDs
# MAGIC
# MAGIC **What happens:** You call `.collect()` on an RDD with 100 million elements.  
# MAGIC **Why it's bad:** `collect()` brings ALL data to the driver node's memory. Too much data = crash.  
# MAGIC **The fix:** Use `.take(n)`, `.first()`, or `.count()` instead. Only use `collect()` on small RDDs.
# MAGIC
# MAGIC ```python
# MAGIC # BAD: Will crash on large data
# MAGIC all_data = huge_rdd.collect()  # 100M elements to one machine!
# MAGIC
# MAGIC # GOOD: Safe alternatives
# MAGIC first_10 = huge_rdd.take(10)  # Only 10 elements
# MAGIC element_count = huge_rdd.count()  # Just the count
# MAGIC sample = huge_rdd.takeSample(False, 100)  # Random 100 elements
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #2: Using RDDs Instead of DataFrames
# MAGIC
# MAGIC **What happens:** You write all your data processing using RDDs with lambda functions.  
# MAGIC **Why it's bad:** RDDs bypass the Catalyst optimizer. Your code will be 2-10x slower.  
# MAGIC **The fix:** Use DataFrames for structured data. Only use RDDs when you truly need low-level control.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #3: Not Understanding Lazy Evaluation
# MAGIC
# MAGIC **What happens:** You write 10 transformations and expect to see results. Nothing prints.  
# MAGIC **Why it's confusing:** Transformations don't DO anything until you call an action.  
# MAGIC **The fix:** Always end your chain with an action: `.collect()`, `.count()`, `.first()`, `.take()`, `.show()`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #4: Thinking RDDs Are Mutable
# MAGIC
# MAGIC **What happens:** You call `.map()` and expect the original RDD to change.  
# MAGIC **Why it fails:** RDDs are IMMUTABLE. `.map()` returns a NEW RDD; the original is unchanged.  
# MAGIC **The fix:** Always assign the result: `new_rdd = old_rdd.map(...)`. Don't expect `old_rdd` to change.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #5: Ignoring Partitions
# MAGIC
# MAGIC **What happens:** You create an RDD from a tiny list and wonder why it has 8 partitions.  
# MAGIC **Why it matters:** Spark defaults to `sc.defaultParallelism` partitions. Too many partitions for small data = overhead.  
# MAGIC **The fix:** Specify partitions explicitly: `sc.parallelize(data, 2)` for small data.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC ### Level 1 (Just Read and Run)
# MAGIC Run Beginner Example 1. Note the 4 ways to create an RDD.
# MAGIC
# MAGIC ### Level 2 (Tiny Change)
# MAGIC Create an RDD of 5 city names. Use `.count()` and `.first()` on it.
# MAGIC
# MAGIC ### Level 3 (Combine Two Things)
# MAGIC Create an RDD of numbers 1-20. Apply `.map(x*3)` and then `.filter(x > 30)`. Show the result.
# MAGIC
# MAGIC ### Level 4 (New Scenario)
# MAGIC Create an RDD from a list of sentences. Use `flatMap` to split into words. Count total words.
# MAGIC
# MAGIC ### Level 5 (Intermediate Project)
# MAGIC Create two RDDs of email addresses. Find: (a) all unique emails, (b) emails in both lists, (c) emails only in the first list.
# MAGIC
# MAGIC ### Level 6 (Design First)
# MAGIC Design a word frequency counter: describe the steps in comments first, then implement it using RDD transformations.
# MAGIC
# MAGIC ### Level 7 (Optimize It)
# MAGIC Compare the performance of an RDD-based sum vs a DataFrame-based sum on 5 million numbers.
# MAGIC
# MAGIC ### Level 8 (Edge Cases)
# MAGIC What happens with an empty RDD? Test: `.count()`, `.collect()`, `.first()`, `.reduce()`. Document the behavior.
# MAGIC
# MAGIC ### Level 9 (Production-Grade)
# MAGIC Build a text analysis pipeline that: reads lines, cleans text (lowercase, remove punctuation), counts words, and returns the top 10.
# MAGIC
# MAGIC ### Level 10 (Teach It)
# MAGIC Explain RDDs to a colleague using a real-world analogy. Cover: what they are, the 5 properties, and when to use them vs DataFrames.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS — All 10 Levels
# ═══════════════════════════════════════════════════════

import time  # For performance comparison
import re  # For text cleaning

# ---- LEVEL 2 ----
print("=== Level 2: City RDD ===")
cities = sc.parallelize(["London", "Paris", "Tokyo", "Berlin", "Mumbai"])  # 5 cities
print(f"Count: {cities.count()}")  # 5
print(f"First: {cities.first()}")  # London

# ---- LEVEL 3 ----
print("\n=== Level 3: Map and Filter ===")
nums = sc.parallelize(range(1, 21))  # Numbers 1-20
result = nums.map(lambda x: x * 3).filter(lambda x: x > 30)  # Multiply by 3, keep > 30
print(f"Result: {result.collect()}")  # [33, 36, 39, 42, 45, 48, 51, 54, 57, 60]
# WHY: 11*3=33, 12*3=36, ... 20*3=60 — all > 30

# ---- LEVEL 4 ----
print("\n=== Level 4: flatMap for Words ===")
sentences = sc.parallelize(["Hello world", "Spark is great", "RDDs are fundamental"])  # 3 sentences
words = sentences.flatMap(lambda s: s.split(" "))  # Split into individual words
print(f"Total words: {words.count()}")  # 8
print(f"Words: {words.collect()}")  # All individual words

# ---- LEVEL 5 ----
print("\n=== Level 5: Email Set Operations ===")
list1 = sc.parallelize(["a@test.com", "b@test.com", "c@test.com", "d@test.com"])  # List 1
list2 = sc.parallelize(["c@test.com", "d@test.com", "e@test.com", "f@test.com"])  # List 2
print(f"All unique: {list1.union(list2).distinct().collect()}")  # All unique emails
print(f"In both: {list1.intersection(list2).collect()}")  # Common emails
print(f"Only in list1: {list1.subtract(list2).collect()}")  # Only in first list

# ---- LEVEL 6 ----
print("\n=== Level 6: Word Frequency (Designed) ===")
# DESIGN:
# 1. Start with lines of text
# 2. flatMap to split into words
# 3. map to lowercase
# 4. map to (word, 1) pairs
# 5. reduceByKey to count
# 6. sortBy count descending
text = sc.parallelize(["Spark Spark Spark", "is is great", "Spark is awesome"])  # Input
freqs = (text
    .flatMap(lambda l: l.lower().split(" "))  # Step 2+3: split and lowercase
    .map(lambda w: (w, 1))  # Step 4: pair up
    .reduceByKey(lambda a, b: a + b)  # Step 5: count
    .sortBy(lambda x: -x[1])  # Step 6: sort
)
print(f"Frequencies: {freqs.collect()}")  # [('spark', 4), ('is', 3), ...]

# ---- LEVEL 7 ----
print("\n=== Level 7: RDD vs DataFrame Performance ===")
data_size = 5000000  # 5 million numbers
# RDD approach
start = time.time()
rdd_sum = sc.parallelize(range(data_size), 8).sum()  # RDD sum
rdd_time = time.time() - start
print(f"RDD sum: {int(rdd_sum):,} in {rdd_time:.3f}s")
# DataFrame approach
start = time.time()
from pyspark.sql.functions import sum as spark_sum  # Import
df_sum = spark.range(data_size).agg(spark_sum("id")).collect()[0][0]  # DF sum
df_time = time.time() - start
print(f"DF sum: {int(df_sum):,} in {df_time:.3f}s")
print(f"DataFrame is {rdd_time/df_time:.1f}x faster")

# ---- LEVEL 8 ----
print("\n=== Level 8: Empty RDD Edge Cases ===")
empty = sc.parallelize([])  # Empty RDD
print(f"count(): {empty.count()}")  # 0
print(f"collect(): {empty.collect()}")  # []
try:
    empty.first()  # Will throw ValueError
except ValueError as e:
    print(f"first(): Error - {e}")  # RDD is empty
try:
    empty.reduce(lambda a, b: a + b)  # Will throw ValueError
except ValueError as e:
    print(f"reduce(): Error - {e}")  # Can't reduce empty

# ---- LEVEL 9 ----
print("\n=== Level 9: Production Text Pipeline ===")
def clean_text(text):
    """Remove punctuation and convert to lowercase."""
    return re.sub(r'[^\w\s]', '', text.lower())  # Remove non-word chars

text_data = sc.parallelize([  # Sample text
    "Apache Spark is FAST! Really fast.",
    "Spark, the big-data engine, processes data.",
    "Data engineering with Spark: powerful & efficient."
])
top_words = (text_data
    .map(clean_text)  # Clean each line
    .flatMap(lambda l: l.split())  # Split into words
    .filter(lambda w: len(w) > 2)  # Remove short words
    .map(lambda w: (w, 1))  # Pair up
    .reduceByKey(lambda a, b: a + b)  # Count
    .sortBy(lambda x: -x[1])  # Sort desc
    .take(10)  # Top 10 only
)
print("Top 10 words:")
for word, count in top_words:
    print(f"  '{word}': {count}")

print("\n\u2705 All homework levels complete!")