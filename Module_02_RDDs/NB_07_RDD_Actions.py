# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 07: RDD Actions (Triggers Execution)
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
# MAGIC ### Real-World Analogy: Pressing the "Print" Button
# MAGIC
# MAGIC Imagine you're writing a document:
# MAGIC - You type sentences, bold words, add images (these are **transformations** — just editing)
# MAGIC - Nothing comes out of the printer until you press **PRINT** (this is an **action**)
# MAGIC
# MAGIC In Spark:
# MAGIC - Transformations = editing the document (lazy, just planning)
# MAGIC - **Actions = pressing PRINT** (triggers all the work, produces a result)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What Makes an Action Special?
# MAGIC
# MAGIC | Property | Transformation | Action |
# MAGIC |----------|---------------|--------|
# MAGIC | Returns | A new RDD | A value to the driver (or writes to disk) |
# MAGIC | Execution | Lazy (nothing happens) | Triggers the ENTIRE pipeline |
# MAGIC | Example | map, filter, flatMap | collect, count, take, save |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Categories of Actions
# MAGIC
# MAGIC 1. **Retrieve data to driver:** `collect()`, `take(n)`, `first()`, `takeOrdered(n)`, `takeSample()`
# MAGIC 2. **Compute a value:** `count()`, `reduce()`, `fold()`, `aggregate()`
# MAGIC 3. **Iterate without returning:** `foreach()`, `foreachPartition()`
# MAGIC 4. **Save to storage:** `saveAsTextFile()`, `saveAsSequenceFile()`, `saveAsObjectFile()`
# MAGIC 5. **Count by key/value:** `countByValue()`, `countByKey()`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Danger of `collect()`
# MAGIC
# MAGIC `collect()` brings **ALL** data to one machine (the driver). If your RDD has 1 billion rows, your driver node will crash with an OutOfMemoryError.
# MAGIC
# MAGIC **Rule of thumb:**
# MAGIC - Data < 1 million rows → `collect()` is probably safe
# MAGIC - Data > 1 million rows → Use `take(n)`, `first()`, or save to a file

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### What Happens When You Call an Action
# MAGIC
# MAGIC ```
# MAGIC    You write:             What Spark does:
# MAGIC    ──────────             ─────────────────
# MAGIC    
# MAGIC    rdd.map(...)          → Adds to the DAG plan (does nothing)
# MAGIC    rdd.filter(...)       → Adds to the DAG plan (does nothing)
# MAGIC    rdd.count()           → TRIGGERS! Spark now:
# MAGIC                             1. Builds the execution plan (DAG)
# MAGIC                             2. Optimizes the plan
# MAGIC                             3. Splits into stages
# MAGIC                             4. Sends tasks to executors
# MAGIC                             5. Executors process partitions
# MAGIC                             6. Results returned to driver
# MAGIC ```
# MAGIC
# MAGIC ### Actions That Return Data to Driver
# MAGIC
# MAGIC ```
# MAGIC    collect()      → ALL elements as a Python list
# MAGIC    take(n)        → First n elements as a list
# MAGIC    first()        → Just the first element
# MAGIC    takeOrdered(n) → Smallest n elements (sorted)
# MAGIC    takeSample()   → Random n elements
# MAGIC    top(n)         → Largest n elements
# MAGIC ```
# MAGIC
# MAGIC ### Actions That Compute a Single Value
# MAGIC
# MAGIC ```
# MAGIC    count()        → Number of elements (long)
# MAGIC    sum()          → Sum of all numeric elements
# MAGIC    mean()         → Average value
# MAGIC    max() / min()  → Largest / smallest
# MAGIC    reduce(f)      → Combine all with function f
# MAGIC    fold(zero, f)  → Like reduce but with initial value
# MAGIC    aggregate()    → Most flexible: different combine logic
# MAGIC ```
# MAGIC
# MAGIC ### Actions That Write to Storage
# MAGIC
# MAGIC ```
# MAGIC    saveAsTextFile(path)      → One file per partition
# MAGIC    saveAsSequenceFile(path)  → Hadoop format
# MAGIC    saveAsObjectFile(path)    → Java serialized
# MAGIC ```
# MAGIC
# MAGIC ### Memory Safety Guide
# MAGIC
# MAGIC ```
# MAGIC    Safe (small result):       Dangerous (entire dataset):
# MAGIC    ────────────────────       ────────────────────────
# MAGIC    count()                    collect() on big RDD
# MAGIC    take(100)                  toLocalIterator() carelessly
# MAGIC    first()                    toPandas() on big DF
# MAGIC    reduce()                   
# MAGIC    sum() / mean()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Retrieve Actions
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 1: All "Retrieve" Actions
# ═══════════════════════════════════════════════════════

sc = spark.sparkContext  # Get SparkContext

print("=== Actions That Retrieve Data to Driver ===")
print()

# Create sample RDD
numbers = sc.parallelize([42, 7, 15, 3, 99, 56, 23, 81, 12, 67])  # 10 random numbers

# 1. collect() — bring ALL elements back as a Python list
print("1. collect() — ALL elements:")
all_data = numbers.collect()  # Returns entire RDD as a list
print(f"   {all_data}")  # [42, 7, 15, 3, 99, 56, 23, 81, 12, 67]
print(f"   Type: {type(all_data)}")  # <class 'list'>

# 2. take(n) — get the first n elements
print("\n2. take(n) — first n elements:")
first_3 = numbers.take(3)  # Get first 3 elements
print(f"   take(3): {first_3}")  # [42, 7, 15]

# 3. first() — get just the first element
print("\n3. first() — single element:")
first_one = numbers.first()  # Get the very first element
print(f"   first(): {first_one}")  # 42

# 4. takeOrdered(n) — get the smallest n elements (sorted ascending)
print("\n4. takeOrdered(n) — smallest n (sorted):")
smallest_3 = numbers.takeOrdered(3)  # Smallest 3
print(f"   takeOrdered(3): {smallest_3}")  # [3, 7, 12]
# Get largest 3 by reversing the sort
largest_3 = numbers.takeOrdered(3, key=lambda x: -x)  # Largest 3
print(f"   takeOrdered(3, desc): {largest_3}")  # [99, 81, 67]

# 5. top(n) — get the largest n elements
print("\n5. top(n) — largest n:")
top_3 = numbers.top(3)  # Largest 3 elements
print(f"   top(3): {top_3}")  # [99, 81, 67]

# 6. takeSample(withReplacement, num, seed) — random sample
print("\n6. takeSample() — random sample:")
sample = numbers.takeSample(False, 4, seed=42)  # 4 random elements, no replacement
print(f"   takeSample(4): {sample}")  # Random 4 elements

print("\n--- Safety Tip ---")
print("collect() is DANGEROUS on large data! Use take(n) or first() instead.")

# Expected Output:
# collect(): full list
# take(3): first 3 elements
# first(): single element
# takeOrdered: sorted smallest/largest
# top: largest n
# takeSample: random elements

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Compute Actions
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 2: All "Compute" Actions
# ═══════════════════════════════════════════════════════

print("=== Actions That Compute a Value ===")
print()

# Create sample RDD
numbers = sc.parallelize([10, 20, 30, 40, 50])  # 5 numbers

# 1. count() — number of elements
print("1. count():")
print(f"   Elements: {numbers.count()}")  # 5

# 2. sum() — total of all elements
print("\n2. sum():")
print(f"   Total: {numbers.sum()}")  # 150

# 3. mean() — average
print("\n3. mean():")
print(f"   Average: {numbers.mean()}")  # 30.0

# 4. max() and min()
print("\n4. max() and min():")
print(f"   Max: {numbers.max()}")  # 50
print(f"   Min: {numbers.min()}")  # 10

# 5. reduce(f) — combine all elements using function f
print("\n5. reduce():")
total = numbers.reduce(lambda a, b: a + b)  # Sum: 10+20+30+40+50
print(f"   reduce(+): {total}")  # 150
product = numbers.reduce(lambda a, b: a * b)  # Product: 10*20*30*40*50
print(f"   reduce(*): {product}")  # 12000000
maximum = numbers.reduce(lambda a, b: a if a > b else b)  # Custom max
print(f"   reduce(max): {maximum}")  # 50

# 6. fold(zeroValue, f) — like reduce but with starting value
print("\n6. fold():")
fold_sum = numbers.fold(0, lambda a, b: a + b)  # Start at 0, then add
print(f"   fold(0, +): {fold_sum}")  # 150
# WARNING: fold applies zero value PER PARTITION + once for combining!

# 7. countByValue() — count occurrences of each value
print("\n7. countByValue():")
colors = sc.parallelize(["red", "blue", "red", "green", "blue", "red"])  # With duplicates
counts = colors.countByValue()  # Returns a dict
print(f"   Counts: {dict(counts)}")  # {'red': 3, 'blue': 2, 'green': 1}

print("\n--- Key Insight ---")
print("All of these return a SINGLE value to the driver (safe for memory).")

# Expected Output:
# count: 5, sum: 150, mean: 30.0
# max: 50, min: 10
# reduce examples
# countByValue: {'red': 3, 'blue': 2, 'green': 1}

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: foreach and countByKey
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: foreach and countByKey
# ═══════════════════════════════════════════════════════

print("=== foreach, foreachPartition, countByKey ===")
print()

# foreach() runs a function on executors (returns nothing to driver)
print("1. foreach() — side-effect only action:")
numbers = sc.parallelize([1, 2, 3, 4, 5])  # Simple numbers
counter = sc.accumulator(0)  # Accumulator to prove foreach ran
numbers.foreach(lambda x: counter.add(x))  # Add each number to counter
print(f"   Accumulator after foreach: {counter.value}")  # 15
print("   Note: print() inside foreach goes to executor logs, not notebook!")

# foreachPartition() — more efficient for I/O operations
print("\n2. foreachPartition() — per-partition processing:")
data = sc.parallelize(range(1, 13), 4)  # 12 items in 4 partitions
partition_counter = sc.accumulator(0)  # Count partitions
def count_partition(iterator):
    items = list(iterator)  # Materialize partition
    partition_counter.add(1)  # Increment counter
data.foreachPartition(count_partition)  # Process each partition
print(f"   Partitions processed: {partition_counter.value}")  # 4

# countByKey() — count items per key in a pair RDD
print("\n3. countByKey() — count per key:")
pair_rdd = sc.parallelize([("fruit","apple"),("veggie","carrot"),("fruit","banana"),("fruit","cherry"),("veggie","pea")])
print(f"   Counts: {dict(pair_rdd.countByKey())}")  # {'fruit': 3, 'veggie': 2}

print("\n--- Key Differences ---")
print("foreach: one call PER ELEMENT (use for simple side effects)")
print("foreachPartition: one call PER PARTITION (better for DB/API writes)")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate: aggregate() Deep Dive
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: aggregate() The Most Flexible Action
# ═══════════════════════════════════════════════════════

import sys  # For sys.maxsize

print("=== aggregate() — Compute Multiple Stats in One Pass ===")
print()

# aggregate(zeroValue, seqOp, combOp)
# - zeroValue: starting accumulator
# - seqOp: how to merge one element into the accumulator (within partition)
# - combOp: how to merge two accumulators (across partitions)

# Example 1: Compute (sum, count) to get average
print("--- Example 1: Average via (sum, count) ---")
numbers = sc.parallelize([10, 20, 30, 40, 50], 2)  # 5 numbers, 2 partitions
result = numbers.aggregate(
    (0, 0),  # zeroValue: (sum=0, count=0)
    lambda acc, val: (acc[0] + val, acc[1] + 1),  # seqOp: add to sum, increment count
    lambda a, b: (a[0] + b[0], a[1] + b[1])  # combOp: merge two (sum,count) pairs
)
print(f"   (sum, count) = {result}")  # (150, 5)
print(f"   Average = {result[0]/result[1]}")  # 30.0

# Example 2: Find (min, max) simultaneously
print("\n--- Example 2: Min and Max in one pass ---")
data = sc.parallelize([42, 7, 99, 3, 56, 81, 15])
min_max = data.aggregate(
    (sys.maxsize, -sys.maxsize),  # (min=very_big, max=very_small)
    lambda acc, v: (min(acc[0], v), max(acc[1], v)),  # Update min/max
    lambda a, b: (min(a[0], b[0]), max(a[1], b[1]))  # Merge
)
print(f"   Min={min_max[0]}, Max={min_max[1]}")  # Min=3, Max=99

# Example 3: Compute (sum, count, min, max) all at once
print("\n--- Example 3: Full stats in one pass ---")
stats = data.aggregate(
    (0, 0, sys.maxsize, -sys.maxsize),  # (sum, count, min, max)
    lambda a, v: (a[0]+v, a[1]+1, min(a[2],v), max(a[3],v)),  # Accumulate
    lambda a, b: (a[0]+b[0], a[1]+b[1], min(a[2],b[2]), max(a[3],b[3]))  # Merge
)
print(f"   Sum={stats[0]}, Count={stats[1]}, Min={stats[2]}, Max={stats[3]}")
print(f"   Mean={stats[0]/stats[1]:.1f}")

print("\n--- When to use aggregate ---")
print("Use when result type differs from element type, or need multiple stats in one pass.")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced: Performance and Safety
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Why collect() is Dangerous + Safe Patterns
# ═══════════════════════════════════════════════════════

import time  # For timing

print("=== collect() Safety and Alternatives ===")
print()

# Create a large RDD to demonstrate performance
large_rdd = sc.parallelize(range(5000000), 8)  # 5 million elements

# count() is safe: returns ONE number
start = time.time()
result = large_rdd.count()
print(f"1. count(): {result:,} in {time.time()-start:.3f}s (SAFE)")

# take() is safe: returns small list
start = time.time()
result = large_rdd.take(5)
print(f"2. take(5): {result} in {time.time()-start:.3f}s (SAFE)")

# reduce() is safe: returns ONE computed value
start = time.time()
result = large_rdd.reduce(lambda a, b: a + b)
print(f"3. reduce(+): {result:,} in {time.time()-start:.3f}s (SAFE)")

# toLocalIterator() — iterate without loading all into memory
print("\n4. toLocalIterator() — memory-safe iteration:")
small_rdd = sc.parallelize(range(10))
iterator = small_rdd.toLocalIterator()  # Yields one element at a time
first_3 = [next(iterator) for _ in range(3)]  # Get 3 elements
print(f"   First 3 via iterator: {first_3}")
print("   Doesn't load ALL data at once like collect()")

# isEmpty() — check if empty efficiently
print("\n5. isEmpty():")
print(f"   Empty: {sc.parallelize([]).isEmpty()}")  # True
print(f"   Non-empty: {sc.parallelize([1]).isEmpty()}")  # False

# Multiple actions = multiple executions (cache to fix)
print("\n6. Multiple actions trigger multiple jobs:")
rdd = sc.parallelize(range(1000000)).map(lambda x: x * 2).filter(lambda x: x > 500000)
start = time.time()
c1 = rdd.count()  # First execution
t1 = time.time() - start
start = time.time()
c2 = rdd.sum()  # Second execution (recomputes everything!)
t2 = time.time() - start
print(f"   count: {c1:,} in {t1:.3f}s")
print(f"   sum: {c2:,} in {t2:.3f}s")
print("   Both triggered full pipeline! Use .cache() to avoid this.")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: collect() on Large RDD
# MAGIC **Fix:** Use `take(n)`, `first()`, or save to file.
# MAGIC
# MAGIC ### Mistake #2: foreach(print) Shows Nothing
# MAGIC **Why:** `print()` runs on executors, not the driver notebook.  
# MAGIC **Fix:** Use `collect()` + loop for small data, or accumulators.
# MAGIC
# MAGIC ### Mistake #3: Multiple Actions Re-Execute Everything
# MAGIC **Fix:** `.cache()` or `.persist()` the RDD if calling multiple actions.
# MAGIC
# MAGIC ### Mistake #4: reduce() on Empty RDD Crashes
# MAGIC **Fix:** Check `isEmpty()` first, or use `fold(zeroValue, f)` which handles empty.
# MAGIC
# MAGIC ### Mistake #5: Confusing fold() Zero Value
# MAGIC **Issue:** Zero value is applied per-partition AND when combining.  
# MAGIC **Fix:** Use `reduce()` for simple cases. Only use `fold()` when you understand the semantics.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC ### Level 1: Run Example 1 and note differences between collect, take, and first.
# MAGIC ### Level 2: Use takeOrdered(5) to get the 5 smallest from random numbers.
# MAGIC ### Level 3: Use reduce() to find the product of numbers 1 through 10.
# MAGIC ### Level 4: Use countByValue() on a list of colors.
# MAGIC ### Level 5: Use aggregate() to compute (sum, count, max) in one pass.
# MAGIC ### Level 6: Write a safe_preview(rdd, n) function using take().
# MAGIC ### Level 7: Time count() vs collect() on 10M elements.
# MAGIC ### Level 8: Handle reduce() on an empty RDD gracefully.
# MAGIC ### Level 9: Write foreachPartition that simulates batch DB writes.
# MAGIC ### Level 10: Explain all action categories with real-world use cases.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════
import time, sys

# Level 2: takeOrdered
print("=== Level 2 ===")
nums = sc.parallelize([42, 7, 99, 3, 56, 81, 15, 23, 67, 12])
print(f"Smallest 5: {nums.takeOrdered(5)}")  # [3, 7, 12, 15, 23]

# Level 3: reduce product
print("\n=== Level 3 ===")
product = sc.parallelize(range(1, 11)).reduce(lambda a, b: a * b)
print(f"Product 1-10: {product}")  # 3628800 (10!)

# Level 4: countByValue
print("\n=== Level 4 ===")
colors = sc.parallelize(["red","blue","red","green","blue","red","yellow"])
print(f"Colors: {dict(colors.countByValue())}")  # red:3, blue:2, green:1, yellow:1

# Level 5: aggregate multi-stat
print("\n=== Level 5 ===")
data = sc.parallelize([10, 20, 5, 40, 15, 30])
stats = data.aggregate(
    (0, 0, sys.maxsize),  # (sum, count, max_initial)
    lambda a, v: (a[0]+v, a[1]+1, max(a[2], v) if a[2] != sys.maxsize else v),
    lambda a, b: (a[0]+b[0], a[1]+b[1], max(a[2], b[2]))
)
print(f"Sum={stats[0]}, Count={stats[1]}, Max={stats[2]}")

# Level 6: Safe preview
print("\n=== Level 6 ===")
def safe_preview(rdd, n=5):
    """Safely preview first n elements."""
    sample = rdd.take(n)  # Only takes n elements (safe)
    print(f"Showing {len(sample)} of ~many elements:")
    for item in sample:
        print(f"  {item}")
safe_preview(sc.parallelize(range(1000000)), 3)

# Level 8: Empty RDD handling
print("\n=== Level 8 ===")
empty = sc.parallelize([])
try:
    empty.reduce(lambda a, b: a + b)
except ValueError as e:
    print(f"reduce on empty: {e}")
result = empty.fold(0, lambda a, b: a + b)  # Safe alternative
print(f"fold on empty: {result}")  # 0

# Level 9: foreachPartition batch writer
print("\n=== Level 9 ===")
batch_counter = sc.accumulator(0)
def batch_write(iterator):
    batch = list(iterator)
    batch_counter.add(len(batch))  # Track items written
    # In production: open connection, write batch, close
sc.parallelize(range(100), 4).foreachPartition(batch_write)
print(f"Total items batch-written: {batch_counter.value}")  # 100

print("\n\u2705 All homework complete!")