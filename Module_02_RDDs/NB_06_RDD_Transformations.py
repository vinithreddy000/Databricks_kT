# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 06: RDD Transformations (Lazy Operations)
# MAGIC # Module: RDDs (Resilient Distributed Datasets)
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 50 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: Writing a Recipe vs Cooking
# MAGIC
# MAGIC **Transformations are like WRITING a recipe:**
# MAGIC - You write "peel potatoes" (nothing peeled yet)
# MAGIC - You write "boil for 20 minutes" (nothing boiled yet)
# MAGIC - You write "add butter and mash" (nothing mashed yet)
# MAGIC
# MAGIC The recipe is just a PLAN. No food is prepared until someone says: **"OK, now cook!"**
# MAGIC
# MAGIC In Spark:
# MAGIC - **Writing the recipe** = applying transformations (lazy, just planning)
# MAGIC - **Saying 'cook!'** = calling an action (triggers execution)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Two Categories of Transformations
# MAGIC
# MAGIC | Category | What Happens | Examples | Performance Impact |
# MAGIC |----------|-------------|----------|-------------------|
# MAGIC | **Narrow** | Each input partition maps to exactly ONE output partition | map, filter, flatMap, mapPartitions | Fast (no network I/O) |
# MAGIC | **Wide** | Input partitions contribute to MULTIPLE output partitions | groupByKey, reduceByKey, sortBy, distinct, join | Slow (requires shuffle over network) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Narrow vs Wide — Visualized
# MAGIC
# MAGIC ```
# MAGIC NARROW (no shuffle):              WIDE (shuffle required):
# MAGIC
# MAGIC Partition 0 → Partition 0         Partition 0 ─┬──────▶ Partition 0
# MAGIC Partition 1 → Partition 1         Partition 1 ─┼──────▶ Partition 1
# MAGIC Partition 2 → Partition 2         Partition 2 ─┴──────▶ Partition 2
# MAGIC
# MAGIC Each worker processes             Data must be SENT across
# MAGIC its own data independently.       the network to redistribute.
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Complete List of RDD Transformations
# MAGIC
# MAGIC **Narrow (fast, no shuffle):**
# MAGIC - `map(f)` — apply f to each element
# MAGIC - `flatMap(f)` — apply f and flatten results
# MAGIC - `filter(f)` — keep elements where f returns True
# MAGIC - `mapPartitions(f)` — apply f to each partition as a whole
# MAGIC - `mapPartitionsWithIndex(f)` — same but with partition index
# MAGIC - `sample(withReplacement, fraction)` — random sample
# MAGIC
# MAGIC **Wide (slow, causes shuffle):**
# MAGIC - `groupByKey()` — group values by key (expensive!)
# MAGIC - `reduceByKey(f)` — reduce values by key (much better)
# MAGIC - `sortBy(f)` / `sortByKey()` — sort elements
# MAGIC - `distinct()` — remove duplicates
# MAGIC - `union(other)` — combine two RDDs
# MAGIC - `intersection(other)` — common elements
# MAGIC - `subtract(other)` — elements not in other
# MAGIC - `join(other)` — join two pair RDDs

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### The Transformation Pipeline
# MAGIC
# MAGIC ```
# MAGIC   Your Code:                What Spark Builds (DAG):
# MAGIC   ──────────                 ───────────────────────
# MAGIC
# MAGIC   rdd = sc.parallelize()    [ParallelCollectionRDD]
# MAGIC        │                            │
# MAGIC   rdd2 = rdd.map(...)       [MappedRDD]   (narrow)
# MAGIC        │                            │
# MAGIC   rdd3 = rdd2.filter(...)   [FilteredRDD] (narrow)
# MAGIC        │                            │
# MAGIC   rdd4 = rdd3.groupByKey()  [ShuffledRDD] (wide → new stage!)
# MAGIC        │                            │
# MAGIC   rdd4.collect()             EXECUTE! (action triggers it all)
# MAGIC ```
# MAGIC
# MAGIC ### Stages and Shuffles
# MAGIC
# MAGIC - Narrow transformations are combined into a **single stage** (pipelined together)
# MAGIC - A wide transformation creates a **new stage** (requires data to move between machines)
# MAGIC - The fewer stages, the faster your job
# MAGIC
# MAGIC ### Key Performance Insight
# MAGIC
# MAGIC ```
# MAGIC   FAST (narrow, pipelined):       SLOW (wide, shuffle):
# MAGIC   
# MAGIC   map → filter → map              groupByKey
# MAGIC   (all in one stage,               (all data sent across
# MAGIC    no network I/O)                  the network, written
# MAGIC                                     to disk, re-read)
# MAGIC ```
# MAGIC
# MAGIC ### The Golden Rule
# MAGIC
# MAGIC > Minimize the number of **wide transformations** in your pipeline.  
# MAGIC > Each wide transformation = a shuffle = network I/O = SLOW.
# MAGIC
# MAGIC Specifically: **Prefer `reduceByKey` over `groupByKey`** because `reduceByKey` does a partial aggregation BEFORE the shuffle, sending less data over the network.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Narrow Transformations
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 1: All Narrow Transformations
# ═══════════════════════════════════════════════════════

sc = spark.sparkContext  # Get SparkContext

print("=== Narrow Transformations (No Shuffle) ===")
print("These are FAST because each partition is processed independently.")
print()

# Create base RDD
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)  # 10 numbers, 2 partitions

# 1. map(f) — apply function to each element, returns one output per input
print("1. map(x * 10):")
mapped = numbers.map(lambda x: x * 10)  # Multiply each by 10
print(f"   {mapped.collect()}")  # [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

# 2. filter(f) — keep elements where function returns True
print("\n2. filter(x > 5):")
filtered = numbers.filter(lambda x: x > 5)  # Keep only numbers > 5
print(f"   {filtered.collect()}")  # [6, 7, 8, 9, 10]

# 3. flatMap(f) — apply function, flatten results (one input can produce many outputs)
print("\n3. flatMap(x → [x, x*10]):")
flat = numbers.flatMap(lambda x: [x, x * 10])  # Each number becomes TWO numbers
print(f"   {flat.collect()}")  # [1, 10, 2, 20, 3, 30, ...]

# 4. mapPartitions(f) — apply function to ENTIRE partition at once
print("\n4. mapPartitions (sum each partition):")
def sum_partition(iterator):  # Function that takes an iterator of elements
    total = sum(iterator)  # Sum all elements in this partition
    yield total  # Return the sum as a single element

partition_sums = numbers.mapPartitions(sum_partition)  # Sum within each partition
print(f"   Partition sums: {partition_sums.collect()}")  # [15, 40] (1+2+3+4+5=15, 6+7+8+9+10=40)

# 5. mapPartitionsWithIndex(f) — same but you know which partition you're in
print("\n5. mapPartitionsWithIndex (label partitions):")
def label_partition(index, iterator):  # Function with partition index
    for item in iterator:  # Loop through elements
        yield (f"partition_{index}", item)  # Label each with its partition

labeled = numbers.mapPartitionsWithIndex(label_partition)  # Apply with index
print(f"   {labeled.collect()[:4]}...")  # [('partition_0', 1), ('partition_0', 2), ...]

# 6. sample(withReplacement, fraction, seed) — random sample
print("\n6. sample(fraction=0.5):")
sampled = numbers.sample(False, 0.5, seed=42)  # ~50% sample, no replacement
print(f"   Sample: {sampled.collect()}")  # Random ~5 elements

print("\n--- Key Insight: All of these are FAST (no shuffle) ---")

# Expected Output:
# Shows results of each narrow transformation
# All execute within a single stage (no data movement between machines)

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Wide Transformations
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 2: Wide Transformations (Cause Shuffle)
# ═══════════════════════════════════════════════════════

print("=== Wide Transformations (Cause Shuffle) ===")
print("These are SLOWER because data must be sent across the network.")
print()

# Create a key-value RDD (pair RDD)
words = sc.parallelize([  # (category, item) pairs
    ("fruit", "apple"), ("veggie", "carrot"), ("fruit", "banana"),
    ("veggie", "pea"), ("fruit", "cherry"), ("veggie", "corn"),
    ("fruit", "date"), ("veggie", "broccoli")
], 4)  # 4 partitions

# 1. groupByKey() — group all values by key (EXPENSIVE!)
print("1. groupByKey():")
grouped = words.groupByKey()  # Groups all values for each key
for key, values in grouped.collect():  # Print results
    print(f"   {key}: {list(values)}")  # Show grouped values

# 2. reduceByKey() — combine values by key (MUCH BETTER than groupByKey!)
print("\n2. reduceByKey():")
number_pairs = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("b", 4), ("a", 5)])  # Key-value pairs
reduced = number_pairs.reduceByKey(lambda a, b: a + b)  # Sum values per key
print(f"   Sums: {reduced.collect()}")  # [('a', 9), ('b', 6)]

# 3. sortBy(f) — sort elements by a function
print("\n3. sortBy():")
nums = sc.parallelize([5, 3, 8, 1, 9, 2, 7, 4, 6])  # Unsorted
sorted_asc = nums.sortBy(lambda x: x)  # Sort ascending
sorted_desc = nums.sortBy(lambda x: -x)  # Sort descending
print(f"   Ascending: {sorted_asc.collect()}")  # [1, 2, 3, 4, 5, 6, 7, 8, 9]
print(f"   Descending: {sorted_desc.collect()}")  # [9, 8, 7, 6, 5, 4, 3, 2, 1]

# 4. distinct() — remove duplicates (requires shuffle to compare across partitions)
print("\n4. distinct():")
with_dups = sc.parallelize([1, 2, 2, 3, 3, 3, 4, 4, 4, 4])  # Has duplicates
unique = with_dups.distinct()  # Remove duplicates
print(f"   Original: [1,2,2,3,3,3,4,4,4,4]")
print(f"   Distinct: {sorted(unique.collect())}")  # [1, 2, 3, 4]

print("\n--- WARNING ---")
print("groupByKey() is DANGEROUS on large data!")
print("It loads ALL values for a key into memory on one machine.")
print("ALWAYS prefer reduceByKey() or aggregateByKey() instead.")

# Expected Output:
# groupByKey: fruit: [apple, banana, cherry, date], veggie: [...]
# reduceByKey: ('a', 9), ('b', 6)
# sortBy: ascending and descending
# distinct: [1, 2, 3, 4]

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: map vs flatMap Comparison
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 3: Deep Dive — map vs flatMap vs filter
# ═══════════════════════════════════════════════════════

print("=== Detailed Comparison: map vs flatMap vs filter ===")
print()

data = sc.parallelize(["hello world", "spark is great", "python rocks"])  # 3 strings

# map: 1 input → 1 output (ALWAYS same number of elements)
print("--- map (1:1) ---")
uppered = data.map(lambda s: s.upper())  # Each string → uppercase string
print(f"Input count: {data.count()}, Output count: {uppered.count()}")  # Both 3
print(f"Result: {uppered.collect()}")  # ['HELLO WORLD', 'SPARK IS GREAT', 'PYTHON ROCKS']

print("\n--- map with split (1:1, but nested!) ---")
split_map = data.map(lambda s: s.split(" "))  # Each string → LIST of words
print(f"Input count: {data.count()}, Output count: {split_map.count()}")  # Both 3!
print(f"Result: {split_map.collect()}")  # [['hello', 'world'], ['spark', 'is', 'great'], ...]
print("Problem: We got a list of LISTS, not individual words!")

# flatMap: 1 input → 0 or more outputs (can change element count)
print("\n--- flatMap (1:many, flattened) ---")
split_flat = data.flatMap(lambda s: s.split(" "))  # Each string → individual words
print(f"Input count: {data.count()}, Output count: {split_flat.count()}")  # 3 → 7!
print(f"Result: {split_flat.collect()}")  # ['hello', 'world', 'spark', 'is', 'great', ...]
print("Perfect: Individual words, not nested!")

# flatMap can also REMOVE elements (return empty list)
print("\n--- flatMap as filter (return [] to skip) ---")
def keep_long_words(sentence):  # Custom function
    words = sentence.split(" ")  # Split into words
    return [w for w in words if len(w) > 4]  # Only return words > 4 chars

long_words = data.flatMap(keep_long_words)  # Only long words survive
print(f"Long words only: {long_words.collect()}")  # ['hello', 'world', 'spark', 'great', 'python', 'rocks']

# filter: same number or fewer elements (never more)
print("\n--- filter (keep or drop) ---")
long_strings = data.filter(lambda s: len(s) > 12)  # Keep strings > 12 chars
print(f"Strings > 12 chars: {long_strings.collect()}")  # ['spark is great', 'python rocks']

print("\n--- Summary ---")
print("map:     1 input → exactly 1 output (same count always)")
print("flatMap: 1 input → 0 or more outputs (count can change)")
print("filter:  1 input → 0 or 1 output (count can only decrease)")

# Expected Output:
# Demonstrates the key difference in element counts between map, flatMap, filter

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate: reduceByKey vs groupByKey
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# WHY reduceByKey is 10x Better Than groupByKey
# ═══════════════════════════════════════════════════════

import time  # For timing comparisons
sc = spark.sparkContext  # Ensure sc is available

print("=== reduceByKey vs groupByKey ===")
print()

# Create large dataset: 1M (key, value) pairs with 100 unique keys
data = sc.parallelize(range(1000000), 8)  # 1M numbers in 8 partitions
pair_data = data.map(lambda x: (str(x % 100), 1))  # 100 unique keys

# Method 1: groupByKey + sum (BAD)
print("--- groupByKey (BAD) ---")
start = time.time()
result_group = pair_data.groupByKey().mapValues(sum)  # Group then sum
result_group.count()  # Trigger
group_time = time.time() - start
print(f"  Time: {group_time:.3f}s")

# Method 2: reduceByKey (GOOD)
print("\n--- reduceByKey (GOOD) ---")
start = time.time()
result_reduce = pair_data.reduceByKey(lambda a, b: a + b)  # Local combine first!
result_reduce.count()  # Trigger
reduce_time = time.time() - start
print(f"  Time: {reduce_time:.3f}s")

print(f"\nreduceByKey is {group_time/reduce_time:.1f}x faster!")
print("Rule: ALWAYS use reduceByKey over groupByKey for aggregations!")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced: mapPartitions for Batch Processing
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Examples
# mapPartitions for Batch Processing
# ═══════════════════════════════════════════════════════

print("=== mapPartitions: Batch Processing Pattern ===")
print()

# Real scenario: Process records in batches (DB writes, API calls)
data = sc.parallelize(range(1000), 4)  # 1000 items in 4 partitions

def batch_processor(partition_iterator):
    """Process a whole partition as one batch."""
    batch = list(partition_iterator)  # Collect all items in this partition
    batch_size = len(batch)  # Size of this batch
    # In production: open DB connection here, write batch, close connection
    # This is WAY more efficient than connecting per-element
    processed = [x * 2 for x in batch]  # Process the batch
    yield (batch_size, sum(processed))  # Return batch stats

results = data.mapPartitions(batch_processor).collect()  # Run batch processing
print("Batch results (size, sum):")
for batch_size, batch_sum in results:  # Print each batch's stats
    print(f"  Batch of {batch_size} items, sum = {batch_sum}")

print("\n--- mapPartitionsWithIndex ---")
def labeled_processor(index, iterator):
    """Same but knows which partition it's processing."""
    items = list(iterator)  # Get all items
    yield f"Partition {index}: {len(items)} items, first={items[0] if items else 'empty'}"

labeled = data.mapPartitionsWithIndex(labeled_processor).collect()  # Apply
for result in labeled:  # Print
    print(f"  {result}")

print("\nWhen to use mapPartitions:")
print("  - Database batch writes (open connection once per partition)")
print("  - ML model inference (load model once per partition)")
print("  - API calls (batch requests instead of one-per-row)")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: groupByKey for Aggregation
# MAGIC **Fix:** Use `reduceByKey`, `aggregateByKey`, or `combineByKey`.
# MAGIC
# MAGIC ### Mistake #2: Not Understanding Narrow vs Wide
# MAGIC **Fix:** Minimize wide transformations. Each one adds a stage boundary and shuffle.
# MAGIC
# MAGIC ### Mistake #3: map When flatMap Needed
# MAGIC **Fix:** If your function returns a collection and you want it flattened, use `flatMap`.
# MAGIC
# MAGIC ### Mistake #4: Forgetting Lazy Evaluation
# MAGIC **Fix:** Transformations do nothing until an action (collect, count, etc.) is called.
# MAGIC
# MAGIC ### Mistake #5: Unnecessary Shuffles
# MAGIC **Fix:** Avoid unneeded `distinct()`, `sortBy()`, or `repartition()` calls.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC ### Level 1: Run Beginner Example 1 and identify narrow transformations.
# MAGIC ### Level 2: Change map to square numbers instead of *10.
# MAGIC ### Level 3: Use flatMap + filter to get words > 3 chars from sentences.
# MAGIC ### Level 4: reduceByKey to sum student scores.
# MAGIC ### Level 5: Full pipeline: numbers → key-value → reduce → sort.
# MAGIC ### Level 6: Design a log parser using RDD transformations.
# MAGIC ### Level 7: Benchmark groupByKey vs reduceByKey on 5M records.
# MAGIC ### Level 8: Test edge cases with empty RDDs and empty partitions.
# MAGIC ### Level 9: Write a mapPartitions batch DB writer.
# MAGIC ### Level 10: Teach narrow vs wide with diagrams and examples.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════
sc = spark.sparkContext

# Level 2: Square numbers
print("=== Level 2 ===")
nums = sc.parallelize(range(1, 11))
print(f"Squared: {nums.map(lambda x: x**2).collect()}")  # [1,4,9,16,25,...100]

# Level 3: flatMap + filter
print("\n=== Level 3 ===")
sents = sc.parallelize(["Apache Spark is fast", "Big data processing"])
result = sents.flatMap(lambda s: s.lower().split(" ")).filter(lambda w: len(w) > 3)
print(f"Long words: {result.collect()}")  # ['apache', 'spark', 'fast', 'data', 'processing']

# Level 4: reduceByKey
print("\n=== Level 4 ===")
scores = sc.parallelize([("Alice",85),("Bob",90),("Alice",92),("Bob",78)])
print(f"Totals: {scores.reduceByKey(lambda a,b: a+b).collect()}")  # Alice=177, Bob=168

# Level 5: Full pipeline
print("\n=== Level 5 ===")
pipeline = (sc.parallelize(range(1, 101))
    .map(lambda x: (x % 5, x))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: -x[1]))
print(f"Result: {pipeline.collect()}")

print("\n\u2705 All solutions complete!")