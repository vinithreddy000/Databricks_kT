# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 08: PairRDD Operations (Key-Value RDDs)
# MAGIC # Module: RDDs (Resilient Distributed Datasets)
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 55 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: A Phone Book
# MAGIC
# MAGIC A **PairRDD** is like a phone book:
# MAGIC - Each entry has a **key** (person's name) and a **value** (phone number)
# MAGIC - You can look things up by key
# MAGIC - You can group all entries by the same key (e.g., all "Smiths")
# MAGIC - You can join two phone books together (e.g., home numbers + work numbers)
# MAGIC
# MAGIC In Spark, a PairRDD is simply an RDD where every element is a **(key, value) tuple**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What Is a PairRDD?
# MAGIC
# MAGIC ```python
# MAGIC # Regular RDD (just elements):
# MAGIC [1, 2, 3, 4, 5]
# MAGIC
# MAGIC # PairRDD (key-value tuples):
# MAGIC [("Alice", 85), ("Bob", 90), ("Alice", 92), ("Bob", 78)]
# MAGIC #  key    value   key   value   key    value   key   value
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Why PairRDDs Matter
# MAGIC
# MAGIC PairRDDs unlock powerful operations:
# MAGIC - **groupByKey** — group values by key
# MAGIC - **reduceByKey** — aggregate values per key (10x faster!)
# MAGIC - **join** — combine two datasets by matching keys
# MAGIC - **sortByKey** — sort by key
# MAGIC - **mapValues** — transform values without touching keys
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Critical Rule: reduceByKey > groupByKey
# MAGIC
# MAGIC | Operation | groupByKey | reduceByKey |
# MAGIC |-----------|-----------|------------|
# MAGIC | How it works | Sends ALL values to one machine, THEN combines | Combines LOCALLY first, then sends subtotals |
# MAGIC | Network data | Entire dataset shuffled | Only subtotals shuffled |
# MAGIC | Memory risk | Can OOM if one key has millions of values | Safe — never holds all values in memory |
# MAGIC | Speed | Slow | 10x faster |
# MAGIC | When to use | When you truly need ALL values for a key | ALWAYS prefer this for aggregation |

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Creating a PairRDD
# MAGIC
# MAGIC ```python
# MAGIC # From a list of tuples:
# MAGIC pair_rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 3)])
# MAGIC
# MAGIC # From a regular RDD using map:
# MAGIC words_rdd = sc.parallelize(["hello", "world", "hello"])
# MAGIC pair_rdd = words_rdd.map(lambda w: (w, 1))  # (word, 1)
# MAGIC ```
# MAGIC
# MAGIC ### PairRDD Operations Overview
# MAGIC
# MAGIC ```
# MAGIC   TRANSFORMATIONS:                 ACTIONS:
# MAGIC   ────────────────                 ────────
# MAGIC   keys() / values()               countByKey()
# MAGIC   mapValues(f)                     collectAsMap()
# MAGIC   flatMapValues(f)                 lookup(key)
# MAGIC   groupByKey()
# MAGIC   reduceByKey(f)
# MAGIC   aggregateByKey(zero, seqOp, combOp)
# MAGIC   combineByKey(createCombiner, mergeValue, mergeCombiners)
# MAGIC   foldByKey(zero, f)
# MAGIC   sortByKey()
# MAGIC   
# MAGIC   JOINS:
# MAGIC   join(other)          → inner join
# MAGIC   leftOuterJoin(other) → left join
# MAGIC   rightOuterJoin(other)→ right join
# MAGIC   fullOuterJoin(other) → full outer join
# MAGIC   cogroup(other)       → group both RDDs by key
# MAGIC   subtractByKey(other) → remove keys present in other
# MAGIC ```
# MAGIC
# MAGIC ### reduceByKey vs groupByKey (Visual)
# MAGIC
# MAGIC ```
# MAGIC   groupByKey:                    reduceByKey:
# MAGIC   ────────────                    ────────────
# MAGIC   Partition 1: (A,1)(A,2)(B,3)   Partition 1: (A,1)(A,2)(B,3)
# MAGIC   Partition 2: (A,4)(B,5)(B,6)   Partition 2: (A,4)(B,5)(B,6)
# MAGIC        │                              │
# MAGIC        │ SHUFFLE ALL VALUES           │ LOCAL REDUCE FIRST
# MAGIC        ▼                              ▼
# MAGIC   A: [1,2,4] → sum → 7          P1: (A,3)(B,3)   [local sums!]
# MAGIC   B: [3,5,6] → sum → 14         P2: (A,4)(B,11)  [local sums!]
# MAGIC                                        │
# MAGIC   Sent over network: 6 values          │ SHUFFLE ONLY SUBTOTALS
# MAGIC                                        ▼
# MAGIC                                  A: 3+4=7, B: 3+11=14
# MAGIC                                  Sent over network: 4 values (less!)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Creating and Basic Ops
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: Creating PairRDDs
# ═══════════════════════════════════════════════════════

sc = spark.sparkContext  # Get SparkContext

print("=== PairRDD Basics ===")
print()

# Create a PairRDD from a list of tuples
scores = sc.parallelize([  # (student, score) pairs
    ("Alice", 85), ("Bob", 90), ("Alice", 92),
    ("Bob", 78), ("Charlie", 88), ("Alice", 95)
])
print(f"PairRDD: {scores.collect()}")

# keys() — get all keys
print(f"\nkeys(): {scores.keys().collect()}")  # ['Alice','Bob','Alice','Bob','Charlie','Alice']

# values() — get all values
print(f"values(): {scores.values().collect()}")  # [85, 90, 92, 78, 88, 95]

# mapValues(f) — apply function to values only (keys unchanged)
print(f"\nmapValues(+10): {scores.mapValues(lambda v: v + 10).collect()}")  # Add 10 to each score

# flatMapValues(f) — one value can produce multiple values
print(f"\nflatMapValues: ")
grades = sc.parallelize([("Alice", "A,B,A"), ("Bob", "B,C")])  # CSV-like values
expanded = grades.flatMapValues(lambda v: v.split(","))  # Split each value
print(f"  {expanded.collect()}")  # [('Alice','A'),('Alice','B'),('Alice','A'),('Bob','B'),('Bob','C')]

# countByKey() — count items per key
print(f"\ncountByKey(): {dict(scores.countByKey())}")  # {'Alice': 3, 'Bob': 2, 'Charlie': 1}

# lookup(key) — get all values for a specific key
print(f"\nlookup('Alice'): {scores.lookup('Alice')}")  # [85, 92, 95]
print(f"lookup('Bob'): {scores.lookup('Bob')}")  # [90, 78]

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Aggregations by Key
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: Aggregating by Key
# ═══════════════════════════════════════════════════════

print("=== Aggregating by Key ===")
print()

# Sample data: (department, salary)
employees = sc.parallelize([
    ("Engineering", 95000), ("Marketing", 72000), ("Engineering", 105000),
    ("Sales", 68000), ("Marketing", 91000), ("Engineering", 88000),
    ("Sales", 75000), ("Marketing", 64000)
])

# 1. reduceByKey — sum salaries per department
print("1. reduceByKey (sum salaries):")
total_salary = employees.reduceByKey(lambda a, b: a + b)  # Sum per key
print(f"   {total_salary.collect()}")  # Total salary per dept

# 2. groupByKey — group all salaries per department (use sparingly!)
print("\n2. groupByKey (group salaries):")
grouped = employees.groupByKey()  # Group values by key
for dept, salaries in grouped.collect():  # Print each group
    print(f"   {dept}: {list(salaries)}")  # Show all salaries

# 3. aggregateByKey — compute (sum, count) per key for average
print("\n3. aggregateByKey (average salary):")
avg_salary = employees.aggregateByKey(
    (0, 0),  # zeroValue: (sum=0, count=0)
    lambda acc, val: (acc[0] + val, acc[1] + 1),  # seqOp: within partition
    lambda a, b: (a[0] + b[0], a[1] + b[1])  # combOp: merge partitions
).mapValues(lambda v: v[0] / v[1])  # Calculate average from (sum, count)
for dept, avg in avg_salary.collect():  # Print results
    print(f"   {dept}: ${avg:,.0f}")  # Formatted average

# 4. sortByKey — sort by key alphabetically
print("\n4. sortByKey:")
sorted_rdd = total_salary.sortByKey()  # Sort alphabetically
print(f"   Ascending: {sorted_rdd.collect()}")  # A-Z
sorted_desc = total_salary.sortByKey(ascending=False)  # Z-A
print(f"   Descending: {sorted_desc.collect()}")  # Z-A

print("\n--- Always prefer reduceByKey over groupByKey for aggregation! ---")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: PairRDD Joins
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: Joins
# ═══════════════════════════════════════════════════════

print("=== PairRDD Joins ===")
print()

# Two datasets to join
names = sc.parallelize([(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Diana")])  # (id, name)
scores = sc.parallelize([(1, 95), (2, 87), (3, 72), (5, 99)])  # (id, score) - note: 4 missing, 5 extra

# 1. join() — INNER JOIN (only matching keys)
print("1. join() — inner (only matches):")
inner = names.join(scores)  # Only keys in BOTH: 1, 2, 3
print(f"   {inner.collect()}")  # [(1, ('Alice', 95)), (2, ('Bob', 87)), (3, ('Charlie', 72))]

# 2. leftOuterJoin() — all from left, match from right (or None)
print("\n2. leftOuterJoin() — all left + matching right:")
left = names.leftOuterJoin(scores)  # All names, scores if available
for id_, (name, score) in left.collect():
    print(f"   ID {id_}: {name}, score={score}")  # Diana has score=None

# 3. rightOuterJoin() — all from right, match from left (or None)
print("\n3. rightOuterJoin() — all right + matching left:")
right = names.rightOuterJoin(scores)  # All scores, names if available
for id_, (name, score) in right.collect():
    print(f"   ID {id_}: name={name}, score={score}")  # ID 5 has name=None

# 4. fullOuterJoin() — all from both (None where missing)
print("\n4. fullOuterJoin() — everything:")
full = names.fullOuterJoin(scores)  # All keys from both
for id_, (name, score) in full.collect():
    print(f"   ID {id_}: name={name}, score={score}")

# 5. subtractByKey() — keys in left but NOT in right
print("\n5. subtractByKey() — left keys not in right:")
only_names = names.subtractByKey(scores)  # IDs with names but no scores
print(f"   {only_names.collect()}")  # [(4, 'Diana')]

# 6. cogroup() — group both RDDs by key
print("\n6. cogroup() — group both by key:")
cogrouped = names.cogroup(scores)  # Returns (key, (iter1, iter2))
for id_, (name_iter, score_iter) in cogrouped.collect():
    print(f"   ID {id_}: names={list(name_iter)}, scores={list(score_iter)}")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate: combineByKey and Advanced Agg
# ═══════════════════════════════════════════════════════
# SECTION 4 — combineByKey: The Most General Aggregation
# ═══════════════════════════════════════════════════════

print("=== combineByKey — Average Salary per Department ===")
print()

# combineByKey(createCombiner, mergeValue, mergeCombiners)
# Most general per-key aggregation. Uses 3 functions:
# 1. createCombiner: first value for a new key → initial accumulator
# 2. mergeValue: merge a new value into existing accumulator (within partition)
# 3. mergeCombiners: merge two accumulators (across partitions)

employees = sc.parallelize([
    ("Eng", 95000), ("Mkt", 72000), ("Eng", 105000),
    ("Sales", 68000), ("Mkt", 91000), ("Eng", 88000),
    ("Sales", 75000), ("Mkt", 64000)
])

# Compute average: need (sum, count) per key
avg_result = employees.combineByKey(
    lambda val: (val, 1),  # createCombiner: first value → (sum, count)
    lambda acc, val: (acc[0] + val, acc[1] + 1),  # mergeValue: add to accumulator
    lambda a, b: (a[0] + b[0], a[1] + b[1])  # mergeCombiners: combine partitions
).mapValues(lambda v: v[0] / v[1])  # Final step: compute average

print("Average salary per department:")
for dept, avg in sorted(avg_result.collect()):  # Print sorted
    print(f"  {dept}: ${avg:,.0f}")

# Comparison: reduceByKey vs aggregateByKey vs combineByKey
print("\n--- When to Use Which ---")
print("reduceByKey: Simple aggregation (sum, max, min) where result type = value type")
print("aggregateByKey: Different result type, but zeroValue is same for all keys")
print("combineByKey: Most flexible — custom init per key, different result type")
print("\nIn practice: reduceByKey handles 90% of cases. Use combineByKey for complex ones.")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: cogroup and Advanced Joins
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 2: cogroup and Advanced Joins
# ═══════════════════════════════════════════════════════

print("=== cogroup: Group Multiple RDDs by Key ===")
print()

# cogroup groups values from MULTIPLE RDDs by key
# Result: (key, (Iterable[valuesFromRDD1], Iterable[valuesFromRDD2]))

# Dataset 1: Student homework scores
homework = sc.parallelize([
    ("Alice", 85), ("Alice", 90), ("Bob", 70),
    ("Bob", 88), ("Charlie", 95)
])

# Dataset 2: Student exam scores
exams = sc.parallelize([
    ("Alice", 92), ("Bob", 85), ("Bob", 91),
    ("Diana", 98)  # Diana has no homework!
])

# Dataset 3: Student project scores
projects = sc.parallelize([
    ("Alice", 88), ("Charlie", 82), ("Diana", 96)
])

# cogroup groups all three RDDs by student name
print("1. cogroup (3 RDDs):")
cogrouped = homework.cogroup(exams, projects)  # Group all by key

for student, (hw_scores, exam_scores, proj_scores) in cogrouped.collect():
    hw = list(hw_scores)      # Convert Iterable to list
    ex = list(exam_scores)    # Convert Iterable to list
    pr = list(proj_scores)    # Convert Iterable to list
    print(f"  {student}: HW={hw}, Exams={ex}, Projects={pr}")

# Expected:
# Alice: HW=[85, 90], Exams=[92], Projects=[88]
# Bob: HW=[70, 88], Exams=[85, 91], Projects=[]
# Charlie: HW=[95], Exams=[], Projects=[82]
# Diana: HW=[], Exams=[98], Projects=[96]

print("\n2. subtractByKey — Anti-join (keys in left but NOT in right):")
# subtractByKey: keep keys from left that DON'T appear in right
rdd_a = sc.parallelize([(1, "a"), (2, "b"), (3, "c"), (4, "d")])  # 4 entries
rdd_b = sc.parallelize([(2, "x"), (4, "y")])  # Keys 2 and 4
result = rdd_a.subtractByKey(rdd_b)  # Remove keys 2, 4 from rdd_a
print(f"  A - B keys: {sorted(result.collect())}")  # [(1, 'a'), (3, 'c')]

print("\n3. Custom anti-join using cogroup:")
# Alternative: anti-join via cogroup (keys in left with EMPTY right)
anti_join = homework.cogroup(exams).filter(
    lambda x: len(list(x[1][1])) == 0  # Keep only if exams list is empty
).mapValues(lambda v: list(v[0]))  # Get homework scores only
print(f"  Students with homework but no exams: {anti_join.collect()}")  # Charlie

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: sortByKey and partitionBy
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 3: sortByKey & partitionBy
# ═══════════════════════════════════════════════════════

print("=== sortByKey & partitionBy ===")
print()

# --- sortByKey ---
print("--- sortByKey ---")
scores = sc.parallelize([
    ("Charlie", 78), ("Alice", 95), ("Eve", 88),
    ("Bob", 82), ("Diana", 91)
])

# Sort ascending by key (name)
print("1. sortByKey(ascending=True):")
asc_sorted = scores.sortByKey(ascending=True)  # A-Z
print(f"   {asc_sorted.collect()}")  # Alice, Bob, Charlie, Diana, Eve

# Sort descending by key
print("\n2. sortByKey(ascending=False):")
desc_sorted = scores.sortByKey(ascending=False)  # Z-A
print(f"   {desc_sorted.collect()}")  # Eve, Diana, Charlie, Bob, Alice

# Sort by value (trick: swap key/value, sort, swap back)
print("\n3. Sort by VALUE (swap trick):")
by_value = scores.map(lambda x: (x[1], x[0]))  # Swap: (score, name)
sorted_by_value = by_value.sortByKey(ascending=False)  # Sort by score desc
result = sorted_by_value.map(lambda x: (x[1], x[0]))  # Swap back: (name, score)
print(f"   Top scores: {result.collect()}")  # Alice(95), Diana(91)...

# --- partitionBy ---
print("\n--- partitionBy (control data distribution) ---")

# Create PairRDD with specific partitioning
data = sc.parallelize([
    ("US", 100), ("UK", 200), ("US", 150),
    ("DE", 300), ("UK", 250), ("DE", 175),
    ("US", 125), ("UK", 90), ("DE", 400)
], 2)  # Default 2 partitions

print(f"4. Before partitionBy:")
print(f"   Partitions: {data.getNumPartitions()}")  # 2
print(f"   Partitioner: {data.partitioner}")  # None

# Repartition by key using hash partitioner
partitioned = data.partitionBy(3)  # 3 partitions, hash on key
print(f"\n5. After partitionBy(3):")
print(f"   Partitions: {partitioned.getNumPartitions()}")  # 3
print(f"   Partitioner: {partitioned.partitioner}")  # HashPartitioner

# Show what's in each partition
print("\n6. Data distribution per partition:")
def show_partition(index, iterator):
    items = list(iterator)  # Collect partition items
    return [(index, items)]  # Return partition index + items

for part_idx, items in partitioned.mapPartitionsWithIndex(show_partition).collect():
    keys = [k for k, v in items]  # Get just the keys
    print(f"   Partition {part_idx}: keys={keys}")
    # Each partition should have all items for SAME keys (co-located!)

print("\n--- Key: partitionBy co-locates same keys = faster joins & aggregations ---")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: groupByKey vs reduceByKey Performance
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 1: Performance Comparison
# ═══════════════════════════════════════════════════════

import time  # For benchmarking

print("=== groupByKey vs reduceByKey: Why It Matters ===")
print()

# Generate large dataset: 500K (key, value) pairs with 1000 unique keys
large_data = sc.parallelize(
    [(f"key_{i % 1000}", i) for i in range(500000)],  # 500K pairs
    numSlices=8  # 8 partitions
)
large_data.cache()  # Cache to make comparison fair
large_data.count()  # Trigger cache

# --- Approach 1: groupByKey + sum (BAD for aggregation) ---
print("--- Approach 1: groupByKey + map (BAD) ---")
start = time.time()
result_group = large_data.groupByKey().mapValues(sum)  # Group then sum
result_group.count()  # Force execution
time_group = time.time() - start
print(f"  Time: {time_group:.3f}s")
print(f"  Problem: ALL values shuffled to one node per key!")

# --- Approach 2: reduceByKey (GOOD) ---
print("\n--- Approach 2: reduceByKey (GOOD) ---")
start = time.time()
result_reduce = large_data.reduceByKey(lambda a, b: a + b)  # Combine locally first!
result_reduce.count()  # Force execution
time_reduce = time.time() - start
print(f"  Time: {time_reduce:.3f}s")
print(f"  Benefit: Partial aggregation BEFORE shuffle (map-side combine)")

# --- Comparison ---
print(f"\n{'=' * 50}")
print(f"PERFORMANCE COMPARISON:")
print(f"{'=' * 50}")
speedup = time_group / time_reduce if time_reduce > 0 else float('inf')
print(f"  groupByKey: {time_group:.3f}s")
print(f"  reduceByKey: {time_reduce:.3f}s")
print(f"  Speedup: {speedup:.1f}x")

print("\nWHY reduceByKey is faster:")
print("  groupByKey:")
print("    1. EVERY value travels across network (shuffle)")
print("    2. All values for a key collected into memory (OOM risk!)")
print("    3. Then you sum them (could have been done earlier)")
print("  reduceByKey:")
print("    1. Partial sum computed LOCALLY on each partition first")
print("    2. Only partial sums travel across network (much less data!)")
print("    3. Final merge on the receiving end")

print("\n\u2705 RULE: NEVER use groupByKey for aggregation. Always use reduceByKey/aggregateByKey.")
large_data.unpersist()  # Cleanup

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Complex Multi-Table Join Pipeline
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 2: Multi-Table Join Pipeline
# ═══════════════════════════════════════════════════════

print("=== Multi-Table Join: E-Commerce Order Pipeline ===")
print()

# Scenario: Build an order summary by joining 4 tables
# Orders + Customers + Products + Regions

# Table 1: Orders (order_id, customer_id, product_id, quantity, date)
orders = sc.parallelize([
    ("ORD001", "C01", "P01", 2, "2024-01-15"),
    ("ORD002", "C02", "P03", 1, "2024-01-15"),
    ("ORD003", "C01", "P02", 5, "2024-01-16"),
    ("ORD004", "C03", "P01", 3, "2024-01-16"),
    ("ORD005", "C04", "P04", 1, "2024-01-17"),
    ("ORD006", "C02", "P02", 2, "2024-01-17"),
])

# Table 2: Customers (customer_id, name, region_code)
customers = sc.parallelize([
    ("C01", ("Alice Johnson", "US-W")),
    ("C02", ("Bob Smith", "UK")),
    ("C03", ("Charlie Brown", "US-E")),
    ("C04", ("Diana Prince", "EU")),
])

# Table 3: Products (product_id, name, price)
products = sc.parallelize([
    ("P01", ("Widget Pro", 29.99)),
    ("P02", ("Gadget X", 49.99)),
    ("P03", ("Super Tool", 99.99)),
    ("P04", ("Mega Device", 199.99)),
])

# Table 4: Regions (region_code, region_name, tax_rate)
regions = sc.parallelize([
    ("US-W", ("US West", 0.08)),
    ("US-E", ("US East", 0.07)),
    ("UK", ("United Kingdom", 0.20)),
    ("EU", ("European Union", 0.19)),
])

# Step 1: Rekey orders by customer_id for customer join
orders_by_customer = orders.map(
    lambda o: (o[1], (o[0], o[2], o[3], o[4]))  # (cust_id, (order, prod, qty, date))
)

# Step 2: Join orders with customers
orders_with_customer = orders_by_customer.join(customers)  # Join on customer_id
# Result: (cust_id, ((order_id, prod_id, qty, date), (name, region)))

# Step 3: Rekey by product_id for product join
orders_rekey_product = orders_with_customer.map(
    lambda x: (x[1][0][1], (x[1][0][0], x[1][0][2], x[1][0][3], x[1][1][0], x[1][1][1]))
    # (prod_id, (order_id, qty, date, cust_name, region_code))
)

# Step 4: Join with products
orders_with_product = orders_rekey_product.join(products)
# Result: (prod_id, ((order_id, qty, date, cust_name, region), (prod_name, price)))

# Step 5: Rekey by region for region join
orders_rekey_region = orders_with_product.map(
    lambda x: (
        x[1][0][4],  # region_code as key
        (x[1][0][0], x[1][0][3], x[1][1][0], x[1][0][1], x[1][1][1], x[1][0][2])
        # (order_id, cust_name, prod_name, qty, price, date)
    )
)

# Step 6: Join with regions
final = orders_rekey_region.join(regions)

# Step 7: Compute final summary
print("ORDER SUMMARY (4-table join):")
print("=" * 85)
print(f"{'Order':<8} {'Customer':<16} {'Product':<12} {'Qty':>3} {'Subtotal':>10} {'Tax':>8} {'Total':>10} {'Region'}")
print("-" * 85)

for region_code, (order_info, region_info) in sorted(final.collect(), key=lambda x: x[1][0][0]):
    order_id, cust_name, prod_name, qty, price, date = order_info  # Unpack order
    region_name, tax_rate = region_info  # Unpack region
    subtotal = qty * price  # Calculate subtotal
    tax = subtotal * tax_rate  # Calculate tax
    total = subtotal + tax  # Grand total
    print(f"  {order_id:<8} {cust_name:<16} {prod_name:<12} {qty:>3} ${subtotal:>8.2f} ${tax:>6.2f} ${total:>8.2f} {region_name}")

print("\n--- Key: Multi-table joins require careful rekeying between each join ---")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Custom Partitioner for Skewed Data
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 3: Handling Data Skew
# ═══════════════════════════════════════════════════════

import time  # For benchmarking
from pyspark import Partitioner  # For custom partitioner

print("=== Advanced: Handling Data Skew in PairRDDs ===")
print()

# PROBLEM: Data skew means one key has WAY more values than others
# This causes ONE partition to be huge while others are tiny
# Result: one task takes forever while others finish instantly

# Create skewed data: key "hot" has 90% of records
skewed_data = sc.parallelize(
    [("hot", i) for i in range(90000)] +  # 90K records for "hot"
    [("warm", i) for i in range(5000)] +   # 5K records for "warm"
    [("cold", i) for i in range(3000)] +   # 3K records for "cold"
    [("tiny", i) for i in range(2000)],    # 2K records for "tiny"
    numSlices=4  # 4 partitions
)

print(f"Total records: {skewed_data.count():,}")  # 100,000
print(f"Distribution: {skewed_data.countByKey()}")  # Shows the skew

# --- Approach 1: Naive reduceByKey (suffers from skew) ---
print("\n--- Approach 1: Naive reduceByKey (skew causes slow partition) ---")
start = time.time()
naive_result = skewed_data.reduceByKey(lambda a, b: a + b)  # Sum per key
naive_result.collect()  # Force execution
naive_time = time.time() - start
print(f"  Time: {naive_time:.3f}s")

# --- Approach 2: Salting to distribute the hot key ---
print("\n--- Approach 2: Key Salting (split hot key across partitions) ---")
import random

NUM_SALTS = 10  # Split "hot" key into 10 sub-keys

# Step 1: Add salt to spread the hot key
def add_salt(pair):
    key, value = pair  # Unpack
    if key == "hot":  # Only salt the hot key
        salt = random.randint(0, NUM_SALTS - 1)  # Random salt 0-9
        return (f"{key}_salt_{salt}", value)  # "hot_salt_3"
    return (key, value)  # Non-hot keys unchanged

salted = skewed_data.map(add_salt)  # Add salt to spread load

# Step 2: Reduce with salted keys (now "hot" is split into 10 partitions)
start = time.time()
partial_sums = salted.reduceByKey(lambda a, b: a + b)  # Sum per salted key

# Step 3: Remove salt and final reduce
def remove_salt(pair):
    key, value = pair  # Unpack
    original_key = key.split("_salt_")[0] if "_salt_" in key else key  # Remove salt
    return (original_key, value)  # Back to original key

final_result = partial_sums.map(remove_salt).reduceByKey(lambda a, b: a + b)  # Final merge
final_result.collect()  # Force execution
salted_time = time.time() - start
print(f"  Time: {salted_time:.3f}s")

# --- Comparison ---
print(f"\n{'=' * 50}")
print("RESULTS:")
for key, total in sorted(final_result.collect()):
    print(f"  {key}: sum = {total:,}")

print(f"\nPerformance:")
print(f"  Naive: {naive_time:.3f}s")
print(f"  Salted: {salted_time:.3f}s")
print(f"  Note: Salting shines with VERY large skewed datasets")

print("\nWHEN to use salting:")
print("  1. One key has >10x the data of other keys")
print("  2. Tasks for that key take much longer (visible in Spark UI)")
print("  3. You're hitting OOM on one executor")
print("\n--- Key: Salt hot keys to spread work evenly across partitions ---")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: groupByKey for Aggregation
# MAGIC **Fix:** Use `reduceByKey`, `aggregateByKey`, or `combineByKey`.
# MAGIC
# MAGIC ### Mistake #2: Joining with Wrong Key Structure
# MAGIC **Issue:** One RDD has `("Alice", 85)` and another has `(("Alice", "Smith"), 95)` — keys don't match.  
# MAGIC **Fix:** Ensure both RDDs have the SAME key type and format before joining.
# MAGIC
# MAGIC ### Mistake #3: Expecting join() to Handle Duplicates Like SQL
# MAGIC **Issue:** If key appears 3 times in left and 2 times in right, you get 3×2=6 rows.  
# MAGIC **Fix:** Deduplicate before joining if you don't want a cartesian product per key.
# MAGIC
# MAGIC ### Mistake #4: Not Realizing subtractByKey Removes by KEY, Not Value
# MAGIC **Issue:** `subtractByKey` removes ALL entries whose KEY appears in the other RDD.  
# MAGIC **Fix:** If you need value-based filtering, use `filter()` after a join.
# MAGIC
# MAGIC ### Mistake #5: Using collect() After groupByKey on Big Data
# MAGIC **Fix:** Never `groupByKey().collect()` on big data. The grouped values can be huge.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC ### Level 1: Create a PairRDD of (city, population). Use keys() and values().
# MAGIC ### Level 2: Use mapValues to double all values in a PairRDD.
# MAGIC ### Level 3: reduceByKey to sum sales per product.
# MAGIC ### Level 4: Join two PairRDDs: (student_id, name) and (student_id, grade).
# MAGIC ### Level 5: Use aggregateByKey to compute average per key.
# MAGIC ### Level 6: Implement a word frequency counter using PairRDD operations.
# MAGIC ### Level 7: Compare performance of groupByKey vs reduceByKey on 1M pairs.
# MAGIC ### Level 8: Use fullOuterJoin and handle None values gracefully.
# MAGIC ### Level 9: Build a combineByKey that computes (count, sum, min, max) per key.
# MAGIC ### Level 10: Explain all PairRDD join types with Venn diagrams to a colleague.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════
import time, sys

# Level 1
print("=== Level 1 ===")
cities = sc.parallelize([("London",9000000),("Tokyo",14000000),("Paris",2200000)])
print(f"Keys: {cities.keys().collect()}")  # ['London','Tokyo','Paris']
print(f"Values: {cities.values().collect()}")  # [9000000, 14000000, 2200000]

# Level 2
print("\n=== Level 2 ===")
print(f"Doubled: {cities.mapValues(lambda v: v*2).collect()}")  # Population doubled

# Level 3
print("\n=== Level 3 ===")
sales = sc.parallelize([("Widget",100),("Gadget",200),("Widget",150),("Gadget",50),("Widget",75)])
totals = sales.reduceByKey(lambda a,b: a+b)  # Sum per product
print(f"Sales totals: {totals.collect()}")  # Widget=325, Gadget=250

# Level 4
print("\n=== Level 4 ===")
students = sc.parallelize([(1,"Alice"),(2,"Bob"),(3,"Charlie")])
grades = sc.parallelize([(1,"A"),(2,"B"),(3,"A"),(1,"B")])
joined = students.join(grades)  # Inner join on student_id
print(f"Joined: {joined.collect()}")  # (1,('Alice','A')), (1,('Alice','B')), etc.

# Level 5
print("\n=== Level 5 ===")
data = sc.parallelize([("A",10),("B",20),("A",30),("B",40),("A",50)])
avg = data.aggregateByKey(
    (0,0),
    lambda acc,v: (acc[0]+v, acc[1]+1),
    lambda a,b: (a[0]+b[0], a[1]+b[1])
).mapValues(lambda v: v[0]/v[1])
print(f"Averages: {avg.collect()}")  # A=30.0, B=30.0

# Level 9
print("\n=== Level 9 ===")
nums = sc.parallelize([("X",5),("Y",10),("X",3),("Y",8),("X",9),("Y",2)])
stats = nums.combineByKey(
    lambda v: (1, v, v, v),  # create: (count, sum, min, max)
    lambda a, v: (a[0]+1, a[1]+v, min(a[2],v), max(a[3],v)),  # merge value
    lambda a, b: (a[0]+b[0], a[1]+b[1], min(a[2],b[2]), max(a[3],b[3]))  # merge combiners
)
for key, (cnt, total, mn, mx) in stats.collect():
    print(f"  {key}: count={cnt}, sum={total}, min={mn}, max={mx}, avg={total/cnt:.1f}")

print("\n\u2705 All homework complete!")