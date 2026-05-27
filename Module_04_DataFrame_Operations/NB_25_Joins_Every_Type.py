# Databricks notebook source
# DBTITLE 1,Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 25: Joins — Every Type With Real Examples
# MAGIC # Module: DataFrame Operations
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 55 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: Matching Guest Lists
# MAGIC
# MAGIC Imagine you have two spreadsheets:
# MAGIC - **Left table**: Employee list (id, name, dept_id)
# MAGIC - **Right table**: Department list (dept_id, dept_name)
# MAGIC
# MAGIC You want to combine them so each employee shows their department name.
# MAGIC
# MAGIC Different join types answer different questions:
# MAGIC
# MAGIC | Join Type | Question Answered |
# MAGIC |-----------|------------------|
# MAGIC | **Inner** | Which employees have a valid department? |
# MAGIC | **Left Outer** | ALL employees, with dept if available |
# MAGIC | **Right Outer** | ALL departments, with employees if any |
# MAGIC | **Full Outer** | Everything from both, matched where possible |
# MAGIC | **Left Semi** | Which employees exist in the dept table? (no dept cols!) |
# MAGIC | **Left Anti** | Which employees have NO matching department? |
# MAGIC | **Cross** | Every employee paired with every department (rarely useful!) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Venn Diagram (Text)
# MAGIC
# MAGIC ```
# MAGIC   INNER:          LEFT:           RIGHT:          FULL:
# MAGIC   ┌───┬───┐       ┌───┬───┐       ┌───┬───┐       ┌───┬───┐
# MAGIC   │ A │###│ B │   │###│###│ B │   │ A │###│###│   │###│###│###│
# MAGIC   └───┴───┘       └───┴───┘       └───┴───┘       └───┴───┘
# MAGIC   Only overlap   All left +      All right +     Everything
# MAGIC                  overlap         overlap         from both
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Facts
# MAGIC - PySpark joins are **lazy** — no data moves until an action is called
# MAGIC - Joins often trigger a **shuffle** (expensive!) unless one side is broadcast
# MAGIC - Always think: "Which rows do I want to KEEP?"

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Join Execution Strategies
# MAGIC
# MAGIC ```
# MAGIC Spark picks one of these depending on data sizes:
# MAGIC
# MAGIC 1. BROADCAST HASH JOIN (fastest!)
# MAGIC    ───────────────────────
# MAGIC    Small table sent to ALL executors
# MAGIC    No shuffle needed!
# MAGIC    Trigger: one side < 10MB (default)
# MAGIC
# MAGIC 2. SORT MERGE JOIN (default for large tables)
# MAGIC    ───────────────────────
# MAGIC    Both sides sorted by join key → merged
# MAGIC    Requires shuffle on BOTH sides
# MAGIC    Works for any size tables
# MAGIC
# MAGIC 3. SHUFFLE HASH JOIN
# MAGIC    ───────────────────────
# MAGIC    Shuffle both, build hash table on one side
# MAGIC    Good when one side is much smaller (but > broadcast limit)
# MAGIC ```
# MAGIC
# MAGIC ### Join Syntax
# MAGIC
# MAGIC ```
# MAGIC # Column name string (same name in both DFs):
# MAGIC df1.join(df2, "key_col", "inner")
# MAGIC
# MAGIC # Column expression (different names):
# MAGIC df1.join(df2, df1.id == df2.emp_id, "left")
# MAGIC
# MAGIC # Multiple columns:
# MAGIC df1.join(df2, ["col1", "col2"], "inner")
# MAGIC
# MAGIC # Inequality join:
# MAGIC df1.join(df2, (df1.date >= df2.start) & (df1.date <= df2.end))
# MAGIC ```
# MAGIC
# MAGIC ### Duplicate Column Problem
# MAGIC
# MAGIC ```
# MAGIC df1: [id, name]     df2: [id, dept]
# MAGIC
# MAGIC df1.join(df2, df1.id == df2.id, "inner")
# MAGIC → Result: [id, name, id, dept]   ← TWO 'id' columns!
# MAGIC
# MAGIC Fix 1: Join on string: df1.join(df2, "id")   → ONE 'id' column
# MAGIC Fix 2: Drop after: result.drop(df2.id)
# MAGIC Fix 3: Alias before: df2.withColumnRenamed("id", "dept_id")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Inner, Left, Right, Full
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: Inner, Left, Right, Full
# ═══════════════════════════════════════════════════════

print("=== The Four Standard Joins ===")
print()

# --- Create two sample DataFrames ---
# Employees: some have dept_id that doesn't exist in departments
employees = spark.createDataFrame([
    (1, "Alice", 10),    # dept 10 exists
    (2, "Bob", 20),      # dept 20 exists
    (3, "Charlie", 30),  # dept 30 does NOT exist in departments!
    (4, "Diana", 10),    # dept 10 exists
], ["emp_id", "name", "dept_id"])

# Departments: dept 40 has no employees
departments = spark.createDataFrame([
    (10, "Engineering"),
    (20, "Marketing"),
    (40, "Research"),    # No employee has dept_id = 40
], ["dept_id", "dept_name"])

print("--- Employees ---")  # Left table
employees.show()
print("--- Departments ---")  # Right table
departments.show()

# --- INNER JOIN: Only matching rows from both sides ---
print("--- 1. INNER JOIN (only matches) ---")
employees.join(departments, "dept_id", "inner").show()
# Expected: Alice(10), Bob(20), Diana(10) — Charlie(30) excluded!

# --- LEFT JOIN: All from left, match from right where possible ---
print("--- 2. LEFT JOIN (all employees, dept if available) ---")
employees.join(departments, "dept_id", "left").show()
# Expected: All 4 employees; Charlie has null dept_name

# --- RIGHT JOIN: All from right, match from left where possible ---
print("--- 3. RIGHT JOIN (all departments, employees if any) ---")
employees.join(departments, "dept_id", "right").show()
# Expected: All 3 depts; Research(40) has null employee cols

# --- FULL OUTER JOIN: Everything from both sides ---
print("--- 4. FULL OUTER JOIN (everything) ---")
employees.join(departments, "dept_id", "full").show()
# Expected: All employees + all depts; nulls where no match

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Left Semi and Left Anti
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: Left Semi and Left Anti
# ═══════════════════════════════════════════════════════

print("=== Semi and Anti Joins ===")
print()
print("Semi = EXISTS, Anti = NOT EXISTS")
print("Both return ONLY columns from the LEFT table!")
print()

# Reuse employees and departments from above
employees = spark.createDataFrame([
    (1, "Alice", 10), (2, "Bob", 20),
    (3, "Charlie", 30), (4, "Diana", 10),
], ["emp_id", "name", "dept_id"])

departments = spark.createDataFrame([
    (10, "Engineering"), (20, "Marketing"), (40, "Research"),
], ["dept_id", "dept_name"])

# --- LEFT SEMI: Employees that HAVE a matching department ---
print("--- LEFT SEMI: Employees with a valid department ---")
employees.join(departments, "dept_id", "left_semi").show()
# Expected: Alice, Bob, Diana (Charlie excluded — dept 30 doesn't exist)
# Note: NO dept_name column! Only left table columns returned.

# --- LEFT ANTI: Employees that have NO matching department ---
print("--- LEFT ANTI: Employees with NO valid department ---")
employees.join(departments, "dept_id", "left_anti").show()
# Expected: Only Charlie (dept_id=30 doesn't exist in departments)
# Use case: Find orphan records, data quality checks

print("--- Key insight ---")
print("  Semi/Anti never add columns from the right table")
print("  Semi = WHERE EXISTS (subquery)")
print("  Anti = WHERE NOT EXISTS (subquery)")
print("  Anti is perfect for finding missing/orphan records!")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Cross Join
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: Cross Join
# ═══════════════════════════════════════════════════════

print("=== Cross Join (Cartesian Product) ===")
print()
print("DANGER: Cross join = LEFT_ROWS × RIGHT_ROWS output rows!")
print("1000 × 1000 = 1,000,000 rows. 1M × 1M = 1 TRILLION rows = crash!")
print()

# --- Small example: all combinations of size and color ---
sizes = spark.createDataFrame([
    ("S",), ("M",), ("L",),   # 3 rows
], ["size"])

colors = spark.createDataFrame([
    ("Red",), ("Blue",),       # 2 rows
], ["color"])

# --- Cross join: every size with every color ---
print("--- Cross Join: All size-color combinations ---")
result = sizes.crossJoin(colors)  # 3 x 2 = 6 rows
result.show()
print(f"  3 sizes × 2 colors = {result.count()} combinations")

# --- Alternative syntax ---
print("\n--- Alternative: join with no condition ---")
result2 = sizes.join(colors)  # Same as crossJoin
result2.show()

# --- Real use case: Generate a calendar scaffold ---
print("--- Real use case: Date × Product scaffold ---")
dates = spark.createDataFrame([("2024-01-01",), ("2024-01-02",)], ["date"])
products = spark.createDataFrame([("A",), ("B",), ("C",)], ["product"])
scaffold = dates.crossJoin(products)  # Every date + every product
scaffold.show()
print("  Use case: Ensure every date+product combo exists (fill gaps)")

print("\n--- WARNING ---")
print("  NEVER cross join large tables without knowing the output size!")
print("  10K × 10K = 100M rows = may crash your cluster.")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Broadcast Join
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Broadcast Join (small table optimization)
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import broadcast

print("=== Broadcast Join ===")
print()
print("When one table is SMALL (< 200MB), broadcast it to all executors.")
print("Result: NO shuffle on the large table = MUCH faster!")
print()

# --- Large table (simulating 100K orders) ---
orders = spark.range(100000).selectExpr(
    "id as order_id",                      # 100K order IDs
    "int(rand() * 50) as product_id",      # Random product (0-49)
    "round(rand() * 1000, 2) as amount"    # Random amount
)

# --- Small lookup table (50 products) ---
products = spark.createDataFrame(
    [(i, f"Product_{i}", ["Electronics", "Books", "Food"][i % 3])
     for i in range(50)],
    ["product_id", "product_name", "category"]
)

print(f"Orders: {orders.count():,} rows (LARGE)")
print(f"Products: {products.count()} rows (SMALL)")

# --- Method 1: broadcast() function ---
print("\n--- Broadcast join (explicit) ---")
result = orders.join(broadcast(products), "product_id", "inner")  # Force broadcast!
result.show(5)
print("  broadcast(products) = send products to ALL executors")
print("  No shuffle on the 100K orders table!")

# --- Method 2: hint ---
print("\n--- Broadcast join using hint ---")
result2 = orders.join(products.hint("broadcast"), "product_id")  # Same effect
print(f"  Result count: {result2.count():,}")

# --- Verify in explain ---
print("\n--- Physical plan (look for BroadcastHashJoin) ---")
result.explain()

print("\n--- When to broadcast ---")
print("  Table < 200MB: safe to broadcast")
print("  Table < 10MB: Spark does it AUTOMATICALLY (autoBroadcastJoinThreshold)")
print("  Default threshold: spark.sql.autoBroadcastJoinThreshold = 10485760 (10MB)")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Multi-column and Inequality Joins
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Multi-column and Inequality Joins
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== Multi-Column Joins & Inequality Joins ===")
print()

# --- Multi-column join (composite key) ---
print("--- 1. Join on MULTIPLE columns ---")
sales = spark.createDataFrame([
    ("US", "Electronics", 5000),
    ("US", "Books", 1200),
    ("EU", "Electronics", 3500),
    ("EU", "Books", 800),
], ["region", "category", "revenue"])

targets = spark.createDataFrame([
    ("US", "Electronics", 4000),   # Target for US Electronics
    ("US", "Books", 1500),         # Target for US Books
    ("EU", "Electronics", 3000),   # Target for EU Electronics
], ["region", "category", "target"])

# Join on BOTH region AND category
result = sales.join(targets, ["region", "category"], "left")  # List of columns!
result.show()
print("  Joined on [region, category] = composite key")

# --- Inequality join (range join) ---
print("\n--- 2. Inequality Join (range-based) ---")
# Match events to the time window they fall in
events = spark.createDataFrame([
    (1, "click", 5),   # event at time=5
    (2, "view", 12),   # event at time=12
    (3, "click", 25),  # event at time=25
], ["event_id", "event_type", "event_time"])

windows = spark.createDataFrame([
    ("W1", 0, 10),    # Window from 0 to 10
    ("W2", 10, 20),   # Window from 10 to 20
    ("W3", 20, 30),   # Window from 20 to 30
], ["window_id", "start_time", "end_time"])

# Join where event_time falls BETWEEN start and end
result = events.join(
    windows,
    (events.event_time >= windows.start_time) &   # >= start
    (events.event_time < windows.end_time),       # < end
    "inner"
)
result.show()
print("  event_time=5 → W1(0-10), time=12 → W2(10-20), time=25 → W3(20-30)")
print("  Inequality joins are powerful but can be SLOW (no equi-join optimization)")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Handling Duplicate Columns
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Handling Duplicate Column Names
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== Handling Duplicate Column Names After Join ===")
print()
print("Problem: Both tables have a column with the same name.")
print("After join, you get TWO columns with same name = ambiguity!")
print()

# --- Both tables have 'id' and 'name' ---
df1 = spark.createDataFrame([
    (1, "Alice", 100), (2, "Bob", 200),
], ["id", "name", "score"])

df2 = spark.createDataFrame([
    (1, "Alice_Dept", "Engineering"), (2, "Bob_Dept", "Marketing"),
], ["id", "name", "department"])

# --- Fix 1: Use string join (auto-deduplicates the join key) ---
print("--- Fix 1: Join on string name (best when key has same name) ---")
result1 = df1.join(df2, "id")  # Only ONE 'id' column, but 'name' still duplicated!
result1.show()
print(f"  Columns: {result1.columns}")  # ['id', 'name', 'score', 'name', 'department']

# --- Fix 2: Rename before join ---
print("\n--- Fix 2: Rename before join (cleanest) ---")
df2_renamed = df2.withColumnRenamed("name", "dept_contact_name")  # Rename conflicting col
result2 = df1.join(df2_renamed, "id")
result2.show()
print(f"  Columns: {result2.columns}")  # No duplicates!

# --- Fix 3: Use aliases and drop ---
print("\n--- Fix 3: Drop duplicate after join ---")
result3 = df1.alias("a").join(df2.alias("b"), col("a.id") == col("b.id"))  # Expression join
result3 = result3.drop(col("b.id")).drop(col("b.name"))  # Drop right-side duplicates
result3.show()
print(f"  Columns: {result3.columns}")

# --- Fix 4: Select specific columns with aliases ---
print("\n--- Fix 4: Select with table aliases ---")
result4 = df1.alias("e").join(df2.alias("d"), col("e.id") == col("d.id")).select(
    col("e.id"),           # From employees
    col("e.name"),         # Employee name
    col("e.score"),        # Score
    col("d.department"),   # From departments
)
result4.show()
print("  Best practice: always select explicitly after join!")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Join Hints
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Join Hints (forcing strategies)
# ═══════════════════════════════════════════════════════

print("=== Join Hints: Controlling Join Strategy ===")
print()
print("Spark picks the join strategy automatically, but you can override.")
print()

# Create test DataFrames
df_large = spark.range(10000).withColumnRenamed("id", "key")  # Large table
df_small = spark.range(100).withColumnRenamed("id", "key")    # Small table

# --- hint("broadcast") — force broadcast ---
print("--- 1. hint('broadcast') — Force broadcast hash join ---")
result1 = df_large.join(df_small.hint("broadcast"), "key")
result1.explain()  # Look for BroadcastHashJoin in plan
print("  Use when: you KNOW one table is small enough")

# --- hint("merge") — force sort-merge join ---
print("\n--- 2. hint('merge') — Force sort merge join ---")
result2 = df_large.join(df_small.hint("merge"), "key")
result2.explain()  # Look for SortMergeJoin in plan
print("  Use when: both tables are large, equi-join")

# --- hint("shuffle_hash") — force shuffle hash join ---
print("\n--- 3. hint('shuffle_hash') — Force shuffle hash join ---")
result3 = df_large.join(df_small.hint("shuffle_hash"), "key")
result3.explain()  # Look for ShuffledHashJoin in plan
print("  Use when: one side is moderately smaller than the other")

# --- hint("shuffle_replicate_nl") — force nested loop ---
print("\n--- 4. hint('shuffle_replicate_nl') — Force nested loop ---")
print("  Used for: inequality joins, cross joins")
print("  WARNING: O(n²) complexity! Use only for small data.")

# --- Summary table ---
print("\n--- Hint Summary ---")
print(f"  {'Hint':<25} {'Strategy':<20} {'Best For'}")
print(f"  {'-'*65}")
print(f"  {'broadcast':<25} {'BroadcastHashJoin':<20} One side < 200MB")
print(f"  {'merge':<25} {'SortMergeJoin':<20} Both large, equi-join")
print(f"  {'shuffle_hash':<25} {'ShuffledHashJoin':<20} One side much smaller")
print(f"  {'shuffle_replicate_nl':<25} {'NestedLoopJoin':<20} Inequality/cross (slow!)")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Self Join and Complex Patterns
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Self Join and Complex Patterns
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== Self Join: Joining a Table with Itself ===")
print()
print("Use case: Find employee + their manager (both in same table)")
print()

# --- Employee table with manager_id pointing to same table ---
employees = spark.createDataFrame([
    (1, "Alice", None),    # Alice has no manager (CEO)
    (2, "Bob", 1),         # Bob's manager is Alice (id=1)
    (3, "Charlie", 1),     # Charlie's manager is Alice
    (4, "Diana", 2),       # Diana's manager is Bob
    (5, "Eve", 2),         # Eve's manager is Bob
], ["emp_id", "name", "manager_id"])

print("--- Employee table ---")
employees.show()

# --- Self join: join employees with itself to get manager names ---
print("--- Self Join: Employee + Manager Name ---")
emp = employees.alias("emp")       # Alias for the employee side
mgr = employees.alias("mgr")       # Alias for the manager side

result = emp.join(
    mgr,
    col("emp.manager_id") == col("mgr.emp_id"),  # emp's manager_id = mgr's emp_id
    "left"  # Left join to keep CEO (null manager)
).select(
    col("emp.emp_id"),
    col("emp.name").alias("employee"),
    col("mgr.name").alias("manager"),  # Manager's name from the same table
)
result.show()
print("  Alice has no manager (CEO)")
print("  Bob and Charlie report to Alice")
print("  Diana and Eve report to Bob")

# --- Practical: Find pairs of co-workers (same manager) ---
print("\n--- Co-workers (same manager, different person) ---")
e1 = employees.alias("e1")  # First employee
e2 = employees.alias("e2")  # Second employee

coworkers = e1.join(
    e2,
    (col("e1.manager_id") == col("e2.manager_id")) &  # Same manager
    (col("e1.emp_id") < col("e2.emp_id")),            # Avoid duplicates (A,B not B,A)
    "inner"
).select(
    col("e1.name").alias("person_1"),
    col("e2.name").alias("person_2"),
    col("e1.manager_id"),
)
coworkers.show()

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production Join Patterns
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Production Join Patterns
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, broadcast, count, when, lit

print("=== Production Join Patterns ===")
print()

# --- Pattern 1: Safe join with dedup + validation ---
def safe_join(left, right, join_key, join_type="inner", broadcast_right=False):
    """
    Production join with validation:
    1. Checks for duplicates on join key
    2. Optionally broadcasts small table
    3. Reports join statistics
    """
    # Check for duplicates on right side (can cause row explosion!)
    right_count = right.count()
    right_distinct = right.select(join_key).distinct().count()
    if right_count != right_distinct:
        print(f"  ⚠️ WARNING: Right table has {right_count - right_distinct} duplicate keys!")
        print(f"  This will cause row multiplication in the join!")
    
    # Perform join
    left_count = left.count()
    right_df = broadcast(right) if broadcast_right else right
    result = left.join(right_df, join_key, join_type)
    result_count = result.count()
    
    # Report join stats
    print(f"  Left: {left_count:,} | Right: {right_count:,} | Result: {result_count:,}")
    if join_type == "inner" and result_count < left_count:
        lost = left_count - result_count
        print(f"  ⚠️ {lost:,} rows from left had no match (dropped in inner join)")
    
    return result

# Demo
orders = spark.createDataFrame([
    (1, 101, 50.0), (2, 102, 75.0), (3, 999, 100.0),  # 999 = invalid product!
], ["order_id", "product_id", "amount"])

products = spark.createDataFrame([
    (101, "Widget"), (102, "Gadget"), (103, "Doohickey"),
], ["product_id", "product_name"])

print("--- Safe join with validation ---")
result = safe_join(orders, products, "product_id", "left", broadcast_right=True)
result.show()

# --- Pattern 2: Join with fallback (coalesce) ---
print("\n--- Pattern 2: Join with default for unmatched ---")
result_with_default = result.withColumn(
    "product_name",
    when(col("product_name").isNull(), lit("UNKNOWN"))  # Replace null from left join
    .otherwise(col("product_name"))
)
result_with_default.show()

print("\n--- Production tips ---")
print("  1. Always check for duplicate keys before joining")
print("  2. Use left join + null check to find orphan records")
print("  3. Broadcast dimension tables (typically < 100MB)")
print("  4. Filter BEFORE join to reduce shuffle data")
print("  5. Select only needed columns before join")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Duplicate Keys Causing Row Explosion
# MAGIC **Problem:** Right table has duplicate join keys → left rows multiply unexpectedly.  
# MAGIC **Example:** 100 orders JOIN products where product_id=101 appears 3 times → 300 rows!  
# MAGIC **Fix:** Always `dropDuplicates` on the join key of lookup tables before joining.
# MAGIC
# MAGIC ### Mistake #2: Forgetting to Handle Duplicate Column Names
# MAGIC **Problem:** `df1.join(df2, df1.id == df2.id)` creates TWO `id` columns → ambiguous references.  
# MAGIC **Fix:** Use string join (`join(df2, "id")`) or rename/drop duplicates after join.
# MAGIC
# MAGIC ### Mistake #3: Using Cross Join Accidentally
# MAGIC **Problem:** `df1.join(df2)` with no condition = cross join = rows explode!  
# MAGIC **Fix:** Always specify a join condition. Set `spark.sql.crossJoin.enabled=false` to catch mistakes.
# MAGIC
# MAGIC ### Mistake #4: Inner Join When You Need Left Join
# MAGIC **Problem:** Inner join silently drops rows that have no match → data loss goes unnoticed.  
# MAGIC **Fix:** Use left join + filter `WHERE right_key IS NULL` to find and investigate missing matches.
# MAGIC
# MAGIC ### Mistake #5: Not Broadcasting Small Lookup Tables
# MAGIC **Problem:** Joining 1B rows with a 50-row country lookup triggers a full shuffle.  
# MAGIC **Fix:** Use `broadcast(small_df)` — sends the small table to all executors, eliminates shuffle.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC **Level 1 (Copy & Run):** Run the inner join example from Section 3. Observe which rows are kept.
# MAGIC
# MAGIC **Level 2 (Tiny Change):** Change the inner join to a left join. Which extra rows appear?
# MAGIC
# MAGIC **Level 3 (Combine Two):** Use left anti join to find employees with no valid department, then count them.
# MAGIC
# MAGIC **Level 4 (New Scenario):** Create a customers table and an orders table. Join them to find customers who never ordered.
# MAGIC
# MAGIC **Level 5 (Mini Project):** Build a 3-table join: orders → products → categories. Use broadcast for the small tables.
# MAGIC
# MAGIC **Level 6 (Design First):** Before coding, draw which join type you need for: "all orders with product info, plus orders for deleted products."
# MAGIC
# MAGIC **Level 7 (Optimize):** Take a sort-merge join and convert it to broadcast. Compare plans with `explain()`.
# MAGIC
# MAGIC **Level 8 (Edge Cases):** What happens when you join on a column that contains NULLs? Test and explain.
# MAGIC
# MAGIC **Level 9 (Production):** Build a safe_join function that: validates keys, logs stats, handles nulls, and reports row changes.
# MAGIC
# MAGIC **Level 10 (Teach It):** Write a 1-page guide explaining all 7 join types with real business examples for each.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, broadcast, count, when, lit

# --- Level 3: Left Anti to find orphans ---
print("=== Level 3: Find employees with no valid department ===")
emps = spark.createDataFrame([
    (1, "Alice", 10), (2, "Bob", 20), (3, "Charlie", 99),  # 99 = invalid!
    (4, "Diana", 10), (5, "Eve", 88),  # 88 = invalid!
], ["emp_id", "name", "dept_id"])
depts = spark.createDataFrame([(10, "Eng"), (20, "Mkt")], ["dept_id", "dept_name"])

orthans = emps.join(depts, "dept_id", "left_anti")  # Employees without valid dept
orphans.show()
print(f"  Orphan count: {orphans.count()}")  # Expected: 2 (Charlie, Eve)

# --- Level 4: Customers who never ordered ---
print("\n=== Level 4: Customers who never ordered ===")
customers = spark.createDataFrame([
    (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Diana"),
], ["cust_id", "cust_name"])
orders = spark.createDataFrame([
    (101, 1, 50.0), (102, 1, 75.0), (103, 3, 100.0),  # Only Alice(1) and Charlie(3) ordered
], ["order_id", "cust_id", "amount"])

never_ordered = customers.join(orders, "cust_id", "left_anti")  # Anti = no match
never_ordered.show()  # Bob and Diana

# --- Level 5: 3-table join with broadcast ---
print("\n=== Level 5: 3-table join ===")
order_items = spark.createDataFrame([
    (1, 101, 2), (2, 102, 1), (3, 101, 5),
], ["item_id", "product_id", "qty"])
products = spark.createDataFrame([
    (101, "Widget", "CAT_A"), (102, "Gadget", "CAT_B"),
], ["product_id", "product_name", "cat_id"])
categories = spark.createDataFrame([
    ("CAT_A", "Electronics"), ("CAT_B", "Tools"),
], ["cat_id", "category_name"])

# Chain joins with broadcast on small tables
result = (
    order_items
    .join(broadcast(products), "product_id")  # Broadcast products (small)
    .join(broadcast(categories), "cat_id")    # Broadcast categories (small)
)
result.show()

# --- Level 8: NULL join key behavior ---
print("\n=== Level 8: NULL in join keys ===")
df_a = spark.createDataFrame([(1, "A"), (None, "B"), (3, "C")], ["key", "val_a"])
df_b = spark.createDataFrame([(1, "X"), (None, "Y"), (4, "Z")], ["key", "val_b"])

# Inner join: NULL != NULL (nulls never match!)
df_a.join(df_b, "key", "inner").show()
print("  NULL keys NEVER match in joins (NULL != NULL)!")
print("  Only key=1 matches. Both NULL rows are excluded.")

print("\n\u2705 All homework solutions complete!")