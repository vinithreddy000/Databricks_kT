# Databricks notebook source
# DBTITLE 1,Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 26: Union, Intersect, Except
# MAGIC # Module: DataFrame Operations
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 40 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: Stacking and Comparing Lists
# MAGIC
# MAGIC - **Union** = Stacking two guest lists on top of each other (combine all names)
# MAGIC - **Intersect** = Highlighting names that appear on BOTH lists
# MAGIC - **Except** = Crossing out names from list A that also appear on list B
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Set Operations Summary
# MAGIC
# MAGIC | Operation | SQL Equivalent | Result |
# MAGIC |-----------|---------------|--------|
# MAGIC | `union()` / `unionAll()` | UNION ALL | All rows from both (keeps duplicates) |
# MAGIC | `unionByName()` | UNION ALL (by name) | Match columns by name, not position |
# MAGIC | `intersect()` | INTERSECT | Rows in BOTH (deduped) |
# MAGIC | `intersectAll()` | INTERSECT ALL | Rows in both (keeps duplicates) |
# MAGIC | `subtract()` / `exceptAll()` | EXCEPT / EXCEPT ALL | Rows in left but NOT in right |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Facts
# MAGIC 1. `union()` and `unionAll()` are **identical** in PySpark (unlike SQL where UNION deduplicates)
# MAGIC 2. `union()` matches by **column position**, NOT by name!
# MAGIC 3. `unionByName()` matches by **column name** (safer!)
# MAGIC 4. All require same number of columns (except `unionByName(allowMissingColumns=True)`)
# MAGIC 5. `intersect()` and `subtract()` deduplicate automatically

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Union: Position-Based vs Name-Based
# MAGIC
# MAGIC ```
# MAGIC union() matches by POSITION:
# MAGIC   df1: [name, age]     df2: [age, name]    ← Column order different!
# MAGIC   union() → [name, age] with WRONG data! (age goes into name col)
# MAGIC
# MAGIC unionByName() matches by NAME:
# MAGIC   df1: [name, age]     df2: [age, name]
# MAGIC   unionByName() → [name, age] — correctly matched!
# MAGIC ```
# MAGIC
# MAGIC ### Set Operations Visualized
# MAGIC
# MAGIC ```
# MAGIC df1: [A, B, C, C]      df2: [B, C, D]
# MAGIC
# MAGIC union:         [A, B, C, C, B, C, D]     ← Stack all rows
# MAGIC intersect:     [B, C]                    ← In both (deduped)
# MAGIC intersectAll:  [B, C]                    ← In both (with min count)
# MAGIC subtract:      [A]                       ← In df1 not df2 (deduped)
# MAGIC exceptAll:     [A, C]                    ← In df1 not df2 (keeps extra)
# MAGIC ```
# MAGIC
# MAGIC ### Schema Requirements
# MAGIC
# MAGIC ```
# MAGIC All set operations require:
# MAGIC   1. Same NUMBER of columns (except unionByName with allowMissing)
# MAGIC   2. Compatible TYPES at each position
# MAGIC
# MAGIC Fails: df1[int, string] union df2[string, string]  → type mismatch!
# MAGIC Works: df1[int, string] union df2[long, string]    → int widened to long
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: union and unionAll
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: union() and unionAll()
# ═══════════════════════════════════════════════════════

print("=== union() and unionAll() ===")
print()
print("In PySpark, union() = unionAll() (both keep ALL rows including duplicates)")
print("This is DIFFERENT from SQL where UNION removes duplicates!")
print()

# --- Two DataFrames with same schema ---
jan_sales = spark.createDataFrame([
    (1, "Alice", 500), (2, "Bob", 300), (3, "Alice", 200),
], ["id", "name", "amount"])

feb_sales = spark.createDataFrame([
    (4, "Charlie", 400), (5, "Alice", 600), (2, "Bob", 300),  # Bob 300 = dup!
], ["id", "name", "amount"])

print("--- January sales (3 rows) ---")
jan_sales.show()
print("--- February sales (3 rows) ---")
feb_sales.show()

# --- union() = unionAll(): stacks all rows ---
print("--- union() result (6 rows — keeps duplicates!) ---")
combined = jan_sales.union(feb_sales)  # Same as unionAll()
combined.show()
print(f"  Total rows: {combined.count()}")  # 3 + 3 = 6

# --- To get SQL UNION behavior (deduplicate), add .distinct() ---
print("--- union() + distinct() = SQL UNION behavior ---")
combined_dedup = jan_sales.union(feb_sales).distinct()  # Remove exact duplicates
combined_dedup.show()
print(f"  After distinct: {combined_dedup.count()} rows (Bob+300 counted once)")

# --- Chaining multiple unions ---
print("--- Chaining: jan.union(feb).union(mar) ---")
mar_sales = spark.createDataFrame([(6, "Diana", 700)], ["id", "name", "amount"])
all_sales = jan_sales.union(feb_sales).union(mar_sales)  # Chain unions
print(f"  Total: {all_sales.count()} rows")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: unionByName
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: unionByName()
# ═══════════════════════════════════════════════════════

print("=== unionByName() — Safer Column Matching ===")
print()

# --- Columns in DIFFERENT ORDER ---
df1 = spark.createDataFrame([
    ("Alice", 30, "Engineering"),
], ["name", "age", "dept"])  # Order: name, age, dept

df2 = spark.createDataFrame([
    ("Marketing", "Bob", 25),
], ["dept", "name", "age"])  # Order: dept, name, age (DIFFERENT!)

print("--- df1 columns:", df1.columns)
print("--- df2 columns:", df2.columns)

# --- union() = WRONG (matches by position!) ---
print("\n--- union() = WRONG (position-based) ---")
df1.union(df2).show()  # "Marketing" goes into 'name' column! WRONG!
print("  BUG! union() matched by position, not name!")

# --- unionByName() = CORRECT (matches by name) ---
print("--- unionByName() = CORRECT (name-based) ---")
df1.unionByName(df2).show()  # Correctly maps columns by name
print("  CORRECT! unionByName() matched by column name.")

# --- unionByName with allowMissingColumns (Spark 3.1+) ---
print("\n--- unionByName(allowMissingColumns=True) ---")
df_a = spark.createDataFrame([(1, "Alice", 100)], ["id", "name", "score"])
df_b = spark.createDataFrame([(2, "Bob", "VIP")], ["id", "name", "tier"])  # Has 'tier' not 'score'

# Without allowMissing: would FAIL (different column sets)
result = df_a.unionByName(df_b, allowMissingColumns=True)  # Fill missing with null
result.show()
print("  Missing columns filled with NULL!")
print("  df_a has no 'tier' → null; df_b has no 'score' → null")

print("\n--- Best Practice: ALWAYS use unionByName() over union() ---")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: intersect and subtract
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: intersect() and subtract()
# ═══════════════════════════════════════════════════════

print("=== intersect() and subtract() ===")
print()

# Two sets of customers
jan_customers = spark.createDataFrame([
    (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Diana"),
], ["id", "name"])

feb_customers = spark.createDataFrame([
    (2, "Bob"), (3, "Charlie"), (5, "Eve"), (6, "Frank"),
], ["id", "name"])

print("--- January customers ---")
jan_customers.show()
print("--- February customers ---")
feb_customers.show()

# --- intersect(): Rows in BOTH (deduplicates) ---
print("--- intersect(): Customers active in BOTH months ---")
jan_customers.intersect(feb_customers).show()
# Expected: Bob, Charlie (in both)

# --- subtract(): Rows in left but NOT in right ---
print("--- subtract(): Jan customers NOT in Feb (churned!) ---")
jan_customers.subtract(feb_customers).show()
# Expected: Alice, Diana (were in Jan but not Feb)

# --- exceptAll(): Same as subtract but keeps duplicates ---
print("--- New in Feb (subtract other direction) ---")
feb_customers.subtract(jan_customers).show()
# Expected: Eve, Frank (new in Feb)

print("--- Key ---")
print("  intersect = AND (in both)")
print("  subtract  = MINUS (in A but not B)")
print("  Both automatically deduplicate!")
print("  For keeping duplicates: use intersectAll() / exceptAll()")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: intersectAll and exceptAll
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: intersectAll and exceptAll
# ═══════════════════════════════════════════════════════

print("=== intersectAll() and exceptAll() — Duplicate-Aware ===")
print()
print("intersect/subtract remove duplicates. *All variants keep them.")
print()

# DataFrames with duplicates
df1 = spark.createDataFrame([
    (1, "A"), (1, "A"), (1, "A"),  # Three copies of (1, A)
    (2, "B"), (3, "C"),
], ["id", "val"])

df2 = spark.createDataFrame([
    (1, "A"), (1, "A"),  # Two copies of (1, A)
    (2, "B"), (4, "D"),
], ["id", "val"])

print("--- df1 (has 3 copies of (1,A)) ---")
df1.show()
print("--- df2 (has 2 copies of (1,A)) ---")
df2.show()

# --- intersect vs intersectAll ---
print("--- intersect() = deduplicates ---")
df1.intersect(df2).show()  # Just one (1,A) and one (2,B)

print("--- intersectAll() = keeps min count ---")
df1.intersectAll(df2).show()  # Two (1,A) (min of 3 and 2) and one (2,B)
print("  (1,A) appears min(3,2)=2 times")

# --- subtract vs exceptAll ---
print("--- subtract() = deduplicates ---")
df1.subtract(df2).show()  # Only (3,C) — (1,A) removed entirely

print("--- exceptAll() = subtracts counts ---")
df1.exceptAll(df2).show()  # One (1,A) (3-2=1) and (3,C)
print("  (1,A): df1 has 3, df2 has 2 → 3-2=1 remains")
print("  (3,C): df1 has 1, df2 has 0 → 1-0=1 remains")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Schema mismatch handling
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Schema mismatch handling
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import lit

print("=== Schema Mismatch: What Fails and How to Fix ===")
print()

# --- Problem: Different number of columns ---
df_a = spark.createDataFrame([(1, "Alice", 100)], ["id", "name", "score"])
df_b = spark.createDataFrame([(2, "Bob")], ["id", "name"])  # Missing 'score'!

print("--- Problem: df_a has 3 cols, df_b has 2 cols ---")
print(f"  df_a: {df_a.columns}")
print(f"  df_b: {df_b.columns}")

# Method 1: Add missing columns with null/default
print("\n--- Fix 1: Add missing column with null ---")
df_b_fixed = df_b.withColumn("score", lit(None).cast("int"))  # Add missing col
df_a.union(df_b_fixed).show()

# Method 2: unionByName with allowMissingColumns
print("--- Fix 2: unionByName(allowMissingColumns=True) ---")
df_a.unionByName(df_b, allowMissingColumns=True).show()
print("  Automatically adds null for missing columns!")

# --- Problem: Type mismatch ---
print("\n--- Problem: Type mismatch ---")
df_int = spark.createDataFrame([(1, 100)], ["id", "value"])     # value is int
df_str = spark.createDataFrame([(2, "hello")], ["id", "value"])  # value is string

try:
    df_int.union(df_str).show()  # May fail or produce unexpected results
    print("  PySpark may auto-cast, but results can be unexpected!")
except Exception as e:
    print(f"  ERROR: {str(e)[:80]}")

# Fix: Cast to common type before union
print("\n--- Fix: Cast to common type before union ---")
df_int_cast = df_int.withColumn("value", df_int["value"].cast("string"))  # Cast to string
df_int_cast.union(df_str).show()
print("  Always ensure compatible types before union!")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Multi-source combine pattern
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Multi-source combine pattern
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import lit
from functools import reduce

print("=== Multi-Source Union Pattern ===")
print()
print("Common pattern: Combine data from multiple sources/months/files")
print()

# --- Simulate monthly data with varying schemas ---
jan = spark.createDataFrame([
    (1, "Alice", 500, "2024-01-15"),
], ["id", "name", "amount", "date"])

feb = spark.createDataFrame([
    (2, "Bob", 600, "2024-02-20", "online"),  # Extra column: channel!
], ["id", "name", "amount", "date", "channel"])

mar = spark.createDataFrame([
    (3, "Charlie", 700, "2024-03-10", "store", "US"),  # Extra: channel + region!
], ["id", "name", "amount", "date", "channel", "region"])

# --- Method: Reduce with unionByName ---
print("--- Combine all months (handle evolving schema) ---")
all_months = [jan, feb, mar]  # List of DataFrames

# Use reduce to chain unionByName across all DataFrames
combined = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),  # Handle missing cols
    all_months
)
combined.show()
combined.printSchema()
print("  Missing columns filled with null automatically!")
print("  Jan has no channel/region → null")
print("  Feb has no region → null")

# --- Add source tracking ---
print("\n--- Best practice: Add source column ---")
months_with_source = [
    jan.withColumn("source_month", lit("2024-01")),
    feb.withColumn("source_month", lit("2024-02")),
    mar.withColumn("source_month", lit("2024-03")),
]
result = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), months_with_source)
result.show()
print("  Always add a source column for traceability!")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Change detection with set ops
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Change detection with set operations
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import lit

print("=== Change Detection: Using Set Operations ===")
print()
print("Use case: Compare yesterday's data with today's to find changes.")
print()

# --- Yesterday's data ---
yesterday = spark.createDataFrame([
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", "Marketing", 72000),
    (3, "Charlie", "HR", 65000),
    (4, "Diana", "Engineering", 88000),
], ["id", "name", "dept", "salary"])

# --- Today's data (some changes) ---
today = spark.createDataFrame([
    (1, "Alice", "Engineering", 100000),  # Salary changed (95K → 100K)
    (2, "Bob", "Marketing", 72000),       # No change
    (3, "Charlie", "Finance", 65000),     # Dept changed (HR → Finance)
    (5, "Eve", "Engineering", 78000),     # New employee!
], ["id", "name", "dept", "salary"])       # Diana deleted!

# --- Find NEW rows (in today, not in yesterday) ---
print("--- NEW rows (added today) ---")
new_rows = today.subtract(yesterday)  # In today but not yesterday
new_rows.show()
print("  Alice and Charlie appear because their data CHANGED")
print("  Eve is genuinely new")

# --- Find DELETED rows (in yesterday, not in today) ---
print("--- DELETED rows (removed today) ---")
deleted_rows = yesterday.subtract(today)  # In yesterday but not today
deleted_rows.show()
print("  Diana was deleted")
print("  Alice and Charlie appear because old version differs")

# --- Find UNCHANGED rows ---
print("--- UNCHANGED rows (identical in both) ---")
unchanged = yesterday.intersect(today)  # In both = no change
unchanged.show()
print("  Only Bob is completely unchanged")

# --- Summary ---
print("--- Change Detection Summary ---")
print(f"  New/Modified: {new_rows.count()} rows")
print(f"  Deleted/Modified: {deleted_rows.count()} rows")
print(f"  Unchanged: {unchanged.count()} rows")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Dedup across sources
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Dedup across multiple sources
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, lit, row_number, desc
from pyspark.sql.window import Window

print("=== Union + Deduplicate: Priority-Based ===")
print()
print("Use case: Data arrives from multiple sources. Keep best version.")
print()

# --- Same customer data from 3 systems (CRM > ERP > Manual) ---
crm_data = spark.createDataFrame([
    (1, "Alice Johnson", "alice@crm.com", "2024-06-01"),
    (2, "Bob Smith", "bob@crm.com", "2024-05-15"),
], ["id", "name", "email", "updated"]).withColumn("source", lit("CRM")).withColumn("priority", lit(1))

erp_data = spark.createDataFrame([
    (1, "Alice J", "alice@erp.com", "2024-03-01"),  # Older, less complete
    (3, "Charlie Brown", "charlie@erp.com", "2024-04-10"),
], ["id", "name", "email", "updated"]).withColumn("source", lit("ERP")).withColumn("priority", lit(2))

manual_data = spark.createDataFrame([
    (2, "Robert Smith", "bob@manual.com", "2024-01-01"),  # Oldest
    (4, "Diana Prince", "diana@manual.com", "2024-02-01"),
], ["id", "name", "email", "updated"]).withColumn("source", lit("Manual")).withColumn("priority", lit(3))

# --- Step 1: Union all sources ---
all_data = crm_data.unionByName(erp_data).unionByName(manual_data)
print("--- All sources combined (with duplicates) ---")
all_data.show(truncate=False)

# --- Step 2: Deduplicate keeping highest priority (lowest number) ---
window = Window.partitionBy("id").orderBy("priority", desc("updated"))  # CRM > ERP > Manual

deduped = (
    all_data
    .withColumn("rn", row_number().over(window))  # Rank within each id
    .filter(col("rn") == 1)  # Keep only the best source
    .drop("rn", "priority")  # Cleanup
)

print("--- Deduplicated (CRM wins for id=1,2; ERP for id=3; Manual for id=4) ---")
deduped.show(truncate=False)
print("  Pattern: union() all → rank by priority → keep rank 1")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production safe_union function
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Production safe_union function
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import lit, current_timestamp
from functools import reduce

print("=== Production: Safe Union Function ===")
print()

def safe_union(dataframes, add_source=True, source_names=None, deduplicate_key=None):
    """
    Production-grade union of multiple DataFrames.
    - Handles schema mismatches (allowMissingColumns)
    - Optionally adds source tracking
    - Optionally deduplicates on a key
    """
    if not dataframes:
        raise ValueError("No DataFrames to union!")
    
    # Add source column if requested
    if add_source and source_names:
        dataframes = [
            df.withColumn("_source", lit(name))
            for df, name in zip(dataframes, source_names)
        ]
    
    # Add load timestamp
    dataframes = [df.withColumn("_loaded_at", current_timestamp()) for df in dataframes]
    
    # Union all with schema handling
    result = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        dataframes
    )
    
    # Report
    total_input = sum(df.count() for df in dataframes)
    print(f"  Input DataFrames: {len(dataframes)}")
    print(f"  Total input rows: {total_input:,}")
    print(f"  Combined columns: {result.columns}")
    
    # Deduplicate if key provided
    if deduplicate_key:
        before = result.count()
        result = result.dropDuplicates([deduplicate_key])
        after = result.count()
        print(f"  Dedup on '{deduplicate_key}': {before} → {after} ({before - after} removed)")
    
    return result

# --- Demo ---
df1 = spark.createDataFrame([(1, "A", 100)], ["id", "name", "val"])
df2 = spark.createDataFrame([(2, "B", 200, "x")], ["id", "name", "val", "extra"])
df3 = spark.createDataFrame([(1, "A", 150)], ["id", "name", "val"])  # Duplicate id=1!

result = safe_union(
    [df1, df2, df3],
    add_source=True,
    source_names=["system_a", "system_b", "system_c"],
    deduplicate_key="id"
)
result.show(truncate=False)
print("\n--- Key: Always use unionByName + allowMissingColumns in production ---")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Using union() when columns are in different order
# MAGIC **Problem:** `union()` matches by position. If df1 has [name, age] and df2 has [age, name], data goes into wrong columns.  
# MAGIC **Fix:** Always use `unionByName()` — it matches by column name, not position.
# MAGIC
# MAGIC ### Mistake #2: Expecting union() to deduplicate (like SQL UNION)
# MAGIC **Problem:** In SQL, `UNION` removes duplicates. In PySpark, `union()` = `UNION ALL` (keeps all).  
# MAGIC **Fix:** Use `union().distinct()` if you want SQL UNION behavior.
# MAGIC
# MAGIC ### Mistake #3: Union with mismatched column counts
# MAGIC **Problem:** `union()` fails if DataFrames have different number of columns.  
# MAGIC **Fix:** Use `unionByName(allowMissingColumns=True)` (Spark 3.1+) or manually add missing columns with `lit(None)`.
# MAGIC
# MAGIC ### Mistake #4: Not checking types before union
# MAGIC **Problem:** Column at position N has type `int` in df1 and `string` in df2 → unexpected casts or errors.  
# MAGIC **Fix:** Always verify schemas match or explicitly cast before union.
# MAGIC
# MAGIC ### Mistake #5: Using subtract() expecting order preservation
# MAGIC **Problem:** `subtract()` deduplicates the result and doesn’t preserve row order.  
# MAGIC **Fix:** If you need to keep duplicates, use `exceptAll()`. If you need order, add an `orderBy()` after.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC **Level 1 (Copy & Run):** Union two DataFrames with the same schema. Count the result.
# MAGIC
# MAGIC **Level 2 (Tiny Change):** Use `unionByName()` instead of `union()`. Verify it handles different column orders.
# MAGIC
# MAGIC **Level 3 (Combine Two):** Union 3 monthly DataFrames, then use `distinct()` to deduplicate.
# MAGIC
# MAGIC **Level 4 (New Scenario):** Use `intersect()` to find customers who bought in BOTH January and February.
# MAGIC
# MAGIC **Level 5 (Mini Project):** Use `subtract()` to find churned customers (Jan but not Feb) and new customers (Feb but not Jan).
# MAGIC
# MAGIC **Level 6 (Design First):** Design a pipeline that unions files with evolving schemas (new columns added over time). Write the approach before coding.
# MAGIC
# MAGIC **Level 7 (Optimize):** Union 10 DataFrames efficiently using `reduce()` with `unionByName`. Compare with chaining.
# MAGIC
# MAGIC **Level 8 (Edge Cases):** Test what happens with: null values in intersect, type mismatches in union, empty DataFrames.
# MAGIC
# MAGIC **Level 9 (Production):** Build a `safe_union` function with: source tracking, schema validation, dedup, and logging.
# MAGIC
# MAGIC **Level 10 (Teach It):** Explain the difference between union/unionByName/intersect/subtract/exceptAll with real business examples for each.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import lit
from functools import reduce

# Level 1: Basic union
print("=== Level 1: Basic Union ===")
df1 = spark.createDataFrame([(1,"A"),(2,"B")], ["id","val"])
df2 = spark.createDataFrame([(3,"C"),(4,"D")], ["id","val"])
combined = df1.union(df2)  # Stack them
print(f"  Union count: {combined.count()}")  # 4
combined.show()

# Level 4: Intersect to find common customers
print("\n=== Level 4: Common Customers ===")
jan = spark.createDataFrame([(1,"Alice"),(2,"Bob"),(3,"Charlie")], ["id","name"])
feb = spark.createDataFrame([(2,"Bob"),(3,"Charlie"),(4,"Diana")], ["id","name"])
both_months = jan.intersect(feb)  # Customers in BOTH months
both_months.show()
print(f"  Customers active both months: {both_months.count()}")  # Bob, Charlie

# Level 5: Churn detection
print("\n=== Level 5: Churn Detection ===")
churned = jan.subtract(feb)  # In Jan but not Feb
new_cust = feb.subtract(jan)  # In Feb but not Jan
print("Churned (Jan only):")
churned.show()  # Alice
print("New (Feb only):")
new_cust.show()  # Diana

# Level 7: Efficient multi-union with reduce
print("\n=== Level 7: Efficient Multi-Union ===")
dfs = [spark.createDataFrame([(i, f"row_{i}")], ["id", "val"]) for i in range(10)]
result = reduce(lambda a, b: a.unionByName(b), dfs)  # Chain all 10
print(f"  Combined {len(dfs)} DataFrames: {result.count()} rows")

# Level 8: Edge cases
print("\n=== Level 8: Null in intersect ===")
df_nulls1 = spark.createDataFrame([(1,"A"),(None,"B")], ["id","val"])
df_nulls2 = spark.createDataFrame([(None,"B"),(2,"C")], ["id","val"])
df_nulls1.intersect(df_nulls2).show()  # (None, "B") IS in the intersect!
print("  Nulls DO match in set operations (unlike joins!)")

print("\n\u2705 All homework solutions complete!")