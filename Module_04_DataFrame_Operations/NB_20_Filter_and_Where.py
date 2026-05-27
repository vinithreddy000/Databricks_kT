# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 20: Filter and Where — Selecting Rows
# MAGIC # Module: DataFrame Operations
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 45 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: A Bouncer at a Club
# MAGIC
# MAGIC `filter()` and `where()` are the bouncers of your DataFrame — they decide which rows get in and which stay out.
# MAGIC
# MAGIC - **Simple filter** = "Only people over 21" → single condition
# MAGIC - **Compound filter** = "Over 21 AND on the guest list" → AND logic
# MAGIC - **OR filter** = "VIP OR has a ticket" → OR logic
# MAGIC - **NOT filter** = "NOT on the banned list" → negation
# MAGIC - **Pattern filter** = "Name starts with 'A'" → like/contains
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Fact
# MAGIC
# MAGIC `filter()` and `where()` are **identical** — they’re aliases of each other. Use whichever reads better.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What You’ll Learn
# MAGIC
# MAGIC 1. Single conditions (==, !=, >, <, >=, <=)
# MAGIC 2. Compound conditions (AND, OR, NOT)
# MAGIC 3. String patterns (contains, startsWith, like, rlike)
# MAGIC 4. NULL handling (isNull, isNotNull)
# MAGIC 5. IN / between / isin
# MAGIC 6. Production filtering patterns

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Filter Execution
# MAGIC
# MAGIC ```text
# MAGIC Input DataFrame (all rows)
# MAGIC    │
# MAGIC    │  Apply condition to EACH row
# MAGIC    │  True  → keep row
# MAGIC    │  False → discard row
# MAGIC    │  Null  → discard row (null is NOT true!)
# MAGIC    ▼
# MAGIC Output DataFrame (fewer rows, same columns)
# MAGIC ```
# MAGIC
# MAGIC ### Condition Operators
# MAGIC
# MAGIC ```text
# MAGIC Python style:          SQL string style:
# MAGIC ───────────────        ──────────────────
# MAGIC col("age") > 25        "age > 25"
# MAGIC col("name") == "Bob"   "name = 'Bob'"
# MAGIC col("x").isNull()      "x IS NULL"
# MAGIC (col("a") > 1) &       "a > 1 AND b < 10"
# MAGIC (col("b") < 10)
# MAGIC ```
# MAGIC
# MAGIC ### Predicate Pushdown
# MAGIC
# MAGIC ```text
# MAGIC When reading Parquet/Delta:
# MAGIC   filter(col("region") == "US")
# MAGIC   → Spark pushes condition INTO the file scan
# MAGIC   → Only reads files/row-groups with region=US
# MAGIC   → Huge performance gain!
# MAGIC
# MAGIC Always filter EARLY in your pipeline.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Basic filter conditions
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: Basic filter conditions
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== Basic Filter Conditions ===")
print()

# Create sample data
employees = [
    (1, "Alice", "Engineering", 95000, 30),
    (2, "Bob", "Marketing", 72000, 25),
    (3, "Charlie", "Engineering", 110000, 35),
    (4, "Diana", "HR", 65000, 28),
    (5, "Eve", "Marketing", 88000, 32),
]
df = spark.createDataFrame(employees, ["id", "name", "dept", "salary", "age"])

print("--- Original DataFrame ---")
df.show()

# --- Equality filter ---
print("--- 1. Equality: dept == 'Engineering' ---")
df.filter(col("dept") == "Engineering").show()

# --- Greater than ---
print("--- 2. Greater than: salary > 80000 ---")
df.filter(col("salary") > 80000).show()

# --- Less than or equal ---
print("--- 3. Less than or equal: age <= 30 ---")
df.filter(col("age") <= 30).show()

# --- Not equal ---
print("--- 4. Not equal: dept != 'HR' ---")
df.filter(col("dept") != "HR").show()

# --- Using SQL string (where is alias of filter) ---
print("--- 5. SQL string style with where() ---")
df.where("salary >= 90000").show()
print("filter() and where() are IDENTICAL")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: AND, OR, NOT
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: AND, OR, NOT
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== Compound Conditions: AND, OR, NOT ===")
print()

# Reuse employee data
employees = [
    (1, "Alice", "Engineering", 95000, 30),
    (2, "Bob", "Marketing", 72000, 25),
    (3, "Charlie", "Engineering", 110000, 35),
    (4, "Diana", "HR", 65000, 28),
    (5, "Eve", "Marketing", 88000, 32),
]
df = spark.createDataFrame(employees, ["id", "name", "dept", "salary", "age"])

# --- AND: both conditions must be true ---
print("--- 1. AND: Engineering AND salary > 100000 ---")
df.filter((col("dept") == "Engineering") & (col("salary") > 100000)).show()
# NOTE: Each condition MUST be in parentheses with &

# --- OR: either condition can be true ---
print("--- 2. OR: HR OR Marketing ---")
df.filter((col("dept") == "HR") | (col("dept") == "Marketing")).show()

# --- NOT: negate a condition ---
print("--- 3. NOT: NOT Engineering ---")
df.filter(~(col("dept") == "Engineering")).show()

# --- Combined: complex logic ---
print("--- 4. Complex: (Engineering OR salary > 80000) AND age > 28 ---")
df.filter(
    ((col("dept") == "Engineering") | (col("salary") > 80000)) & (col("age") > 28)
).show()

print("--- CRITICAL: Always wrap each condition in () when using & or | ---")
print("  Wrong: col('a') > 1 & col('b') < 10  (operator precedence bug!)")
print("  Right: (col('a') > 1) & (col('b') < 10)")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: isin, between, isNull
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: isin, between, isNull
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== isin, between, NULL handling ===")
print()

# Data with some nulls
products = [
    (1, "Laptop", "Electronics", 999.0),
    (2, "Shirt", "Clothing", 29.0),
    (3, "Phone", "Electronics", 699.0),
    (4, "Book", "Education", None),  # Null price!
    (5, "Tablet", "Electronics", 449.0),
    (6, "Pen", None, 2.0),  # Null category!
]
df = spark.createDataFrame(products, ["id", "name", "category", "price"])

print("--- Original (note nulls) ---")
df.show()

# --- isin: match against a list ---
print("--- 1. isin: category in ['Electronics', 'Education'] ---")
df.filter(col("category").isin("Electronics", "Education")).show()

# --- between: range filter ---
print("--- 2. between: price between 100 and 700 ---")
df.filter(col("price").between(100, 700)).show()

# --- isNull: find null values ---
print("--- 3. isNull: find rows with null price ---")
df.filter(col("price").isNull()).show()

# --- isNotNull: exclude nulls ---
print("--- 4. isNotNull: only rows with non-null category ---")
df.filter(col("category").isNotNull()).show()

# --- Important: null behavior ---
print("--- 5. NULL behavior in filters ---")
print("  filter(col('price') > 100):")
df.filter(col("price") > 100).show()
print("  NULL rows are EXCLUDED (null > 100 is NOT true)")
print("  If you need nulls, explicitly handle them!")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: String pattern matching
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: String pattern matching
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== String Filtering: contains, startsWith, like, rlike ===")
print()

names = [
    (1, "Alice Johnson", "alice.johnson@company.com"),
    (2, "Bob Smith", "bob.smith@company.com"),
    (3, "Charlie Brown", "charlie.b@external.org"),
    (4, "Diana Prince", "diana.prince@company.com"),
    (5, "Eve Adams", "eve.adams@partner.net"),
]
df = spark.createDataFrame(names, ["id", "name", "email"])

# --- contains ---
print("--- 1. contains: name contains 'li' ---")
df.filter(col("name").contains("li")).show(truncate=False)

# --- startsWith / endsWith ---
print("--- 2. startsWith: name starts with 'D' ---")
df.filter(col("name").startsWith("D")).show(truncate=False)

print("--- 3. endsWith: email ends with 'company.com' ---")
df.filter(col("email").endsWith("company.com")).show(truncate=False)

# --- like: SQL LIKE pattern (% = any chars, _ = one char) ---
print("--- 4. like: name like '%own%' ---")
df.filter(col("name").like("%own%")).show(truncate=False)

# --- rlike: regex pattern ---
print("--- 5. rlike (regex): email matches '@company|@partner' ---")
df.filter(col("email").rlike(r"@(company|partner)")).show(truncate=False)

print("--- Summary ---")
print("  contains('x')     → substring match")
print("  startsWith('x')   → prefix match")
print("  endsWith('x')     → suffix match")
print("  like('%x%')        → SQL wildcard match")
print("  rlike('pattern')   → full regex power")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Filtering with dates
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Filtering with dates
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, to_date, current_date, datediff, lit
from datetime import date

print("=== Filtering by Dates ===")
print()

# Create data with dates
events = [
    (1, "Login", "2024-01-15"),
    (2, "Purchase", "2024-03-20"),
    (3, "Login", "2024-06-10"),
    (4, "Signup", "2024-09-05"),
    (5, "Purchase", "2024-12-25"),
]
df = spark.createDataFrame(events, ["id", "event", "event_date"])
df = df.withColumn("event_date", to_date("event_date"))  # Convert string to date

print("--- Original ---")
df.show()

# --- Filter by specific date ---
print("--- 1. After a specific date ---")
df.filter(col("event_date") > lit("2024-06-01")).show()

# --- Filter date range ---
print("--- 2. Between two dates ---")
df.filter(col("event_date").between("2024-03-01", "2024-09-30")).show()

# --- Filter by year/month (SQL string style) ---
print("--- 3. Filter by year and quarter (SQL string) ---")
df.filter("month(event_date) >= 6 AND month(event_date) <= 9").show()

# --- Last N days pattern ---
print("--- 4. Last N days pattern (production pattern) ---")
days_back = 365  # Last year
df_recent = df.filter(datediff(current_date(), col("event_date")) <= days_back)
df_recent.show()
print(f"  Events within last {days_back} days: {df_recent.count()} rows")

print("\n--- Best practice: Filter dates EARLY for partition pruning ---")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Chained and dynamic filters
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Chained and dynamic filters
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col
from functools import reduce

print("=== Chained Filters & Dynamic Filter Building ===")
print()

data = [
    (1, "Alice", "Engineering", 95000, "US"),
    (2, "Bob", "Marketing", 72000, "UK"),
    (3, "Charlie", "Engineering", 110000, "US"),
    (4, "Diana", "HR", 65000, "IN"),
    (5, "Eve", "Engineering", 88000, "UK"),
]
df = spark.createDataFrame(data, ["id", "name", "dept", "salary", "country"])

# --- Chaining filters (each is an AND) ---
print("--- 1. Chained filters (equivalent to AND) ---")
result = (
    df
    .filter(col("dept") == "Engineering")  # First filter
    .filter(col("salary") > 90000)         # AND second filter
    .filter(col("country") == "US")        # AND third filter
)
result.show()
print("Chaining .filter() = implicit AND between each condition")

# --- Dynamic filter building ---
print("\n--- 2. Dynamic filter from a dictionary ---")
def build_filters(df, filter_dict):
    """Apply filters dynamically from a config dictionary."""
    for column, value in filter_dict.items():
        if isinstance(value, list):  # If list, use isin
            df = df.filter(col(column).isin(value))
        else:  # Single value, use equality
            df = df.filter(col(column) == value)
    return df

# Simulating config-driven filters
filters = {"dept": "Engineering", "country": ["US", "UK"]}
result = build_filters(df, filters)
result.show()
print(f"  Applied filters: {filters}")

# --- Combining conditions dynamically ---
print("\n--- 3. Building OR conditions dynamically ---")
depts_wanted = ["Engineering", "Marketing"]
condition = reduce(lambda a, b: a | b, [col("dept") == d for d in depts_wanted])
df.filter(condition).show()
print(f"  Dynamic OR across: {depts_wanted}")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Performance-aware filtering
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Performance-aware filtering
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, year

print("=== Filter Performance: Pushdown & Partition Pruning ===")
print()

# Create partitioned Delta data
df_big = (
    spark.range(100000)
    .withColumn("region", (col("id") % 4).cast("string"))
    .withColumn("amount", col("id") * 1.5)
    .withColumn("year", (col("id") % 3 + 2022).cast("int"))
)

path = "/tmp/filter_demo/partitioned"
df_big.write.mode("overwrite").partitionBy("region", "year").parquet(path)

# --- Demonstrate predicate pushdown ---
print("--- 1. Predicate pushdown (check physical plan) ---")
df_read = spark.read.parquet(path)
df_filtered = df_read.filter((col("region") == "2") & (col("year") == 2023))

print(f"  Filtered count: {df_filtered.count():,}")
print("\n  Physical plan:")
df_filtered.explain()
print("  Look for 'PartitionFilters' — Spark skips entire folders!")

# --- Filter order matters for readability (not performance) ---
print("\n--- 2. Spark optimizer reorders filters automatically ---")
# These produce the SAME physical plan:
plan_a = df_read.filter(col("amount") > 50000).filter(col("region") == "1")
plan_b = df_read.filter(col("region") == "1").filter(col("amount") > 50000)
print(f"  Plan A count: {plan_a.count():,}")
print(f"  Plan B count: {plan_b.count():,}")
print("  Catalyst optimizer pushes partition filters first regardless of order")

# --- Anti-pattern: UDF in filter ---
print("\n--- 3. Anti-pattern: UDFs prevent pushdown ---")
print("  BAD:  df.filter(my_udf(col('x')) > 10)  → Cannot push down!")
print("  GOOD: df.filter(col('x') > 10)           → Pushed to file scan!")
print("  Rule: Use built-in functions whenever possible for filter conditions")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Null-safe filtering patterns
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Null-safe filtering
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, coalesce, lit, when

print("=== Null-Safe Filtering Patterns ===")
print()

# Data with various nulls
data = [
    (1, "Alice", "Premium", 100.0),
    (2, "Bob", None, 50.0),        # Null segment
    (3, "Charlie", "Standard", None),  # Null amount
    (4, "Diana", None, None),       # Both null
    (5, "Eve", "Premium", 200.0),
]
df = spark.createDataFrame(data, ["id", "name", "segment", "amount"])

print("--- Original (with nulls) ---")
df.show()

# --- Problem: equality doesn't match null ---
print("--- 1. Problem: col == None doesn't work! ---")
df.filter(col("segment") == None).show()  # Returns 0 rows!
print("  EMPTY! Use .isNull() instead of == None")

# --- Correct null check ---
print("--- 2. Correct: isNull() ---")
df.filter(col("segment").isNull()).show()

# --- Null-safe equality (<=>) ---
print("--- 3. Null-safe equality: eqNullSafe ---")
# This returns true when BOTH sides are null
df.filter(col("segment").eqNullSafe(None)).show()
print("  eqNullSafe: treats null == null as TRUE")

# --- coalesce for null defaults ---
print("--- 4. Filter with coalesce (default for nulls) ---")
# Treat null amounts as 0, then filter
df.filter(coalesce(col("amount"), lit(0)) > 50).show()
print("  coalesce replaces null with 0 before comparing")

# --- Production pattern: explicit null handling ---
print("--- 5. Production: Include nulls explicitly ---")
df.filter(
    (col("segment") == "Premium") | col("segment").isNull()
).show()
print("  'Premium OR null' — explicitly decide what happens with nulls")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Reusable filter builder
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Reusable filter builder
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from functools import reduce

print("=== Production: Reusable Filter Builder ===")
print()

class DataFilter:
    """Production-grade filter builder for reusable conditions."""
    
    def __init__(self):
        self.conditions = []  # List of Column conditions
    
    def equals(self, column, value):
        """Add equality condition."""
        self.conditions.append(col(column) == value)
        return self  # Enable chaining
    
    def in_list(self, column, values):
        """Add isin condition."""
        self.conditions.append(col(column).isin(values))
        return self
    
    def between(self, column, low, high):
        """Add between condition."""
        self.conditions.append(col(column).between(low, high))
        return self
    
    def not_null(self, column):
        """Add isNotNull condition."""
        self.conditions.append(col(column).isNotNull())
        return self
    
    def apply(self, df):
        """Apply all conditions as AND to a DataFrame."""
        if not self.conditions:
            return df
        combined = reduce(lambda a, b: a & b, self.conditions)
        return df.filter(combined)

# Demo usage
data = [
    (1, "Alice", "Engineering", 95000, "US"),
    (2, "Bob", "Marketing", 72000, "UK"),
    (3, "Charlie", "Engineering", 110000, "US"),
    (4, "Diana", "HR", 65000, "IN"),
    (5, "Eve", "Engineering", 88000, None),
]
df = spark.createDataFrame(data, ["id", "name", "dept", "salary", "country"])

# Build and apply filter
f = DataFilter()
result = (
    f.equals("dept", "Engineering")
     .between("salary", 80000, 120000)
     .not_null("country")
     .apply(df)
)

print("--- Filter: Engineering AND salary 80K-120K AND country not null ---")
result.show()
print("--- Key: Encapsulate filter logic for reuse across notebooks ---")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Missing Parentheses with & and |
# MAGIC **Problem:** `col("a") > 1 & col("b") < 10` → wrong result due to operator precedence.  
# MAGIC **Fix:** Always wrap: `(col("a") > 1) & (col("b") < 10)`.
# MAGIC
# MAGIC ### Mistake #2: Using Python `and`/`or` Instead of `&`/`|`
# MAGIC **Problem:** `col("a") > 1 and col("b") < 10` throws an error.  
# MAGIC **Fix:** Use bitwise `&` (AND) and `|` (OR) for PySpark Column conditions.
# MAGIC
# MAGIC ### Mistake #3: Comparing to None with ==
# MAGIC **Problem:** `col("x") == None` returns zero rows (not valid comparison).  
# MAGIC **Fix:** Use `col("x").isNull()` or `col("x").isNotNull()`.
# MAGIC
# MAGIC ### Mistake #4: Forgetting That Nulls Are Excluded
# MAGIC **Problem:** `filter(col("score") != 100)` silently drops rows where score is null.  
# MAGIC **Fix:** Add explicit null handling: `(col("score") != 100) | col("score").isNull()`.
# MAGIC
# MAGIC ### Mistake #5: Filtering AFTER Expensive Operations
# MAGIC **Problem:** Running joins/aggregations on full data, then filtering afterward.  
# MAGIC **Fix:** Push filters as early as possible in the pipeline (before joins, before shuffles).

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC **Level 1:** Filter a DataFrame for rows where salary > 50000.
# MAGIC
# MAGIC **Level 2:** Filter with two AND conditions (dept = 'IT' AND age > 30).
# MAGIC
# MAGIC **Level 3:** Filter with OR (dept = 'IT' OR dept = 'HR').
# MAGIC
# MAGIC **Level 4:** Use isin() and between() to filter.
# MAGIC
# MAGIC **Level 5:** Filter by string pattern (names starting with 'A' using startsWith or like).
# MAGIC
# MAGIC **Level 6:** Handle nulls: find rows with null values, then filter excluding nulls.
# MAGIC
# MAGIC **Level 7:** Filter by date range (events in last 90 days).
# MAGIC
# MAGIC **Level 8:** Build a dynamic filter function that accepts a dictionary of conditions.
# MAGIC
# MAGIC **Level 9:** Demonstrate predicate pushdown: write partitioned data, filter on partition column, check explain().
# MAGIC
# MAGIC **Level 10:** Build a production DataFilter class with method chaining and null-safe options.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, to_date, current_date, datediff, lit
from functools import reduce

# Sample data for solutions
df = spark.createDataFrame([
    (1, "Alice", "IT", 75000, 32, "2024-01-15"),
    (2, "Bob", "HR", 55000, 28, "2024-06-20"),
    (3, "Charlie", "IT", 90000, 35, "2024-09-10"),
    (4, "Diana", "Finance", 48000, 24, None),
    (5, "Eve", "IT", 62000, None, "2024-03-05"),
], ["id", "name", "dept", "salary", "age", "join_date"])

# Level 1
print("=== Level 1 ===")
df.filter(col("salary") > 50000).show()

# Level 2
print("=== Level 2 ===")
df.filter((col("dept") == "IT") & (col("age") > 30)).show()

# Level 3
print("=== Level 3 ===")
df.filter((col("dept") == "IT") | (col("dept") == "HR")).show()

# Level 4
print("=== Level 4 ===")
df.filter(col("dept").isin("IT", "HR")).show()
df.filter(col("salary").between(50000, 80000)).show()

# Level 5
print("=== Level 5 ===")
df.filter(col("name").startsWith("A")).show()
df.filter(col("name").like("A%")).show()

# Level 6
print("=== Level 6 ===")
df.filter(col("age").isNull()).show()  # Find nulls
df.filter(col("age").isNotNull()).show()  # Exclude nulls

# Level 7
print("=== Level 7 ===")
df_dates = df.withColumn("join_date", to_date("join_date"))
df_dates.filter(datediff(current_date(), col("join_date")) <= 365).show()

# Level 8
print("=== Level 8 ===")
def apply_filters(df, filters):
    for c, v in filters.items():
        df = df.filter(col(c).isin(v) if isinstance(v, list) else col(c) == v)
    return df

apply_filters(df, {"dept": ["IT", "HR"], "salary": 75000}).show()

print("\n\u2705 All homework complete!")