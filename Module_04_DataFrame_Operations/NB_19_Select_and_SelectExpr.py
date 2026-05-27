# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 19: Select and SelectExpr — Choosing Columns
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
# MAGIC ### Real-World Analogy: Picking Items from a Shopping Cart
# MAGIC
# MAGIC A DataFrame often has many columns, but you rarely need all of them.
# MAGIC
# MAGIC Think of `select()` like choosing only the groceries you want from a shopping cart:
# MAGIC - You may only want **milk and bread** → pick specific columns
# MAGIC - You may want to **rename items** → alias columns
# MAGIC - You may want to **calculate totals** → create expressions
# MAGIC - You may want a shortcut with SQL-like expressions → `selectExpr()`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What These Functions Do
# MAGIC
# MAGIC * `select()` → choose columns using column objects or names
# MAGIC * `selectExpr()` → choose columns using SQL expressions as strings
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When You Use Them
# MAGIC
# MAGIC You use these constantly for:
# MAGIC * reducing wide tables
# MAGIC * renaming columns
# MAGIC * creating calculated columns
# MAGIC * flattening nested fields
# MAGIC * writing cleaner pipelines

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### `select()` Flow
# MAGIC
# MAGIC ```text
# MAGIC Input DataFrame
# MAGIC    ↓
# MAGIC Choose wanted columns
# MAGIC    ↓
# MAGIC Optional transformations / aliases
# MAGIC    ↓
# MAGIC New narrower DataFrame
# MAGIC ```
# MAGIC
# MAGIC ### `selectExpr()` Flow
# MAGIC
# MAGIC ```text
# MAGIC Input DataFrame
# MAGIC    ↓
# MAGIC Pass SQL-style strings
# MAGIC    ↓
# MAGIC Spark parses expressions
# MAGIC    ↓
# MAGIC New DataFrame with derived columns
# MAGIC ```
# MAGIC
# MAGIC ### Key Difference
# MAGIC
# MAGIC ```text
# MAGIC select()      = Python / Column API style
# MAGIC selectExpr()  = SQL expression string style
# MAGIC ```
# MAGIC
# MAGIC ### Example Mental Model
# MAGIC
# MAGIC ```text
# MAGIC Original row:
# MAGIC [id=1, name=Alice, salary=5000, dept=IT]
# MAGIC
# MAGIC select("name", "dept")
# MAGIC → [name=Alice, dept=IT]
# MAGIC
# MAGIC selectExpr("name", "salary * 12 as annual_salary")
# MAGIC → [name=Alice, annual_salary=60000]
# MAGIC ```
# MAGIC
# MAGIC ### Important Rule
# MAGIC
# MAGIC Both return a **new DataFrame**.
# MAGIC They do **not** change the original DataFrame in place.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Basic select
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: Basic select
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== Basic select() ===")
print()

# Create sample employee data
employees = [
    (1, "Alice", "IT", 5000),
    (2, "Bob", "HR", 4500),
    (3, "Charlie", "Finance", 6000),
]

df = spark.createDataFrame(employees, ["id", "name", "dept", "salary"])

print("--- Original DataFrame ---")
df.show()

print("--- Select only name and dept ---")
df_selected = df.select("name", "dept")
df_selected.show()

print("--- Expected Output ---")
print("Two columns only: name and dept")
print("Alice IT | Bob HR | Charlie Finance")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Alias with select
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: Alias with select
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== Alias columns with select() ===")
print()

sales = [
    (101, "Laptop", 800),
    (102, "Phone", 500),
    (103, "Tablet", 300),
]

df = spark.createDataFrame(sales, ["product_id", "product_name", "price"])

print("--- Rename columns during select ---")
df_alias = df.select(
    col("product_id").alias("id"),
    col("product_name").alias("item"),
    col("price").alias("unit_price")
)
df_alias.show()

print("--- Expected Output ---")
print("Columns renamed to: id, item, unit_price")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: selectExpr basics
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: selectExpr basics
# ═══════════════════════════════════════════════════════

print("=== Basic selectExpr() ===")
print()

orders = [
    (1, "Alice", 2, 250),
    (2, "Bob", 1, 700),
    (3, "Charlie", 4, 100),
]

df = spark.createDataFrame(orders, ["order_id", "customer", "qty", "price"])

print("--- Original DataFrame ---")
df.show()

print("--- Use selectExpr to compute total ---")
df_expr = df.selectExpr(
    "customer",
    "qty",
    "price",
    "qty * price as total_amount"
)
df_expr.show()

print("--- Expected Output ---")
print("Alice 2 250 500 | Bob 1 700 700 | Charlie 4 100 400")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Mixed expressions
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 1: Mixed expressions
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, upper, round as spark_round

print("=== Mixing raw columns and expressions ===")
print()

students = [
    (1, "alice", 78.456),
    (2, "bob", 88.912),
    (3, "charlie", 91.111),
]

df = spark.createDataFrame(students, ["id", "name", "score"])

# Use select with functions
result = df.select(
    col("id"),
    upper(col("name")).alias("name_upper"),
    spark_round(col("score"), 1).alias("score_rounded")
)

result.show()

print("--- What happened ---")
print("name converted to uppercase")
print("score rounded to 1 decimal place")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: selectExpr with SQL functions
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 2: selectExpr with SQL functions
# ═══════════════════════════════════════════════════════

print("=== selectExpr with SQL-style functions ===")
print()

transactions = [
    (1, "North", 1000.0),
    (2, "South", 1200.5),
    (3, "East", 950.25),
]

df = spark.createDataFrame(transactions, ["txn_id", "region", "amount"])

result = df.selectExpr(
    "txn_id",
    "upper(region) as region_upper",
    "amount",
    "amount * 0.18 as tax",
    "amount + (amount * 0.18) as gross_amount"
)

result.show()

print("--- Why selectExpr is useful ---")
print("You can write quick SQL-like logic without importing many functions")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Nested field selection
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 3: Nested field selection
# ═══════════════════════════════════════════════════════

from pyspark.sql import Row

print("=== Selecting nested fields ===")
print()

# Create nested data
rows = [
    Row(id=1, name="Alice", address=Row(city="Bangalore", zip="560001")),
    Row(id=2, name="Bob", address=Row(city="Mumbai", zip="400001")),
]

df = spark.createDataFrame(rows)

print("--- Original schema ---")
df.printSchema()

print("--- Select nested fields ---")
df_nested = df.select(
    "name",
    "address.city",
    "address.zip"
)
df_nested.show()

print("--- Expected Output ---")
print("Columns: name, city, zip")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Dynamic projection helper
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 1: Dynamic projection helper
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== Dynamic column projection helper ===")
print()

# Production-style reusable function
def project_columns(df, keep_cols, rename_map=None):
    """
    Select only needed columns and optionally rename them.
    """
    rename_map = rename_map or {}
    selected_exprs = []
    
    for c in keep_cols:
        if c in rename_map:
            selected_exprs.append(col(c).alias(rename_map[c]))
        else:
            selected_exprs.append(col(c))
    
    return df.select(*selected_exprs)

customers = [
    (1, "Alice", "Premium", "NY"),
    (2, "Bob", "Standard", "CA"),
    (3, "Charlie", "Premium", "TX"),
]

df = spark.createDataFrame(customers, ["customer_id", "name", "segment", "state"])

result = project_columns(
    df,
    keep_cols=["customer_id", "name", "segment"],
    rename_map={"customer_id": "id", "segment": "customer_segment"}
)

result.show()

print("--- Use case ---")
print("Useful in ETL pipelines when downstream teams need only approved columns")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Standardizing columns with selectExpr
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 2: Standardizing columns with selectExpr
# ═══════════════════════════════════════════════════════

print("=== Standardizing data with selectExpr ===")
print()

raw_people = [
    (1, " alice ", "IT", 5000),
    (2, " BOB", "HR", 4500),
    (3, "charlie ", "Finance", 7000),
]

df = spark.createDataFrame(raw_people, ["id", "name", "dept", "salary"])

clean_df = df.selectExpr(
    "id",
    "trim(name) as clean_name",
    "upper(dept) as dept_upper",
    "salary",
    "salary * 12 as annual_salary"
)

clean_df.show()

print("--- Production pattern ---")
print("selectExpr is handy for quick standardization, trimming, casing, and calculations")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Select for downstream aggregations
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 3: Select for downstream aggregations
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== Prepare narrow DataFrames for downstream work ===")
print()

wide_data = [
    (1, "Alice", "IT", 5000, "NY", "2024-01-01"),
    (2, "Bob", "IT", 6000, "CA", "2024-01-02"),
    (3, "Charlie", "HR", 4500, "TX", "2024-01-03"),
]

df = spark.createDataFrame(wide_data, ["id", "name", "dept", "salary", "state", "join_date"])

# Reduce width before expensive operations
narrow_df = df.select("dept", "salary")

print("--- Original schema ---")
df.printSchema()

print("--- Narrow schema ---")
narrow_df.printSchema()

print("--- Why this matters ---")
print("Selecting only needed columns reduces memory, shuffle size, and overall cost")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake 1: Thinking `select()` changes the original DataFrame
# MAGIC **Problem:** Users expect the original DataFrame to lose columns.  
# MAGIC **Fix:** Assign the result to a new variable.
# MAGIC
# MAGIC ### Mistake 2: Mixing Python syntax inside `selectExpr()` incorrectly
# MAGIC **Problem:** Writing Python logic in SQL strings causes parse errors.  
# MAGIC **Fix:** Use SQL syntax inside `selectExpr()` strings.
# MAGIC
# MAGIC ### Mistake 3: Forgetting `.alias()` for derived columns
# MAGIC **Problem:** Expression column names become ugly like `(qty * price)`.  
# MAGIC **Fix:** Always alias computed columns.
# MAGIC
# MAGIC ### Mistake 4: Selecting too many columns too early
# MAGIC **Problem:** Wide DataFrames increase memory and shuffle costs.  
# MAGIC **Fix:** Project only needed columns as early as possible.
# MAGIC
# MAGIC ### Mistake 5: Incorrect nested column references
# MAGIC **Problem:** Using wrong nested paths like `address_city` instead of `address.city`.  
# MAGIC **Fix:** Use dot notation for nested structs.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC * Level 1: Create a DataFrame and select two columns.
# MAGIC * Level 2: Rename three columns using `alias()`.
# MAGIC * Level 3: Use `selectExpr()` to calculate total price.
# MAGIC * Level 4: Use both `select()` and `selectExpr()` on the same DataFrame and compare outputs.
# MAGIC * Level 5: Select nested fields from a struct column.
# MAGIC * Level 6: Create a reusable function that keeps only approved columns.
# MAGIC * Level 7: Standardize text columns using `trim()` and `upper()` inside `selectExpr()`.
# MAGIC * Level 8: Create calculated KPI columns with `selectExpr()`.
# MAGIC * Level 9: Build a projection step before aggregation in a mini ETL flow.
# MAGIC * Level 10: Teach someone when to choose `select()` vs `selectExpr()` with examples.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

from pyspark.sql import Row
from pyspark.sql.functions import col

# Level 1: Select two columns
print("=== Level 1 ===")
df1 = spark.createDataFrame([(1, "Alice", 25), (2, "Bob", 30)], ["id", "name", "age"])
df1.select("name", "age").show()

# Level 2: Rename three columns
print("\n=== Level 2 ===")
df2 = spark.createDataFrame([(101, "Laptop", 800)], ["product_id", "product_name", "price"])
df2.select(
    col("product_id").alias("id"),
    col("product_name").alias("item"),
    col("price").alias("cost")
).show()

# Level 3: selectExpr total
print("\n=== Level 3 ===")
df3 = spark.createDataFrame([(1, 2, 100), (2, 3, 150)], ["order_id", "qty", "price"])
df3.selectExpr("order_id", "qty * price as total").show()

# Level 5: Nested fields
print("\n=== Level 5 ===")
df5 = spark.createDataFrame([
    Row(id=1, info=Row(city="Delhi", pin="110001")),
    Row(id=2, info=Row(city="Pune", pin="411001"))
])
df5.select("id", "info.city", "info.pin").show()

# Level 7: Standardization
print("\n=== Level 7 ===")
df7 = spark.createDataFrame([(1, " alice ", "it")], ["id", "name", "dept"])
df7.selectExpr("id", "trim(name) as clean_name", "upper(dept) as dept_upper").show()

# Level 9: Projection before aggregation
print("\n=== Level 9 ===")
df9 = spark.createDataFrame([
    (1, "IT", 5000, "NY"),
    (2, "IT", 6000, "CA"),
    (3, "HR", 4500, "TX")
], ["id", "dept", "salary", "state"])
narrow = df9.select("dept", "salary")
narrow.groupBy("dept").avg("salary").show()

print("\n✅ Homework solutions complete!")