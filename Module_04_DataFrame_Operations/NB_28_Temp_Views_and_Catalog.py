# Databricks notebook source
# DBTITLE 1,Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 28: Temp Views and the Catalog
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
# MAGIC ### Real-World Analogy: Naming Your Spreadsheet Tabs
# MAGIC
# MAGIC A **temp view** gives your DataFrame a SQL name, like naming a tab in a spreadsheet so you can reference it from other formulas.
# MAGIC
# MAGIC - `createTempView("sales")` = Name this DataFrame "sales" so I can `SELECT * FROM sales`
# MAGIC - It lives only in YOUR session (disappears when notebook detaches)
# MAGIC - It's the bridge between Python DataFrames and SQL queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### View Types
# MAGIC
# MAGIC | Type | Scope | Lifetime | Access |
# MAGIC |------|-------|----------|--------|
# MAGIC | `createTempView` | Session | Until session ends | Same notebook |
# MAGIC | `createOrReplaceTempView` | Session | Until session ends | Same notebook (overwrites!) |
# MAGIC | `createGlobalTempView` | Cluster | Until cluster restarts | All notebooks (via `global_temp.name`) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Spark Catalog
# MAGIC
# MAGIC The **catalog** is Spark's metadata registry — it knows about all databases, tables, views, and columns.
# MAGIC
# MAGIC - `spark.catalog.listTables()` — What tables/views exist?
# MAGIC - `spark.catalog.tableExists("name")` — Does this table exist?
# MAGIC - `spark.catalog.cacheTable("name")` — Cache for faster repeated queries

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### View Lifecycle
# MAGIC
# MAGIC ```
# MAGIC Python world:           SQL world:
# MAGIC ──────────────         ───────────────
# MAGIC
# MAGIC df = spark.read...      (DataFrame exists only in Python)
# MAGIC         │
# MAGIC         ▼
# MAGIC df.createOrReplaceTempView("my_data")
# MAGIC         │
# MAGIC         ▼
# MAGIC                         spark.sql("SELECT * FROM my_data")
# MAGIC                         %sql SELECT * FROM my_data
# MAGIC ```
# MAGIC
# MAGIC ### Session vs Global Temp Views
# MAGIC
# MAGIC ```
# MAGIC Session Temp View:              Global Temp View:
# MAGIC ─────────────────────         ─────────────────────
# MAGIC
# MAGIC Notebook A: sees it             Notebook A: sees it
# MAGIC Notebook B: does NOT see it     Notebook B: ALSO sees it!
# MAGIC
# MAGIC Access: SELECT * FROM my_view   Access: SELECT * FROM global_temp.my_view
# MAGIC Dies: when session ends         Dies: when cluster restarts
# MAGIC ```
# MAGIC
# MAGIC ### Mixing Python + SQL
# MAGIC
# MAGIC ```
# MAGIC # Python → SQL:
# MAGIC df.createOrReplaceTempView("employees")
# MAGIC result = spark.sql("SELECT dept, AVG(salary) FROM employees GROUP BY dept")
# MAGIC
# MAGIC # SQL → Python:
# MAGIC %sql CREATE TEMP VIEW high_earners AS SELECT * FROM employees WHERE salary > 100000
# MAGIC df = spark.table("high_earners")  # Back to Python!
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Creating temp views
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: Creating temp views
# ═══════════════════════════════════════════════════════

print("=== Creating Temp Views ===")
print()

# Create a DataFrame
employees = spark.createDataFrame([
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", "Marketing", 72000),
    (3, "Charlie", "Engineering", 110000),
    (4, "Diana", "HR", 65000),
], ["id", "name", "dept", "salary"])

# --- createOrReplaceTempView (most common) ---
print("--- 1. createOrReplaceTempView ---")
employees.createOrReplaceTempView("emp")  # Register as SQL view
print("  Created view: 'emp'")

# Now query it with SQL!
result = spark.sql("SELECT dept, COUNT(*) as count, AVG(salary) as avg_sal FROM emp GROUP BY dept")
result.show()
print("  DataFrame → temp view → SQL query!")

# --- createTempView (fails if already exists) ---
print("\n--- 2. createTempView (errors if exists) ---")
try:
    employees.createTempView("emp")  # Already exists!
    print("  Created successfully")
except Exception as e:
    print(f"  ERROR: {str(e)[:60]}...")
    print("  Use createOrReplaceTempView to overwrite safely!")

# --- Reading a view back as DataFrame ---
print("\n--- 3. spark.table() — view back to DataFrame ---")
df_from_view = spark.table("emp")  # View → DataFrame
df_from_view.show()
print("  spark.table('view_name') converts view back to DataFrame")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Global temp views
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: Global temp views
# ═══════════════════════════════════════════════════════

print("=== Global Temp Views ===")
print()
print("Global temp views are shared across ALL notebooks on the same cluster.")
print("Access them via: global_temp.view_name")
print()

# Create a DataFrame
shared_data = spark.createDataFrame([
    ("config_key_1", "value_1"),
    ("config_key_2", "value_2"),
], ["key", "value"])

# --- Create global temp view ---
print("--- 1. Create global temp view ---")
try:
    shared_data.createGlobalTempView("shared_config")
    print("  Created global_temp.shared_config")
except Exception as e:
    print(f"  Already exists (from previous run): {str(e)[:50]}")
    shared_data.createOrReplaceGlobalTempView("shared_config")  # Spark 3.4+
    print("  Replaced existing global temp view")

# --- Access: MUST use global_temp prefix ---
print("\n--- 2. Querying global temp view ---")
result = spark.sql("SELECT * FROM global_temp.shared_config")  # Must prefix!
result.show()

# --- Accessing from spark.table ---
print("--- 3. spark.table with global_temp prefix ---")
df = spark.table("global_temp.shared_config")  # Also needs prefix
print(f"  Rows: {df.count()}")

# --- When to use global temp views ---
print("\n--- When to use global temp views ---")
print("  1. Sharing lookup data between notebooks on same cluster")
print("  2. Pre-computed results that multiple jobs need")
print("  3. NOT for production data sharing (use Delta tables instead!)")
print("\n--- Cleanup ---")
spark.catalog.dropGlobalTempView("shared_config")  # Clean up
print("  Dropped global_temp.shared_config")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Mixing Python and SQL
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: Mixing Python and SQL
# ═══════════════════════════════════════════════════════

print("=== Mixing Python and SQL in One Pipeline ===")
print()

# Step 1: Python — Create and transform data
raw = spark.createDataFrame([
    (1, "Alice", "Eng", 95000), (2, "Bob", "Mkt", 72000),
    (3, "Charlie", "Eng", 110000), (4, "Diana", "HR", 65000),
    (5, "Eve", "Eng", 88000),
], ["id", "name", "dept", "salary"])

# Step 2: Register as view for SQL access
raw.createOrReplaceTempView("employees")  # Python → SQL bridge

# Step 3: SQL — Complex query (SQL is often easier for analytics!)
result = spark.sql("""
    SELECT 
        dept,
        COUNT(*) as headcount,
        ROUND(AVG(salary), 0) as avg_salary,
        MAX(salary) as max_salary,
        MIN(salary) as min_salary
    FROM employees
    WHERE salary > 60000
    GROUP BY dept
    HAVING COUNT(*) >= 1
    ORDER BY avg_salary DESC
""")

print("--- SQL aggregation result ---")
result.show()

# Step 4: Back to Python for further processing
result.createOrReplaceTempView("dept_summary")  # SQL result → view

# Step 5: More Python operations
final = result.filter(result.headcount >= 2)  # Python filter
print("--- Departments with 2+ people ---")
final.show()

print("--- Pattern: Python (load) → SQL (transform) → Python (save) ---")
print("  Use SQL when the logic is naturally set-based/aggregation")
print("  Use Python when you need loops, UDFs, or ML")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: spark.catalog methods
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: spark.catalog methods
# ═══════════════════════════════════════════════════════

print("=== spark.catalog — Metadata API ===")
print()

# Ensure we have some views to inspect
df = spark.createDataFrame([(1,"a"),(2,"b")], ["id","val"])
df.createOrReplaceTempView("demo_view")
df.createOrReplaceTempView("another_view")

# --- listTables: See all tables and views ---
print("--- 1. spark.catalog.listTables() ---")
tables = spark.catalog.listTables()
for t in tables:
    print(f"  {t.name:<20} type={t.tableType:<12} isTemp={t.isTemporary}")

# --- tableExists: Check if a table/view exists ---
print("\n--- 2. spark.catalog.tableExists() ---")
print(f"  'demo_view' exists: {spark.catalog.tableExists('demo_view')}")  # True
print(f"  'fake_table' exists: {spark.catalog.tableExists('fake_table')}")  # False

# --- listColumns: See columns of a table/view ---
print("\n--- 3. spark.catalog.listColumns('demo_view') ---")
cols = spark.catalog.listColumns("demo_view")
for c in cols:
    print(f"  {c.name:<10} type={c.dataType:<10} nullable={c.nullable}")

# --- listDatabases ---
print("\n--- 4. spark.catalog.listDatabases() ---")
for db in spark.catalog.listDatabases():
    print(f"  {db.name}")

# --- currentDatabase ---
print(f"\n--- 5. Current database: {spark.catalog.currentDatabase()} ---")

print("\n--- Key catalog methods ---")
print("  listTables(), listDatabases(), listColumns(table)")
print("  tableExists(name), currentDatabase()")
print("  cacheTable(name), uncacheTable(name), clearCache()")
print("  refreshTable(name) — refresh metadata after external changes")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Caching tables
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Caching tables
# ═══════════════════════════════════════════════════════

import time

print("=== Table Caching via Catalog ===")
print()

# Create a sizeable DataFrame and register as view
df = spark.range(100000).selectExpr("id", "id * 2 as doubled", "id % 100 as category")
df.createOrReplaceTempView("big_data")

# --- Cache a table/view ---
print("--- 1. cacheTable() — cache for repeated queries ---")
spark.catalog.cacheTable("big_data")  # Cache in memory
print("  Cached 'big_data'")

# --- Check if cached ---
print(f"  Is cached: {spark.catalog.isCached('big_data')}")  # True

# --- Query cached table (faster on repeated access) ---
print("\n--- 2. Querying cached table ---")
start = time.time()
spark.sql("SELECT category, AVG(doubled) FROM big_data GROUP BY category").count()
t1 = time.time() - start

start = time.time()
spark.sql("SELECT category, AVG(doubled) FROM big_data GROUP BY category").count()
t2 = time.time() - start
print(f"  First query: {t1:.3f}s (builds cache)")
print(f"  Second query: {t2:.3f}s (reads from cache)")

# --- uncacheTable ---
print("\n--- 3. uncacheTable() — release memory ---")
spark.catalog.uncacheTable("big_data")
print(f"  Is cached after uncache: {spark.catalog.isCached('big_data')}")  # False

# --- clearCache: remove ALL cached tables ---
print("\n--- 4. clearCache() — release all caches ---")
spark.catalog.clearCache()
print("  All table caches cleared!")

print("\n--- When to cache ---")
print("  Good: Table queried multiple times in same session")
print("  Bad: Table queried once, or too large for memory")
print("  Rule: Cache < 30% of total cluster memory")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Dynamic SQL with f-strings
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Dynamic SQL with Python variables
# ═══════════════════════════════════════════════════════

print("=== Dynamic SQL with Python Variables ===")
print()

# Setup
df = spark.createDataFrame([
    (1, "Alice", "Eng", 95000), (2, "Bob", "Mkt", 72000),
    (3, "Charlie", "Eng", 110000), (4, "Diana", "HR", 65000),
], ["id", "name", "dept", "salary"])
df.createOrReplaceTempView("staff")

# --- Build SQL dynamically ---
print("--- 1. f-string SQL (simple interpolation) ---")
target_dept = "Eng"          # Python variable
min_salary = 90000            # Python variable

query = f"""
    SELECT * FROM staff 
    WHERE dept = '{target_dept}' AND salary > {min_salary}
"""
print(f"  Query: {query.strip()}")
spark.sql(query).show()

# --- Parameterized queries (safer!) ---
print("--- 2. Parameterized SQL (safer against injection) ---")
result = spark.sql(
    "SELECT * FROM staff WHERE dept = :dept AND salary > :min_sal",
    {"dept": "Eng", "min_sal": 90000}  # Named parameters
)
result.show()
print("  Named parameters prevent SQL injection!")

# --- Generate SQL for multiple tables ---
print("--- 3. Looping SQL across tables ---")
tables_to_check = ["staff"]  # In production: list of many tables
for table in tables_to_check:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0][0]
    print(f"  {table}: {count} rows")

print("\n--- Best practice: Use parameterized queries (:param) for user input ---")
print("  f-strings OK for table/column names from trusted sources")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: View-based pipeline architecture
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: View-based pipeline
# ═══════════════════════════════════════════════════════

print("=== View-Based ETL Pipeline ===")
print()
print("Pattern: Each transformation = a view. Final step = write to Delta.")
print()

# Raw data
raw = spark.createDataFrame([
    (1, " Alice ", "eng", "95000", "2024-01-15"),
    (2, " Bob ", "mkt", "72000", "2024-02-20"),
    (3, " Charlie ", "eng", "110000", None),  # Null date!
    (4, " ", "hr", "65000", "2024-03-10"),    # Empty name!
], ["id", "name", "dept", "salary", "hire_date"])
raw.createOrReplaceTempView("raw_employees")

# --- Layer 1: Clean (Bronze → Silver) ---
spark.sql("""
    CREATE OR REPLACE TEMP VIEW clean_employees AS
    SELECT 
        id,
        TRIM(name) as name,
        UPPER(dept) as dept,
        CAST(salary AS INT) as salary,
        TO_DATE(hire_date) as hire_date
    FROM raw_employees
    WHERE TRIM(name) != ''  -- Remove empty names
""")
print("--- Layer 1: clean_employees (trimmed, typed, filtered) ---")
spark.table("clean_employees").show()

# --- Layer 2: Enrich (Silver → Gold) ---
spark.sql("""
    CREATE OR REPLACE TEMP VIEW enriched_employees AS
    SELECT 
        *,
        CASE 
            WHEN salary >= 100000 THEN 'Senior'
            WHEN salary >= 75000 THEN 'Mid'
            ELSE 'Junior'
        END as tier,
        salary / 12 as monthly_salary
    FROM clean_employees
""")
print("--- Layer 2: enriched_employees (with tier and monthly) ---")
spark.table("enriched_employees").show()

# --- Final: Convert to Python for writing ---
final_df = spark.table("enriched_employees")
print(f"  Final pipeline output: {final_df.count()} rows, {len(final_df.columns)} columns")
print("  In production: final_df.write.format('delta').save(...)")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Catalog inspection utility
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Catalog inspection utility
# ═══════════════════════════════════════════════════════

print("=== Catalog Inspection Utility ===")
print()

def inspect_catalog(database=None):
    """Print all tables/views in the current or specified database."""
    db = database or spark.catalog.currentDatabase()
    print(f"  Database: {db}")
    print(f"  {'Name':<25} {'Type':<15} {'IsTemp':<10}")
    print(f"  {'-'*50}")
    
    for t in spark.catalog.listTables(db):
        print(f"  {t.name:<25} {t.tableType:<15} {t.isTemporary}")

def inspect_table(table_name):
    """Print columns and their types for a table/view."""
    if not spark.catalog.tableExists(table_name):
        print(f"  Table '{table_name}' does not exist!")
        return
    
    print(f"  Table: {table_name}")
    print(f"  {'Column':<20} {'Type':<15} {'Nullable'}")
    print(f"  {'-'*45}")
    
    for c in spark.catalog.listColumns(table_name):
        print(f"  {c.name:<20} {c.dataType:<15} {c.nullable}")
    
    # Row count
    count = spark.table(table_name).count()
    print(f"  \n  Total rows: {count:,}")

# --- Demo ---
print("--- All tables in current database ---")
inspect_catalog()

print("\n--- Inspect 'clean_employees' ---")
inspect_table("clean_employees")

print("\n--- Inspect non-existent table ---")
inspect_table("fake_table")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: refreshTable and production patterns
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: refreshTable and production patterns
# ═══════════════════════════════════════════════════════

print("=== refreshTable and Production Patterns ===")
print()

# --- refreshTable: Invalidate cached metadata ---
print("--- 1. refreshTable() — When to use ---")
print("  Use when: External process modified a table (e.g., ADF wrote new files)")
print("  Spark's metadata cache may not know about new partitions/files")
print("  spark.catalog.refreshTable('schema.table') forces Spark to rescan")
print()

# --- Production: Safe view creation pattern ---
print("--- 2. Production: Idempotent view creation ---")

def register_views(dataframes_dict):
    """
    Register multiple DataFrames as temp views.
    Idempotent: safe to re-run without errors.
    """
    for name, df in dataframes_dict.items():
        df.createOrReplaceTempView(name)  # Always use OrReplace!
        print(f"  Registered view: {name} ({df.count()} rows, {len(df.columns)} cols)")

# Demo
df1 = spark.createDataFrame([(1,"a")], ["id","val"])
df2 = spark.createDataFrame([(1,"x"),(2,"y")], ["id","data"])

register_views({"raw_input": df1, "lookup_table": df2})

# --- Production: Check view exists before querying ---
print("\n--- 3. Safe query pattern ---")
def safe_query(view_name, sql):
    """Query a view only if it exists."""
    if spark.catalog.tableExists(view_name):
        return spark.sql(sql)
    else:
        print(f"  WARNING: View '{view_name}' does not exist!")
        return None

result = safe_query("raw_input", "SELECT * FROM raw_input")
if result:
    result.show()

# --- Cleanup all temp views ---
print("\n--- 4. Cleanup: Drop all temp views ---")
for t in spark.catalog.listTables():
    if t.isTemporary:  # Only drop temp views, not real tables!
        spark.catalog.dropTempView(t.name)
        print(f"  Dropped: {t.name}")
print("  All temp views cleaned up!")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Using createTempView instead of createOrReplaceTempView
# MAGIC **Problem:** `createTempView` fails if the view already exists (from a previous cell run).  
# MAGIC **Fix:** Always use `createOrReplaceTempView` unless you specifically WANT the error.
# MAGIC
# MAGIC ### Mistake #2: Forgetting global_temp prefix
# MAGIC **Problem:** `spark.sql("SELECT * FROM my_global_view")` fails with "table not found".  
# MAGIC **Fix:** Global views require prefix: `SELECT * FROM global_temp.my_global_view`.
# MAGIC
# MAGIC ### Mistake #3: Expecting temp views to persist across sessions
# MAGIC **Problem:** Restarting the cluster or detaching the notebook loses all temp views.  
# MAGIC **Fix:** For persistent data, write to Delta tables. Recreate views at notebook start.
# MAGIC
# MAGIC ### Mistake #4: SQL injection with f-strings
# MAGIC **Problem:** `spark.sql(f"SELECT * FROM t WHERE name = '{user_input}'")` is unsafe.  
# MAGIC **Fix:** Use parameterized queries: `spark.sql("...WHERE name = :name", {"name": val})`.
# MAGIC
# MAGIC ### Mistake #5: Caching too much data
# MAGIC **Problem:** Caching a 100GB table fills memory and causes OOM for other operations.  
# MAGIC **Fix:** Only cache tables queried multiple times AND that fit in memory. Always `uncacheTable` when done.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC **Level 1 (Copy & Run):** Create a DataFrame, register as temp view, query with spark.sql().
# MAGIC
# MAGIC **Level 2 (Tiny Change):** Change the SQL query to add a WHERE clause and ORDER BY.
# MAGIC
# MAGIC **Level 3 (Combine Two):** Create two views, then JOIN them using SQL.
# MAGIC
# MAGIC **Level 4 (New Scenario):** Build a 3-layer view pipeline: raw → clean → enriched.
# MAGIC
# MAGIC **Level 5 (Mini Project):** Use spark.catalog to list all views, their columns, and row counts.
# MAGIC
# MAGIC **Level 6 (Design First):** Design a pattern for mixing Python transformations with SQL analytics.
# MAGIC
# MAGIC **Level 7 (Optimize):** Cache a frequently-queried view, benchmark before/after.
# MAGIC
# MAGIC **Level 8 (Edge Cases):** What happens when you query a dropped view? When you cache an empty view?
# MAGIC
# MAGIC **Level 9 (Production):** Build a utility that: registers views, validates they exist, caches them, and cleans up.
# MAGIC
# MAGIC **Level 10 (Teach It):** Explain when to use temp views vs permanent tables vs Delta tables.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

import time

# Level 3: Two views + SQL JOIN
print("=== Level 3: Two Views + JOIN ===")
spark.createDataFrame([(1,"A",10),(2,"B",20)], ["id","name","dept_id"]).createOrReplaceTempView("hw_emp")
spark.createDataFrame([(10,"Engineering"),(20,"Marketing")], ["dept_id","dept_name"]).createOrReplaceTempView("hw_dept")

spark.sql("""
    SELECT e.name, d.dept_name 
    FROM hw_emp e JOIN hw_dept d ON e.dept_id = d.dept_id
""").show()

# Level 5: Catalog inspection
print("\n=== Level 5: Catalog Inspection ===")
for t in spark.catalog.listTables():
    if t.isTemporary:
        count = spark.table(t.name).count()
        cols = len(spark.catalog.listColumns(t.name))
        print(f"  {t.name:<20} rows={count:<6} cols={cols}")

# Level 7: Cache benchmark
print("\n=== Level 7: Cache Benchmark ===")
df_big = spark.range(500000).selectExpr("id", "id % 50 as grp", "rand() as val")
df_big.createOrReplaceTempView("bench")

# Without cache
start = time.time()
spark.sql("SELECT grp, AVG(val) FROM bench GROUP BY grp").count()
t_no_cache = time.time() - start

# With cache
spark.catalog.cacheTable("bench")
spark.sql("SELECT COUNT(*) FROM bench").collect()  # Materialize cache
start = time.time()
spark.sql("SELECT grp, AVG(val) FROM bench GROUP BY grp").count()
t_cached = time.time() - start

print(f"  Without cache: {t_no_cache:.3f}s")
print(f"  With cache:    {t_cached:.3f}s")
spark.catalog.uncacheTable("bench")

# Cleanup
for t in spark.catalog.listTables():
    if t.isTemporary:
        spark.catalog.dropTempView(t.name)

print("\n\u2705 All homework solutions complete!")