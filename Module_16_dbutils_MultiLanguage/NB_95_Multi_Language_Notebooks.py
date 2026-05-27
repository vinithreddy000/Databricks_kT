# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 95: Multi-Language Notebooks
# MAGIC ## Module 16: dbutils & Multi-Language
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Databricks notebooks have a **default language** (Python, SQL, Scala, or R), but any cell can use a DIFFERENT language by adding a **magic command** (`%sql`, `%python`, `%scala`, `%r`, `%md`, `%sh`) at the first line. This lets you use the best language for each task within a single notebook.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine a **multilingual meeting**: The default language is English (Python), but you can switch to French (SQL) for the data discussion, Japanese (Scala) for performance-critical code, and German (R) for statistical analysis — all in the same meeting (notebook).
# MAGIC
# MAGIC ### Magic Commands:
# MAGIC | Command | Language | Best For |
# MAGIC |---------|----------|----------|
# MAGIC | `%python` | Python | General logic, ML, UDFs, libraries |
# MAGIC | `%sql` | SQL | Queries, DDL, quick exploration |
# MAGIC | `%scala` | Scala | Performance, Java interop, type safety |
# MAGIC | `%r` | R | Statistics, ggplot, specialized packages |
# MAGIC | `%md` | Markdown | Documentation, formatted text |
# MAGIC | `%sh` | Shell/Bash | System commands, file ops, pip |
# MAGIC | `%run` | (special) | Execute another notebook inline |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Multi-Language Architecture:
# MAGIC
# MAGIC   Single Notebook (default: Python)
# MAGIC   ──────────────────────────────────
# MAGIC   Cell 1: [Python]  ← default language (no magic needed)
# MAGIC   Cell 2: [%sql]    ← SQL cell (magic at top)
# MAGIC   Cell 3: [%scala]  ← Scala cell
# MAGIC   Cell 4: [Python]  ← back to default
# MAGIC   Cell 5: [%r]      ← R cell
# MAGIC   Cell 6: [%md]     ← Markdown (documentation)
# MAGIC   Cell 7: [%sh]     ← Shell commands
# MAGIC
# MAGIC Sharing Data Between Languages:
# MAGIC
# MAGIC   The SPARK SESSION is shared across all languages!
# MAGIC   Python, SQL, Scala, and R all access the SAME SparkSession.
# MAGIC
# MAGIC   Method 1: Temp Views (recommended).
# MAGIC     Python:  df.createOrReplaceTempView("my_data")
# MAGIC     SQL:     SELECT * FROM my_data  -- Same data!
# MAGIC     Scala:   spark.table("my_data") -- Same data!
# MAGIC     R:       df <- sql("SELECT * FROM my_data")
# MAGIC
# MAGIC   Method 2: spark.table() for catalog tables.
# MAGIC     Write once to Delta table, read from any language.
# MAGIC
# MAGIC   What's NOT shared:
# MAGIC     - Python variables (lists, dicts, strings)
# MAGIC     - R objects
# MAGIC     - Scala vals/vars
# MAGIC     Each language has its OWN memory space for local variables.
# MAGIC     Only SparkSession (tables, temp views) is shared.
# MAGIC
# MAGIC %run (inline execution):
# MAGIC   %run /path/to/other_notebook
# MAGIC   - Executes ALL cells of the other notebook IN THIS context.
# MAGIC   - Variables defined there become available here.
# MAGIC   - Like a Python import, but for notebooks.
# MAGIC   - Must be alone in the cell (no other code).
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: Beginner Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTION 3 — BEGINNER: Multi-Language Basics")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Creating data in Python, querying in SQL
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Python → SQL communication via temp views")
print("-"*60)

# Step 1: Create data in Python.
employees = spark.createDataFrame([
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", "Marketing", 72000),
    (3, "Charlie", "Engineering", 88000),
    (4, "Diana", "Sales", 67000),
    (5, "Eve", "Marketing", 78000),
    (6, "Frank", "Engineering", 102000)
], ["id", "name", "dept", "salary"])

# Step 2: Register as temp view (makes it accessible from SQL/Scala/R).
employees.createOrReplaceTempView("employees")  # Shared across languages!
print("\n✓ Created temp view 'employees' — accessible from %sql cells.")
print("  In a %sql cell, you can now: SELECT * FROM employees")
print("  This is HOW you pass data between Python and SQL.")

# Step 3: Query from Python using spark.sql() (simulates %sql).
result = spark.sql("SELECT dept, AVG(salary) as avg_salary FROM employees GROUP BY dept")
print("\nSQL query result (from Python):")
display(result)  # display() for output.

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Magic commands reference
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Magic commands reference")
print("-"*60)

print("""
Magic commands (must be FIRST line of cell):

  %python
  # Python code here (default in Python notebooks).
  df = spark.table("my_table")

  %sql
  -- SQL code here.
  SELECT * FROM my_table WHERE date > '2024-01-01'

  %scala
  // Scala code here.
  val df = spark.table("my_table")
  df.show()

  %r
  # R code here.
  library(SparkR)
  df <- sql("SELECT * FROM my_table")
  head(df)

  %md
  ## Markdown cell
  This renders as **formatted text**.

  %sh
  # Shell commands (run on driver node only).
  ls -la /tmp
  whoami
  curl https://api.example.com/status

  %run /path/to/notebook
  # Executes another notebook in THIS context.
  # All its variables become available here.

Rules:
  1. Magic MUST be the first line (no spaces/comments before it).
  2. Only ONE magic per cell (can't mix languages in one cell).
  3. %sh runs on DRIVER only (not workers).
  4. %run must be ALONE in the cell.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Using %sh for system info
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Shell commands from Python (os module)")
print("-"*60)

import os  # OS module for system operations.
import subprocess  # For running shell commands.

# Get system info (equivalent to %sh).
print(f"\n  Current user: {os.environ.get('USER', 'unknown')}")
print(f"  Working dir: {os.getcwd()}")
print(f"  Python path: {os.sys.executable}")

# Run a shell command from Python.
result = subprocess.run(["hostname"], capture_output=True, text=True)  # Get hostname.
print(f"  Hostname: {result.stdout.strip()}")

print("\n  In a %sh cell, you could simply write:")
print("    whoami")
print("    hostname")
print("    ls /tmp")
print("\n✓ %sh runs on the DRIVER node only (not distributed).")
print("  Use for: system info, curl API calls, file inspection.")

# COMMAND ----------

# DBTITLE 1,Sections 4-5: Intermediate and Advanced
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4-5 — INTERMEDIATE & ADVANCED
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTIONS 4-5: Intermediate & Advanced Multi-Language")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Passing data between Python and SQL
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Bidirectional data flow (Python ↔ SQL)")
print("-"*60)

from pyspark.sql.functions import col, avg, count  # Spark functions.

# Python → SQL: Create temp view.
sales = spark.createDataFrame([
    ("2024-01", "Electronics", 5000.0),
    ("2024-01", "Clothing", 3000.0),
    ("2024-02", "Electronics", 7000.0),
    ("2024-02", "Clothing", 4500.0),
    ("2024-03", "Electronics", 6000.0),
    ("2024-03", "Clothing", 3500.0)
], ["month", "category", "revenue"])
sales.createOrReplaceTempView("monthly_sales")  # Python → shared view.
print("\nPython created view: 'monthly_sales'")

# SQL → Python: Use spark.sql() to get SQL results as DataFrame.
sql_result = spark.sql("""
    SELECT 
        category,
        SUM(revenue) as total_revenue,
        AVG(revenue) as avg_monthly_revenue
    FROM monthly_sales
    GROUP BY category
    ORDER BY total_revenue DESC
""")
print("\nSQL query result (captured in Python variable):")
display(sql_result)  # display() for output.

# The special _sqldf variable (auto-created by %sql cells).
print("\n✓ When you run a %sql cell, the result is in '_sqldf' variable.")
print("  Access in next Python cell: df = _sqldf")
print("  Or use temp views for explicit sharing.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: %run for shared utilities
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: %run for shared code and configuration")
print("-"*60)

print("""
%run Pattern (shared utilities notebook):

  # Notebook: /Shared/utils/config
  # Contains:
  ENV = "production"
  DATABASE = f"{ENV}_db"
  def get_table(name): return f"{DATABASE}.{name}"

  # Your notebook:
  %run /Shared/utils/config    ← Cell with ONLY this line.

  # After %run, you can use:
  print(ENV)                  # "production"
  print(get_table("orders"))  # "production_db.orders"

%run Rules:
  1. Must be the ONLY content in the cell.
  2. Executes ALL cells of the target notebook.
  3. ALL variables/functions from target become available.
  4. Runs in the SAME Spark session.
  5. Like Python's 'from module import *' but for notebooks.

Common %run patterns:
  %run ./utils          ← Relative path (same folder).
  %run ../shared/config ← Relative path (parent folder).
  %run /Users/team@co.com/shared/helpers ← Absolute path.

When to use %run vs dbutils.notebook.run:
  %run: Share code/variables (like import). Same execution context.
  notebook.run: Orchestrate tasks. Separate context. Returns a string.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Best language for each task
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Choosing the right language for each task")
print("-"*60)

print("""
Use Python (%python) for:
  ✓ General logic, loops, conditionals.
  ✓ ML (scikit-learn, MLlib, PyTorch, TensorFlow).
  ✓ UDFs (especially pandas UDFs).
  ✓ Library ecosystem (requests, pandas, matplotlib).
  ✓ Complex string processing, regex.

Use SQL (%sql) for:
  ✓ Quick data exploration (SELECT * FROM table LIMIT 10).
  ✓ DDL statements (CREATE TABLE, ALTER TABLE).
  ✓ Complex joins and aggregations.
  ✓ Built-in visualizations (auto-rendered charts).
  ✓ Window functions.

Use Scala (%scala) for:
  ✓ Performance-critical transformations.
  ✓ Java library interop.
  ✓ Type-safe code.
  ✓ Custom Spark plugins/extensions.

Use R (%r) for:
  ✓ Statistical modeling (lm, glm, mixed models).
  ✓ ggplot2 visualizations.
  ✓ R-specific packages (tidyverse, caret).
  ✓ Time series (forecast, prophet).

Use Shell (%sh) for:
  ✓ System commands (ls, cat, grep, curl).
  ✓ Check disk space, memory, processes.
  ✓ Download files (wget, curl).
  ✓ Git operations.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 7: Complete multi-language workflow example
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 7: Multi-language workflow (Python → SQL → Python)")
print("-"*60)

# Step 1: Python — Create and transform data.
from pyspark.sql.functions import col, when, lit  # Imports.

orders = spark.createDataFrame([
    (1, "Alice", 150.0, "2024-01-15"),
    (2, "Bob", 25.0, "2024-01-16"),
    (3, "Charlie", 300.0, "2024-01-17"),
    (4, "Diana", 75.0, "2024-01-18"),
    (5, "Eve", 500.0, "2024-01-19")
], ["order_id", "customer", "amount", "date"])

# Add category using Python logic.
orders_enriched = orders.withColumn(
    "tier",
    when(col("amount") >= 200, "premium")  # Python conditional logic.
    .when(col("amount") >= 50, "standard")
    .otherwise("basic")
)
orders_enriched.createOrReplaceTempView("orders")  # Share with SQL.
print("Step 1 (Python): Created and enriched orders data.")

# Step 2: SQL analysis (via spark.sql since we're in a Python cell).
analysis = spark.sql("""
    SELECT 
        tier,
        COUNT(*) as order_count,
        ROUND(AVG(amount), 2) as avg_amount,
        ROUND(SUM(amount), 2) as total_revenue
    FROM orders
    GROUP BY tier
    ORDER BY total_revenue DESC
""")
print("\nStep 2 (SQL): Aggregated analysis:")
display(analysis)  # display() for output.

# Step 3: Python — Further processing on SQL results.
analysis_pd = analysis.toPandas()  # Collect small result to pandas.
total_rev = analysis_pd['total_revenue'].sum()  # Python calculation.
print(f"\nStep 3 (Python): Total revenue across all tiers: ${total_rev:,.2f}")
print("\n✓ Workflow: Python (transform) → SQL (analyze) → Python (post-process).")

# COMMAND ----------

# DBTITLE 1,Section 6: Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Trying to access Python variables from SQL
# MAGIC ```python
# MAGIC # BAD: SQL can't see Python variables!
# MAGIC # Python cell:
# MAGIC my_filter = "Electronics"
# MAGIC
# MAGIC # SQL cell:
# MAGIC # %sql
# MAGIC # SELECT * FROM sales WHERE category = my_filter  -- ERROR!
# MAGIC
# MAGIC # GOOD Option 1: Use temp view + literal value.
# MAGIC # %sql
# MAGIC # SELECT * FROM sales WHERE category = 'Electronics'
# MAGIC
# MAGIC # GOOD Option 2: Use Python f-string with spark.sql().
# MAGIC df = spark.sql(f"SELECT * FROM sales WHERE category = '{my_filter}'")
# MAGIC
# MAGIC # GOOD Option 3: Use a widget.
# MAGIC dbutils.widgets.text("filter_cat", "Electronics")
# MAGIC # %sql SELECT * FROM sales WHERE category = getArgument('filter_cat')
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Forgetting that %sh runs on driver only
# MAGIC ```python
# MAGIC # BAD: Thinking %sh runs on all workers.
# MAGIC # %sh
# MAGIC # pip install my_package  # Only installs on driver!
# MAGIC
# MAGIC # GOOD: Use %pip for distributed installation.
# MAGIC # %pip install my_package  # Installs on ALL nodes.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Magic command not on the first line
# MAGIC ```python
# MAGIC # BAD: Comment before magic.
# MAGIC # This is a SQL query
# MAGIC # %sql  -- This is NOT the first line! Won't work.
# MAGIC # SELECT * FROM table
# MAGIC
# MAGIC # GOOD: Magic must be THE FIRST LINE.
# MAGIC # %sql
# MAGIC # SELECT * FROM table
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Mixing languages in one cell
# MAGIC ```python
# MAGIC # BAD: Two languages in one cell.
# MAGIC # %sql
# MAGIC # SELECT * FROM table
# MAGIC # %python  -- ERROR! Can't switch mid-cell.
# MAGIC # print("hello")
# MAGIC
# MAGIC # GOOD: One language per cell.
# MAGIC # Cell 1: %sql
# MAGIC # SELECT * FROM table
# MAGIC # Cell 2: %python
# MAGIC # print("hello")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Using %run with other code in the cell
# MAGIC ```python
# MAGIC # BAD: %run with additional code.
# MAGIC # %run /path/to/notebook
# MAGIC # print("done")  # ERROR! %run must be alone.
# MAGIC
# MAGIC # GOOD: %run alone in its own cell.
# MAGIC # Cell 1:
# MAGIC # %run /path/to/notebook
# MAGIC # Cell 2:
# MAGIC # print("done")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7: Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("HOMEWORK — Multi-Language Notebooks")
print("="*70)

# Level 1: Create a temp view in Python.
print("\n--- Level 1: Create temp view ---")
df1 = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "val"])
df1.createOrReplaceTempView("hw_table")  # Makes data accessible to %sql.
print("Created view 'hw_table'. In %sql cell: SELECT * FROM hw_table")
# WHY: Temp views are THE bridge between Python and SQL.

# Level 2: Query temp view from spark.sql.
print("\n--- Level 2: Query via spark.sql ---")
result = spark.sql("SELECT COUNT(*) as cnt FROM hw_table")  # SQL in Python.
display(result)  # display() for output.
# WHY: spark.sql() lets you run SQL and capture results in Python.

# Level 3: Access SQL results in Python.
print("\n--- Level 3: SQL results in Python ---")
row_count = spark.sql("SELECT COUNT(*) as cnt FROM hw_table").collect()[0]['cnt']
print(f"Row count from SQL: {row_count}")
# WHY: .collect()[0]['col'] extracts a scalar from SQL results.

# Level 4: Dynamic SQL with f-strings.
print("\n--- Level 4: Dynamic SQL ---")
table_name = "hw_table"  # Python variable.
filter_val = "a"  # Python variable.
dynamic_result = spark.sql(f"SELECT * FROM {table_name} WHERE val = '{filter_val}'")
display(dynamic_result)  # display() for output.
# WHY: f-strings let you inject Python variables into SQL strings.

# Level 5: Using _sqldf from %sql cells.
print("\n--- Level 5: _sqldf variable ---")
print("After a %sql cell executes, results are stored in '_sqldf'.")
print("  %sql SELECT * FROM hw_table  -- runs in SQL cell")
print("  # Next Python cell:")
print("  df = _sqldf  # Contains the SQL result as a DataFrame!")
# WHY: _sqldf auto-bridges %sql cell results to Python.

# Levels 6-10: Conceptual.
print("\n--- Level 6: When to use %sql vs spark.sql() ---")
print("  %sql: Quick exploration, auto-visualization, standalone queries.")
print("  spark.sql(): Need result in Python variable, dynamic queries.")

print("\n--- Level 7: %run vs import ---")
print("  %run: Notebooks (all variables shared). Must be alone in cell.")
print("  import: .py files (structured modules). Normal Python import.")

print("\n--- Level 8: Shared SparkSession ---")
print("  Python, SQL, Scala, R — ALL share the same SparkSession.")
print("  Temp views and catalog tables are accessible from all languages.")
print("  Local variables (Python lists, R vectors) are NOT shared.")

print("\n--- Level 9: Security of %sh ---")
print("  %sh runs with the Spark user's permissions on the driver.")
print("  Can see env vars, files on driver node.")
print("  NOT sandboxed — be careful in shared clusters!")

print("\n--- Level 10: Teach multi-language ---")
print("""
"Multi-language notebooks: use %python, %sql, %scala, %r, %sh, %md.
  Magic command must be FIRST line of cell.
  Data sharing: use temp views (createOrReplaceTempView).
  SparkSession is shared; local variables are NOT shared.
  %run: execute another notebook inline (shares variables).
  %sh: shell commands on driver only (not distributed).
  Best practice: Python for logic, SQL for queries, %md for docs."
""")

# Cleanup.
spark.catalog.dropTempView("hw_table")  # Remove temp view.
spark.catalog.dropTempView("monthly_sales")  # Cleanup from earlier.
spark.catalog.dropTempView("orders")  # Cleanup from earlier.
spark.catalog.dropTempView("employees")  # Cleanup from earlier.

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 95")
print("✓ MODULE 16 COMPLETE! Both notebooks (94-95) done.")
print("="*70)