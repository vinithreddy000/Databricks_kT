# Databricks notebook source
# DBTITLE 1,Sections 1-2 Overview
# MAGIC %md
# MAGIC # Notebook 62: Advanced SQL — CTEs, Subqueries, QUALIFY, PIVOT
# MAGIC ## Module 10: Spark SQL Complete Reference
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Advanced SQL features let you write **complex queries elegantly**. Instead of nesting query inside query inside query (unreadable!), you use CTEs, QUALIFY, PIVOT, and LATERAL VIEW to express complex logic clearly.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC - **CTE** = Writing your essay outline before the essay (organize thoughts first, then write)
# MAGIC - **Subquery** = A calculation within a calculation ("find the average, then find people above it")
# MAGIC - **QUALIFY** = A filter specifically for window function results ("keep only rank=1")
# MAGIC - **PIVOT** = Rotating a table 90 degrees (rows become columns)
# MAGIC - **LATERAL VIEW EXPLODE** = Unpacking a box (one item per row)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC CTE Structure:
# MAGIC   WITH
# MAGIC     step1 AS (SELECT ...),    -- First prepare this
# MAGIC     step2 AS (SELECT ...),    -- Then this
# MAGIC     step3 AS (SELECT ...)     -- Then this
# MAGIC   SELECT * FROM step3;        -- Final result
# MAGIC
# MAGIC QUALIFY (Databricks extension):
# MAGIC   SELECT name, salary,
# MAGIC          RANK() OVER (ORDER BY salary DESC) as rnk
# MAGIC   FROM employees
# MAGIC   QUALIFY rnk = 1;            -- Filter window result directly!
# MAGIC
# MAGIC   Without QUALIFY you need a subquery:
# MAGIC   SELECT * FROM (
# MAGIC     SELECT name, salary, RANK() OVER (...) as rnk FROM employees
# MAGIC   ) WHERE rnk = 1;            -- Extra nesting required
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3 - CTEs and Subqueries
# SECTION 3 — BEGINNER EXAMPLES: CTEs and Subqueries

print("=== CTEs (Common Table Expressions) ===")  # Heading.

# Setup data.
spark.sql("USE sql_kt_demo")

# Simple CTE.
print("--- Simple CTE ---")
display(spark.sql("""
    WITH engineering AS (
        SELECT name, salary FROM employees WHERE department = 'Engineering'
    )
    SELECT name, salary, salary - (SELECT AVG(salary) FROM engineering) as diff_from_avg
    FROM engineering
    ORDER BY salary DESC
"""))

# Chained CTEs.
print("\n--- Chained CTEs ---")
display(spark.sql("""
    WITH dept_stats AS (
        SELECT department, 
               COUNT(*) as headcount,
               ROUND(AVG(salary), 0) as avg_salary,
               SUM(salary) as total_cost
        FROM employees
        GROUP BY department
    ),
    ranked AS (
        SELECT *, 
               RANK() OVER (ORDER BY avg_salary DESC) as salary_rank,
               ROUND(total_cost * 100.0 / SUM(total_cost) OVER (), 1) as pct_of_budget
        FROM dept_stats
    )
    SELECT department, headcount, avg_salary, total_cost, salary_rank, 
           CONCAT(pct_of_budget, '%') as budget_share
    FROM ranked
    ORDER BY salary_rank
"""))

# Correlated subquery.
print("\n--- Correlated Subquery ---")
print("Find employees earning more than their department average:")
display(spark.sql("""
    SELECT e.name, e.department, e.salary,
           (SELECT ROUND(AVG(salary),0) FROM employees e2 WHERE e2.department = e.department) as dept_avg
    FROM employees e
    WHERE e.salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.department = e.department)
    ORDER BY e.salary DESC
"""))

# EXISTS subquery.
print("\n--- EXISTS subquery ---")
print("Departments that have at least one person earning > 100K:")
display(spark.sql("""
    SELECT DISTINCT d.dept_name, d.location
    FROM departments d
    WHERE EXISTS (
        SELECT 1 FROM employees e 
        WHERE e.department = d.dept_name AND e.salary > 100000
    )
"""))

# COMMAND ----------

# DBTITLE 1,Section 4 - QUALIFY, PIVOT, UNPIVOT
# SECTION 4 — INTERMEDIATE: QUALIFY, PIVOT, UNPIVOT, LATERAL VIEW

print("=== QUALIFY — Filter Window Results ===")  # Heading.
spark.sql("USE sql_kt_demo")

# QUALIFY: Get top earner per department.
print("--- QUALIFY: Top earner per department ---")
display(spark.sql("""
    SELECT name, department, salary,
           RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
    FROM employees
    QUALIFY dept_rank = 1
"""))

# QUALIFY with multiple conditions.
print("\n--- QUALIFY: Top 2 per department ---")
display(spark.sql("""
    SELECT name, department, salary,
           ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rn
    FROM employees
    QUALIFY rn <= 2
    ORDER BY department, rn
"""))

# PIVOT: Rows to Columns.
print("\n=== PIVOT — Rows to Columns ===")
# First, let's create quarterly sales data.
spark.sql("DROP TABLE IF EXISTS quarterly_sales")
spark.sql("""
    CREATE TABLE quarterly_sales AS
    SELECT * FROM VALUES
        ('Alice', 'Q1', 50000), ('Alice', 'Q2', 55000), ('Alice', 'Q3', 60000), ('Alice', 'Q4', 65000),
        ('Bob', 'Q1', 40000), ('Bob', 'Q2', 42000), ('Bob', 'Q3', 45000), ('Bob', 'Q4', 48000),
        ('Carol', 'Q1', 60000), ('Carol', 'Q2', 62000), ('Carol', 'Q3', 58000), ('Carol', 'Q4', 70000)
    AS t(salesperson, quarter, revenue)
""")
print("Before PIVOT (long format):")
display(spark.sql("SELECT * FROM quarterly_sales ORDER BY salesperson, quarter"))

print("\nAfter PIVOT (wide format):")
display(spark.sql("""
    SELECT * FROM quarterly_sales
    PIVOT (
        SUM(revenue)
        FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4')
    )
    ORDER BY salesperson
"""))

# UNPIVOT: Columns to Rows.
print("\n=== UNPIVOT — Columns to Rows ===")
spark.sql("DROP TABLE IF EXISTS wide_data")
spark.sql("""
    CREATE TABLE wide_data AS
    SELECT 'Alice' as name, 90 as math, 85 as science, 92 as english
    UNION ALL SELECT 'Bob', 78, 88, 72
""")
print("Before UNPIVOT (wide):")
display(spark.sql("SELECT * FROM wide_data"))
print("\nAfter UNPIVOT (long):")
display(spark.sql("""
    SELECT * FROM wide_data
    UNPIVOT (
        score FOR subject IN (math, science, english)
    )
    ORDER BY name, subject
"""))

# LATERAL VIEW EXPLODE.
print("\n=== LATERAL VIEW EXPLODE ===")
spark.sql("DROP TABLE IF EXISTS array_demo")
spark.sql("""
    CREATE TABLE array_demo AS
    SELECT 1 as id, 'Alice' as name, array('Python','SQL','Spark') as skills
    UNION ALL SELECT 2, 'Bob', array('Java','Scala')
""")
print("Before explode:")
display(spark.sql("SELECT * FROM array_demo"))
print("\nAfter LATERAL VIEW EXPLODE:")
display(spark.sql("""
    SELECT id, name, skill
    FROM array_demo
    LATERAL VIEW EXPLODE(skills) t AS skill
    ORDER BY id, skill
"""))

# COMMAND ----------

# DBTITLE 1,Section 5 and Exercises
# SECTION 5 — ADVANCED: Recursive CTEs and Complex Patterns

print("=== Advanced SQL Patterns ===")  # Heading.
spark.sql("USE sql_kt_demo")

# Recursive CTE: Generate a date series.
print("--- Recursive CTE: Date Series ---")
display(spark.sql("""
    WITH RECURSIVE dates AS (
        SELECT DATE('2024-01-01') as dt
        UNION ALL
        SELECT DATE_ADD(dt, 1) FROM dates WHERE dt < DATE('2024-01-10')
    )
    SELECT dt, DAYOFWEEK(dt) as dow, DATE_FORMAT(dt, 'EEEE') as day_name
    FROM dates
"""))

# Recursive CTE: Org hierarchy.
print("\n--- Recursive CTE: Org Hierarchy ---")
spark.sql("DROP TABLE IF EXISTS org_chart")
spark.sql("""
    CREATE TABLE org_chart AS
    SELECT * FROM VALUES
        (1, 'CEO', NULL), (2, 'VP Eng', 1), (3, 'VP Sales', 1),
        (4, 'Dir Backend', 2), (5, 'Dir Frontend', 2),
        (6, 'Manager A', 4), (7, 'Manager B', 5),
        (8, 'Dev 1', 6), (9, 'Dev 2', 6)
    AS t(id, title, manager_id)
""")

display(spark.sql("""
    WITH RECURSIVE hierarchy AS (
        SELECT id, title, manager_id, 0 as level, title as path
        FROM org_chart WHERE manager_id IS NULL
        UNION ALL
        SELECT o.id, o.title, o.manager_id, h.level + 1,
               CONCAT(h.path, ' > ', o.title)
        FROM org_chart o
        JOIN hierarchy h ON o.manager_id = h.id
    )
    SELECT CONCAT(REPEAT('  ', level), title) as org_tree, level, path
    FROM hierarchy
    ORDER BY path
"""))

# Complex window + CTE pattern.
print("\n--- Complex Analytics Query ---")
display(spark.sql("""
    WITH monthly_stats AS (
        SELECT department,
               salary,
               name,
               AVG(salary) OVER (PARTITION BY department) as dept_avg,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) OVER (PARTITION BY department) as dept_median
        FROM employees
    )
    SELECT name, department, salary,
           ROUND(dept_avg, 0) as dept_avg,
           ROUND(dept_median, 0) as dept_median,
           ROUND((salary - dept_avg) / dept_avg * 100, 1) as pct_above_avg
    FROM monthly_stats
    ORDER BY department, salary DESC
"""))

# HOMEWORK.
print("\n" + "="*60)
print("HOMEWORK — Advanced SQL")
print("="*60)

print("\nLevel 1: Write a CTE that gets department counts")
display(spark.sql("WITH dc AS (SELECT department, count(*) c FROM employees GROUP BY department) SELECT * FROM dc"))

print("\nLevel 2: Use QUALIFY to get the lowest earner per department")
display(spark.sql("""
    SELECT name, department, salary,
           RANK() OVER (PARTITION BY department ORDER BY salary ASC) as rnk
    FROM employees QUALIFY rnk = 1
"""))

print("\nLevel 3: PIVOT quarterly_sales to show quarters as columns")
display(spark.sql("SELECT * FROM quarterly_sales PIVOT (SUM(revenue) FOR quarter IN ('Q1','Q2','Q3','Q4'))"))

print("\nAll exercises completed!")