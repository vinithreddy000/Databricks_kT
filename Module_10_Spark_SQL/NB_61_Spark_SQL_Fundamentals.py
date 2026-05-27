# Databricks notebook source
# DBTITLE 1,Sections 1-2 Overview
# MAGIC %md
# MAGIC # Notebook 61: Spark SQL Fundamentals
# MAGIC ## Module 10: Spark SQL Complete Reference
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Spark SQL lets you query data using **standard SQL** — the same language used by databases worldwide. You can create tables, insert data, query it, and manage databases — all with familiar SQL syntax, but running on Spark's distributed engine.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC If PySpark DataFrames are like cooking with a recipe book (step-by-step instructions), then Spark SQL is like telling someone **what you want to eat** in plain English — "Give me all orders over $100 from last month, sorted by amount."
# MAGIC
# MAGIC ### Why Spark SQL:
# MAGIC 1. **Familiar** — If you know SQL, you already know 90% of Spark SQL
# MAGIC 2. **Optimized** — Same Catalyst optimizer as DataFrame API
# MAGIC 3. **Interoperable** — Mix SQL and Python in the same notebook
# MAGIC 4. **Universal** — Works with Delta, Parquet, CSV, JSON, JDBC sources
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Two Ways to Write Queries in Databricks:
# MAGIC
# MAGIC   Method 1: SQL in Python         Method 2: Pure SQL Cell
# MAGIC   ───────────────────────       ──────────────────────
# MAGIC   result = spark.sql("""          %sql
# MAGIC       SELECT * FROM table         SELECT * FROM table
# MAGIC       WHERE amount > 100          WHERE amount > 100
# MAGIC   """)                            -- Auto-displayed!
# MAGIC   display(result)
# MAGIC
# MAGIC SQL Command Categories:
# MAGIC   DDL (Data Definition):  CREATE, ALTER, DROP, DESCRIBE, SHOW
# MAGIC   DML (Data Manipulation): INSERT, UPDATE, DELETE, MERGE
# MAGIC   DQL (Data Query):       SELECT, WITH, EXPLAIN
# MAGIC   DCL (Data Control):     GRANT, REVOKE, DENY
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 1: DDL Commands
# SECTION 3 — BEGINNER EXAMPLE 1: DDL (Data Definition Language)
# Real-world: Create and manage databases and tables.

print("=== DDL: Data Definition Language ===")  # Heading.

# CREATE DATABASE (Schema).
print("--- CREATE DATABASE ---")
spark.sql("CREATE DATABASE IF NOT EXISTS sql_kt_demo")  # Create.
spark.sql("USE sql_kt_demo")  # Switch to it.
print("Created and using database: sql_kt_demo")

# SHOW DATABASES.
print("\n--- SHOW DATABASES ---")
display(spark.sql("SHOW DATABASES"))

# CREATE TABLE (Managed).
print("\n--- CREATE TABLE (Managed Delta) ---")
spark.sql("DROP TABLE IF EXISTS employees")  # Clean.
spark.sql("""
    CREATE TABLE employees (
        emp_id INT COMMENT 'Employee ID - Primary Key',
        name STRING COMMENT 'Full name',
        department STRING COMMENT 'Department name',
        salary DOUBLE COMMENT 'Annual salary in USD',
        hire_date DATE COMMENT 'Date hired'
    )
    USING DELTA
    COMMENT 'Employee master table for SQL fundamentals demo'
""")
print("Table 'employees' created")

# CREATE TABLE with CTAS (Create Table As Select).
print("\n--- CREATE TABLE AS SELECT (CTAS) ---")
spark.sql("DROP TABLE IF EXISTS high_earners")
spark.sql("""
    CREATE TABLE high_earners AS
    SELECT * FROM employees WHERE salary > 90000
""")
print("Table 'high_earners' created from query")

# SHOW TABLES.
print("\n--- SHOW TABLES ---")
display(spark.sql("SHOW TABLES IN sql_kt_demo"))

# DESCRIBE TABLE.
print("\n--- DESCRIBE TABLE ---")
display(spark.sql("DESCRIBE TABLE employees"))

# SHOW CREATE TABLE.
print("\n--- SHOW CREATE TABLE ---")
display(spark.sql("SHOW CREATE TABLE employees"))

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 2: DML Commands
# SECTION 3 — BEGINNER EXAMPLE 2: DML (Data Manipulation Language)
# Real-world: Insert, update, delete data in tables.

print("=== DML: Data Manipulation Language ===")  # Heading.
spark.sql("USE sql_kt_demo")  # Ensure correct database.

# INSERT INTO — add rows.
print("--- INSERT INTO ---")
spark.sql("""
    INSERT INTO employees VALUES
    (1, 'Alice Johnson', 'Engineering', 95000, '2022-03-15'),
    (2, 'Bob Smith', 'Marketing', 72000, '2021-07-20'),
    (3, 'Carol Davis', 'Engineering', 88000, '2023-01-10'),
    (4, 'David Wilson', 'Sales', 68000, '2022-11-05'),
    (5, 'Eve Martinez', 'Engineering', 102000, '2020-05-22'),
    (6, 'Frank Brown', 'Marketing', 75000, '2023-06-01'),
    (7, 'Grace Lee', 'Sales', 71000, '2021-09-15'),
    (8, 'Henry Chen', 'Engineering', 115000, '2019-02-28')
""")
print(f"Inserted 8 rows. Count: {spark.sql('SELECT count(*) FROM employees').collect()[0][0]}")

# INSERT OVERWRITE — replace all data.
print("\n--- INSERT OVERWRITE ---")
spark.sql("DROP TABLE IF EXISTS high_earners")
spark.sql("CREATE TABLE high_earners USING DELTA AS SELECT 1 as x")  # Dummy.
spark.sql("""
    INSERT OVERWRITE high_earners
    SELECT * FROM employees WHERE salary > 90000
""")
print(f"High earners: {spark.sql('SELECT count(*) FROM high_earners').collect()[0][0]} rows")

# UPDATE.
print("\n--- UPDATE ---")
spark.sql("UPDATE employees SET salary = salary * 1.10 WHERE department = 'Engineering'")
print("Gave 10% raise to all Engineering employees")
display(spark.sql("SELECT name, department, salary FROM employees WHERE department = 'Engineering' ORDER BY emp_id"))

# DELETE.
print("\n--- DELETE ---")
spark.sql("DELETE FROM employees WHERE emp_id = 7")  # Remove Grace.
print(f"Deleted emp_id=7. Count: {spark.sql('SELECT count(*) FROM employees').collect()[0][0]}")

# TRUNCATE (remove all rows, keep table).
print("\n--- TRUNCATE (demo on high_earners) ---")
print(f"Before TRUNCATE: {spark.sql('SELECT count(*) FROM high_earners').collect()[0][0]} rows")
spark.sql("TRUNCATE TABLE high_earners")
print(f"After TRUNCATE: {spark.sql('SELECT count(*) FROM high_earners').collect()[0][0]} rows")
print("Table structure remains, only data removed.")

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Examples
# SECTION 4 — INTERMEDIATE: Joins, Views, and Catalog

from pyspark.sql.functions import col  # Import.

print("=== Intermediate SQL: Joins, Views, Catalog ===")  # Heading.
spark.sql("USE sql_kt_demo")

# Create departments reference table.
spark.sql("DROP TABLE IF EXISTS departments")
spark.sql("""
    CREATE TABLE departments (dept_name STRING, location STRING, budget DOUBLE) USING DELTA
""")
spark.sql("""
    INSERT INTO departments VALUES
    ('Engineering', 'Building A', 5000000),
    ('Marketing', 'Building B', 2000000),
    ('Sales', 'Building C', 3000000),
    ('HR', 'Building B', 1500000)
""")

# JOIN.
print("--- JOIN ---")
display(spark.sql("""
    SELECT e.name, e.department, e.salary, d.location, d.budget
    FROM employees e
    INNER JOIN departments d ON e.department = d.dept_name
    ORDER BY e.salary DESC
"""))

# LEFT JOIN (show all employees even without matching department).
print("\n--- LEFT JOIN ---")
display(spark.sql("""
    SELECT e.name, e.department, d.location
    FROM employees e
    LEFT JOIN departments d ON e.department = d.dept_name
"""))

# CREATE VIEW.
print("\n--- CREATE VIEW ---")
spark.sql("DROP VIEW IF EXISTS engineering_team")
spark.sql("""
    CREATE VIEW engineering_team AS
    SELECT emp_id, name, salary, hire_date
    FROM employees
    WHERE department = 'Engineering'
""")
print("View 'engineering_team' created")
display(spark.sql("SELECT * FROM engineering_team ORDER BY salary DESC"))

# SHOW commands.
print("\n--- SHOW commands ---")
print("Tables:")
display(spark.sql("SHOW TABLES"))
print("\nViews:")
display(spark.sql("SHOW VIEWS"))
print("\nColumns in employees:")
display(spark.sql("SHOW COLUMNS IN employees"))

# Mixing SQL and Python.
print("\n--- Mixing SQL and Python ---")
result_df = spark.sql("SELECT department, AVG(salary) as avg_sal FROM employees GROUP BY department")
# Now use DataFrame API on SQL result.
result_df.filter(col("avg_sal") > 75000).show()
print("SQL result is a DataFrame — you can chain DataFrame operations on it!")

# COMMAND ----------

# DBTITLE 1,Section 5 and Exercises
# SECTION 5 — ADVANCED: Production SQL Patterns

print("=== Advanced SQL Patterns ===")  # Heading.
spark.sql("USE sql_kt_demo")

# Window functions in SQL.
print("--- Window Functions ---")
display(spark.sql("""
    SELECT 
        name, department, salary,
        RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
        ROUND(salary - AVG(salary) OVER (PARTITION BY department), 2) as diff_from_dept_avg,
        SUM(salary) OVER (ORDER BY salary DESC ROWS UNBOUNDED PRECEDING) as running_total
    FROM employees
    ORDER BY department, dept_rank
"""))

# CASE WHEN.
print("\n--- CASE WHEN ---")
display(spark.sql("""
    SELECT name, salary,
        CASE 
            WHEN salary >= 100000 THEN 'Senior'
            WHEN salary >= 80000 THEN 'Mid'
            ELSE 'Junior'
        END as level,
        CASE 
            WHEN salary >= 100000 THEN salary * 0.15
            WHEN salary >= 80000 THEN salary * 0.10
            ELSE salary * 0.05
        END as bonus
    FROM employees
    ORDER BY salary DESC
"""))

# CTE (Common Table Expression).
print("\n--- CTE (WITH clause) ---")
display(spark.sql("""
    WITH dept_stats AS (
        SELECT department, AVG(salary) as avg_sal, COUNT(*) as cnt
        FROM employees GROUP BY department
    ),
    overall AS (
        SELECT AVG(salary) as company_avg FROM employees
    )
    SELECT ds.department, ds.cnt, ROUND(ds.avg_sal,0) as dept_avg,
           ROUND(o.company_avg,0) as company_avg,
           CASE WHEN ds.avg_sal > o.company_avg THEN 'Above' ELSE 'Below' END as vs_company
    FROM dept_stats ds CROSS JOIN overall o
    ORDER BY ds.avg_sal DESC
"""))

# Cleanup.
print("\n--- Cleanup ---")
spark.sql("DROP VIEW IF EXISTS engineering_team")
spark.sql("DROP TABLE IF EXISTS high_earners")
print("Cleaned up views and temp tables.")

# HOMEWORK.
print("\n" + "="*60)
print("HOMEWORK — Spark SQL Fundamentals")
print("="*60)
print("\nLevel 1: SELECT * FROM employees WHERE department = 'Sales'")
display(spark.sql("SELECT * FROM sql_kt_demo.employees WHERE department = 'Sales'"))
print("\nLevel 2: GROUP BY department, count and average salary")
display(spark.sql("SELECT department, count(*) cnt, round(avg(salary),0) avg_sal FROM sql_kt_demo.employees GROUP BY department"))
print("\nLevel 3: Find employees earning above their department average")
display(spark.sql("""
    SELECT name, department, salary, round(dept_avg,0) as dept_avg
    FROM (
        SELECT *, AVG(salary) OVER (PARTITION BY department) as dept_avg
        FROM sql_kt_demo.employees
    ) WHERE salary > dept_avg
"""))
print("\nAll exercises completed!")

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 3: SELECT Queries
# SECTION 3 — BEGINNER EXAMPLE 3: SELECT Queries
# Real-world: The bread and butter of data analysis.

print("=== SELECT Queries ===")  # Heading.
spark.sql("USE sql_kt_demo")

# Basic SELECT.
print("--- Basic SELECT ---")
display(spark.sql("SELECT * FROM employees ORDER BY emp_id"))

# SELECT with WHERE.
print("\n--- WHERE clause ---")
display(spark.sql("""
    SELECT name, department, salary
    FROM employees
    WHERE salary > 80000 AND department = 'Engineering'
    ORDER BY salary DESC
"""))

# Aggregations.
print("\n--- GROUP BY with aggregations ---")
display(spark.sql("""
    SELECT 
        department,
        COUNT(*) as num_employees,
        ROUND(AVG(salary), 2) as avg_salary,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
"""))

# HAVING (filter on aggregates).
print("\n--- HAVING (filter after GROUP BY) ---")
display(spark.sql("""
    SELECT department, COUNT(*) as cnt, ROUND(AVG(salary),0) as avg_sal
    FROM employees
    GROUP BY department
    HAVING COUNT(*) >= 2
    ORDER BY avg_sal DESC
"""))

# Subqueries.
print("\n--- Subquery ---")
display(spark.sql("""
    SELECT name, salary, department
    FROM employees
    WHERE salary > (SELECT AVG(salary) FROM employees)
    ORDER BY salary DESC
"""))
print("Employees earning above average.")