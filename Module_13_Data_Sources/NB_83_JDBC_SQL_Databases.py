# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 83: JDBC Connections — SQL Server, PostgreSQL, MySQL, Oracle
# MAGIC ## Module 13: Data Sources & Connectors
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **JDBC** (Java Database Connectivity) lets Spark read from and write to **any relational database** — SQL Server, PostgreSQL, MySQL, Oracle, etc. Spark connects over the network, pulls data in parallel, and processes it with all its distributed power.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine you have a warehouse (Spark) and a small shop (SQL Database):
# MAGIC - **Without parallelism**: One truck drives to the shop, loads ALL inventory, drives back. Very slow for large tables.
# MAGIC - **With parallelism** (`numPartitions`): 10 trucks go simultaneously, each taking a different section of inventory. 10x faster!
# MAGIC
# MAGIC The key to fast JDBC reads is **parallel extraction** using `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions`.
# MAGIC
# MAGIC ### Supported Databases:
# MAGIC | Database | JDBC URL Pattern |
# MAGIC |----------|------------------|
# MAGIC | SQL Server | `jdbc:sqlserver://host:1433;databaseName=db` |
# MAGIC | PostgreSQL | `jdbc:postgresql://host:5432/database` |
# MAGIC | MySQL | `jdbc:mysql://host:3306/database` |
# MAGIC | Oracle | `jdbc:oracle:thin:@host:1521:SID` |
# MAGIC | Azure SQL | `jdbc:sqlserver://server.database.windows.net:1433;database=db` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC JDBC Read Architecture:
# MAGIC
# MAGIC   Single-threaded (SLOW):          Parallel (FAST):
# MAGIC   ──────────────────────          ───────────────────
# MAGIC   [Spark Driver]                   [Spark Executors]
# MAGIC        │                            │   │   │   │
# MAGIC        │  SELECT * FROM table       │   │   │   │  (4 parallel queries)
# MAGIC        │                            │   │   │   │
# MAGIC        ▼                            ▼   ▼   ▼   ▼
# MAGIC   [SQL Database]                   [SQL Database]
# MAGIC   (one query,                      (4 queries, each reading a range:
# MAGIC    returns ALL rows)                 WHERE id BETWEEN 1 AND 250000
# MAGIC                                      WHERE id BETWEEN 250001 AND 500000
# MAGIC                                      WHERE id BETWEEN 500001 AND 750000
# MAGIC                                      WHERE id BETWEEN 750001 AND 1000000)
# MAGIC
# MAGIC Parallel Read Configuration:
# MAGIC   .option("partitionColumn", "id")     # Column to split on (numeric/date).
# MAGIC   .option("lowerBound", "1")           # Min value of partition column.
# MAGIC   .option("upperBound", "1000000")     # Max value of partition column.
# MAGIC   .option("numPartitions", "8")        # Number of parallel connections.
# MAGIC
# MAGIC   These DO NOT filter data! They only determine how to split the work.
# MAGIC   All rows are read regardless of bounds.
# MAGIC
# MAGIC Pushdown:
# MAGIC   Spark pushes WHERE clauses and column selection to the database:
# MAGIC     df = spark.read.jdbc(...).filter("status = 'active'").select("id", "name")
# MAGIC     → Database executes: SELECT id, name FROM table WHERE status = 'active'
# MAGIC     → Only matching rows travel over the network!
# MAGIC
# MAGIC Write Modes:
# MAGIC   .mode("overwrite")  → DROP + CREATE + INSERT (dangerous for prod!).
# MAGIC   .mode("append")     → INSERT INTO existing table.
# MAGIC   .mode("ignore")     → Skip if table exists.
# MAGIC   .mode("error")      → Fail if table exists (default).
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: Examples and Homework
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7 — JDBC EXAMPLES & HOMEWORK
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTIONS 3-7: JDBC SQL Database Connections")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Basic JDBC read (SQL Server / Azure SQL)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Basic JDBC read from SQL Server")
print("-"*60)

print("""
# Connection parameters (store credentials in secrets!).
jdbc_url = "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=mydb"
properties = {
    "user": dbutils.secrets.get(scope="sql", key="username"),
    "password": dbutils.secrets.get(scope="sql", key="password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Simple read (entire table, SINGLE-THREADED — slow for large tables!).
df = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.customers",
    properties=properties
)

# Better: Read with query (only get what you need).
query = "(SELECT id, name, email FROM dbo.customers WHERE active=1) AS t"
df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

# display(df)
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Parallel JDBC read (CRITICAL for large tables)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Parallel JDBC read (10x faster!)")
print("-"*60)

print("""
# Parallel read: splits table into N ranges, reads simultaneously.
df = spark.read.format("jdbc") \\
    .option("url", jdbc_url) \\
    .option("dbtable", "dbo.orders") \\
    .option("user", username) \\
    .option("password", password) \\
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \\
    .option("partitionColumn", "order_id")  # Must be numeric/date! \\
    .option("lowerBound", "1")              # Min value (doesn't filter!) \\
    .option("upperBound", "10000000")       # Max value (doesn't filter!) \\
    .option("numPartitions", "8")           # 8 parallel connections. \\
    .option("fetchsize", "10000")           # Rows per network round-trip. \\
    .load()

This generates 8 parallel queries:
  SELECT * FROM dbo.orders WHERE order_id >= 1 AND order_id < 1250001
  SELECT * FROM dbo.orders WHERE order_id >= 1250001 AND order_id < 2500001
  ... (8 total, running simultaneously)

Tuning rules:
  numPartitions: 2-4x the number of Spark cores. Don't exceed 50.
  fetchsize: 10000-100000 for large tables (default 10 is too low!).
  partitionColumn: Choose a roughly uniform distribution (ID is ideal).
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: JDBC write (append/overwrite)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Writing to a SQL database via JDBC")
print("-"*60)

print("""
# Write DataFrame to SQL table.
df.write.format("jdbc") \\
    .option("url", jdbc_url) \\
    .option("dbtable", "dbo.output_table") \\
    .option("user", username) \\
    .option("password", password) \\
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \\
    .option("batchsize", "10000")  # Rows per INSERT batch. \\
    .mode("append")  # or "overwrite" (DROPS table first!) \\
    .save()

Write tuning:
  batchsize: 5000-50000 (higher = fewer round-trips, but more memory).
  numPartitions: Controls write parallelism (coalesce before write).
  truncate: "true" with overwrite mode = TRUNCATE instead of DROP+CREATE.

# Safer overwrite (truncate, preserving table structure):
df.write.format("jdbc") \\
    .option("truncate", "true")  # TRUNCATE not DROP! \\
    .mode("overwrite") \\
    .save()
""")

# ─── SECTION 6: Common Mistakes ───
print("\n" + "="*70)
print("SECTION 6 — COMMON MISTAKES")
print("="*70)
print("""
1. Single-threaded read (missing numPartitions) on large tables.
   Fix: Always set partitionColumn + numPartitions for tables > 1M rows.

2. fetchsize=10 (default) causing millions of round-trips.
   Fix: Set fetchsize=10000 or higher for bulk reads.

3. Using .mode('overwrite') which DROPS the table (losing indexes/constraints).
   Fix: Use .option('truncate', 'true') with overwrite, or use .mode('append').

4. Opening too many parallel connections (numPartitions=100).
   Fix: Keep numPartitions between 4-50. Check DB connection limits.

5. Not using query pushdown (reading entire table then filtering in Spark).
   Fix: Use a subquery as dbtable: "(SELECT ... WHERE ...) AS t"
   Or filter immediately after read (Spark pushes predicates to DB).
""")

# ─── SECTION 7: Homework ───
print("="*70)
print("SECTION 7 — HOMEWORK")
print("="*70)
print("""
Level 1: JDBC URL for SQL Server?
  jdbc:sqlserver://host:1433;databaseName=mydb

Level 2: JDBC URL for PostgreSQL?
  jdbc:postgresql://host:5432/database

Level 3: How to read in parallel?
  .option("partitionColumn", "id").option("numPartitions", "8")
  .option("lowerBound", "1").option("upperBound", "1000000")

Level 4: What does fetchsize do?
  Controls rows fetched per network round-trip. Default 10 is too low!
  Set to 10000-100000 for large tables.

Level 5: How to avoid DROP TABLE on overwrite?
  .option("truncate", "true").mode("overwrite")

Level 6: How to read only specific columns/rows?
  Use subquery as table: "(SELECT col1, col2 FROM t WHERE x>5) AS sub"

Level 7: Lakehouse Federation (modern approach)?
  CREATE CONNECTION sql_conn TYPE SQLSERVER ...
  CREATE FOREIGN CATALOG sql_cat USING CONNECTION sql_conn;
  -- Now query directly: SELECT * FROM sql_cat.dbo.my_table;

Level 10: Teach JDBC to a colleague:
  "JDBC = read/write any SQL database from Spark.
   Key: set numPartitions for parallel reads (10x faster).
   Set fetchsize=10000 for bulk reads.
   Use secrets for credentials, never hardcode.
   Modern: Lakehouse Federation = query SQL DBs like local tables."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 83")
print("="*70)