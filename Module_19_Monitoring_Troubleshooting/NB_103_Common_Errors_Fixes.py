# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 103: Common Errors & Fixes
# MAGIC ## Module 19: Monitoring & Troubleshooting
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC This is a **troubleshooting reference** for the most common errors you'll encounter in Databricks/PySpark development. Each error includes the exact error message, the root cause, and the fix — so you can resolve issues in minutes instead of hours.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Think of this as a **car repair manual**: "Engine won't start? Check battery (most common), then fuel pump, then starter motor." For each symptom, the manual gives you the most likely causes in order of probability.
# MAGIC
# MAGIC ### Error Categories:
# MAGIC | Category | Examples |
# MAGIC |----------|----------|
# MAGIC | Analysis errors | Column not found, table not found, type mismatch |
# MAGIC | Runtime errors | OOM, task failures, serialization errors |
# MAGIC | Permission errors | Access denied, insufficient privileges |
# MAGIC | Data errors | Null handling, schema mismatch, corrupt records |
# MAGIC | Configuration | Wrong cluster, missing library, version conflicts |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Debugging Workflow:
# MAGIC
# MAGIC   1. READ the error message carefully (it usually tells you what's wrong).
# MAGIC   2. IDENTIFY the error type:
# MAGIC      - AnalysisException   → Query plan issue (column/table not found).
# MAGIC      - Py4JJavaError       → JVM-side error (OOM, serialization, I/O).
# MAGIC      - PermissionDenied    → UC access issue.
# MAGIC      - ParseException      → SQL syntax error.
# MAGIC   3. CHECK the stack trace for the FIRST meaningful line.
# MAGIC   4. SEARCH for the specific error class + message.
# MAGIC   5. FIX and verify.
# MAGIC
# MAGIC Most Common Root Causes (ranked by frequency):
# MAGIC
# MAGIC   #1: Column name typo or case mismatch (40% of beginner errors).
# MAGIC   #2: Table/schema doesn't exist or no permission (20%).
# MAGIC   #3: Type mismatch in operations or joins (15%).
# MAGIC   #4: Null values causing unexpected behavior (10%).
# MAGIC   #5: Memory issues (OOM) on large data (10%).
# MAGIC   #6: Missing library or version conflict (5%).
# MAGIC
# MAGIC Error Message Anatomy:
# MAGIC
# MAGIC   Py4JJavaError: An error occurred while calling o123.count.
# MAGIC   : org.apache.spark.SparkException: Job aborted due to stage failure:
# MAGIC     Task 5 in stage 12.0 failed 4 times, most recent failure:
# MAGIC       java.lang.OutOfMemoryError: GC overhead limit exceeded
# MAGIC          ^
# MAGIC          |
# MAGIC          This is the KEY information. Everything above is wrapper.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: Beginner Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER: COMMON ERRORS & FIXES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, lit  # Spark functions.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType  # Types.

print("="*70)
print("SECTION 3 — COMMON ERRORS & FIXES")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# ERROR 1: AnalysisException - Column not found
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print('ERROR 1: AnalysisException: Column "xyz" not found')
print("-"*60)

df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])  # Sample data.

# THE ERROR:
print("\n  Error: AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION]")
print('         Column "Name" not found. Did you mean "name"?')

# CAUSE: Column names are CASE-SENSITIVE in PySpark.
print("\n  Cause: Case mismatch ('Name' vs 'name').")

# FIX:
print("  Fix: Check actual column names:")
print(f"    df.columns = {df.columns}")  # Shows actual names.
print("    Use exact case: df.select('name')  # lowercase.")
print("    Or: df.select(col('name'))")

# ─────────────────────────────────────────────────────────────────
# ERROR 2: Table or view not found
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print('ERROR 2: TABLE_OR_VIEW_NOT_FOUND')
print("-"*60)

print("\n  Error: [TABLE_OR_VIEW_NOT_FOUND] The table or view")
print('         `catalog`.`schema`.`my_table` cannot be found.')

print("\n  Causes:")
print("    1. Table doesn't exist (typo in name).")
print("    2. Wrong catalog/schema context.")
print("    3. You don't have USE CATALOG/USE SCHEMA permission.")

print("\n  Fixes:")
print("    1. Check spelling: SHOW TABLES IN catalog.schema;")
print("    2. Use fully qualified name: catalog.schema.table")
print("    3. Check permissions: SHOW GRANTS ON CATALOG catalog;")
print(f"    4. Current context: catalog={spark.sql('SELECT current_catalog()').collect()[0][0]}, "
       f"schema={spark.sql('SELECT current_schema()').collect()[0][0]}")

# ─────────────────────────────────────────────────────────────────
# ERROR 3: Type mismatch
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print('ERROR 3: Cannot resolve due to data type mismatch')
print("-"*60)

print("\n  Error: [DATATYPE_MISMATCH] Cannot resolve 'a = b' due to")
print("         data type mismatch: 'INT' vs 'STRING'.")

print("\n  Cause: Joining or comparing columns of different types.")

# Demo: type mismatch.
df_int = spark.createDataFrame([(1,), (2,)], ["id"])       # id is INT.
df_str = spark.createDataFrame([("1",), ("2",)], ["id"])   # id is STRING.

print("\n  Fix: Cast to matching type before joining:")
print("    df_str.withColumn('id', col('id').cast('int'))  # Cast string to int.")
print("    OR: df_int.withColumn('id', col('id').cast('string'))  # Cast int to string.")

# Verify types.
print(f"\n  df_int schema: {df_int.dtypes}")
print(f"  df_str schema: {df_str.dtypes}")
print("  Always check schemas before joining!")

# COMMAND ----------

# DBTITLE 1,Sections 4-5: Intermediate and Advanced Errors
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4-5 — INTERMEDIATE & ADVANCED ERRORS
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTIONS 4-5: Runtime & Configuration Errors")
print("="*70)

# ─── ERROR 4: OutOfMemoryError ───
print("\n" + "-"*60)
print("ERROR 4: java.lang.OutOfMemoryError")
print("-"*60)

print("""
  Error: Py4JJavaError: java.lang.OutOfMemoryError: Java heap space
  OR:    java.lang.OutOfMemoryError: GC overhead limit exceeded

  Causes:
    1. Driver OOM: .collect() on large DataFrame.
    2. Executor OOM: Skewed partition or too much data per task.
    3. Python OOM: Large pandas operation on driver.

  Fixes (by cause):
    1. Driver OOM:
       - Never .collect() large data. Use .limit(N).collect().
       - Increase: spark.driver.memory = "8g"
    
    2. Executor OOM:
       - Increase partitions: spark.sql.shuffle.partitions = 400
       - Increase memory: spark.executor.memory = "8g"
       - Fix data skew (salt keys, AQE).
    
    3. Python OOM:
       - Avoid .toPandas() on large DataFrames.
       - Use pyspark.pandas instead.
       - Aggregate in Spark, collect only summary.
""")

# ─── ERROR 5: Permission Denied ───
print("-"*60)
print("ERROR 5: INSUFFICIENT_PERMISSIONS / Access Denied")
print("-"*60)

print("""
  Error: [INSUFFICIENT_PERMISSIONS] User does not have permission
         SELECT on TABLE `catalog`.`schema`.`table`.

  Causes:
    1. Missing GRANT for the specific table.
    2. Missing USE CATALOG or USE SCHEMA (can't even see it).
    3. Cluster not in USER_ISOLATION mode (UC not enforced).

  Fixes:
    1. Ask admin to grant access:
       GRANT USE CATALOG ON CATALOG cat TO `your_group`;
       GRANT USE SCHEMA ON SCHEMA cat.schema TO `your_group`;
       GRANT SELECT ON TABLE cat.schema.table TO `your_group`;
    
    2. Check your groups:
       SELECT is_account_group_member('data_engineers');  -- true/false
    
    3. Check what you CAN access:
       SHOW TABLES IN catalog.schema;  -- Only shows tables you can see.
""")

# ─── ERROR 6: Schema Mismatch (Delta writes) ───
print("-"*60)
print("ERROR 6: Schema mismatch on Delta write")
print("-"*60)

print("""
  Error: [DELTA_SCHEMA_MISMATCH] A schema mismatch detected when writing.
         Expected: id INT, name STRING
         Provided: id INT, name STRING, email STRING

  Cause: Writing data with extra/missing/different columns.

  Fixes:
    1. Enable schema evolution (add new columns automatically):
       df.write.option("mergeSchema", "true").mode("append").saveAsTable("t")
    
    2. Overwrite schema entirely:
       df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("t")
    
    3. Align schemas manually:
       df = df.select("id", "name")  # Only write expected columns.
""")

# ─── ERROR 7: Serialization / Py4J errors ───
print("-"*60)
print("ERROR 7: Serialization errors in UDFs")
print("-"*60)

print("""
  Error: PicklingError: Could not serialize object.
  OR:    Py4JError: An error occurred while calling None.None.

  Causes:
    1. Referencing a non-serializable object inside a UDF.
    2. Using SparkSession/SparkContext inside a UDF (they run on workers!).
    3. Closure captures a large object.

  Fixes:
    1. Don't reference spark/sc inside UDFs:
       # BAD:
       @udf("string")
       def bad_udf(x):
           return spark.conf.get("some.setting")  # Can't use spark on workers!
       
       # GOOD: Pass the value as a parameter or broadcast.
       setting = spark.conf.get("some.setting")  # Get on driver.
       @udf("string")
       def good_udf(x):
           return f"{x}_{setting}"  # Closure captures the string value.
    
    2. Use broadcast variables for large lookups:
       lookup_bc = spark.sparkContext.broadcast(large_dict)  # Broadcast once.
       @udf("string")
       def lookup_udf(key):
           return lookup_bc.value.get(key, "unknown")  # Access broadcast.
""")

print("✓ Most errors have clear messages. Always read the LAST line of the traceback.")

# COMMAND ----------

# DBTITLE 1,Section 6: Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Not reading the error message carefully
# MAGIC ```
# MAGIC # The error message TELLS you what's wrong:
# MAGIC #   "Column 'amont' not found. Did you mean 'amount'?"  → TYPO!
# MAGIC #   "Table 'orders' not found"  → Wrong catalog context.
# MAGIC #   "Permission denied"  → Need GRANT.
# MAGIC
# MAGIC # TIP: Read the LAST line of a long traceback. That's the actual error.
# MAGIC # Everything before it is the call stack (usually not helpful).
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Googling the wrong part of the error
# MAGIC ```
# MAGIC # BAD search: "Py4JJavaError: An error occurred while calling o123.count"
# MAGIC # (The "o123" is random, changes every run. Useless for search.)
# MAGIC
# MAGIC # GOOD search: "java.lang.OutOfMemoryError: GC overhead limit exceeded spark"
# MAGIC # (The actual exception class + message = useful search terms.)
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Not checking data types before operations
# MAGIC ```python
# MAGIC # BAD: Assuming types match.
# MAGIC df1.join(df2, "customer_id")  # What if one is INT and other is STRING?
# MAGIC
# MAGIC # GOOD: Always verify schemas first.
# MAGIC print(df1.schema["customer_id"].dataType)  # IntegerType.
# MAGIC print(df2.schema["customer_id"].dataType)  # StringType! Mismatch!
# MAGIC # Fix: df2 = df2.withColumn("customer_id", col("customer_id").cast("int"))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Using .collect() without .limit()
# MAGIC ```python
# MAGIC # BAD: Collecting potentially huge DataFrame.
# MAGIC results = df.collect()  # What if df has 100M rows? OOM!
# MAGIC
# MAGIC # GOOD: Always limit or aggregate first.
# MAGIC results = df.limit(1000).collect()  # Safe.
# MAGIC results = df.groupBy("key").count().collect()  # Aggregated = small.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not using try/except in production code
# MAGIC ```python
# MAGIC # BAD: Uncaught exception kills the entire pipeline.
# MAGIC df = spark.table("might_not_exist")  # Throws if missing!
# MAGIC
# MAGIC # GOOD: Handle gracefully.
# MAGIC try:
# MAGIC     df = spark.table("might_not_exist")
# MAGIC except Exception as e:
# MAGIC     logger.error(f"Table not found: {e}")
# MAGIC     dbutils.notebook.exit('{"status": "failed", "error": "table_missing"}')
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7: Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("HOMEWORK — Common Errors & Fixes")
print("="*70)

print("\n--- Level 1: Column not found ---")
print("  Check: df.columns or df.printSchema()")
print("  Fix: Use exact column name (case-sensitive).")

print("\n--- Level 2: Table not found ---")
print("  Check: SHOW TABLES IN catalog.schema;")
print("  Fix: Use fully qualified name OR check USE CATALOG/SCHEMA grants.")

print("\n--- Level 3: Type mismatch ---")
print("  Check: df.dtypes or df.schema")
print("  Fix: .cast('int') or .cast('string') before join/compare.")

print("\n--- Level 4: OOM errors ---")
print("  Driver OOM: Don't .collect() large data. Use .limit().")
print("  Executor OOM: Increase partitions or memory. Fix skew.")

print("\n--- Level 5: Permission denied ---")
print("  Check: SHOW GRANTS ON TABLE cat.schema.table;")
print("  Fix: Ask admin for GRANT USE CATALOG + USE SCHEMA + SELECT.")

print("\n--- Level 6: Schema mismatch on write ---")
print("  Fix: .option('mergeSchema', 'true')  # Auto-add new columns.")
print("  Or: .option('overwriteSchema', 'true')  # Replace schema.")

print("\n--- Level 7: Serialization errors in UDFs ---")
print("  Fix: Don't use spark/sc inside UDFs. Use broadcast for lookups.")

print("\n--- Level 8: Null handling ---")
print("  NullPointerException in UDF: check for None.")
print("  Unexpected results: NULL != NULL. Use .isNull() or coalesce().")

print("\n--- Level 9: Cluster/driver issues ---")
print("  'Cluster terminated': check event log (cost limit? idle timeout?).")
print("  'Driver unresponsive': too much data collected to driver.")

print("\n--- Level 10: Teach debugging ---")
print("""
"Databricks debugging workflow:
  1. Read the LAST line of the error (actual exception).
  2. Identify type: AnalysisException, OOM, Permission, Schema.
  3. Quick checks: df.columns, df.dtypes, SHOW TABLES, SHOW GRANTS.
  4. Fix: cast types, fully-qualify names, increase memory, add grants.
  5. Prevent: .explain() before running, assert counts, try/except.
  Top causes: typos (40%), permissions (20%), types (15%), nulls (10%).
  Always: check schema first, use fully qualified names, handle nulls."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 103")
print("✓ MODULE 19 (Monitoring & Troubleshooting) COMPLETE!")
print("="*70)