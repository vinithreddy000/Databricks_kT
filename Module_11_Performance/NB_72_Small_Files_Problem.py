# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Notebook 72: The Small Files Problem — Cause and Cure
# MAGIC ## Module 11: Performance Optimization
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 40 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC The **small files problem** happens when your table has thousands of tiny files instead of a few optimally-sized ones. Reading 10,000 files of 1KB each is **100x slower** than reading 10 files of 1MB, even though total data is the same. Each file requires a separate network connection, metadata lookup, and task overhead.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine receiving a package delivery:
# MAGIC - **Small files** = 10,000 individual envelopes, each containing one sheet of paper. The mailman knocks 10,000 times, you open 10,000 envelopes.
# MAGIC - **Optimized** = 10 boxes, each containing 1,000 sheets. Just 10 door opens.
# MAGIC
# MAGIC Same total paper, but handling 10 boxes is 1000x more efficient than 10,000 envelopes.
# MAGIC
# MAGIC ### Target file size: **128MB–256MB** per file for Delta/Parquet.
# MAGIC
# MAGIC ### Common causes:
# MAGIC - Streaming micro-batches (each batch writes tiny files)
# MAGIC - Frequent small appends (ETL writing every minute)
# MAGIC - Over-partitioning (partitionBy on high-cardinality column)
# MAGIC - Too many shuffle partitions (200 partitions for 10MB of data)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC The Problem:
# MAGIC   Table with 10GB of data:
# MAGIC
# MAGIC   HEALTHY (10 files × 1GB):         UNHEALTHY (10,000 files × 1MB):
# MAGIC   ┌───────────────────────┐      ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐... (x10,000)
# MAGIC   │ File 1: 1GB             │      Tasks: 10,000 (most finish instantly)
# MAGIC   │ File 2: 1GB             │      Scheduling overhead: HUGE
# MAGIC   │ ...                     │      Metadata reads: 10,000
# MAGIC   │ File 10: 1GB            │      List operations: slow
# MAGIC   └───────────────────────┘
# MAGIC   Tasks: 10 (good parallelism)
# MAGIC   Scheduling: minimal
# MAGIC   Read speed: optimal
# MAGIC
# MAGIC Solutions (in order of preference):
# MAGIC   1. OPTIMIZE (Delta)         → Compacts existing files post-hoc
# MAGIC   2. Auto Optimize            → Automatic compaction on writes
# MAGIC   3. Optimized Writes         → Coalesce partitions at write time
# MAGIC   4. coalesce(n) before write → Manual control of output files
# MAGIC   5. maxRecordsPerFile        → Cap records per output file
# MAGIC   6. Liquid Clustering        → Replaces partitioning + ZORDER
# MAGIC
# MAGIC Detection:
# MAGIC   DESCRIBE DETAIL my_table;  → check numFiles and sizeInBytes
# MAGIC   If avgFileSize < 64MB AND numFiles > 100 → small files problem!
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Small Files Demo
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-5 — EXAMPLES (Beginner to Advanced)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, lit  # Imports.

print("="*70)
print("SECTIONS 3-5: Small Files Problem — Cause and Cure")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Creating the small files problem
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Creating small files (the problem)")
print("-"*60)

path = "/tmp/delta_kt/small_files_problem"

# Simulate streaming micro-batches: 20 tiny appends.
for i in range(20):
    spark.range(i * 100, (i + 1) * 100).select(  # 100 rows each.
        col("id"), rand().alias("value")
    ).write.format("delta").mode("append").save(path)

# Check the damage.
active_files = spark.read.format("delta").load(path).inputFiles()
total_rows = spark.read.format("delta").load(path).count()
print(f"Total rows: {total_rows:,}")
print(f"Number of files: {len(active_files)}")
print(f"Avg rows per file: {total_rows // max(len(active_files), 1)}")
print("")
print("⚠️ Problem: 20+ tiny files for just 2,000 rows!")
print("  Each file has ~100 rows (should have hundreds of thousands).")
print("  Reading this table creates 20+ tasks with massive overhead.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Solution — OPTIMIZE (compact files)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: OPTIMIZE compacts small files")
print("-"*60)

before = len(spark.read.format("delta").load(path).inputFiles())
print(f"Before OPTIMIZE: {before} files")

spark.sql(f"OPTIMIZE delta.`{path}`")  # Compact all small files into optimal sizes.

after = len(spark.read.format("delta").load(path).inputFiles())
print(f"After OPTIMIZE: {after} files")
print(f"  Reduced from {before} to {after} files!")
print("  OPTIMIZE merges small files into ~1GB target size.")
print("  Old small files are marked for deletion (VACUUM cleans them).")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Prevention — coalesce before write
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: coalesce before write (prevent small files)")
print("-"*60)

path_fixed = "/tmp/delta_kt/small_files_prevented"
df = spark.range(10000).select(col("id"), rand().alias("val"))  # 10K rows.

# Control output files with coalesce.
df.coalesce(2).write.format("delta").mode("overwrite").save(path_fixed)  # 2 files.
files = len(spark.read.format("delta").load(path_fixed).inputFiles())
print(f"With coalesce(2): {files} output files (controlled!)")
print("  Rule: coalesce(n) where n = data_size_MB / 256")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Auto Optimize settings (production best practice)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Auto Optimize table properties")
print("-"*60)

print("""
For production tables, enable Auto Optimize:

  ALTER TABLE my_catalog.my_schema.my_table SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',   -- Coalesce at write time
    'delta.autoOptimize.autoCompact' = 'true'      -- Auto-compact after write
  );

  optimizeWrite: Repartitions data at write time to avoid tiny files.
    (Slight write latency increase, but MUCH better read performance.)

  autoCompact: After each write, checks if files are small and compacts.
    (Runs in background, transparent to the writer.)

For new tables:
  CREATE TABLE my_table (...) 
  TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
  );

Or set at workspace level (applies to all new tables):
  SET spark.databricks.delta.optimizeWrite.enabled = true;
  SET spark.databricks.delta.autoCompact.enabled = true;
""")
print("✓ Auto Optimize is the #1 recommended setting for production tables.")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Writing data with too many shuffle partitions
# MAGIC ```python
# MAGIC # BAD: 200 partitions writing 1MB of data = 200 tiny files.
# MAGIC df.groupBy("key").count().write.format("delta").save("/out")  # 200 files!
# MAGIC
# MAGIC # GOOD: coalesce before write.
# MAGIC df.groupBy("key").count().coalesce(4).write.format("delta").save("/out")  # 4 files.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Over-partitioning with partitionBy on high-cardinality column
# MAGIC ```python
# MAGIC # BAD: 1M customers = 1M directories with 1 tiny file each.
# MAGIC df.write.partitionBy("customer_id").save("/out")  # 1 million folders!
# MAGIC
# MAGIC # GOOD: partitionBy on LOW cardinality only (year, month, region).
# MAGIC df.write.partitionBy("year", "month").save("/out")  # ~36 folders.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Running OPTIMIZE too frequently (or never)
# MAGIC ```python
# MAGIC # Too often: OPTIMIZE every 5 minutes on a streaming table = overhead.
# MAGIC # Never: Files accumulate, reads get slower and slower.
# MAGIC # GOOD: Schedule OPTIMIZE daily or use Auto Compact.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Forgetting VACUUM after OPTIMIZE
# MAGIC ```python
# MAGIC # OPTIMIZE marks old files as deleted but doesn't remove them from disk.
# MAGIC -- GOOD: Run VACUUM periodically to actually delete old files.
# MAGIC VACUUM delta.`/path` RETAIN 168 HOURS;  -- Keep 7 days of history.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Using coalesce(1) on large data
# MAGIC ```python
# MAGIC # BAD: Forces ALL data through 1 task (single-threaded bottleneck).
# MAGIC huge_df.coalesce(1).write.save("/out")  # 1 core does everything!
# MAGIC
# MAGIC # GOOD: Target 128-256MB per file.
# MAGIC # For 10GB: coalesce(40) gives ~250MB per file.
# MAGIC huge_df.coalesce(40).write.save("/out")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand  # Imports.

print("="*70)
print("HOMEWORK — Small Files Problem")
print("="*70)

# Level 1: Count files in a table.
print("\n--- Level 1: Check file count ---")
path = "/tmp/delta_kt/small_files_problem"
files = spark.read.format("delta").load(path).inputFiles()
print(f"Files: {len(files)}")
# WHY: First step in diagnosing small files = check file count.

# Level 2: Run OPTIMIZE.
print("\n--- Level 2: Compact with OPTIMIZE ---")
spark.sql(f"OPTIMIZE delta.`{path}`")
print(f"After OPTIMIZE: {len(spark.read.format('delta').load(path).inputFiles())} files")
# WHY: OPTIMIZE merges small files into target size (~1GB).

# Level 3: Write with controlled file count.
print("\n--- Level 3: coalesce before write ---")
hw_path = "/tmp/delta_kt/hw72_l3"
spark.range(5000).select(col("id"), rand().alias("v")).coalesce(2) \
    .write.format("delta").mode("overwrite").save(hw_path)
print(f"Files: {len(spark.read.format('delta').load(hw_path).inputFiles())}")
# WHY: coalesce(2) ensures exactly 2 output files.

# Level 4: Use maxRecordsPerFile.
print("\n--- Level 4: maxRecordsPerFile ---")
hw_path4 = "/tmp/delta_kt/hw72_l4"
spark.range(10000).select(col("id"), rand().alias("v")) \
    .write.format("delta").mode("overwrite") \
    .option("maxRecordsPerFile", 5000).save(hw_path4)
print(f"Files: {len(spark.read.format('delta').load(hw_path4).inputFiles())}")
# WHY: maxRecordsPerFile caps each file at N rows (useful for even distribution).

# Level 5: Calculate optimal file count.
print("\n--- Level 5: Calculate target files ---")
print("Data: 50GB. Target file size: 256MB.")
print(f"Optimal files: {int(50 * 1024 / 256)} files")
print("Use: df.coalesce(200).write...")
# WHY: Target 128-256MB per file for optimal read performance.

# Levels 6-10.
print("\n--- Level 6: Auto Optimize settings ---")
print("ALTER TABLE t SET TBLPROPERTIES (")
print("  'delta.autoOptimize.optimizeWrite'='true',")
print("  'delta.autoOptimize.autoCompact'='true');")

print("\n--- Level 7: Detect small files with DESCRIBE DETAIL ---")
print("DESCRIBE DETAIL delta.`/path` → check numFiles vs sizeInBytes.")
print("If avgSize < 64MB and numFiles > 100 = problem!")

print("\n--- Level 8: Streaming small files ---")
print("Streaming writes tiny files per micro-batch.")
print("Fix: Trigger.availableNow + OPTIMIZE, or auto-compact.")

print("\n--- Level 9: VACUUM to reclaim space ---")
print("OPTIMIZE doesn't delete old files. VACUUM does.")
print("VACUUM delta.`/path` RETAIN 168 HOURS;")

print("\n--- Level 10: Teach small files ---")
print("""
"Too many small files = slow reads (overhead per file).
 Target: 128-256MB per file.
 Cause: streaming, frequent appends, over-partitioning.
 Fix: OPTIMIZE, Auto Compact, coalesce before write.
 Prevent: Enable optimizeWrite + autoCompact on all prod tables."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 72")
print("="*70)