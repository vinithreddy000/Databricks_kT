# Databricks notebook source
# DBTITLE 1,Section 1 - What Is This
# MAGIC %md
# MAGIC # Notebook 57: Delta Optimization — OPTIMIZE, ZORDER, VACUUM, Liquid Clustering
# MAGIC ## Module 09: Delta Lake Deep Dive
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Delta tables can become **slow** over time if you don't maintain them. Small files pile up, data gets scattered, and old files waste storage. **Optimization** is the process of cleaning up and reorganizing your Delta table so queries run fast.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Think of a **library**:
# MAGIC - **OPTIMIZE** = Consolidating many small sticky notes into proper bound books (compaction)
# MAGIC - **ZORDER** = Reorganizing books so related topics are on the same shelf (co-location)
# MAGIC - **VACUUM** = Throwing away old newspapers that nobody will ever read again (cleanup)
# MAGIC - **Liquid Clustering** = A smart librarian who automatically keeps related books together as new ones arrive
# MAGIC
# MAGIC ### Why This Matters:
# MAGIC - Without OPTIMIZE: A table with 10,000 tiny files takes 100x longer to read than one with 100 properly-sized files
# MAGIC - Without ZORDER: A query filtering by `date` AND `region` reads ALL files instead of just the relevant ones
# MAGIC - Without VACUUM: Deleted data still takes up disk space forever
# MAGIC - With Liquid Clustering: You get optimal layout automatically, no manual OPTIMIZE/ZORDER needed

# COMMAND ----------

# DBTITLE 1,Section 2 - How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC The Small Files Problem:
# MAGIC
# MAGIC   Before OPTIMIZE:                After OPTIMIZE:
# MAGIC   ┌───┐┌───┐┌───┐┌───┐┌───┐        ┌───────────────────────┐
# MAGIC   │1MB││2MB││0.5││3MB││1MB│        │      ~1GB file        │
# MAGIC   └───┘└───┘└───┘└───┘└───┘        └───────────────────────┘
# MAGIC   ┌───┐┌───┐┌───┐┌───┐┌───┐
# MAGIC   │0.1││4MB││0.3││2MB││5MB│        Target: ~1GB per file
# MAGIC   └───┘└───┘└───┘└───┘└───┘        (configurable)
# MAGIC   (10 small files = 10 open/read ops)
# MAGIC
# MAGIC
# MAGIC ZORDER (Data Co-location):
# MAGIC
# MAGIC   Without ZORDER:                 With ZORDER BY (date):
# MAGIC   File1: dates 1,5,9,3,7          File1: dates 1,2,3,4,5
# MAGIC   File2: dates 2,8,4,10,6         File2: dates 6,7,8,9,10
# MAGIC
# MAGIC   Query: WHERE date = 3           Query: WHERE date = 3
# MAGIC   Must scan: ALL files             Must scan: File1 only!
# MAGIC   (min/max stats can't help)       (min=1,max=5 → might be here)
# MAGIC
# MAGIC
# MAGIC Liquid Clustering (Modern Approach):
# MAGIC
# MAGIC   CLUSTER BY (region, date)
# MAGIC   → Data automatically reorganized during writes
# MAGIC   → No manual OPTIMIZE ZORDER needed
# MAGIC   → Incremental: only new/changed data is re-clustered
# MAGIC   → Replaces both PARTITIONING and ZORDER
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 1: OPTIMIZE
# SECTION 3 — BEGINNER EXAMPLE 1: OPTIMIZE (File Compaction)
# Real-world: Your streaming job created thousands of tiny files. Fix it.

from pyspark.sql.functions import col, rand, expr  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== OPTIMIZE: Compacting Small Files ===")  # Heading.

# Create a table with MANY small files (simulate streaming writes).
opt_path = "/tmp/delta_kt/optimize_demo"  # Path.

# Write 20 small batches (creates 20+ small files).
for i in range(20):  # 20 micro-batches.
    batch = spark.range(i*50, (i+1)*50).select(
        col("id"),
        (rand() * 1000).alias("value"),
        expr(f"'batch_{i}'").alias("source")
    )  # 50 rows per batch.
    batch.write.format("delta").mode("append").save(opt_path)  # Append.

print("After 20 small writes:")

# Count files BEFORE optimize.
all_files = [f for f in dbutils.fs.ls(opt_path) if f.name.endswith(".parquet")]  # Count.
print(f"  Data files: {len(all_files)}")
print(f"  Total rows: {spark.read.format('delta').load(opt_path).count()}")
print(f"  Avg file size: {sum(f.size for f in all_files) // len(all_files)} bytes")

# Run OPTIMIZE.
print("\n--- Running OPTIMIZE ---")
result = spark.sql(f"OPTIMIZE delta.`{opt_path}`")  # Compact!
display(result)  # Show metrics.

# Count files AFTER optimize.
all_files_after = [f for f in dbutils.fs.ls(opt_path) if f.name.endswith(".parquet")]  # Count.
active_files = spark.read.format("delta").load(opt_path).inputFiles()  # Active only.
print(f"\nAfter OPTIMIZE:")
print(f"  Active data files: {len(active_files)}")
print(f"  Total files on disk: {len(all_files_after)} (old ones still there until VACUUM)")
print(f"  Total rows: {spark.read.format('delta').load(opt_path).count()} (unchanged!)")

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 2: ZORDER
# SECTION 3 — BEGINNER EXAMPLE 2: ZORDER BY (Data Co-location)
# Real-world: Speed up queries that filter by specific columns.

from pyspark.sql.functions import col, rand, expr, round as spark_round  # Imports.

print("=== ZORDER BY: Co-locate Related Data ===")  # Heading.

# Create a larger table for meaningful ZORDER.
zorder_path = "/tmp/delta_kt/zorder_demo"  # Path.
data = spark.range(100000).select(  # 100K rows.
    col("id").alias("order_id"),
    (rand() * 1000).cast("int").alias("customer_id"),
    spark_round(rand() * 500 + 10, 2).alias("amount"),
    expr("date_add('2024-01-01', cast(rand()*365 as int))").alias("order_date"),
    expr("CASE WHEN rand()<0.25 THEN 'North' WHEN rand()<0.5 THEN 'South' WHEN rand()<0.75 THEN 'East' ELSE 'West' END").alias("region")
)  # Orders data.
data.write.format("delta").mode("overwrite").save(zorder_path)  # Write.

print("Table created: 100K orders")
print(f"Files: {len(spark.read.format('delta').load(zorder_path).inputFiles())}")

# Run OPTIMIZE with ZORDER.
print("\n--- Running OPTIMIZE ZORDER BY (region, order_date) ---")
result = spark.sql(f"OPTIMIZE delta.`{zorder_path}` ZORDER BY (region, order_date)")  # ZORDER!
display(result)

# Demonstrate data skipping.
print("\n--- Data Skipping in Action ---")
# This query benefits from ZORDER because it filters on region AND order_date.
print("Query: WHERE region = 'North' AND order_date > '2024-06-01'")
filtered = spark.read.format("delta").load(zorder_path).filter(
    (col("region") == "North") & (col("order_date") > "2024-06-01")
)  # Filtered.
print(f"Result: {filtered.count()} rows")
print("With ZORDER, Spark skips files where min/max stats prove no matching rows exist!")

# Show the explain plan to see file pruning.
print("\n--- Query Plan (notice 'numFiles read') ---")
filtered.explain(True)

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 3: VACUUM
# SECTION 3 — BEGINNER EXAMPLE 3: VACUUM (Cleanup Old Files)
# Real-world: Reclaim disk space by removing files no longer needed.

from pyspark.sql.functions import col, lit  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== VACUUM: Cleaning Up Old Files ===")  # Heading.

# Setup: Create a table and make changes.
vac_path = "/tmp/delta_kt/vacuum_demo"  # Path.
spark.range(1000).withColumn("value", col("id") * 10) \
    .write.format("delta").mode("overwrite").save(vac_path)  # V0.

# Make updates (creates orphaned files).
dt_vac = DeltaTable.forPath(spark, vac_path)  # Load.
dt_vac.update("id < 100", {"value": lit(999)})  # V1 (old files become orphans).
dt_vac.delete("id > 900")  # V2.

# Count files.
all_files = [f for f in dbutils.fs.ls(vac_path) if f.name.endswith(".parquet")]  # All.
active = spark.read.format("delta").load(vac_path).inputFiles()  # Active.
print(f"Total files on disk: {len(all_files)}")
print(f"Active files (current version): {len(active)}")
print(f"Orphaned files (old versions): {len(all_files) - len(active)}")

# DRY RUN: See what VACUUM would delete.
print("\n--- VACUUM DRY RUN (what would be deleted) ---")
# Must set retention check to false for demo (normally don't do this!).
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")  # Disable safety.
result = spark.sql(f"VACUUM delta.`{vac_path}` RETAIN 0 HOURS DRY RUN")  # Dry run.
display(result)  # Shows files that WOULD be deleted.

# Actually VACUUM.
print("\n--- Running VACUUM RETAIN 0 HOURS ---")
spark.sql(f"VACUUM delta.`{vac_path}` RETAIN 0 HOURS")  # Delete old files.

# Count after vacuum.
all_files_after = [f for f in dbutils.fs.ls(vac_path) if f.name.endswith(".parquet")]  # After.
print(f"\nAfter VACUUM:")
print(f"  Files on disk: {len(all_files_after)}")
print(f"  Active files: {len(spark.read.format('delta').load(vac_path).inputFiles())}")
print(f"  Space reclaimed by removing {len(all_files) - len(all_files_after)} files!")

# Re-enable the safety check.
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")  # Re-enable.

# WARNING about time travel after VACUUM.
print("\n⚠️ WARNING: After VACUUM, old versions are NO LONGER READABLE!")
try:
    spark.read.format("delta").option("versionAsOf", 0).load(vac_path).count()  # Try V0.
    print("V0 still readable (files not yet deleted)")
except Exception as e:
    print(f"V0 ERROR: {str(e)[:100]}")
    print("Old files were deleted — time travel broken for vacuumed versions!")

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Example 1: Auto-Optimize
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Auto-Optimize (Write Optimization)
# Real-world: Prevent small files from ever being created.

from pyspark.sql.functions import col, rand, expr  # Imports.

print("=== Auto-Optimize: Preventing Small Files ===")  # Heading.
print("Two features that prevent the small files problem at write time:\n")

# Feature 1: Optimized Writes.
print("--- Feature 1: Optimized Writes ---")
print("What it does: Automatically repartitions data before writing to reduce small files.")
print("When to use: Streaming, frequent appends, high-concurrency writes.\n")

auto_path = "/tmp/delta_kt/auto_optimize"  # Path.

# Create table with auto-optimize enabled.
spark.sql(f"""CREATE TABLE IF NOT EXISTS delta.`{auto_path}` (id BIGINT, value DOUBLE, category STRING)
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )""")
print("Table created with:")
print("  delta.autoOptimize.optimizeWrite = true")
print("  delta.autoOptimize.autoCompact = true")

# Write multiple small batches.
for i in range(10):
    spark.range(i*100, (i+1)*100).select(
        col("id"),
        (rand() * 100).alias("value"),
        expr(f"'cat_{i % 3}'").alias("category")
    ).write.format("delta").mode("append").save(auto_path)

files_auto = spark.read.format("delta").load(auto_path).inputFiles()
print(f"\nAfter 10 appends WITH auto-optimize: {len(files_auto)} files")

# Compare: Without auto-optimize.
no_auto_path = "/tmp/delta_kt/no_auto_optimize"  # Path.
for i in range(10):
    spark.range(i*100, (i+1)*100).select(
        col("id"),
        (rand() * 100).alias("value"),
        expr(f"'cat_{i % 3}'").alias("category")
    ).write.format("delta").mode("append").save(no_auto_path)

files_no_auto = spark.read.format("delta").load(no_auto_path).inputFiles()
print(f"After 10 appends WITHOUT auto-optimize: {len(files_no_auto)} files")
print(f"\n→ Auto-optimize reduces file count significantly!")

# Feature 2: Auto Compact.
print("\n--- Feature 2: Auto Compact ---")
print("What it does: After each write, automatically runs a small OPTIMIZE if too many small files.")
print("Triggers when: More than a threshold of small files accumulate.")
print("Cost: Adds slight latency to writes (~seconds), but saves much more on reads.")

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Example 2: Liquid Clustering
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Liquid Clustering (Modern Approach)
# Real-world: The replacement for PARTITION BY + ZORDER. Simpler and better.

from pyspark.sql.functions import col, rand, expr, round as spark_round  # Imports.

print("=== Liquid Clustering (DBR 13.3+) ===")  # Heading.
print("Liquid Clustering replaces BOTH partitioning AND ZORDER with a single, simpler concept.\n")

# Why Liquid Clustering is better.
print("--- Old Way vs New Way ---")
print("""
OLD WAY (problems):
  PARTITION BY (region)           → Can't change later, too many/few partitions
  + OPTIMIZE ZORDER BY (date)    → Must run manually, rewrites ALL data

NEW WAY (Liquid Clustering):
  CLUSTER BY (region, date)      → Automatic, incremental, changeable
""")

# Create a table with Liquid Clustering.
print("--- Creating Table with Liquid Clustering ---")
spark.sql("DROP TABLE IF EXISTS lc_demo_orders")  # Clean.
spark.sql("""
    CREATE TABLE lc_demo_orders (
        order_id BIGINT,
        customer_id INT,
        amount DOUBLE,
        order_date DATE,
        region STRING
    )
    USING DELTA
    CLUSTER BY (region, order_date)
""")  # Liquid Clustering!

print("Table created with CLUSTER BY (region, order_date)")
print("  → No PARTITION BY needed")
print("  → No manual OPTIMIZE ZORDER needed")
print("  → Data auto-clustered on writes")

# Insert data.
data = spark.range(50000).select(
    (col("id") + 1).alias("order_id"),
    (rand() * 200).cast("int").alias("customer_id"),
    spark_round(rand() * 500 + 10, 2).alias("amount"),
    expr("date_add('2024-01-01', cast(rand()*365 as int))").alias("order_date"),
    expr("CASE WHEN rand()<0.25 THEN 'North' WHEN rand()<0.5 THEN 'South' WHEN rand()<0.75 THEN 'East' ELSE 'West' END").alias("region")
)
data.write.format("delta").mode("append").insertInto("lc_demo_orders")  # Insert.
print(f"\nInserted {data.count()} rows")

# Run OPTIMIZE (triggers clustering).
print("\n--- OPTIMIZE triggers Liquid Clustering ---")
result = spark.sql("OPTIMIZE lc_demo_orders")
display(result)

# Show clustering benefit.
print("\n--- Query benefits from clustering ---")
print("Query: WHERE region = 'North' AND order_date BETWEEN '2024-06-01' AND '2024-06-30'")
result_df = spark.sql("""
    SELECT count(*) as cnt, round(avg(amount),2) as avg_amount
    FROM lc_demo_orders
    WHERE region = 'North' AND order_date BETWEEN '2024-06-01' AND '2024-06-30'
""")
display(result_df)

# Key advantage: Can change clustering columns later!
print("\n--- Key Advantage: ALTER CLUSTER BY ---")
print("ALTER TABLE lc_demo_orders CLUSTER BY (customer_id, order_date)")
print("→ Unlike PARTITION BY, you can change clustering columns anytime!")
print("→ New writes use new clustering; OPTIMIZE applies it to old data incrementally.")

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Example 3: OPTIMIZE WHERE
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Targeted OPTIMIZE with WHERE
# Real-world: Only optimize the partition/data range that changed.

from pyspark.sql.functions import col, rand, expr, round as spark_round  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== Targeted OPTIMIZE (WHERE clause) ===")  # Heading.
print("Don't optimize the WHOLE table — just the part that changed.\n")

# Create partitioned table.
tgt_path = "/tmp/delta_kt/targeted_optimize"  # Path.
data = spark.range(50000).select(
    col("id").alias("event_id"),
    expr("date_add('2024-01-01', cast(rand()*90 as int))").alias("event_date"),
    expr("CASE WHEN rand()<0.5 THEN 'web' ELSE 'mobile' END").alias("channel"),
    spark_round(rand() * 100, 2).alias("metric")
)
data.write.format("delta").mode("overwrite").partitionBy("channel").save(tgt_path)  # Partitioned.
print("Created partitioned table (by channel)")

# Simulate: only 'web' partition got new small files.
for i in range(15):  # Many small appends to web only.
    spark.range(5).select(
        (col("id") + 50000 + i*5).alias("event_id"),
        expr("date_add('2024-04-01', cast(rand()*7 as int))").alias("event_date"),
        lit("web").alias("channel"),
        spark_round(rand() * 100, 2).alias("metric")
    ).write.format("delta").mode("append").save(tgt_path)

from pyspark.sql.functions import lit  # Import.
print("Added 15 small files to 'web' partition only")

# Count files per partition.
web_files = [f for f in dbutils.fs.ls(f"{tgt_path}/channel=web") if f.name.endswith(".parquet")]
mobile_files = [f for f in dbutils.fs.ls(f"{tgt_path}/channel=mobile") if f.name.endswith(".parquet")]
print(f"  web partition: {len(web_files)} files")
print(f"  mobile partition: {len(mobile_files)} files")

# Targeted OPTIMIZE: only web partition.
print("\n--- OPTIMIZE only the 'web' partition ---")
result = spark.sql(f"OPTIMIZE delta.`{tgt_path}` WHERE channel = 'web'")  # Targeted!
display(result)

# Verify.
web_active = [f for f in spark.read.format("delta").load(tgt_path).filter("channel='web'").inputFiles()]
print(f"\nAfter targeted OPTIMIZE:")
print(f"  web files (active): optimized!")
print(f"  mobile: untouched (no wasted compute)")
print("\n→ In production, OPTIMIZE WHERE saves hours vs full-table OPTIMIZE")

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Example 1: Optimization Strategy
# SECTION 5 — ADVANCED EXAMPLE 1: Complete Optimization Strategy
# Real-world: Production table optimization playbook.

from pyspark.sql.functions import col, rand, expr, round as spark_round  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== Production Optimization Strategy ===")  # Heading.

# Create a production-like table.
prod_path = "/tmp/delta_kt/prod_optimization"  # Path.

# Simulate months of data accumulation.
for month in range(1, 7):  # 6 months.
    monthly_data = spark.range(10000).select(
        (col("id") + month * 10000).alias("txn_id"),
        (rand() * 500).cast("int").alias("account_id"),
        spark_round(rand() * 10000, 2).alias("amount"),
        expr(f"date_add('2024-{month:02d}-01', cast(rand()*28 as int))").alias("txn_date"),
        expr("CASE WHEN rand()<0.3 THEN 'debit' WHEN rand()<0.7 THEN 'credit' ELSE 'transfer' END").alias("txn_type"),
        expr("CASE WHEN rand()<0.25 THEN 'North' WHEN rand()<0.5 THEN 'South' WHEN rand()<0.75 THEN 'East' ELSE 'West' END").alias("region")
    )
    monthly_data.write.format("delta").mode("append").save(prod_path)  # Append monthly.

print("Table built: 60K transactions over 6 months")

# STRATEGY STEP 1: Assess current state.
print("\n--- Step 1: ASSESS current state ---")
dt = DeltaTable.forPath(spark, prod_path)  # Load.
detail = dt.detail().collect()[0]  # Detail.
print(f"  Num files: {detail['numFiles']}")
print(f"  Size: {detail['sizeInBytes'] / 1024 / 1024:.1f} MB")
print(f"  Avg file size: {detail['sizeInBytes'] / detail['numFiles'] / 1024:.1f} KB")
print(f"  Rows: {spark.read.format('delta').load(prod_path).count()}")

# STRATEGY STEP 2: OPTIMIZE with ZORDER on query patterns.
print("\n--- Step 2: OPTIMIZE ZORDER (based on query patterns) ---")
print("Query patterns: usually filter by region + txn_date")
result = spark.sql(f"OPTIMIZE delta.`{prod_path}` ZORDER BY (region, txn_date)")
metrics = result.collect()[0]
print(f"  Files compacted: {metrics['metrics']['numFilesRemoved']} → {metrics['metrics']['numFilesAdded']}")

# STRATEGY STEP 3: Set table properties for ongoing optimization.
print("\n--- Step 3: Set table properties ---")
spark.sql(f"""ALTER TABLE delta.`{prod_path}` SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.logRetentionDuration' = '30 days',
    'delta.deletedFileRetentionDuration' = '7 days'
)""")
print("Properties set:")
print("  autoOptimize.optimizeWrite = true (prevents future small files)")
print("  autoOptimize.autoCompact = true (auto-compacts after writes)")
print("  logRetentionDuration = 30 days (keep history for a month)")
print("  deletedFileRetentionDuration = 7 days (VACUUM after 7 days)")

# STRATEGY STEP 4: VACUUM.
print("\n--- Step 4: VACUUM (reclaim space) ---")
print("  VACUUM delta.`path` RETAIN 168 HOURS (7 days)")
print("  → In production, schedule VACUUM weekly.")
print("  → Never vacuum below your time-travel needs!")

# Final state.
print("\n--- Final Optimized State ---")
dt2 = DeltaTable.forPath(spark, prod_path)
detail2 = dt2.detail().collect()[0]
print(f"  Num files: {detail2['numFiles']} (from {detail['numFiles']})")
print(f"  Size: {detail2['sizeInBytes'] / 1024 / 1024:.1f} MB")

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Example 2: Comparing Approaches
# SECTION 5 — ADVANCED EXAMPLE 2: Partition vs ZORDER vs Liquid Clustering Comparison
# Real-world: Choose the right optimization strategy for your table.

from pyspark.sql.functions import col, rand, expr, round as spark_round  # Imports.
import time  # Timing.

print("=== Comparison: Partition vs ZORDER vs Liquid Clustering ===")  # Heading.

# Generate identical data for fair comparison.
def generate_data(n=100000):
    """Generate test data."""
    return spark.range(n).select(
        col("id").alias("order_id"),
        (rand() * 1000).cast("int").alias("customer_id"),
        spark_round(rand() * 500 + 10, 2).alias("amount"),
        expr("date_add('2024-01-01', cast(rand()*365 as int))").alias("order_date"),
        expr("CASE WHEN rand()<0.25 THEN 'North' WHEN rand()<0.5 THEN 'South' WHEN rand()<0.75 THEN 'East' ELSE 'West' END").alias("region")
    )

# Approach 1: Partitioning.
print("--- Approach 1: PARTITION BY ---")
part_path = "/tmp/delta_kt/cmp_partition"
generate_data().write.format("delta").mode("overwrite").partitionBy("region").save(part_path)
part_files = len(spark.read.format("delta").load(part_path).inputFiles())
print(f"  Files: {part_files}")
print("  Pros: Perfect pruning on partition column")
print("  Cons: Can't change later, bad if too many/few values")

# Approach 2: ZORDER (flat table + OPTIMIZE ZORDER).
print("\n--- Approach 2: OPTIMIZE ZORDER ---")
zorder_path = "/tmp/delta_kt/cmp_zorder"
generate_data().write.format("delta").mode("overwrite").save(zorder_path)
spark.sql(f"OPTIMIZE delta.`{zorder_path}` ZORDER BY (region, order_date)")
zo_files = len(spark.read.format("delta").load(zorder_path).inputFiles())
print(f"  Files: {zo_files}")
print("  Pros: Multi-column optimization, good for multi-filter queries")
print("  Cons: Must run manually, rewrites ALL data each time")

# Approach 3: Liquid Clustering.
print("\n--- Approach 3: Liquid Clustering ---")
spark.sql("DROP TABLE IF EXISTS cmp_liquid")
spark.sql("CREATE TABLE cmp_liquid (order_id BIGINT, customer_id INT, amount DOUBLE, order_date DATE, region STRING) USING DELTA CLUSTER BY (region, order_date)")
generate_data().write.format("delta").mode("append").insertInto("cmp_liquid")
spark.sql("OPTIMIZE cmp_liquid")
lc_files = len(spark.table("cmp_liquid").inputFiles())
print(f"  Files: {lc_files}")
print("  Pros: Automatic, incremental, changeable, replaces both partition+ZORDER")
print("  Cons: Requires DBR 13.3+")

# Comparison table.
print("\n" + "="*70)
print(f"{'Feature':<25} {'Partition':<15} {'ZORDER':<15} {'Liquid Cluster':<15}")
print("-"*70)
print(f"{'Change columns later':<25} {'No':<15} {'Yes':<15} {'Yes':<15}")
print(f"{'Automatic on write':<25} {'Yes':<15} {'No':<15} {'Yes*':<15}")
print(f"{'Multi-column optimize':<25} {'No':<15} {'Yes':<15} {'Yes':<15}")
print(f"{'Incremental':<25} {'N/A':<15} {'No (full)':<15} {'Yes':<15}")
print(f"{'Recommended for new':<25} {'Rarely':<15} {'Legacy':<15} {'Always':<15}")
print("="*70)
print("\n* With auto-optimize enabled. Best practice: use Liquid Clustering for all new tables.")

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Example 3: VACUUM Safety
# SECTION 5 — ADVANCED EXAMPLE 3: VACUUM Safety and Retention Policies
# Real-world: Set up proper retention and vacuum scheduling.

from pyspark.sql.functions import col, lit  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== VACUUM Safety and Best Practices ===")  # Heading.

# Setup demo table.
safety_path = "/tmp/delta_kt/vacuum_safety"  # Path.
spark.range(1000).withColumn("val", col("id") * 2) \
    .write.format("delta").mode("overwrite").save(safety_path)  # V0.
DeltaTable.forPath(spark, safety_path).update("id < 100", {"val": lit(0)})  # V1.
DeltaTable.forPath(spark, safety_path).delete("id > 900")  # V2.

print("=== Rule 1: VACUUM retention must be >= time travel needs ===")
print("""
Scenario: You need time travel for 7 days.
  ✓ VACUUM RETAIN 168 HOURS (7 days) — safe
  ✗ VACUUM RETAIN 24 HOURS — breaks time travel for days 2-7!
  ✗ VACUUM RETAIN 0 HOURS — breaks ALL time travel immediately!
""")

print("=== Rule 2: Safety check prevents accidental low retention ===")
try:
    spark.sql(f"VACUUM delta.`{safety_path}` RETAIN 1 HOURS")  # Too low!
except Exception as e:
    print(f"  ERROR (expected): {str(e)[:200]}")
    print("  → Spark blocks VACUUM < 168 hours by default (safety check)")

print("\n=== Rule 3: Set proper table properties ===")
print("""
Recommended production settings:

ALTER TABLE my_table SET TBLPROPERTIES (
    'delta.logRetentionDuration' = '30 days',       -- How long to keep history metadata
    'delta.deletedFileRetentionDuration' = '7 days' -- How long before VACUUM can delete files
);

Schedule VACUUM to run weekly:
  VACUUM my_table RETAIN 168 HOURS  -- matches the 7-day retention
""")

print("=== Rule 4: Never VACUUM concurrent readers' data ===")
print("""
If a long-running query is reading a file, and VACUUM deletes it mid-read:
  → The query FAILS with FileNotFoundException!

Solution: Set retention longer than your longest query runtime.
  If queries take up to 6 hours: VACUUM RETAIN 174 HOURS (168 + 6)
""")

print("=== Rule 5: VACUUM + time travel interaction ===")
dt = DeltaTable.forPath(spark, safety_path)  # Load.
print("Current history:")
display(dt.history().select("version", "timestamp", "operation"))
print("\nAfter VACUUM RETAIN 0 HOURS:")
print("  Version 0 → Files deleted, CANNOT read anymore")
print("  Version 2 (current) → Still works fine")
print("  History metadata → Still visible in DESCRIBE HISTORY")
print("\n→ VACUUM deletes DATA files, not LOG files.")
print("→ You can still SEE what happened, just can't READ the old data.")

# COMMAND ----------

# DBTITLE 1,Section 6 - Key Takeaways
# MAGIC %md
# MAGIC ## SECTION 6 — Key Takeaways
# MAGIC
# MAGIC ### Quick Reference
# MAGIC
# MAGIC | Command | Purpose | Frequency |
# MAGIC |---------|---------|----------|
# MAGIC | `OPTIMIZE table` | Compact small files | Weekly or after bulk loads |
# MAGIC | `OPTIMIZE table ZORDER BY (cols)` | Co-locate data for faster filters | Weekly |
# MAGIC | `VACUUM table RETAIN N HOURS` | Delete old unused files | Weekly |
# MAGIC | `CLUSTER BY (cols)` | Liquid clustering (create time) | Once at creation |
# MAGIC | `ALTER TABLE CLUSTER BY` | Change clustering columns | As needed |
# MAGIC
# MAGIC ### Decision Tree: Which Approach?
# MAGIC 1. **New table?** → Use Liquid Clustering (`CLUSTER BY`)
# MAGIC 2. **Existing partitioned table?** → Keep partitions + add ZORDER
# MAGIC 3. **Legacy table with small files?** → Run `OPTIMIZE` then set auto-optimize
# MAGIC 4. **Running out of storage?** → Run `VACUUM`
# MAGIC
# MAGIC ### Best Practices
# MAGIC * Always enable `optimizeWrite` and `autoCompact` on production tables
# MAGIC * Schedule `OPTIMIZE` and `VACUUM` as recurring jobs
# MAGIC * Never `VACUUM RETAIN 0 HOURS` in production
# MAGIC * Set retention based on compliance + longest query runtime
# MAGIC * Use Liquid Clustering for ALL new tables (DBR 13.3+)
# MAGIC * Monitor file counts with `DESCRIBE DETAIL`

# COMMAND ----------

# DBTITLE 1,Section 7 - Practice Exercises
# SECTION 7 — HOMEWORK & SOLUTIONS

from pyspark.sql.functions import col, rand, expr, lit  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("="*60)
print("HOMEWORK — Delta Optimization")
print("="*60)

# Level 1: Create a table with many small files, then OPTIMIZE.
print("\n=== Level 1: Basic OPTIMIZE ===")
l1_path = "/tmp/delta_kt/hw57_l1"
for i in range(10):
    spark.range(i*10, (i+1)*10).withColumn("v", rand()).write.format("delta").mode("append").save(l1_path)
print(f"Before: {len(spark.read.format('delta').load(l1_path).inputFiles())} files")
spark.sql(f"OPTIMIZE delta.`{l1_path}`")
print(f"After: {len(spark.read.format('delta').load(l1_path).inputFiles())} files")

# Level 2: OPTIMIZE with ZORDER.
print("\n=== Level 2: ZORDER ===")
l2_path = "/tmp/delta_kt/hw57_l2"
spark.range(10000).select(col("id"), (rand()*100).alias("score"), expr("CASE WHEN rand()<0.5 THEN 'A' ELSE 'B' END").alias("grp")).write.format("delta").mode("overwrite").save(l2_path)
spark.sql(f"OPTIMIZE delta.`{l2_path}` ZORDER BY (grp, score)")
print("ZORDER applied on (grp, score)")

# Level 3: Set auto-optimize properties.
print("\n=== Level 3: Table Properties ===")
spark.sql(f"""ALTER TABLE delta.`{l2_path}` SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)""")
print("Auto-optimize enabled")

# Level 4: VACUUM with DRY RUN.
print("\n=== Level 4: VACUUM DRY RUN ===")
DeltaTable.forPath(spark, l2_path).update("id < 100", {"score": lit(0.0)})
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
display(spark.sql(f"VACUUM delta.`{l2_path}` RETAIN 0 HOURS DRY RUN"))
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

# Level 5: Create Liquid Clustering table.
print("\n=== Level 5: Liquid Clustering ===")
spark.sql("DROP TABLE IF EXISTS hw57_liquid")
spark.sql("CREATE TABLE hw57_liquid (id BIGINT, category STRING, amount DOUBLE) USING DELTA CLUSTER BY (category)")
spark.range(5000).select(col("id"), expr("CASE WHEN rand()<0.33 THEN 'X' WHEN rand()<0.66 THEN 'Y' ELSE 'Z' END").alias("category"), (rand()*100).alias("amount")).write.format("delta").mode("append").insertInto("hw57_liquid")
spark.sql("OPTIMIZE hw57_liquid")
print(f"Liquid clustered table: {spark.table('hw57_liquid').count()} rows")

print("\n" + "="*60)
print("All exercises completed!")
print("="*60)