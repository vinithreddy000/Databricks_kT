# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 18: Writing DataFrames — Every Method
# MAGIC # Module: DataFrames — Creation & Basics
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 50 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: Saving Your Document
# MAGIC
# MAGIC Writing a DataFrame is like saving a document:
# MAGIC - **Write Mode** = What happens if the file already exists? (Overwrite? Append? Error?)
# MAGIC - **Format** = What file type? (PDF? Word? Text? → Parquet? CSV? Delta?)
# MAGIC - **Partitioning** = How to organize into folders? (By year? By region?)
# MAGIC - **Compression** = How much to shrink it? (ZIP? RAR? None?)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Write API
# MAGIC
# MAGIC ```python
# MAGIC df.write                       # Get the DataFrameWriter
# MAGIC   .mode("overwrite")           # What if destination exists?
# MAGIC   .format("parquet")           # Output format
# MAGIC   .option("compression", "snappy")  # Format-specific options
# MAGIC   .partitionBy("year", "month")    # Folder partitioning
# MAGIC   .bucketBy(8, "user_id")     # Hash bucketing (Hive tables only)
# MAGIC   .save("/path/to/output")    # Write!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Write Modes
# MAGIC
# MAGIC | Mode | Behavior | Use Case |
# MAGIC |------|----------|----------|
# MAGIC | `overwrite` | Delete existing, write new | Rebuild entire table |
# MAGIC | `append` | Add to existing data | Incremental loading |
# MAGIC | `ignore` | Do nothing if exists | Idempotent writes |
# MAGIC | `errorIfExists` | Throw error if exists | Safety (default!) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Format Decision Tree
# MAGIC
# MAGIC 1. **Need transactions/history?** → Delta (always first choice)
# MAGIC 2. **Sharing with non-Spark tools?** → Parquet
# MAGIC 3. **Human-readable output?** → CSV or JSON
# MAGIC 4. **Hive ecosystem?** → ORC
# MAGIC 5. **Kafka/streaming?** → Avro

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Write Process Internals
# MAGIC
# MAGIC ```
# MAGIC   df.write.mode("overwrite").partitionBy("region").parquet("/output/")
# MAGIC   
# MAGIC   Step 1: Plan output partitions
# MAGIC     Spark looks at df.rdd.getNumPartitions() and data distribution
# MAGIC   
# MAGIC   Step 2: Write files (parallel!)
# MAGIC     Each executor writes its partition of data to a separate file:
# MAGIC     /output/region=US/part-00000.snappy.parquet
# MAGIC     /output/region=US/part-00001.snappy.parquet
# MAGIC     /output/region=EU/part-00000.snappy.parquet
# MAGIC   
# MAGIC   Step 3: Commit
# MAGIC     Write _SUCCESS marker file
# MAGIC     (Delta writes to _delta_log/ instead)
# MAGIC ```
# MAGIC
# MAGIC ### File Count = Number of Spark Partitions
# MAGIC
# MAGIC ```
# MAGIC   df with 200 Spark partitions
# MAGIC   → .write.parquet("/out/")
# MAGIC   → 200 output files!  (probably too many!)
# MAGIC   
# MAGIC   Fix: .coalesce(4).write.parquet("/out/")
# MAGIC   → 4 output files (much better for downstream reads!)
# MAGIC   
# MAGIC   Rule of thumb:
# MAGIC     Target file size: 128MB - 256MB
# MAGIC     Tiny files (< 1MB) = terrible read performance
# MAGIC     Huge files (> 1GB) = bad parallelism
# MAGIC ```
# MAGIC
# MAGIC ### PartitionBy vs BucketBy
# MAGIC
# MAGIC ```
# MAGIC   partitionBy("region"):
# MAGIC   ────────────────────
# MAGIC   /output/
# MAGIC   ├─ region=US/part-00000.parquet   ← Physical folders!
# MAGIC   ├─ region=EU/part-00000.parquet   ← Filter skips folders
# MAGIC   └─ region=APAC/part-00000.parquet
# MAGIC   
# MAGIC   Best for: Low cardinality columns (< 10K distinct values)
# MAGIC   Danger: 1M distinct regions = 1M folders (disaster!)
# MAGIC   
# MAGIC   bucketBy(8, "user_id"):
# MAGIC   ────────────────────
# MAGIC   /output/
# MAGIC   ├─ part-00000.parquet   ← Contains users where hash(id) % 8 == 0
# MAGIC   ├─ part-00001.parquet   ← Contains users where hash(id) % 8 == 1
# MAGIC   └─ ...                  ← Pre-sorted for joins!
# MAGIC   
# MAGIC   Best for: High cardinality join keys (user_id, order_id)
# MAGIC   Benefit: Joins on bucketed tables skip shuffle!
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Write Modes
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: Write Modes
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== Write Modes: overwrite, append, ignore, errorIfExists ===")
print()

# Create sample data
df_batch1 = spark.createDataFrame([
    (1, "Alice", 95000.0), (2, "Bob", 72000.0)
], ["id", "name", "salary"])

df_batch2 = spark.createDataFrame([
    (3, "Charlie", 110000.0), (4, "Diana", 88000.0)
], ["id", "name", "salary"])

base_path = "/tmp/write_demo"

# --- Mode: overwrite (replace everything) ---
print("--- 1. mode('overwrite') — Replace existing data ---")
df_batch1.write.mode("overwrite").parquet(f"{base_path}/overwrite_demo")
print(f"  After batch 1: {spark.read.parquet(f'{base_path}/overwrite_demo').count()} rows")

df_batch2.write.mode("overwrite").parquet(f"{base_path}/overwrite_demo")
print(f"  After batch 2 (overwrite): {spark.read.parquet(f'{base_path}/overwrite_demo').count()} rows")
print("  Batch 1 data is GONE! Overwrite = full replacement.")

# --- Mode: append (add to existing) ---
print("\n--- 2. mode('append') — Add new data to existing ---")
df_batch1.write.mode("overwrite").parquet(f"{base_path}/append_demo")  # Start fresh
df_batch2.write.mode("append").parquet(f"{base_path}/append_demo")
print(f"  After append: {spark.read.parquet(f'{base_path}/append_demo').count()} rows")
print("  Both batch 1 AND batch 2 data present!")

# --- Mode: ignore (skip if exists) ---
print("\n--- 3. mode('ignore') — Skip write if path exists ---")
df_batch1.write.mode("overwrite").parquet(f"{base_path}/ignore_demo")
df_batch2.write.mode("ignore").parquet(f"{base_path}/ignore_demo")  # Silently skipped!
df_check = spark.read.parquet(f"{base_path}/ignore_demo")
print(f"  After ignore: {df_check.count()} rows (batch 2 was IGNORED)")
df_check.show()
print("  Useful for idempotent jobs that might restart")

# --- Mode: errorIfExists (default — fail if path exists) ---
print("\n--- 4. mode('errorIfExists') — Fail if path exists (DEFAULT) ---")
try:
    df_batch1.write.mode("overwrite").parquet(f"{base_path}/error_demo")  # Create first
    df_batch2.write.mode("errorIfExists").parquet(f"{base_path}/error_demo")  # Error!
except Exception as e:
    print(f"  ERROR: {str(e)[:80]}...")
    print("  This is the DEFAULT mode — always specify mode explicitly!")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Writing All Formats
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: Writing in Every Format
# ═══════════════════════════════════════════════════════

print("=== Writing DataFrames: Every Format ===")
print()

# Create a larger sample dataset
data = [(i, f"product_{i}", i * 9.99, ["electronics", "clothing", "food"][i % 3]) for i in range(1000)]
df = spark.createDataFrame(data, ["id", "name", "price", "category"])
base = "/tmp/write_demo/formats"

# --- CSV ---
print("--- 1. CSV ---")
df.write.mode("overwrite").option("header", "true").csv(f"{base}/csv")
print("  df.write.option('header','true').csv(path)")
print("  Human-readable, largest file size")

# --- JSON ---
print("\n--- 2. JSON ---")
df.write.mode("overwrite").json(f"{base}/json")
print("  df.write.json(path)")
print("  Human-readable, self-describing, good for APIs")

# --- Parquet (recommended for sharing) ---
print("\n--- 3. Parquet ---")
df.write.mode("overwrite").parquet(f"{base}/parquet")
print("  df.write.parquet(path)")
print("  Columnar, compressed, schema embedded, FAST")

# --- ORC ---
print("\n--- 4. ORC ---")
df.write.mode("overwrite").orc(f"{base}/orc")
print("  df.write.orc(path)")
print("  Columnar, for Hive ecosystem")

# --- Avro ---
print("\n--- 5. Avro ---")
df.write.mode("overwrite").format("avro").save(f"{base}/avro")
print("  df.write.format('avro').save(path)")
print("  Row-based, for streaming/Kafka")

# --- Delta (recommended for data lake!) ---
print("\n--- 6. Delta ---")
df.write.mode("overwrite").format("delta").save(f"{base}/delta")
print("  df.write.format('delta').save(path)")
print("  ACID transactions + time travel + schema enforcement")

# --- Compare file sizes ---
print("\n--- File Size Comparison ---")
print(f"  {'Format':<10} {'Files':<8} {'Total Size':<15} {'Recommendation'}")
print(f"  {'-'*60}")
for fmt, path in [("CSV", "csv"), ("JSON", "json"), ("Parquet", "parquet"),
                  ("ORC", "orc"), ("Avro", "avro"), ("Delta", "delta")]:
    try:
        files = dbutils.fs.ls(f"{base}/{path}")
        data_files = [f for f in files if not f.name.startswith("_")]
        total = sum(f.size for f in data_files)
        rec = "⭐ USE THIS" if fmt == "Delta" else ("Good" if fmt == "Parquet" else "")
        print(f"  {fmt:<10} {len(data_files):<8} {total:>10,} bytes  {rec}")
    except:
        pass

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: partitionBy
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: partitionBy
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, year, month, dayofmonth, date_add, lit
from datetime import date

print("=== partitionBy: Organizing Data into Folders ===")
print()

# Create time-series data (simulating daily events)
df_events = (
    spark.range(10000)
    .withColumn("event_date", date_add(lit("2024-01-01"), (col("id") % 90).cast("int")))
    .withColumn("user_id", (col("id") % 100).cast("int"))
    .withColumn("event_type", 
        col("id").cast("int") % 3)  # 0, 1, 2
    .withColumn("amount", col("id") * 0.5)
    .withColumn("year", year("event_date"))
    .withColumn("month", month("event_date"))
)

# --- Single column partition ---
print("--- 1. partitionBy single column ---")
path1 = "/tmp/write_demo/events_by_type"
df_events.write.mode("overwrite").partitionBy("event_type").parquet(path1)

print("  Folder structure:")
for item in dbutils.fs.ls(path1):
    if not item.name.startswith("_"):
        print(f"    {item.name}")
print("  Each event_type gets its own folder!")

# --- Multi-column partition (year/month) ---
print("\n--- 2. partitionBy multiple columns (year/month) ---")
path2 = "/tmp/write_demo/events_by_date"
df_events.write.mode("overwrite").partitionBy("year", "month").parquet(path2)

print("  Nested folder structure:")
for y_dir in dbutils.fs.ls(path2):
    if not y_dir.name.startswith("_"):
        print(f"    {y_dir.name}")
        for m_dir in dbutils.fs.ls(y_dir.path)[:3]:  # Show first 3
            if not m_dir.name.startswith("_"):
                print(f"      {m_dir.name}")

# --- Demonstrate partition pruning ---
print("\n--- 3. Partition pruning (reads only relevant folders) ---")
df_jan = spark.read.parquet(path2).filter((col("year") == 2024) & (col("month") == 1))
print(f"  January 2024 rows: {df_jan.count()}")
print("  Spark reads ONLY /year=2024/month=1/ folder (skips rest!)")

print("\n--- When to partitionBy ---")
print("  GOOD: date/year/month (filter queries use these)")
print("  GOOD: region, country (< 100 distinct values)")
print("  BAD:  user_id (millions of values = millions of folders!)")
print("  BAD:  timestamp (unique per row = chaos!)")
print("  Rule: < 10,000 distinct values in partition column")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Compression Options
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Compression Options
# ═══════════════════════════════════════════════════════

import time
from pyspark.sql.functions import col, concat, lit

print("=== Compression Options: snappy vs gzip vs zstd vs lz4 ===")
print()

# Create larger dataset for meaningful compression comparison
df_big = (
    spark.range(100000)
    .withColumn("name", concat(lit("user_"), col("id").cast("string")))
    .withColumn("email", concat(lit("user_"), col("id").cast("string"), lit("@company.com")))
    .withColumn("salary", col("id") * 1.5 + 50000)
    .withColumn("department", (col("id") % 10).cast("string"))
)
print(f"Dataset: {df_big.count():,} rows, {len(df_big.columns)} columns")

# --- Write with different compression codecs ---
base = "/tmp/write_demo/compression"
results = []

for codec in ["none", "snappy", "gzip", "zstd", "lz4"]:
    path = f"{base}/{codec}"
    
    # Time the write
    start = time.time()
    df_big.write.mode("overwrite").option("compression", codec).parquet(path)
    write_time = time.time() - start
    
    # Measure file size
    files = dbutils.fs.ls(path)
    total_size = sum(f.size for f in files if not f.name.startswith("_"))
    
    # Time the read
    start = time.time()
    spark.read.parquet(path).count()
    read_time = time.time() - start
    
    results.append((codec, total_size, write_time, read_time))

# --- Print comparison ---
print(f"\n{'Codec':<10} {'Size (KB)':<12} {'Write (s)':<12} {'Read (s)':<12} {'Notes'}")
print("-" * 70)
for codec, size, w_time, r_time in results:
    notes = ""
    if codec == "snappy":
        notes = "⭐ DEFAULT (best speed/ratio balance)"
    elif codec == "gzip":
        notes = "Best compression, slowest"
    elif codec == "zstd":
        notes = "⭐ RECOMMENDED (great ratio + fast)"
    elif codec == "lz4":
        notes = "Fastest, less compression"
    elif codec == "none":
        notes = "No compression (baseline)"
    print(f"{codec:<10} {size//1024:<12,} {w_time:<12.3f} {r_time:<12.3f} {notes}")

print("\n--- Recommendations ---")
print("  Interactive analytics: snappy (default) or lz4 (fastest reads)")
print("  Cold storage/archival: gzip or zstd (smallest files)")
print("  Best overall:          zstd (great compression + fast)")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: coalesce and repartition
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Controlling Output File Count
# ═══════════════════════════════════════════════════════

print("=== Controlling Output Files: coalesce vs repartition ===")
print()

# Create a DataFrame with many partitions
df = spark.range(100000).repartition(200)  # Simulate wide parallelism
print(f"Input partitions: {df.rdd.getNumPartitions()}")
print(f"Without control: 200 files written! (most are tiny)")

base = "/tmp/write_demo/file_count"

# --- coalesce(1): Single file output ---
print("\n--- 1. coalesce(1) — Single output file ---")
df.coalesce(1).write.mode("overwrite").parquet(f"{base}/single")
file_count = len([f for f in dbutils.fs.ls(f"{base}/single") if f.name.endswith(".parquet")])
print(f"  Output files: {file_count}")
print("  Use case: Exporting for download, small datasets")
print("  WARNING: Kills parallelism! Only for small data or final export.")

# --- coalesce(4): Reduce to 4 files ---
print("\n--- 2. coalesce(4) — Reduce to 4 files ---")
df.coalesce(4).write.mode("overwrite").parquet(f"{base}/four")
file_count = len([f for f in dbutils.fs.ls(f"{base}/four") if f.name.endswith(".parquet")])
print(f"  Output files: {file_count}")
print("  coalesce: Merges partitions WITHOUT shuffle (fast, but uneven sizes)")

# --- repartition(4): Exactly 4 equal-sized files ---
print("\n--- 3. repartition(4) — 4 equal-sized files ---")
df.repartition(4).write.mode("overwrite").parquet(f"{base}/repartitioned")
file_count = len([f for f in dbutils.fs.ls(f"{base}/repartitioned") if f.name.endswith(".parquet")])
print(f"  Output files: {file_count}")
print("  repartition: Full shuffle (slower, but even distribution)")

# --- maxRecordsPerFile ---
print("\n--- 4. maxRecordsPerFile (automatic splitting) ---")
df.write.mode("overwrite").option("maxRecordsPerFile", 25000).parquet(f"{base}/max_records")
file_count = len([f for f in dbutils.fs.ls(f"{base}/max_records") if f.name.endswith(".parquet")])
print(f"  Output files: {file_count} (100K rows / 25K per file = ~4 files per partition)")
print("  Guarantees no file exceeds N rows (but might create many files!)")

# --- Decision guide ---
print("\n--- When to use what ---")
print(f"  {'Method':<25} {'Use Case'}")
print(f"  {'-'*60}")
print(f"  {'coalesce(1)':<25} Export for download, very small data")
print(f"  {'coalesce(N)':<25} Reduce files without shuffle (fast)")
print(f"  {'repartition(N)':<25} Even-sized files (uses shuffle)")
print(f"  {'maxRecordsPerFile':<25} Cap file size by row count")
print(f"  {'Auto-optimize (Delta)':<25} Let Delta handle it automatically!")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Writing to Tables
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Writing to Managed Tables
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, current_date

print("=== Writing to Tables (saveAsTable vs insertInto) ===")
print()

# Create sample data
df = spark.createDataFrame([
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", "Marketing", 72000),
    (3, "Charlie", "Engineering", 110000),
], ["id", "name", "dept", "salary"])

# --- saveAsTable: Creates or replaces a table ---
print("--- 1. saveAsTable (creates table in metastore) ---")
df.write.mode("overwrite").saveAsTable("default.write_demo_employees")
print("  Created table: default.write_demo_employees")
spark.sql("SELECT * FROM default.write_demo_employees").show()

# --- insertInto: Append to existing table ---
print("--- 2. insertInto (append to existing table) ---")
new_data = spark.createDataFrame([
    (4, "Diana", "Sales", 88000),
], ["id", "name", "dept", "salary"])
new_data.write.mode("append").insertInto("default.write_demo_employees")
print(f"  After insertInto: {spark.table('default.write_demo_employees').count()} rows")

# --- CREATE TABLE AS SELECT (CTAS) via SQL ---
print("\n--- 3. CTAS (CREATE TABLE AS SELECT) ---")
spark.sql("""
    CREATE OR REPLACE TABLE default.write_demo_eng_team AS
    SELECT * FROM default.write_demo_employees
    WHERE dept = 'Engineering'
""")
spark.sql("SELECT * FROM default.write_demo_eng_team").show()
print("  CTAS is great for creating tables from query results")

# --- saveAsTable vs save ---
print("\n--- 4. saveAsTable vs save (key differences) ---")
print(f"  {'Method':<25} {'Creates Table?':<18} {'Metastore Entry?':<18} {'Use Case'}")
print(f"  {'-'*80}")
print(f"  {'df.write.save(path)':<25} {'No':<18} {'No':<18} {'Files only (external use)'}")
print(f"  {'df.write.saveAsTable()':<25} {'Yes':<18} {'Yes':<18} {'Registered, queryable'}")
print(f"  {'CTAS (SQL)':<25} {'Yes':<18} {'Yes':<18} {'From query result'}")

# Cleanup
spark.sql("DROP TABLE IF EXISTS default.write_demo_employees")
spark.sql("DROP TABLE IF EXISTS default.write_demo_eng_team")
print("\n  Cleaned up demo tables")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Dynamic Partition Overwrite
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Dynamic Partition Overwrite
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, lit

print("=== Dynamic Partition Overwrite ===")
print()
print("Problem: Regular overwrite DELETES ALL partitions, even untouched ones!")
print("Solution: Dynamic partition overwrite only replaces partitions in your data.")
print()

# Create initial partitioned data (3 regions)
initial = spark.createDataFrame([
    (1, "US", 100), (2, "US", 200),
    (3, "EU", 300), (4, "EU", 400),
    (5, "APAC", 500), (6, "APAC", 600),
], ["id", "region", "amount"])

path = "/tmp/write_demo/dynamic_overwrite"
initial.write.mode("overwrite").partitionBy("region").parquet(path)
print("Initial data: 3 regions (US, EU, APAC), 6 rows total")

# New data: only updating US region
us_update = spark.createDataFrame([
    (7, "US", 700), (8, "US", 800),  # Fresh US data
], ["id", "region", "amount"])

# --- Without dynamic overwrite (DANGEROUS!) ---
print("\n--- WITHOUT dynamic overwrite (mode=overwrite) ---")
us_update.write.mode("overwrite").partitionBy("region").parquet(f"{path}_static")
result = spark.read.parquet(f"{path}_static")
print(f"  After static overwrite: {result.count()} rows")
print(f"  Regions remaining: {[r.region for r in result.select('region').distinct().collect()]}")
print("  EU and APAC are GONE! Static overwrite deleted everything!")

# --- WITH dynamic partition overwrite (SAFE!) ---
print("\n--- WITH dynamic partition overwrite ---")
# Enable dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Start fresh
initial.write.mode("overwrite").partitionBy("region").parquet(f"{path}_dynamic")

# Now overwrite with only US data
us_update.write.mode("overwrite").partitionBy("region").parquet(f"{path}_dynamic")

result = spark.read.parquet(f"{path}_dynamic")
print(f"  After dynamic overwrite: {result.count()} rows")
print(f"  Regions remaining: {sorted([r.region for r in result.select('region').distinct().collect()])}")
result.orderBy("region", "id").show()
print("  EU and APAC untouched! Only US partition was replaced!")

# Reset to default
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

print("\n--- Key: Dynamic overwrite = surgical partition replacement ---")
print("  Set: spark.sql.sources.partitionOverwriteMode = dynamic")
print("  Or with Delta: replaceWhere option (even safer!)")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: bucketBy and sortBy
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: bucketBy and sortBy
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, avg

print("=== bucketBy: Pre-organize Data for Faster Joins ===")
print()

# Create two large DataFrames that will be joined
df_orders = (
    spark.range(100000)
    .withColumn("customer_id", (col("id") % 1000).cast("int"))
    .withColumn("amount", col("id") * 0.5)
    .withColumnRenamed("id", "order_id")
)

df_customers = (
    spark.range(1000)
    .withColumn("customer_id", col("id").cast("int"))
    .withColumn("name", concat(lit("customer_"), col("id").cast("string")))
    .drop("id")
)

# --- Write bucketed tables ---
print("--- 1. Writing bucketed tables ---")
# NOTE: bucketBy ONLY works with saveAsTable (not .save()!)
df_orders.write.mode("overwrite").bucketBy(8, "customer_id").sortBy("customer_id").saveAsTable("default.orders_bucketed")
df_customers.write.mode("overwrite").bucketBy(8, "customer_id").sortBy("customer_id").saveAsTable("default.customers_bucketed")
print("  Both tables bucketed by 'customer_id' into 8 buckets")
print("  Each bucket file contains all rows for specific customer_id hash values")

# --- Join bucketed tables (no shuffle!) ---
print("\n--- 2. Join with bucketing (shuffle-free!) ---")
# Disable broadcast join to see shuffle behavior
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

df_joined = spark.table("default.orders_bucketed").join(
    spark.table("default.customers_bucketed"),
    "customer_id"
)
print(f"  Joined rows: {df_joined.count():,}")
print("\n  Physical plan (look for 'Exchange' = shuffle):")
df_joined.explain()
print("  With bucketing: NO Exchange (shuffle) needed!")
print("  Without bucketing: would shuffle ALL 100K rows!")

# Reset
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

# --- Limitations ---
print("\n--- 3. bucketBy limitations ---")
print("  1. ONLY works with saveAsTable (not .save(path))")
print("  2. Both tables must use same number of buckets for shuffle-free join")
print("  3. Bucket column must match join key exactly")
print("  4. Delta Lake has its own optimization (Liquid Clustering) instead")

# Cleanup
spark.sql("DROP TABLE IF EXISTS default.orders_bucketed")
spark.sql("DROP TABLE IF EXISTS default.customers_bucketed")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production Write Framework
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Production Write Framework
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, current_timestamp, lit, count
from datetime import datetime

print("=== Production Write Framework ===")
print()

def write_delta_production(df, target_path, mode="overwrite", partition_cols=None,
                           optimize=True, vacuum_hours=168, z_order_cols=None,
                           target_file_size_mb=128):
    """
    Production-grade Delta writer with best practices built in.
    
    Args:
        df: Source DataFrame
        target_path: Delta output path
        mode: overwrite/append
        partition_cols: List of partition columns
        optimize: Run OPTIMIZE after write
        vacuum_hours: Hours of history to retain
        z_order_cols: Columns to Z-ORDER for query speed
        target_file_size_mb: Target file size (for repartitioning)
    """
    print(f"  Writing to: {target_path}")
    print(f"  Mode: {mode}, Rows: {df.count():,}")
    
    # Estimate optimal file count
    row_count = df.count()
    estimated_size_mb = row_count * len(df.columns) * 50 / (1024 * 1024)  # Rough estimate
    optimal_files = max(1, int(estimated_size_mb / target_file_size_mb))
    
    # Repartition for optimal file sizes
    if optimal_files < df.rdd.getNumPartitions():
        df = df.coalesce(optimal_files)
        print(f"  Coalesced to {optimal_files} files (target: {target_file_size_mb}MB each)")
    
    # Add write metadata
    df = df.withColumn("_written_at", current_timestamp())
    
    # Build writer
    writer = df.write.format("delta").mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    # Write!
    writer.save(target_path)
    print(f"  ✓ Write complete")
    
    # Post-write optimization
    if optimize:
        if z_order_cols:
            z_cols = ", ".join(z_order_cols)
            spark.sql(f"OPTIMIZE delta.`{target_path}` ZORDER BY ({z_cols})")
            print(f"  ✓ OPTIMIZE + Z-ORDER by ({z_cols})")
        else:
            spark.sql(f"OPTIMIZE delta.`{target_path}`")
            print(f"  ✓ OPTIMIZE complete")
    
    # Report
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{target_path}")
    num_files = detail.select("numFiles").collect()[0][0]
    size_bytes = detail.select("sizeInBytes").collect()[0][0]
    print(f"  ✓ Result: {num_files} files, {size_bytes/1024/1024:.1f} MB total")
    
    return {"files": num_files, "size_mb": size_bytes/1024/1024}

# --- Demo ---
df_demo = spark.range(50000).withColumn("region", (col("id") % 4).cast("string")).withColumn("val", col("id") * 1.5)

print("--- Demo: Production write ---")
result = write_delta_production(
    df_demo,
    "/tmp/write_demo/production_output",
    mode="overwrite",
    partition_cols=["region"],
    optimize=True
)
print(f"\n  Final result: {result}")
print("\n--- Key: Always write Delta in production with OPTIMIZE! ---")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Not Specifying Write Mode
# MAGIC **Problem:** Default mode is `errorIfExists` — job fails on second run.  
# MAGIC **Fix:** Always explicitly set `.mode("overwrite")` or `.mode("append")`.
# MAGIC
# MAGIC ### Mistake #2: Writing Too Many Small Files
# MAGIC **Problem:** 10,000 tiny files (< 1MB each) makes reads 100x slower.  
# MAGIC **Fix:** Use `.coalesce(N)` before write, or enable Delta auto-optimize.
# MAGIC
# MAGIC ### Mistake #3: PartitionBy on High Cardinality Columns
# MAGIC **Problem:** `partitionBy("user_id")` with 1M users = 1M folders (filesystem meltdown).  
# MAGIC **Fix:** Only partition by low cardinality (< 10K values): date, region, category.
# MAGIC
# MAGIC ### Mistake #4: Using overwrite When You Mean append
# MAGIC **Problem:** `mode("overwrite")` deletes ALL existing data before writing new batch.  
# MAGIC **Fix:** Use `mode("append")` for incremental loads. Use dynamic partition overwrite for targeted replacement.
# MAGIC
# MAGIC ### Mistake #5: Not Using Delta Format
# MAGIC **Problem:** Writing as Parquet means no transactions, no time travel, no MERGE.  
# MAGIC **Fix:** Always use `.format("delta")` unless sharing with external non-Spark tools.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC **Level 1:** Write a DataFrame as Parquet. Read it back and verify row count matches.
# MAGIC
# MAGIC **Level 2:** Write the same data in all 6 formats (CSV, JSON, Parquet, ORC, Avro, Delta). Compare file sizes.
# MAGIC
# MAGIC **Level 3:** Demonstrate all 4 write modes (overwrite, append, ignore, errorIfExists) with verification.
# MAGIC
# MAGIC **Level 4:** Write partitioned data by year/month. Verify folder structure with dbutils.fs.ls().
# MAGIC
# MAGIC **Level 5:** Use coalesce(1) to create a single output file. Then use repartition(4) for even distribution.
# MAGIC
# MAGIC **Level 6:** Write with different compression codecs (snappy, gzip, zstd). Benchmark read performance.
# MAGIC
# MAGIC **Level 7:** Implement dynamic partition overwrite: update one region without affecting others.
# MAGIC
# MAGIC **Level 8:** Create bucketed tables and demonstrate shuffle-free joins with explain().
# MAGIC
# MAGIC **Level 9:** Build a production writer: auto-calculate optimal file count, add audit columns, OPTIMIZE + VACUUM.
# MAGIC
# MAGIC **Level 10:** Design a full Bronze/Silver/Gold write strategy with different modes, partitioning, and optimization at each layer.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

import time
from pyspark.sql.functions import col, lit, current_timestamp, year, month, avg

# Level 1: Write + verify
print("=== Level 1: Write and Verify ===")
df1 = spark.range(5000).withColumn("val", col("id") * 2.0)
df1.write.mode("overwrite").parquet("/tmp/write_demo/hw/level1")
df_back = spark.read.parquet("/tmp/write_demo/hw/level1")
print(f"  Written: {df1.count()}, Read back: {df_back.count()}, Match: {df1.count() == df_back.count()}")

# Level 3: All 4 modes
print("\n=== Level 3: Write Modes ===")
df3 = spark.createDataFrame([(1,"A"),(2,"B")],["id","val"])
df3b = spark.createDataFrame([(3,"C")],["id","val"])
path3 = "/tmp/write_demo/hw/level3"

df3.write.mode("overwrite").parquet(path3)
print(f"  After overwrite: {spark.read.parquet(path3).count()} rows")

df3b.write.mode("append").parquet(path3)
print(f"  After append: {spark.read.parquet(path3).count()} rows")

df3b.write.mode("ignore").parquet(path3)
print(f"  After ignore: {spark.read.parquet(path3).count()} rows (unchanged)")

# Level 5: coalesce vs repartition
print("\n=== Level 5: File Count Control ===")
df5 = spark.range(50000).repartition(100)  # 100 input partitions

df5.coalesce(1).write.mode("overwrite").parquet("/tmp/write_demo/hw/single")
files_1 = len([f for f in dbutils.fs.ls("/tmp/write_demo/hw/single") if f.name.endswith(".parquet")])

df5.repartition(4).write.mode("overwrite").parquet("/tmp/write_demo/hw/four")
files_4 = len([f for f in dbutils.fs.ls("/tmp/write_demo/hw/four") if f.name.endswith(".parquet")])

print(f"  coalesce(1): {files_1} file")
print(f"  repartition(4): {files_4} files")

# Level 7: Dynamic partition overwrite
print("\n=== Level 7: Dynamic Partition Overwrite ===")
df7 = spark.createDataFrame([(1,"A",100),(2,"A",200),(3,"B",300),(4,"B",400)],["id","region","val"])
path7 = "/tmp/write_demo/hw/dynamic"
df7.write.mode("overwrite").partitionBy("region").parquet(path7)
print(f"  Initial: {spark.read.parquet(path7).count()} rows, regions: A, B")

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
update = spark.createDataFrame([(5,"A",500)],["id","region","val"])
update.write.mode("overwrite").partitionBy("region").parquet(path7)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

result = spark.read.parquet(path7)
print(f"  After dynamic overwrite of A: {result.count()} rows")
result.show()
print("  Region B untouched, Region A replaced!")

# Level 10: Bronze/Silver/Gold
print("\n=== Level 10: Lakehouse Strategy ===")
print("  BRONZE (raw landing):")
print("    mode=append, format=delta, partitionBy=ingestion_date")
print("    No OPTIMIZE (data arrives constantly)")
print("    Retain raw for reprocessing")
print("  SILVER (cleaned):")
print("    mode=overwrite (full rebuild) OR merge (incremental)")
print("    format=delta, partitionBy=business_date")
print("    OPTIMIZE + Z-ORDER by common filter columns")
print("  GOLD (aggregated):")
print("    mode=overwrite, format=delta")
print("    Small tables, no partitioning needed")
print("    OPTIMIZE after each rebuild")

print("\n\u2705 All homework complete!")