# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 76: Auto Loader (cloudFiles)
# MAGIC ## Module 12: Streaming
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Auto Loader** is Databricks' recommended way to **ingest new files as they arrive** in cloud storage (ADLS, S3, GCS). It automatically discovers new files, tracks which ones have been processed, and handles schema evolution — all using Structured Streaming under the hood.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine a mailroom worker:
# MAGIC - **Without Auto Loader**: Every morning, the worker opens EVERY mailbox (even empty ones), checks every letter against a massive ledger to see if it was already delivered, then delivers only new ones. Extremely slow with millions of mailboxes.
# MAGIC - **With Auto Loader**: The mailroom has sensors. When a new letter arrives, a notification fires. The worker goes directly to that mailbox, picks up only the new letter, and delivers it. No ledger needed, no scanning empty boxes.
# MAGIC
# MAGIC Auto Loader uses **file notification** (event-driven) or **directory listing** to find new files efficiently, even in directories with millions of files.
# MAGIC
# MAGIC ### Why Auto Loader over plain readStream?
# MAGIC | Feature | readStream (Delta) | Auto Loader (cloudFiles) |
# MAGIC |---------|-------------------|-------------------------|
# MAGIC | Handles raw files (CSV, JSON, Parquet) | No | Yes |
# MAGIC | Schema inference & evolution | Manual | Automatic |
# MAGIC | Millions of files in one directory | Slow (full listing) | Fast (notifications) |
# MAGIC | Tracks processed files | Via Delta log | Built-in checkpoint |
# MAGIC | Rescue column for bad data | No | Yes |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Auto Loader Architecture:
# MAGIC
# MAGIC   Cloud Storage (ADLS/S3/GCS)
# MAGIC   ┌───────────────────────────┐
# MAGIC   │ /landing/data/                  │
# MAGIC   │   file_001.csv  (old, done)     │
# MAGIC   │   file_002.csv  (old, done)     │
# MAGIC   │   file_003.csv  (NEW!)       ────────┐
# MAGIC   │   file_004.csv  (NEW!)       ────────┤
# MAGIC   └───────────────────────────┘         │
# MAGIC                                            │
# MAGIC   Auto Loader discovers new files:         │
# MAGIC     Mode A: Directory Listing              │
# MAGIC       (Scans directory, compares to        │
# MAGIC        checkpoint to find new files)       │
# MAGIC     Mode B: File Notification (default)    │
# MAGIC       (Azure Event Grid / AWS SNS+SQS     │
# MAGIC        sends event when file arrives)      │
# MAGIC                                            ▼
# MAGIC   ┌─────────────────────────────────────┐
# MAGIC   │  Auto Loader (cloudFiles format)     │
# MAGIC   │  1. Discover new files                │
# MAGIC   │  2. Infer/evolve schema               │
# MAGIC   │  3. Parse data (with rescue column)   │
# MAGIC   │  4. Write to Delta (bronze table)     │
# MAGIC   │  5. Update checkpoint (track progress) │
# MAGIC   └─────────────────────────────────────┘
# MAGIC
# MAGIC Code Pattern:
# MAGIC   spark.readStream
# MAGIC     .format("cloudFiles")                    # Auto Loader format.
# MAGIC     .option("cloudFiles.format", "csv")      # Underlying file format.
# MAGIC     .option("cloudFiles.schemaLocation", "/schema")  # Schema tracking.
# MAGIC     .schema(my_schema)                       # Or let it infer.
# MAGIC     .load("/landing/data/")                  # Source directory.
# MAGIC
# MAGIC Key Options:
# MAGIC   cloudFiles.format:          csv, json, parquet, avro, text, binaryFile
# MAGIC   cloudFiles.schemaLocation:  Where to store inferred schema
# MAGIC   cloudFiles.inferColumnTypes: true = infer types (default: all strings)
# MAGIC   cloudFiles.schemaEvolutionMode: addNewColumns, rescue, failOnNewColumns, none
# MAGIC   cloudFiles.useNotifications: true = event-driven (fastest for huge dirs)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3-5: Examples
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-5 — EXAMPLES (Beginner to Advanced)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, current_timestamp, input_file_name  # Imports.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType  # Types.

print("="*70)
print("SECTIONS 3-5: Auto Loader in Practice")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# Setup: Create sample CSV files to simulate landing zone.
# ─────────────────────────────────────────────────────────────────
print("\n--- Setup: Creating sample landing zone ---")

landing_path = "/tmp/delta_kt/autoloader_landing"

# Clean up any previous runs.
dbutils.fs.rm(landing_path, recurse=True)
dbutils.fs.mkdirs(landing_path)

# Write some CSV files (simulating data arriving from external system).
from pyspark.sql.functions import rand, lit
for i in range(3):
    spark.range(i*100, (i+1)*100).select(
        col("id").alias("order_id"),
        (rand() * 500 + 10).alias("amount"),
        lit(f"region_{i%3}").alias("region")
    ).write.format("csv").mode("overwrite").option("header", "true") \
     .save(f"{landing_path}/batch_{i}")

print(f"Created 3 batches of CSV files in {landing_path}")
print(f"Files: {[f.name for f in dbutils.fs.ls(landing_path)]}")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Basic Auto Loader with schema provided
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Auto Loader with explicit schema")
print("-"*60)

# Define expected schema.
my_schema = StructType([
    StructField("order_id", IntegerType(), True),   # Order ID.
    StructField("amount", DoubleType(), True),       # Amount.
    StructField("region", StringType(), True)        # Region.
])

# Paths.
output_path = "/tmp/delta_kt/autoloader_output"
checkpoint_path = "/tmp/delta_kt/autoloader_checkpoint"
schema_path = "/tmp/delta_kt/autoloader_schema"

# Auto Loader stream.
query = (
    spark.readStream
    .format("cloudFiles")                               # Auto Loader format.
    .option("cloudFiles.format", "csv")                 # Source files are CSV.
    .option("cloudFiles.schemaLocation", schema_path)   # Store inferred schema.
    .option("header", "true")                           # CSV has header row.
    .schema(my_schema)                                  # Provide schema explicitly.
    .load(landing_path)                                 # Landing directory.
    .withColumn("_ingestion_time", current_timestamp()) # Add ingestion timestamp.
    .withColumn("_source_file", input_file_name())      # Track source file.
    .writeStream
    .format("delta")                                    # Write to Delta.
    .outputMode("append")                               # Append new rows.
    .option("checkpointLocation", checkpoint_path)      # Checkpoint for exactly-once.
    .trigger(availableNow=True)                         # Process all, then stop.
    .start(output_path)                                 # Output path.
)

query.awaitTermination()  # Wait for completion.

# Check results.
result = spark.read.format("delta").load(output_path)
print(f"\nRows ingested: {result.count()}")
print("Sample:")
display(result.limit(5))
print("\n✓ Auto Loader ingested all CSV files and wrote to Delta!")
print("  It tracks which files are processed in the checkpoint.")
print("  New files in landing_path will be picked up on next run.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Auto Loader with schema inference
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Auto Loader with schema inference")
print("-"*60)

print("""
When you DON'T provide a schema, Auto Loader can INFER it:

  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")  # Infer types!
    .option("cloudFiles.schemaLocation", "/schema/location")
    .option("header", "true")
    .load("/landing/")

Schema inference behavior:
  - First run: Infers schema from a sample of files.
  - Stores inferred schema in schemaLocation.
  - Subsequent runs: Uses stored schema (fast!).
  - If new columns appear: depends on schemaEvolutionMode.

Schema evolution modes:
  addNewColumns: Automatically adds new columns to schema.
  rescue:        New/mismatched data goes to _rescued_data column.
  failOnNewColumns: Fails the stream (safest for production).
  none:          Ignores new columns silently.

The Rescue Column (_rescued_data):
  Any data that doesn't match the schema goes here as JSON.
  Prevents data loss from schema mismatches!
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Production Auto Loader pattern (Azure ADLS)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Production Auto Loader template (Azure)")
print("-"*60)

print("""
Production template for Azure ADLS Gen2:

  # Source: ADLS landing zone.
  source_path = "abfss://landing@storageaccount.dfs.core.windows.net/data/"

  # Auto Loader stream.
  bronze_df = (
      spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")           # or json, parquet.
      .option("cloudFiles.schemaLocation", "/mnt/schema/my_table")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .option("header", "true")
      .option("cloudFiles.useNotifications", "true")  # Event-driven!
      .load(source_path)
      .withColumn("_ingested_at", current_timestamp())
      .withColumn("_source_file", input_file_name())
  )

  # Write to Bronze Delta table.
  query = (
      bronze_df.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", "/mnt/checkpoints/my_table")
      .option("mergeSchema", "true")  # Handle schema evolution.
      .trigger(availableNow=True)     # For scheduled jobs.
      .toTable("catalog.bronze.my_table")  # Unity Catalog table.
  )

  query.awaitTermination()

Best practices:
  1. Use cloudFiles.useNotifications=true for dirs with 10K+ files.
  2. Always add _ingested_at and _source_file for lineage.
  3. Use schemaEvolutionMode=addNewColumns for flexibility.
  4. Set mergeSchema=true on the writer for schema evolution.
  5. Use availableNow trigger in scheduled Lakeflow Jobs.
""")
print("✓ This pattern handles millions of files efficiently on Azure.")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Not setting schemaLocation
# MAGIC ```python
# MAGIC # BAD: Without schemaLocation, schema is re-inferred every restart (slow + inconsistent).
# MAGIC spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").load("/data")
# MAGIC
# MAGIC # GOOD: Always provide schemaLocation.
# MAGIC spark.readStream.format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "csv")
# MAGIC     .option("cloudFiles.schemaLocation", "/schema/my_table")
# MAGIC     .load("/data")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Using directory listing mode for huge directories
# MAGIC ```python
# MAGIC # BAD: Directory listing scans ALL files every batch (slow for 1M+ files).
# MAGIC # (This is the default on some platforms.)
# MAGIC
# MAGIC # GOOD: Use file notifications for large directories.
# MAGIC .option("cloudFiles.useNotifications", "true")  # Event-driven, O(1) per new file.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Ignoring the _rescued_data column
# MAGIC ```python
# MAGIC # If data doesn't match schema, it goes to _rescued_data (not lost!).
# MAGIC # BAD: Never checking _rescued_data = silent data quality issues.
# MAGIC
# MAGIC # GOOD: Monitor _rescued_data for problems.
# MAGIC df.filter(col("_rescued_data").isNotNull()).count()  # Should be 0 in healthy pipeline.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Using readStream with raw file format instead of cloudFiles
# MAGIC ```python
# MAGIC # BAD: Plain readStream.format("csv") doesn't track files efficiently.
# MAGIC spark.readStream.format("csv").load("/landing")  # No file tracking!
# MAGIC
# MAGIC # GOOD: Use cloudFiles for all file-based ingestion.
# MAGIC spark.readStream.format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "csv").load("/landing")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Forgetting header=true for CSV files
# MAGIC ```python
# MAGIC # BAD: First row treated as data, columns named _c0, _c1, ...
# MAGIC .option("cloudFiles.format", "csv").load("/data")  # No header option!
# MAGIC
# MAGIC # GOOD: Specify header.
# MAGIC .option("cloudFiles.format", "csv").option("header", "true").load("/data")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, current_timestamp, input_file_name  # Imports.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

print("="*70)
print("HOMEWORK — Auto Loader")
print("="*70)

# Level 1: Verify Auto Loader format name.
print("\n--- Level 1: Format name ---")
print("Auto Loader format = 'cloudFiles'")
print("It's NOT 'autoloader' or 'auto_loader'.")
# WHY: The format identifier is 'cloudFiles' in all readStream calls.

# Level 2: Create an Auto Loader stream (reuse landing zone).
print("\n--- Level 2: Basic Auto Loader ---")
landing = "/tmp/delta_kt/autoloader_landing"
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("region", StringType())
])
stream_df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/tmp/delta_kt/hw76_schema") \
    .option("header", "true") \
    .schema(schema).load(landing)
print(f"Is streaming: {stream_df.isStreaming}")
print(f"Schema: {stream_df.schema.simpleString()}")
# WHY: cloudFiles format creates a streaming reader that tracks new files.

# Level 3: Add metadata columns.
print("\n--- Level 3: Add ingestion metadata ---")
enriched = stream_df \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", input_file_name())
print(f"Columns: {enriched.columns}")
print("✓ Added _ingested_at and _source_file for lineage tracking.")
# WHY: Metadata columns help debug data issues (which file, when ingested).

# Level 4: Write with checkpoint.
print("\n--- Level 4: Write to Delta with checkpoint ---")
q = enriched.writeStream.format("delta").outputMode("append") \
    .option("checkpointLocation", "/tmp/delta_kt/hw76_l4_cp") \
    .trigger(availableNow=True).start("/tmp/delta_kt/hw76_l4_out")
q.awaitTermination()
rows = spark.read.format("delta").load("/tmp/delta_kt/hw76_l4_out").count()
print(f"Output rows: {rows}")
# WHY: Checkpoint enables exactly-once processing and resume after failure.

# Level 5-10: Conceptual.
print("\n--- Level 5: Schema evolution modes ---")
print("addNewColumns: auto-add new columns. rescue: send to _rescued_data.")
print("failOnNewColumns: fail stream. none: ignore silently.")

print("\n--- Level 6: Notification vs Directory listing ---")
print("Notifications: event-driven (fast for 1M+ files).")
print("Directory listing: scans all files (OK for <10K files).")

print("\n--- Level 7: cloudFiles vs plain readStream ---")
print("cloudFiles: tracks files, handles schema, efficient discovery.")
print("Plain readStream: no file tracking, no schema evolution.")

print("\n--- Level 8: Rescue column ---")
print("_rescued_data stores data that doesn't match schema (as JSON).")
print("Monitor it: df.filter(col('_rescued_data').isNotNull()).count()")

print("\n--- Level 9: Best practices ---")
print("1. Always set schemaLocation. 2. Use notifications for big dirs.")
print("3. Add _ingested_at. 4. Monitor _rescued_data. 5. Use availableNow for ETL.")

print("\n--- Level 10: Teach Auto Loader ---")
print("""
"Auto Loader (cloudFiles) = best way to ingest new files in Databricks.
  Automatically discovers new files, tracks what's processed,
  handles schema evolution, and scales to millions of files.
  Pattern: readStream.format('cloudFiles').option('cloudFiles.format','csv')
           .load('/landing/').writeStream.format('delta').start()
  Always provide: schemaLocation, checkpointLocation, header."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 76")
print("="*70)