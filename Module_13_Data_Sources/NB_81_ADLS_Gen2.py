# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 81: Azure Data Lake Storage Gen2 (ADLS)
# MAGIC ## Module 13: Data Sources & Connectors
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **ADLS Gen2** (Azure Data Lake Storage Gen2) is Azure's scalable storage for big data analytics. It's the most common storage backend for Databricks on Azure — your Delta tables, raw files, and landing zones all live here. Understanding how to read/write directly to ADLS is essential for building ETL pipelines.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC ADLS Gen2 is like a **massive warehouse with infinite shelves**:
# MAGIC - **Storage Account** = The warehouse building (e.g., `adl2hubprod`)
# MAGIC - **Container** = A section/floor of the warehouse (e.g., `landing`, `curated`, `raw`)
# MAGIC - **Directory** = An aisle on that floor (e.g., `/ptloadgenerator/PT_Load/`)
# MAGIC - **File** = An individual item on the shelf (e.g., `data_001.csv`)
# MAGIC
# MAGIC You access it via URLs like: `abfss://container@account.dfs.core.windows.net/path/to/file`
# MAGIC
# MAGIC ### Access Methods (in order of preference for Databricks):
# MAGIC | Method | Security | Use Case |
# MAGIC |--------|----------|----------|
# MAGIC | Unity Catalog External Location | Best | Production tables, governed access |
# MAGIC | Service Principal + Secret Scope | Good | Direct file access with credentials |
# MAGIC | Managed Identity (System-assigned) | Good | Cluster-level access, no secrets |
# MAGIC | Storage Account Key | Basic | Dev/testing only (not for production) |
# MAGIC | SAS Token | Limited | Temporary, scoped access |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC ADLS Gen2 URL Anatomy:
# MAGIC
# MAGIC   abfss://landing@adl2hubprod.dfs.core.windows.net/ptloadgenerator/PT_Load/csv_generic
# MAGIC   │       │       │                              │
# MAGIC   │       │       └─ Storage Account              └─ Path (directory/file)
# MAGIC   │       └─ Container name
# MAGIC   └─ Protocol (abfss = secure Azure Blob File System)
# MAGIC
# MAGIC Protocols:
# MAGIC   abfss://  = Secure (TLS). ALWAYS use this in production.
# MAGIC   abfs://   = Non-secure. Avoid.
# MAGIC   wasbs://  = Legacy Blob storage protocol. Works but use abfss.
# MAGIC
# MAGIC Authentication Flow:
# MAGIC
# MAGIC   [Databricks Cluster]  ─── credentials ───▶  [ADLS Gen2]
# MAGIC         │                                          │
# MAGIC   Credentials come from:                     Storage validates:
# MAGIC     1. Unity Catalog (external location)       - SPN has RBAC role
# MAGIC     2. Cluster Spark config (SPN/key)          - Or key matches
# MAGIC     3. Session config (spark.conf.set)         - Or SAS is valid
# MAGIC     4. Databricks Secret Scope                 Returns data ◀─────
# MAGIC
# MAGIC Reading/Writing Patterns:
# MAGIC
# MAGIC   # Direct file read.
# MAGIC   df = spark.read.format("csv").load("abfss://container@account.dfs.core.windows.net/path/")
# MAGIC
# MAGIC   # Direct file write.
# MAGIC   df.write.format("delta").save("abfss://container@account.dfs.core.windows.net/output/")
# MAGIC
# MAGIC   # Auto Loader (streaming ingestion from ADLS).
# MAGIC   spark.readStream.format("cloudFiles")
# MAGIC       .option("cloudFiles.format", "csv")
# MAGIC       .load("abfss://landing@account.dfs.core.windows.net/data/")
# MAGIC
# MAGIC   # Unity Catalog (recommended: no URL needed!).
# MAGIC   df = spark.table("catalog.schema.table")  # UC manages the path.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3-5: Examples
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-5 — EXAMPLES (Beginner to Advanced)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, current_timestamp, input_file_name  # Imports.

print("="*70)
print("SECTIONS 3-5: ADLS Gen2 Access Patterns")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Authentication with Service Principal (most common)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Service Principal authentication (OAuth2)")
print("-"*60)

print("""
Setting up Service Principal access in Databricks:

  # Step 1: Get credentials from Databricks Secret Scope.
  client_id     = dbutils.secrets.get(scope="my-scope", key="sp-client-id")
  client_secret = dbutils.secrets.get(scope="my-scope", key="sp-client-secret")
  tenant_id     = dbutils.secrets.get(scope="my-scope", key="sp-tenant-id")

  # Step 2: Configure Spark session for ADLS access.
  storage_account = "adl2hubprod"  # Your storage account name.
  
  spark.conf.set(
      f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
      "OAuth"
  )
  spark.conf.set(
      f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
      "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
  )
  spark.conf.set(
      f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
      client_id
  )
  spark.conf.set(
      f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
      client_secret
  )
  spark.conf.set(
      f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
      f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
  )

  # Step 3: Now read/write directly.
  df = spark.read.format("csv") \\
      .option("header", "true") \\
      .load(f"abfss://landing@{storage_account}.dfs.core.windows.net/data/")

Note: In production, these configs go in the CLUSTER Spark config tab
(so every notebook on the cluster has access without repeating this).
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Reading different file formats from ADLS
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Reading various file formats from ADLS")
print("-"*60)

print("""
# CSV (semicolon-delimited, common in European systems).
df_csv = spark.read.format("csv") \\
    .option("header", "true") \\
    .option("delimiter", ";") \\
    .option("inferSchema", "true") \\
    .load("abfss://landing@account.dfs.core.windows.net/data/*.csv")

# JSON (multiline JSON files).
df_json = spark.read.format("json") \\
    .option("multiLine", "true") \\
    .load("abfss://landing@account.dfs.core.windows.net/events/")

# Parquet (binary columnar format).
df_parquet = spark.read.format("parquet") \\
    .load("abfss://curated@account.dfs.core.windows.net/processed/")

# Delta (versioned, ACID table).
df_delta = spark.read.format("delta") \\
    .load("abfss://curated@account.dfs.core.windows.net/delta_table/")

# Binary files (images, PDFs).
df_binary = spark.read.format("binaryFile") \\
    .load("abfss://raw@account.dfs.core.windows.net/images/")

# Text files (line by line).
df_text = spark.read.format("text") \\
    .load("abfss://raw@account.dfs.core.windows.net/logs/*.log")
""")

# Demonstrate with local paths (same API, just different path).
print("\nDemo: Reading CSV with local paths (same API as ADLS):")
demo_df = spark.range(100).selectExpr("id", "id * 2 as doubled", "id % 5 as category")
demo_df.write.format("csv").mode("overwrite").option("header", "true") \
    .save("/tmp/delta_kt/adls_demo_csv")

# Read back.
read_back = spark.read.format("csv").option("header", "true") \
    .option("inferSchema", "true").load("/tmp/delta_kt/adls_demo_csv")
print(f"Rows read: {read_back.count()}, Columns: {read_back.columns}")
display(read_back.limit(3))

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Listing and managing files with dbutils.fs
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: File operations with dbutils.fs")
print("-"*60)

# List files in a directory.
print("\nListing /tmp/delta_kt/adls_demo_csv/:")
files = dbutils.fs.ls("/tmp/delta_kt/adls_demo_csv/")
for f in files[:5]:  # Show first 5.
    print(f"  {f.name:30s}  size={f.size:>8} bytes")

print("""
\ndbutils.fs commands for ADLS:
  dbutils.fs.ls(path)           → List directory contents.
  dbutils.fs.head(path, n)      → Show first n bytes of file.
  dbutils.fs.cp(src, dst)       → Copy file/directory.
  dbutils.fs.mv(src, dst)       → Move file/directory.
  dbutils.fs.rm(path, True)     → Delete recursively.
  dbutils.fs.mkdirs(path)       → Create directory.
  dbutils.fs.put(path, content) → Write small text file.

All work with ADLS paths:
  dbutils.fs.ls("abfss://container@account.dfs.core.windows.net/path/")
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Unity Catalog Volumes (modern approach)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Unity Catalog Volumes (recommended for new projects)")
print("-"*60)

print("""
Unity Catalog Volumes provide governed access to files:

  # Read from a UC Volume (no abfss URL needed!).
  df = spark.read.format("csv") \\
      .option("header", "true") \\
      .load("/Volumes/catalog/schema/volume_name/data.csv")

  # Write to a UC Volume.
  df.write.format("parquet") \\
      .save("/Volumes/catalog/schema/volume_name/output/")

  # Auto Loader from a UC Volume.
  spark.readStream.format("cloudFiles") \\
      .option("cloudFiles.format", "csv") \\
      .load("/Volumes/catalog/schema/landing_vol/")

Benefits over raw abfss:// paths:
  1. No credentials needed (UC manages access).
  2. Governed by UC permissions (GRANT READ on volume).
  3. Portable (no storage account names in code).
  4. Audited (all access logged).

Volume types:
  Managed: UC controls the storage location.
  External: Points to existing ADLS path (you manage storage).
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: Writing to ADLS with partitioning
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: Writing to ADLS with partitioning")
print("-"*60)

from pyspark.sql.functions import rand, expr  # Extra imports.

# Create sample data.
sales = spark.range(10000).select(
    col("id").alias("order_id"),
    expr("CASE WHEN id%3=0 THEN '2025' WHEN id%3=1 THEN '2024' ELSE '2023' END").alias("year"),
    expr("CASE WHEN id%4=0 THEN 'Q1' WHEN id%4=1 THEN 'Q2' WHEN id%4=2 THEN 'Q3' ELSE 'Q4' END").alias("quarter"),
    (rand() * 1000).alias("amount")
)

# Write partitioned by year and quarter.
output_path = "/tmp/delta_kt/adls_partitioned_write"
sales.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "quarter") \
    .save(output_path)

# Check the directory structure.
print("\nPartitioned directory structure:")
for f in dbutils.fs.ls(output_path):
    if not f.name.startswith("_"):
        print(f"  {f.name}")

# Reading with partition pruning.
print("\nPartition pruning (only reads year=2025):")
df_2025 = spark.read.format("delta").load(output_path).filter("year = '2025'")
print(f"Rows for 2025: {df_2025.count()}")
df_2025.explain()  # Check for PartitionFilters in the plan.
print("\n✓ PartitionFilters in plan = Spark only reads year=2025 partition!")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Hardcoding storage account keys in notebooks
# MAGIC ```python
# MAGIC # BAD: Key exposed in notebook code (visible to everyone with access).
# MAGIC spark.conf.set("fs.azure.account.key.myaccount.dfs.core.windows.net", "abc123secret")
# MAGIC
# MAGIC # GOOD: Use Databricks Secret Scope.
# MAGIC key = dbutils.secrets.get(scope="my-scope", key="storage-account-key")
# MAGIC spark.conf.set("fs.azure.account.key.myaccount.dfs.core.windows.net", key)
# MAGIC
# MAGIC # BEST: Use Unity Catalog (no credentials in code at all).
# MAGIC df = spark.read.format("csv").load("/Volumes/catalog/schema/vol/data.csv")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Using abfs:// instead of abfss://
# MAGIC ```python
# MAGIC # BAD: Non-secure protocol.
# MAGIC df = spark.read.load("abfs://container@account.dfs.core.windows.net/...")
# MAGIC
# MAGIC # GOOD: Always use abfss (TLS encrypted).
# MAGIC df = spark.read.load("abfss://container@account.dfs.core.windows.net/...")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Listing millions of files with dbutils.fs.ls
# MAGIC ```python
# MAGIC # BAD: ls on a directory with 1M files = timeout/OOM.
# MAGIC all_files = dbutils.fs.ls("abfss://landing@acct.../huge_dir/")  # Hangs!
# MAGIC
# MAGIC # GOOD: Use Auto Loader for large directories (event-driven).
# MAGIC spark.readStream.format("cloudFiles")...  # Handles millions efficiently.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Writing too many small files with frequent appends
# MAGIC ```python
# MAGIC # BAD: Appending 1 row at a time = 1 file per append.
# MAGIC for record in records:
# MAGIC     spark.createDataFrame([record]).write.mode("append").save(path)  # Tiny files!
# MAGIC
# MAGIC # GOOD: Batch your writes. Enable Auto Optimize.
# MAGIC df_batch.coalesce(4).write.format("delta").mode("append").save(path)
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not using partition pruning for large datasets
# MAGIC ```python
# MAGIC # BAD: Reading all partitions when you only need one year.
# MAGIC df = spark.read.load(path)  # Reads all years!
# MAGIC df.filter("year = 2025")    # Filter AFTER reading everything.
# MAGIC
# MAGIC # GOOD: Store data partitioned + filter early.
# MAGIC # Delta stats + partitionBy = only relevant files are read.
# MAGIC df = spark.read.format("delta").load(path).filter("year = '2025'")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, expr  # Imports.

print("="*70)
print("HOMEWORK — ADLS Gen2")
print("="*70)

# Level 1: Write and read CSV.
print("\n--- Level 1: Write/Read CSV ---")
df1 = spark.range(50).select(col("id"), (rand()*100).alias("metric"))
df1.write.format("csv").mode("overwrite").option("header", "true") \
    .save("/tmp/delta_kt/hw81_csv")
read1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
    .load("/tmp/delta_kt/hw81_csv")
print(f"Written & read: {read1.count()} rows, schema: {read1.schema.simpleString()}")
# WHY: Same read/write API works for ADLS (just change path to abfss://).

# Level 2: Write Delta.
print("\n--- Level 2: Write/Read Delta ---")
df1.write.format("delta").mode("overwrite").save("/tmp/delta_kt/hw81_delta")
read2 = spark.read.format("delta").load("/tmp/delta_kt/hw81_delta")
print(f"Delta: {read2.count()} rows")
# WHY: Delta is the preferred format for ADLS (ACID, versioning, stats).

# Level 3: List files.
print("\n--- Level 3: List files ---")
files = dbutils.fs.ls("/tmp/delta_kt/hw81_delta")
print(f"Files in delta dir: {len(files)}")
for f in files[:3]:
    print(f"  {f.name} ({f.size} bytes)")
# WHY: dbutils.fs.ls works the same on ADLS as on DBFS.

# Level 4: Partitioned write.
print("\n--- Level 4: Partitioned write ---")
spark.range(1000).select(col("id"), (col("id")%3).alias("group")) \
    .write.format("delta").mode("overwrite").partitionBy("group") \
    .save("/tmp/delta_kt/hw81_partitioned")
print("Partitions: ", [f.name for f in dbutils.fs.ls("/tmp/delta_kt/hw81_partitioned") if "group=" in f.name])
# WHY: Partitioning enables partition pruning (read only needed partitions).

# Level 5-10: Conceptual.
print("\n--- Level 5: abfss URL anatomy ---")
print("abfss://container@account.dfs.core.windows.net/path/to/data")
print("Protocol://Container@StorageAccount.endpoint/Path")

print("\n--- Level 6: Authentication methods (ranked) ---")
print("1. Unity Catalog (best, no creds). 2. Service Principal + Secrets.")
print("3. Managed Identity. 4. Storage Key (dev only). 5. SAS Token.")

print("\n--- Level 7: Secret Scopes ---")
print("dbutils.secrets.get(scope='name', key='key') → retrieves secrets securely.")
print("Secrets never displayed in notebook output.")

print("\n--- Level 8: UC Volumes vs raw ADLS paths ---")
print("Volumes: /Volumes/catalog/schema/vol/ (governed, portable, no creds).")
print("Raw ADLS: abfss://... (requires auth config, not portable).")

print("\n--- Level 9: Auto Loader from ADLS ---")
print("spark.readStream.format('cloudFiles').option('cloudFiles.format','csv')")
print(".load('abfss://landing@acct.../path/') → streaming ingestion.")

print("\n--- Level 10: Teach ADLS access ---")
print("""
"ADLS Gen2 = Azure's data lake storage for Databricks.
  URL: abfss://container@account.dfs.core.windows.net/path
  Auth: Unity Catalog (best) > Service Principal > Storage Key.
  Read: spark.read.format('csv'/'delta'/'parquet').load(url)
  Write: df.write.format('delta').save(url)
  Modern: Use UC Volumes (/Volumes/...) — no URLs needed."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 81")
print("="*70)