# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 82: AWS S3 and Google Cloud Storage (GCS)
# MAGIC ## Module 13: Data Sources & Connectors
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 40 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC While Azure uses ADLS Gen2, the other major clouds have their own object storage:
# MAGIC - **AWS S3** (Simple Storage Service) — The most widely used cloud storage globally
# MAGIC - **Google Cloud Storage (GCS)** — Google's equivalent
# MAGIC
# MAGIC Databricks runs on all three clouds, and the Spark read/write API is **identical** — only the path prefix and authentication differ.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC S3, ADLS, and GCS are like three different **postal systems** (USPS, Royal Mail, DHL):
# MAGIC - Different addresses (URL formats)
# MAGIC - Different identification (authentication)
# MAGIC - But you're sending the same packages (data)
# MAGIC - And using the same shipping software (Spark)
# MAGIC
# MAGIC ### URL Formats:
# MAGIC | Cloud | Protocol | Example |
# MAGIC |-------|----------|--------|
# MAGIC | Azure ADLS | `abfss://` | `abfss://container@account.dfs.core.windows.net/path` |
# MAGIC | AWS S3 | `s3://` or `s3a://` | `s3://bucket-name/path/to/data` |
# MAGIC | Google GCS | `gs://` | `gs://bucket-name/path/to/data` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Cross-Cloud Storage Comparison:
# MAGIC
# MAGIC   ┌──────────────┬──────────────────┬──────────────────┬──────────────────┐
# MAGIC   │ Feature      │ Azure ADLS       │ AWS S3           │ Google GCS       │
# MAGIC   ├──────────────┼──────────────────┼──────────────────┼──────────────────┤
# MAGIC   │ Protocol     │ abfss://         │ s3://            │ gs://            │
# MAGIC   │ Top-level    │ Container        │ Bucket           │ Bucket           │
# MAGIC   │ Auth         │ SPN/MI/Key       │ IAM Role/Keys    │ Service Account  │
# MAGIC   │ DB Auth      │ Instance Profile │ Instance Profile │ Service Account  │
# MAGIC   │ Consistency  │ Strong           │ Strong (2020+)   │ Strong           │
# MAGIC   │ Namespaces   │ Hierarchical     │ Flat (prefix)    │ Flat (prefix)    │
# MAGIC   └──────────────┴──────────────────┴──────────────────┴──────────────────┘
# MAGIC
# MAGIC AWS S3 Authentication in Databricks:
# MAGIC   1. Instance Profile (recommended): Cluster IAM role grants S3 access.
# MAGIC   2. Access Keys: spark.hadoop.fs.s3a.access.key / secret.key.
# MAGIC   3. Unity Catalog: External Location with storage credential.
# MAGIC
# MAGIC GCS Authentication in Databricks:
# MAGIC   1. Service Account: Configured at cluster level.
# MAGIC   2. Unity Catalog: External Location with GCS storage credential.
# MAGIC
# MAGIC Code (identical API, just different path):
# MAGIC   # AWS S3:
# MAGIC   df = spark.read.format("parquet").load("s3://my-bucket/path/data/")
# MAGIC   df.write.format("delta").save("s3://my-bucket/output/")
# MAGIC
# MAGIC   # GCS:
# MAGIC   df = spark.read.format("parquet").load("gs://my-bucket/path/data/")
# MAGIC   df.write.format("delta").save("gs://my-bucket/output/")
# MAGIC
# MAGIC   # Auto Loader (works on all clouds):
# MAGIC   spark.readStream.format("cloudFiles")
# MAGIC       .option("cloudFiles.format", "csv")
# MAGIC       .load("s3://landing-bucket/incoming/")  # or gs:// or abfss://
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3-7: Examples and Homework
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7 — EXAMPLES & HOMEWORK
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand  # Imports.

print("="*70)
print("SECTIONS 3-7: AWS S3 & GCS Patterns")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: AWS S3 authentication patterns
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: AWS S3 authentication")
print("-"*60)

print("""
Method 1: Instance Profile (RECOMMENDED for Databricks on AWS):
  - Attach IAM Instance Profile to the Databricks cluster.
  - The profile grants S3 access automatically.
  - No credentials in code!
  - Setup: AWS Admin → IAM Role → S3 policy → Instance Profile → Databricks.

Method 2: Access Key + Secret (for cross-account or testing):
  access_key = dbutils.secrets.get(scope="aws", key="s3-access-key")
  secret_key = dbutils.secrets.get(scope="aws", key="s3-secret-key")
  
  spark.conf.set("fs.s3a.access.key", access_key)
  spark.conf.set("fs.s3a.secret.key", secret_key)
  spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

  df = spark.read.parquet("s3a://my-bucket/data/")

Method 3: Unity Catalog External Location (BEST for governance):
  -- SQL: Create storage credential + external location.
  CREATE STORAGE CREDENTIAL s3_cred WITH (AWS_IAM_ROLE = 'arn:aws:iam::...');
  CREATE EXTERNAL LOCATION s3_loc URL 's3://bucket/path' WITH (CREDENTIAL s3_cred);
  
  -- Now just use the path:
  df = spark.read.parquet("s3://bucket/path/data/")

Note: Use s3:// (not s3a://) in newer Databricks runtimes.
      s3a:// still works but s3:// is simpler.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: GCS authentication patterns
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Google Cloud Storage (GCS) authentication")
print("-"*60)

print("""
Method 1: Service Account (configured on cluster):
  - Create GCP Service Account with Storage Object Viewer role.
  - Download JSON key → store in Databricks Secret Scope.
  - Set in cluster Spark config:
    spark.hadoop.google.cloud.auth.service.account.enable true
    spark.hadoop.google.cloud.auth.service.account.json.keyfile /path/key.json

Method 2: Unity Catalog (Databricks on GCP):
  CREATE STORAGE CREDENTIAL gcs_cred WITH (GCP_SERVICE_ACCOUNT_EMAIL = '...');
  CREATE EXTERNAL LOCATION gcs_loc URL 'gs://bucket/' WITH (CREDENTIAL gcs_cred);
  
  df = spark.read.parquet("gs://my-bucket/data/")

Reading/Writing (same API as S3/ADLS):
  df = spark.read.format("csv").load("gs://bucket/path/*.csv")
  df.write.format("delta").save("gs://bucket/output/")
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Cross-cloud portable code pattern
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Portable code (works on any cloud)")
print("-"*60)

# Best practice: Use Unity Catalog tables (cloud-agnostic).
print("""
Cloud-Agnostic Pattern (RECOMMENDED):

  # Instead of hardcoding cloud paths:
  # BAD: df = spark.read.load("abfss://container@acct.../path")
  # BAD: df = spark.read.load("s3://bucket/path")

  # GOOD: Use Unity Catalog (same code on any cloud):
  df = spark.table("catalog.schema.my_table")
  df.write.mode("overwrite").saveAsTable("catalog.schema.output")

  # Or use Volumes for files:
  df = spark.read.csv("/Volumes/catalog/schema/vol/data.csv")

  # If you MUST use raw paths, parameterize:
  BASE_PATH = spark.conf.get("pipeline.base_path")  # Set per environment.
  df = spark.read.format("delta").load(f"{BASE_PATH}/my_table")
""")

# Demo: same read/write API works regardless of path.
print("\nDemo (local path, same API as cloud):")
df = spark.range(100).select(col("id"), (rand()*100).alias("score"))
df.write.format("delta").mode("overwrite").save("/tmp/delta_kt/cloud_agnostic")
read = spark.read.format("delta").load("/tmp/delta_kt/cloud_agnostic")
print(f"Written and read: {read.count()} rows")
print("✓ Same spark.read/write API works for S3, GCS, ADLS, and local.")

# ─── SECTION 6: Common Mistakes ───
print("\n" + "="*70)
print("SECTION 6 — COMMON MISTAKES")
print("="*70)
print("""
1. Using s3a:// with older Hadoop configs (use s3:// in newer runtimes).
2. Not using Instance Profile on AWS (hardcoding keys instead).
3. Forgetting region-specific endpoints for cross-region S3 access.
4. Mixing cloud paths in code (not portable between Azure/AWS/GCP).
5. Not using Unity Catalog for governed, cloud-agnostic access.
""")

# ─── SECTION 7: Homework ───
print("="*70)
print("SECTION 7 — HOMEWORK")
print("="*70)

print("""
Level 1: What's the URL format for S3?
  Answer: s3://bucket-name/path/to/data

Level 2: What's the URL format for GCS?
  Answer: gs://bucket-name/path/to/data

Level 3: What's the URL format for ADLS Gen2?
  Answer: abfss://container@account.dfs.core.windows.net/path

Level 4: Best auth for Databricks on AWS?
  Answer: Instance Profile (IAM Role attached to cluster).

Level 5: Best auth for Databricks on GCP?
  Answer: Service Account configured on cluster.

Level 6: How to make code cloud-agnostic?
  Answer: Use Unity Catalog tables/volumes (no cloud URLs in code).

Level 7: Auto Loader on S3?
  spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load("s3://bucket/landing/")

Level 8: Cross-account S3 access?
  Use IAM cross-account role or Unity Catalog storage credential.

Level 10: Teach cloud storage to a colleague:
  "S3, ADLS, GCS = same API (spark.read/write), different URLs.
   Auth: Instance Profile (AWS), SPN (Azure), Service Account (GCP).
   Best practice: Use Unity Catalog for governance + portability."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 82")
print("="*70)