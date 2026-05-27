# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 16: Reading JSON, Parquet, ORC, Avro
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
# MAGIC ### Real-World Analogy: Different File Cabinets
# MAGIC
# MAGIC Imagine 4 ways to store employee records:
# MAGIC - **JSON** = A notebook with flexible, nested notes (great for APIs, messy for big data)
# MAGIC - **Parquet** = A perfectly organized filing cabinet with column labels on every drawer (FASTEST for analytics)
# MAGIC - **ORC** = Similar to Parquet, optimized for a different filing system (Hive ecosystem)
# MAGIC - **Avro** = A conveyor belt of individual records (great for streaming/Kafka)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Format Comparison
# MAGIC
# MAGIC | Feature | JSON | Parquet | ORC | Avro |
# MAGIC |---------|------|---------|-----|------|
# MAGIC | Storage | Row-based | **Columnar** | **Columnar** | Row-based |
# MAGIC | Schema | Inferred/partial | **Embedded in file** | **Embedded in file** | **Embedded in file** |
# MAGIC | Compression | None/GZIP | **Snappy (default)** | **Zlib (default)** | Snappy/Deflate |
# MAGIC | Read speed | Slow | **Fastest** | Fast | Medium |
# MAGIC | File size | Large | **Small** | Small | Medium |
# MAGIC | Nested data | Excellent | Good | Good | Good |
# MAGIC | Splittable | Yes* | **Yes** | **Yes** | Yes |
# MAGIC | Best for | APIs, logs | **Analytics** | Hive workloads | Streaming/Kafka |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use Each
# MAGIC
# MAGIC 1. **Parquet** — Default choice for 90% of use cases. Always use for data lakes.
# MAGIC 2. **JSON** — When receiving data from APIs or when humans need to read the file.
# MAGIC 3. **ORC** — When working with existing Hive/HDP ecosystems.
# MAGIC 4. **Avro** — When streaming with Kafka or need schema registry support.

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Row-Based vs Columnar Storage
# MAGIC
# MAGIC ```
# MAGIC   ROW-BASED (JSON, Avro):        COLUMNAR (Parquet, ORC):
# MAGIC   ───────────────────────         ─────────────────────────
# MAGIC   
# MAGIC   Row 1: [Alice, 30, 95000]      Col "name":   [Alice, Bob, Charlie]
# MAGIC   Row 2: [Bob, 25, 72000]        Col "age":    [30, 25, 35]
# MAGIC   Row 3: [Charlie, 35, 110000]   Col "salary": [95000, 72000, 110000]
# MAGIC   
# MAGIC   To read ONE column:            To read ONE column:
# MAGIC   Must read ALL rows             Read ONLY that column’s data
# MAGIC   (wasteful!)                    (super efficient!)
# MAGIC ```
# MAGIC
# MAGIC ### Why Parquet is Fastest for Analytics
# MAGIC
# MAGIC ```
# MAGIC   Query: SELECT avg(salary) FROM employees
# MAGIC   
# MAGIC   With JSON (row-based):
# MAGIC     Read ALL data: name + age + salary (3 columns)
# MAGIC     Discard name and age
# MAGIC     Compute average of salary
# MAGIC     → Reads 3x more data than needed!
# MAGIC   
# MAGIC   With Parquet (columnar):
# MAGIC     Read ONLY the salary column (skip name, age entirely)
# MAGIC     Compute average
# MAGIC     → Reads exactly what’s needed! (Column Pruning)
# MAGIC ```
# MAGIC
# MAGIC ### Parquet File Internals
# MAGIC
# MAGIC ```
# MAGIC   my_data.parquet (actually a folder!):
# MAGIC   ├─ part-00000.snappy.parquet   (data file 1)
# MAGIC   ├─ part-00001.snappy.parquet   (data file 2)
# MAGIC   ├─ _SUCCESS                    (write completed marker)
# MAGIC   └─ _metadata / _common_metadata (schema info)
# MAGIC   
# MAGIC   Inside each .parquet file:
# MAGIC   ┌─────────────────────────────┐
# MAGIC   │ Row Group 1                   │
# MAGIC   │   Column Chunk: name          │  ← min/max stats for pruning
# MAGIC   │   Column Chunk: age           │  ← compressed independently
# MAGIC   │   Column Chunk: salary        │  ← read only what you need
# MAGIC   ├─────────────────────────────┤
# MAGIC   │ Footer: Schema + Statistics   │  ← types, row counts, min/max
# MAGIC   └─────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Reading JSON
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: Reading JSON
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, explode

print("=== Reading JSON Files ===")
print()

# Create sample JSON (one JSON object per line = JSON Lines format)
json_lines = """{"id": 1, "name": "Alice", "age": 30, "skills": ["Python", "SQL"], "address": {"city": "NYC", "zip": "10001"}}
{"id": 2, "name": "Bob", "age": 25, "skills": ["Java"], "address": {"city": "LA", "zip": "90001"}}
{"id": 3, "name": "Charlie", "age": 35, "skills": ["Python", "Spark", "SQL"], "address": {"city": "Chicago", "zip": "60601"}}"""

json_path = "/tmp/format_demo/users.json"
dbutils.fs.put(json_path, json_lines, overwrite=True)
print(f"JSON written to: {json_path}")

# --- Basic JSON read ---
print("\n--- 1. Basic read (schema auto-inferred from JSON keys) ---")
df = spark.read.json(json_path)  # Schema inferred from JSON structure!
df.show(truncate=False)
df.printSchema()  # Nested types detected automatically!

# --- Accessing nested fields ---
print("--- 2. Accessing nested struct fields ---")
df.select("name", col("address.city"), col("address.zip")).show()

# --- Exploding arrays ---
print("--- 3. Exploding array field ---")
df.select("name", explode("skills").alias("skill")).show()

# --- Multi-line JSON (pretty-printed, not one-per-line) ---
print("--- 4. Multi-line JSON ---")
multi_json = """[
  {"id": 1, "name": "Alice", "score": 95},
  {"id": 2, "name": "Bob", "score": 87}
]"""
multi_path = "/tmp/format_demo/multi.json"
dbutils.fs.put(multi_path, multi_json, overwrite=True)

df_multi = spark.read.option("multiLine", "true").json(multi_path)  # Must set multiLine!
df_multi.show()
print("multiLine=true needed when JSON is pretty-printed (not one object per line)")

print("\n--- Key JSON options ---")
print("  multiLine: true for pretty-printed JSON")
print("  mode: PERMISSIVE/DROPMALFORMED/FAILFAST (same as CSV)")
print("  dateFormat/timestampFormat: same as CSV")
print("  schema: explicit StructType (recommended for production)")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Reading Parquet
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: Reading Parquet
# ═══════════════════════════════════════════════════════

import time
from pyspark.sql.functions import col, concat, lit

print("=== Reading Parquet Files ===")
print()

# Create sample Parquet data
data = [(i, f"user_{i}", i * 10.5, i % 5) for i in range(10000)]  # 10K rows
df_source = spark.createDataFrame(data, ["id", "name", "amount", "category"])

parquet_path = "/tmp/format_demo/users.parquet"
df_source.write.mode("overwrite").parquet(parquet_path)
print(f"Parquet written to: {parquet_path}")

# --- Reading Parquet (simplest of all formats!) ---
print("\n--- 1. Basic Parquet read ---")
df = spark.read.parquet(parquet_path)  # That's it! No options needed!
df.show(5)
df.printSchema()  # Schema is embedded IN the file (no inference needed!)
print("Schema came from the file itself (stored in Parquet footer)")

# --- Why Parquet is best: Column Pruning ---
print("\n--- 2. Column pruning (read only needed columns) ---")
start = time.time()
df_all = spark.read.parquet(parquet_path).count()  # Read all columns
t_all = time.time() - start

start = time.time()
df_one = spark.read.parquet(parquet_path).select("amount").count()  # Only 1 column!
t_one = time.time() - start

print(f"  Read all 4 columns: {t_all:.3f}s")
print(f"  Read 1 column only: {t_one:.3f}s")
print("  Parquet skips columns you don't need (columnar benefit!)")

# --- Parquet with partitions ---
print("\n--- 3. Partitioned Parquet ---")
part_path = "/tmp/format_demo/partitioned_parquet"
df_source.write.mode("overwrite").partitionBy("category").parquet(part_path)

# Read only one partition (partition pruning!)
df_pruned = spark.read.parquet(part_path).filter(col("category") == 2)
print(f"  Partition filter: only {df_pruned.count()} rows read (out of 10K)")
print("  Spark only reads files in category=2/ folder!")

# --- mergeSchema for evolving Parquet ---
print("\n--- 4. mergeSchema (combine different schema files) ---")
print("  Use: spark.read.option('mergeSchema', 'true').parquet(path)")
print("  When: Files written at different times have different columns")
print("  Missing columns become null for older files")

print("\n--- Key: Parquet = best format for analytics (fast, compact, self-describing) ---")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Reading ORC and Avro
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: ORC and Avro
# ═══════════════════════════════════════════════════════

print("=== Reading ORC and Avro Files ===")
print()

# Create sample data
data = [(i, f"product_{i}", i * 9.99, i % 3 == 0) for i in range(1000)]
df_source = spark.createDataFrame(data, ["id", "name", "price", "in_stock"])

# --- ORC Format ---
print("--- 1. ORC (Optimized Row Columnar) ---")
orc_path = "/tmp/format_demo/products.orc"
df_source.write.mode("overwrite").orc(orc_path)  # Write as ORC

df_orc = spark.read.orc(orc_path)  # Read ORC (schema embedded, like Parquet)
df_orc.show(5)
df_orc.printSchema()
print(f"  ORC rows: {df_orc.count()}")
print("  ORC is columnar like Parquet")
print("  Best for: Hive ecosystem, slightly better compression than Parquet")
print("  Default compression: ZLIB (vs Snappy for Parquet)")

# --- Avro Format ---
print("\n--- 2. Avro (Apache Avro) ---")
avro_path = "/tmp/format_demo/products.avro"
df_source.write.mode("overwrite").format("avro").save(avro_path)  # Write Avro

df_avro = spark.read.format("avro").load(avro_path)  # Read Avro
df_avro.show(5)
df_avro.printSchema()
print(f"  Avro rows: {df_avro.count()}")
print("  Avro is ROW-based (not columnar)")
print("  Best for: Streaming (Kafka), schema registry, row-level operations")
print("  Schema stored as JSON in file header")

# --- Comparison: File Sizes ---
print("\n--- 3. File Size Comparison (same 1000 rows) ---")
import functools

def get_folder_size(path):
    """Get total size of files in a folder."""
    try:
        files = dbutils.fs.ls(path)
        total = sum(f.size for f in files if not f.name.startswith("_"))
        return total
    except:
        return 0

# Also write as JSON and CSV for comparison
json_cmp_path = "/tmp/format_demo/products_cmp.json"
csv_cmp_path = "/tmp/format_demo/products_cmp.csv"
df_source.write.mode("overwrite").json(json_cmp_path)
df_source.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_cmp_path)

formats = [
    ("CSV", csv_cmp_path), ("JSON", json_cmp_path),
    ("Parquet", "/tmp/format_demo/users.parquet"),
    ("ORC", orc_path), ("Avro", avro_path)
]

print(f"  {'Format':<10} {'Size (bytes)':<15} {'Notes'}")
print(f"  {'-'*50}")
for name, path in formats:
    size = get_folder_size(path)
    notes = "Smallest (columnar+compression)" if name in ["Parquet", "ORC"] else ""
    if name == "CSV":
        notes = "Largest (plain text, no compression)"
    elif name == "JSON":
        notes = "Large (field names repeated every row)"
    print(f"  {name:<10} {size:<15,} {notes}")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: JSON with Explicit Schema
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: JSON with Schema & Nested Handling
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
from pyspark.sql.functions import col, explode, size, from_json, schema_of_json

print("=== JSON: Explicit Schema & Nested Data ===")
print()

# --- Reading JSON with explicit schema (production pattern) ---
print("--- 1. JSON with explicit schema ---")
json_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("skills", ArrayType(StringType()), True),
    StructField("address", StructType([
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
    ]), True),
])

df = spark.read.schema(json_schema).json("/tmp/format_demo/users.json")
df.show(truncate=False)
print("With explicit schema: no inference scan, exact types guaranteed")

# --- Handling JSON with inconsistent fields ---
print("\n--- 2. JSON with inconsistent fields ---")
inconsistent_json = """{"id": 1, "name": "Alice", "score": 95}
{"id": 2, "name": "Bob", "score": 87, "bonus": 10}
{"id": 3, "name": "Charlie"}"""
# Some rows have 'bonus', some don't. Some missing 'score'.

incon_path = "/tmp/format_demo/inconsistent.json"
dbutils.fs.put(incon_path, inconsistent_json, overwrite=True)

df_incon = spark.read.json(incon_path)  # Spark handles missing fields as null
df_incon.show()
print("Missing fields become null (Spark handles gracefully)")

# --- schema_of_json: Discover schema from a sample ---
print("\n--- 3. schema_of_json(): Discover schema automatically ---")
sample = '{"id": 1, "name": "Alice", "scores": [95, 88], "meta": {"source": "api"}}'
inferred = schema_of_json(sample)  # Returns a Column with DDL schema string
print(f"  Inferred DDL: {spark.range(1).select(inferred).collect()[0][0]}")
print("  Use this to discover schema, then hardcode it for production")

# --- JSON in a string column (from_json) ---
print("\n--- 4. Parsing JSON inside a string column ---")
df_raw = spark.createDataFrame([
    (1, '{"city": "NYC", "temp": 72}'),
    (2, '{"city": "LA", "temp": 85}'),
], ["id", "json_data"])

json_col_schema = StructType([
    StructField("city", StringType()),
    StructField("temp", IntegerType()),
])

df_parsed = df_raw.withColumn("parsed", from_json(col("json_data"), json_col_schema))
df_parsed.select("id", col("parsed.city"), col("parsed.temp")).show()
print("from_json() parses a JSON string column into a struct")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Parquet Advanced Features
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Parquet Advanced Features
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col

print("=== Parquet: Schema Evolution & Predicate Pushdown ===")
print()

# --- Schema Evolution with mergeSchema ---
print("--- 1. Schema Evolution (mergeSchema) ---")
# Version 1: Original schema
df_v1 = spark.createDataFrame([
    (1, "Alice", 100.0), (2, "Bob", 200.0)
], ["id", "name", "amount"])
v1_path = "/tmp/format_demo/evolving/v1"
df_v1.write.mode("overwrite").parquet(v1_path)

# Version 2: Added column
df_v2 = spark.createDataFrame([
    (3, "Charlie", 300.0, "premium"), (4, "Diana", 400.0, "standard")
], ["id", "name", "amount", "tier"])
v2_path = "/tmp/format_demo/evolving/v2"
df_v2.write.mode("overwrite").parquet(v2_path)

# Read both versions together
df_merged = spark.read.option("mergeSchema", "true").parquet(v1_path, v2_path)
df_merged.show()  # V1 rows have null for 'tier' column
print("mergeSchema combines ALL columns from all files")
print("Old files get null for new columns")

# --- Predicate Pushdown ---
print("\n--- 2. Predicate Pushdown (filtering at file level) ---")
# Create partitioned data
part_path = "/tmp/format_demo/partitioned_sales"
df_big = spark.range(100000).withColumn("region", (col("id") % 4).cast("string")).withColumn("amount", col("id") * 1.5)
df_big.write.mode("overwrite").partitionBy("region").parquet(part_path)

# Filter on partition column (skips entire folders!)
df_filtered = spark.read.parquet(part_path).filter(col("region") == "2")
print(f"  Filter on partition: {df_filtered.count():,} rows (skipped 3/4 of data files!)")

# See the physical plan to verify pushdown
print("\n  Physical plan (look for 'PushedFilters'):")
df_filtered.explain()

# --- Compression Options ---
print("\n--- 3. Compression Options ---")
compressions = ["snappy", "gzip", "lz4", "zstd", "none"]
print(f"  {'Compression':<12} {'Use Case'}")
print(f"  {'-'*40}")
for comp in compressions:
    if comp == "snappy":
        use = "Default. Fast compress/decompress, good ratio"
    elif comp == "gzip":
        use = "Best ratio, slow. Use for archival."
    elif comp == "lz4":
        use = "Fastest. Use for speed-critical workloads."
    elif comp == "zstd":
        use = "Best of both worlds. Great ratio + fast."
    else:
        use = "No compression. Largest files, fastest writes."
    print(f"  {comp:<12} {use}")

print("\n  Set via: .option('compression', 'zstd')")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Format Performance Benchmark
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Format Performance Comparison
# ═══════════════════════════════════════════════════════

import time
from pyspark.sql.functions import col, avg

print("=== Performance Benchmark: JSON vs Parquet vs ORC vs Avro ===")
print()

# Create test dataset (50K rows)
df_test = (
    spark.range(50000)
    .withColumn("name", concat(lit("user_"), col("id").cast("string")))
    .withColumn("salary", (col("id") * 2.5 + 30000))
    .withColumn("dept", (col("id") % 10).cast("string"))
)

# Write in all formats
base = "/tmp/format_demo/benchmark"
df_test.write.mode("overwrite").json(f"{base}/json")
df_test.write.mode("overwrite").parquet(f"{base}/parquet")
df_test.write.mode("overwrite").orc(f"{base}/orc")
df_test.write.mode("overwrite").format("avro").save(f"{base}/avro")
print("Written 50K rows in all 4 formats")

# Benchmark: Read + aggregate
print("\nTask: Read all data + compute AVG(salary) GROUP BY dept")
print(f"{'Format':<10} {'Read+Agg Time':<15} {'Notes'}")
print("-" * 50)

results = []
for fmt, path, reader in [
    ("JSON", f"{base}/json", lambda: spark.read.json(f"{base}/json")),
    ("Parquet", f"{base}/parquet", lambda: spark.read.parquet(f"{base}/parquet")),
    ("ORC", f"{base}/orc", lambda: spark.read.orc(f"{base}/orc")),
    ("Avro", f"{base}/avro", lambda: spark.read.format("avro").load(f"{base}/avro")),
]:
    start = time.time()
    reader().groupBy("dept").agg(avg("salary")).collect()  # Force execution
    elapsed = time.time() - start
    results.append((fmt, elapsed))

# Print sorted by speed
for fmt, t in sorted(results, key=lambda x: x[1]):
    rank = "⭐ FASTEST" if t == min(r[1] for r in results) else ""
    print(f"{fmt:<10} {t:<15.3f} {rank}")

print("\n--- Conclusion ---")
print("  Parquet/ORC: Fastest (columnar = reads only 'salary' + 'dept')")
print("  JSON/Avro: Slower (row-based = must read ALL columns)")
print("  \u2192 ALWAYS use Parquet for analytics workloads")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Schema Evolution Strategy
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Schema Evolution Across Formats
# ═══════════════════════════════════════════════════════

print("=== Schema Evolution: How Each Format Handles Changes ===")
print()

# Schema evolution = adding/removing/renaming columns over time
# Common in production: new features get added to data monthly

print("--- Schema Evolution Capabilities ---")
print(f"{'Operation':<25} {'JSON':<10} {'Parquet':<10} {'ORC':<10} {'Avro':<10} {'Delta':<10}")
print("-" * 75)
print(f"{'Add column':<25} {'Auto':<10} {'mergeSchema':<10} {'Yes':<10} {'Yes':<10} {'Auto':<10}")
print(f"{'Remove column':<25} {'Auto':<10} {'Manual':<10} {'Manual':<10} {'Manual':<10} {'Auto':<10}")
print(f"{'Rename column':<25} {'Auto':<10} {'No':<10} {'No':<10} {'Aliases':<10} {'ALTER':<10}")
print(f"{'Type widening':<25} {'Auto':<10} {'No*':<10} {'No*':<10} {'Rules':<10} {'ALTER':<10}")
print(f"{'Enforcement':<25} {'None':<10} {'Read-time':<10} {'Read-time':<10} {'Schema Reg':<10} {'Write-time':<10}")

# Demonstrate Parquet mergeSchema
print("\n--- Parquet mergeSchema Demo ---")
base = "/tmp/format_demo/evolution"

# Month 1: 3 columns
df_m1 = spark.createDataFrame([(1, "A", 100)], ["id", "type", "value"])
df_m1.write.mode("overwrite").parquet(f"{base}/month1")

# Month 2: Added 'category' column  
df_m2 = spark.createDataFrame([(2, "B", 200, "premium")], ["id", "type", "value", "category"])
df_m2.write.mode("overwrite").parquet(f"{base}/month2")

# Month 3: Added 'score' column
df_m3 = spark.createDataFrame([(3, "C", 300, "standard", 95.5)], ["id", "type", "value", "category", "score"])
df_m3.write.mode("overwrite").parquet(f"{base}/month3")

# Read all with mergeSchema
df_all = spark.read.option("mergeSchema", "true").parquet(f"{base}/month1", f"{base}/month2", f"{base}/month3")
df_all.show()
df_all.printSchema()
print("Month 1 data has null for columns that didn't exist yet")

# JSON handles this automatically (no mergeSchema needed)
print("\n--- JSON: Automatic schema evolution ---")
for i, (m, data) in enumerate([("m1", '{"id":1,"type":"A"}'), ("m2", '{"id":2,"type":"B","extra":"new"}')], 1):
    dbutils.fs.put(f"{base}/json_{m}.json", data, overwrite=True)

df_json_all = spark.read.json(f"{base}/json_*.json")
df_json_all.show()
print("JSON naturally handles missing fields as null (no special option needed)")

print("\n--- Recommendation ---")
print("  For production: Use Delta Lake (best schema evolution + enforcement)")
print("  For raw landing: Parquet with mergeSchema=true")
print("  For APIs/logs: JSON (naturally flexible)")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Reading from ADLS/Cloud Paths
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Cloud Storage Paths & Best Practices
# ═══════════════════════════════════════════════════════

print("=== Reading from Cloud Storage (ADLS, S3, GCS) ===")
print()

# In production, data lives in cloud storage, not local /tmp/
# The path format changes, but spark.read stays the same!

print("--- 1. Path Formats by Cloud Provider ---")
print()
print("  AZURE (ADLS Gen2):")
print('    abfss://container@storageaccount.dfs.core.windows.net/path/data.parquet')
print('    Example: abfss://landing@adl2hubprod.dfs.core.windows.net/raw/sales/')
print()
print("  AWS (S3):")
print('    s3a://bucket-name/path/data.parquet')
print('    Example: s3a://my-data-lake/raw/sales/')
print()
print("  GCP (GCS):")
print('    gs://bucket-name/path/data.parquet')
print('    Example: gs://my-project-lake/raw/sales/')
print()
print("  DBFS (Databricks File System):")
print('    dbfs:/mnt/datalake/path/data.parquet (mounted)')
print('    /mnt/datalake/path/data.parquet (shorthand)')
print()
print("  Unity Catalog Volumes:")
print('    /Volumes/catalog/schema/volume/path/data.parquet')

# --- Reading pattern is IDENTICAL regardless of cloud ---
print("\n--- 2. Reading pattern (same for ALL clouds) ---")
print()
print("  # Azure ADLS")
print('  df = spark.read.parquet("abfss://landing@storage.dfs.core.windows.net/sales/")')
print()
print("  # AWS S3")
print('  df = spark.read.parquet("s3a://my-bucket/sales/")')
print()
print("  # Mounted path (any cloud)")
print('  df = spark.read.parquet("/mnt/datalake/sales/")')
print()
print("  # Unity Catalog Volume (recommended for Databricks)")
print('  df = spark.read.parquet("/Volumes/main/raw/landing/sales/")')

# --- Best practices ---
print("\n--- 3. Production Best Practices ---")
print("  1. Use Unity Catalog Volumes (/Volumes/...) for new projects")
print("  2. Avoid DBFS mounts (deprecated pattern)")
print("  3. Use abfss:// directly for Azure (with service principal auth)")
print("  4. Store credentials in secret scopes, NEVER hardcode")
print("  5. Use Parquet/Delta for all data lake storage")
print("  6. Partition by date for time-series data")
print("  7. Use AutoLoader (cloudFiles) for incremental ingestion")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Universal File Reader
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Universal Format Reader Function
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import input_file_name, current_timestamp, lit
import os

print("=== Universal Format Reader ===")
print()

def read_any_format(spark, path, format_hint=None, schema=None, **options):
    """
    Universal reader that auto-detects format from file extension.
    Adds audit columns and handles common options.
    """
    # Auto-detect format from path extension
    if format_hint is None:
        ext = path.rstrip("/").split(".")[-1].lower()
        format_map = {
            "json": "json", "parquet": "parquet", "orc": "orc",
            "avro": "avro", "csv": "csv", "tsv": "csv",
        }
        format_hint = format_map.get(ext, "parquet")  # Default to parquet
    
    # Build reader
    reader = spark.read.format(format_hint)
    
    # Apply schema if provided
    if schema:
        reader = reader.schema(schema)
    
    # Apply format-specific defaults
    if format_hint == "csv":
        reader = reader.option("header", "true")
    elif format_hint == "json":
        if options.get("multiLine"):
            reader = reader.option("multiLine", "true")
    
    # Apply custom options
    for key, value in options.items():
        reader = reader.option(key, str(value))
    
    # Read the data
    df = reader.load(path)
    
    # Add audit columns
    df = (
        df
        .withColumn("_source_file", input_file_name())
        .withColumn("_load_time", current_timestamp())
        .withColumn("_format", lit(format_hint))
    )
    
    return df

# --- Test with different formats ---
print("--- Testing universal reader ---")

# Read JSON
df_json = read_any_format(spark, "/tmp/format_demo/users.json")
print(f"JSON: {df_json.count()} rows, format={df_json.select('_format').first()[0]}")

# Read Parquet
df_pq = read_any_format(spark, "/tmp/format_demo/users.parquet")
print(f"Parquet: {df_pq.count()} rows, format={df_pq.select('_format').first()[0]}")

# Read ORC
df_orc = read_any_format(spark, "/tmp/format_demo/products.orc")
print(f"ORC: {df_orc.count()} rows, format={df_orc.select('_format').first()[0]}")

# Read Avro
df_avro = read_any_format(spark, "/tmp/format_demo/products.avro", format_hint="avro")
print(f"Avro: {df_avro.count()} rows, format={df_avro.select('_format').first()[0]}")

print("\n--- Key: One function handles all formats with consistent audit columns ---")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Using JSON for Large Analytics Workloads
# MAGIC **Problem:** JSON is row-based and uncompressed — 10x larger and slower than Parquet.  
# MAGIC **Fix:** Convert JSON to Parquet/Delta once, then always read the optimized format.
# MAGIC
# MAGIC ### Mistake #2: Forgetting multiLine=true for Pretty-Printed JSON
# MAGIC **Problem:** Spark expects one JSON object per line by default. Pretty-printed JSON fails.  
# MAGIC **Fix:** Set `option("multiLine", "true")` for arrays or formatted JSON.
# MAGIC
# MAGIC ### Mistake #3: Not Using Column Pruning with Parquet
# MAGIC **Problem:** Reading `SELECT *` from Parquet still reads all columns (wastes I/O).  
# MAGIC **Fix:** Always `.select()` only needed columns BEFORE any action.
# MAGIC
# MAGIC ### Mistake #4: Writing Many Small Parquet Files
# MAGIC **Problem:** Thousands of tiny files (< 1MB each) destroy read performance.  
# MAGIC **Fix:** Use `.coalesce(n)` or `.repartition(n)` before writing. Target 128MB-256MB per file.
# MAGIC
# MAGIC ### Mistake #5: Not Partitioning Large Parquet Datasets
# MAGIC **Problem:** 100GB of Parquet in one folder — every query reads ALL of it.  
# MAGIC **Fix:** `partitionBy("date")` so queries filtering on date skip irrelevant files entirely.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC **Level 1:** Read the JSON file from Example 1. Print schema and show 3 rows.
# MAGIC
# MAGIC **Level 2:** Write a DataFrame as Parquet, then read it back. Verify schemas match.
# MAGIC
# MAGIC **Level 3:** Read the same data as JSON, Parquet, and ORC. Compare file sizes.
# MAGIC
# MAGIC **Level 4:** Create nested JSON (with arrays and structs). Read and access nested fields.
# MAGIC
# MAGIC **Level 5:** Write partitioned Parquet by 2 columns. Read with a filter on both. Verify partition pruning in explain().
# MAGIC
# MAGIC **Level 6:** Write data in 3 versions with different schemas. Read all with mergeSchema. Handle nulls.
# MAGIC
# MAGIC **Level 7:** Benchmark all 4 formats on 100K rows: write time, read time, aggregation time, file size.
# MAGIC
# MAGIC **Level 8:** Parse a column containing JSON strings using from_json(). Handle malformed JSON gracefully.
# MAGIC
# MAGIC **Level 9:** Build a production ingestion function: detect format → read with schema → validate → write as Delta.
# MAGIC
# MAGIC **Level 10:** Write a comparison guide for a team choosing between Parquet, ORC, Avro, and Delta for their data lake.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

import time
from pyspark.sql.functions import col, avg, from_json, schema_of_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Level 2: Write + Read Parquet
print("=== Level 2 ===")
df_orig = spark.createDataFrame([(1,"A",10.0),(2,"B",20.0)], ["id","cat","val"])
hw_path = "/tmp/format_demo/hw_parquet"
df_orig.write.mode("overwrite").parquet(hw_path)
df_back = spark.read.parquet(hw_path)
print(f"Original schema: {df_orig.schema.simpleString()}")
print(f"Read-back schema: {df_back.schema.simpleString()}")
print(f"Match: {df_orig.schema == df_back.schema}")

# Level 5: Partitioned + explain
print("\n=== Level 5 ===")
df5 = spark.range(10000).withColumn("region",(col("id")%4).cast("string")).withColumn("year",(col("id")%3+2022).cast("string"))
df5.write.mode("overwrite").partitionBy("region","year").parquet("/tmp/format_demo/hw_part")
df_pruned = spark.read.parquet("/tmp/format_demo/hw_part").filter((col("region")=="2")&(col("year")=="2023"))
print(f"Filtered rows: {df_pruned.count()}")
df_pruned.explain()  # Should show PartitionFilters in scan

# Level 7: Benchmark
print("\n=== Level 7 ===")
df7 = spark.range(100000).withColumn("val", col("id")*1.5).withColumn("cat",(col("id")%5).cast("string"))
for fmt in ["json", "parquet", "orc"]:
    p = f"/tmp/format_demo/hw_bench/{fmt}"
    start = time.time()
    if fmt == "avro":
        df7.write.mode("overwrite").format("avro").save(p)
    elif fmt == "json":
        df7.write.mode("overwrite").json(p)
    elif fmt == "parquet":
        df7.write.mode("overwrite").parquet(p)
    else:
        df7.write.mode("overwrite").orc(p)
    w_time = time.time() - start
    start = time.time()
    spark.read.format(fmt).load(p).groupBy("cat").agg(avg("val")).collect()
    r_time = time.time() - start
    print(f"  {fmt:<8} write={w_time:.2f}s  read+agg={r_time:.2f}s")

# Level 8: from_json with bad data
print("\n=== Level 8 ===")
df8 = spark.createDataFrame([
    (1, '{"city":"NYC","temp":72}'),
    (2, '{"city":"LA","temp":85}'),
    (3, 'INVALID JSON!!!'),  # Bad!
], ["id", "raw_json"])
json_schema = StructType([StructField("city",StringType()), StructField("temp",IntegerType())])
df8_parsed = df8.withColumn("parsed", from_json(col("raw_json"), json_schema))
df8_parsed.show(truncate=False)  # Bad row gets null for 'parsed'
print("Malformed JSON → null struct (graceful failure)")

print("\n\u2705 All homework complete!")