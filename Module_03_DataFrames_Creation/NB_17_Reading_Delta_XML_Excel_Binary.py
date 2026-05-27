# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 17: Reading Delta, XML, Excel, Binary Files
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
# MAGIC ### Real-World Analogy: Specialized File Readers
# MAGIC
# MAGIC Think of these 4 formats as specialized tools in your workshop:
# MAGIC - **Delta Lake** = A smart filing cabinet with a time machine (undo, history, ACID transactions)
# MAGIC - **XML** = A Russian nesting doll (deeply nested, tag-based, legacy systems love it)
# MAGIC - **Excel** = That spreadsheet your finance team emails you (ubiquitous but messy)
# MAGIC - **Binary Files** = A sealed box (images, PDFs, videos — just the raw bytes)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Format Comparison
# MAGIC
# MAGIC | Feature | Delta | XML | Excel | Binary |
# MAGIC |---------|-------|-----|-------|--------|
# MAGIC | Use case | **Data Lake standard** | Legacy/enterprise APIs | Business reports | Images/PDFs |
# MAGIC | ACID transactions | **Yes** | No | No | N/A |
# MAGIC | Time travel | **Yes** | No | No | N/A |
# MAGIC | Schema enforcement | **Yes** | Via XSD | No | N/A |
# MAGIC | Native Spark | **Yes** | Plugin | Plugin | Built-in |
# MAGIC | Nested data | Yes | **Excellent** | No | N/A |
# MAGIC | Human-readable | Via SQL | Yes | Via GUI | No |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use Each
# MAGIC
# MAGIC 1. **Delta** — ALWAYS for your data lake tables. It IS your data lake.
# MAGIC 2. **XML** — When receiving from enterprise systems (SOAP APIs, SAP, banking)
# MAGIC 3. **Excel** — When business users provide data (one-time loads, reports)
# MAGIC 4. **Binary** — When processing images, PDFs, or any non-text files as DataFrames

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Delta Lake Architecture
# MAGIC
# MAGIC ```
# MAGIC   Delta Table = Parquet files + Transaction Log
# MAGIC   ──────────────────────────────────────
# MAGIC   
# MAGIC   my_delta_table/
# MAGIC   ├─ _delta_log/                    ← Transaction log (THE magic!)
# MAGIC   │   ├─ 00000000000000000000.json  ← First commit (CREATE TABLE)
# MAGIC   │   ├─ 00000000000000000001.json  ← Second commit (INSERT)
# MAGIC   │   ├─ 00000000000000000002.json  ← Third commit (UPDATE)
# MAGIC   │   └─ 00000000000000000010.checkpoint.parquet
# MAGIC   ├─ part-00000.snappy.parquet      ← Actual data (Parquet!)
# MAGIC   ├─ part-00001.snappy.parquet
# MAGIC   └─ part-00002.snappy.parquet
# MAGIC ```
# MAGIC
# MAGIC ### Delta Time Travel
# MAGIC
# MAGIC ```
# MAGIC   Version 0: [Alice=100, Bob=200]        ← Initial insert
# MAGIC   Version 1: [Alice=150, Bob=200]        ← Update Alice
# MAGIC   Version 2: [Alice=150, Bob=200, C=300] ← Insert Charlie
# MAGIC   
# MAGIC   You can read ANY version:
# MAGIC     spark.read.format("delta").option("versionAsOf", 0)  → See original!
# MAGIC     spark.read.format("delta").option("timestampAsOf", "2024-01-01")  → Point in time!
# MAGIC ```
# MAGIC
# MAGIC ### XML Structure
# MAGIC
# MAGIC ```
# MAGIC   <employees>                    ← Root element
# MAGIC     <employee>                   ← rowTag (each = 1 row)
# MAGIC       <id>1</id>
# MAGIC       <name>Alice</name>
# MAGIC       <dept>
# MAGIC         <name>Engineering</name> ← Nested struct
# MAGIC         <floor>3</floor>
# MAGIC       </dept>
# MAGIC     </employee>
# MAGIC     <employee>...</employee>
# MAGIC   </employees>
# MAGIC ```
# MAGIC
# MAGIC ### Binary File Reading
# MAGIC
# MAGIC ```
# MAGIC   spark.read.format("binaryFile").load("/images/")
# MAGIC   
# MAGIC   Returns DataFrame with columns:
# MAGIC   ┌─────────────┬───────────────────────────────────┐
# MAGIC   │ Column      │ Type                                │
# MAGIC   ├─────────────┼───────────────────────────────────┤
# MAGIC   │ path        │ StringType (full file path)          │
# MAGIC   │ modificationTime │ TimestampType               │
# MAGIC   │ length      │ LongType (file size in bytes)        │
# MAGIC   │ content     │ BinaryType (raw bytes)               │
# MAGIC   └─────────────┴───────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Reading Delta Tables
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: Reading Delta Tables
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable

print("=== Reading Delta Tables ===")
print()

# --- Create a sample Delta table ---
data = [(1, "Alice", 95000.0), (2, "Bob", 72000.0), (3, "Charlie", 110000.0)]
df = spark.createDataFrame(data, ["id", "name", "salary"])

delta_path = "/tmp/format_demo/employees_delta"
df.write.format("delta").mode("overwrite").save(delta_path)
print(f"Delta table written to: {delta_path}")

# --- Method 1: Read by path ---
print("\n--- 1. Read Delta by path ---")
df_delta = spark.read.format("delta").load(delta_path)
df_delta.show()

# --- Method 2: Read as table (if registered) ---
print("--- 2. Read Delta as table ---")
spark.sql(f"CREATE OR REPLACE TEMP VIEW emp_delta USING delta LOCATION '{delta_path}'")
df_table = spark.sql("SELECT * FROM emp_delta")
df_table.show()
print("Same data, accessed via SQL table name")

# --- Make some updates for time travel demo ---
print("--- 3. Making changes (for time travel) ---")
# Update: give Alice a raise
dt = DeltaTable.forPath(spark, delta_path)
dt.update(condition=col("name") == "Alice", set={"salary": lit(120000.0)})
print("Version 1: Updated Alice salary to 120000")

# Insert: new employee
new_emp = spark.createDataFrame([(4, "Diana", 88000.0)], ["id", "name", "salary"])
new_emp.write.format("delta").mode("append").save(delta_path)
print("Version 2: Inserted Diana")

# Current state
print("\n--- Current state (latest version) ---")
spark.read.format("delta").load(delta_path).show()
print("Delta keeps history of ALL changes!")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Delta Time Travel
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: Delta Time Travel
# ═══════════════════════════════════════════════════════

print("=== Delta Time Travel ===")
print()

delta_path = "/tmp/format_demo/employees_delta"

# --- View history ---
print("--- 1. View Delta history (DESCRIBE HISTORY) ---")
history_df = spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`")
history_df.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

# --- Time travel by version number ---
print("--- 2. Read specific version (version 0 = original) ---")
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
df_v0.show()
print("This is the ORIGINAL data before any updates!")

# --- Read version 1 (after Alice update) ---
print("\n--- 3. Read version 1 (after Alice's raise) ---")
df_v1 = spark.read.format("delta").option("versionAsOf", 1).load(delta_path)
df_v1.show()

# --- Compare versions (audit trail!) ---
print("--- 4. Compare versions (who changed what?) ---")
df_v0_named = df_v0.withColumnRenamed("salary", "salary_before")
df_v1_named = df_v1.withColumnRenamed("salary", "salary_after")
df_diff = df_v0_named.join(df_v1_named.select("id", "salary_after"), "id")
df_diff = df_diff.filter(col("salary_before") != col("salary_after"))
df_diff.show()
print("Time travel lets you audit EVERY change!")

# --- DESCRIBE DETAIL (table metadata) ---
print("\n--- 5. DESCRIBE DETAIL (Delta metadata) ---")
detail = spark.sql(f"DESCRIBE DETAIL delta.`{delta_path}`")
detail.select("format", "numFiles", "sizeInBytes", "properties").show(truncate=False)
print("Use DESCRIBE DETAIL to see file count, size, properties")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Reading XML Files
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: Reading XML
# ═══════════════════════════════════════════════════════

print("=== Reading XML Files ===")
print()

# Create sample XML file
xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<employees>
  <employee>
    <id>1</id>
    <name>Alice</name>
    <department>Engineering</department>
    <salary>95000</salary>
    <skills>
      <skill>Python</skill>
      <skill>SQL</skill>
    </skills>
  </employee>
  <employee>
    <id>2</id>
    <name>Bob</name>
    <department>Marketing</department>
    <salary>72000</salary>
    <skills>
      <skill>Analytics</skill>
    </skills>
  </employee>
  <employee>
    <id>3</id>
    <name>Charlie</name>
    <department>Engineering</department>
    <salary>110000</salary>
    <skills>
      <skill>Python</skill>
      <skill>Spark</skill>
      <skill>Scala</skill>
    </skills>
  </employee>
</employees>"""

xml_path = "/tmp/format_demo/employees.xml"
dbutils.fs.put(xml_path, xml_content, overwrite=True)
print(f"XML written to: {xml_path}")

# --- Read XML with rowTag ---
print("\n--- 1. Basic XML read (rowTag = 'employee') ---")
df_xml = (
    spark.read.format("xml")
    .option("rowTag", "employee")  # Each <employee> = one DataFrame row!
    .load(xml_path)
)
df_xml.show(truncate=False)
df_xml.printSchema()
print("rowTag tells Spark which XML element represents a single row")

# --- Accessing nested XML fields ---
print("\n--- 2. Accessing fields ---")
df_xml.select("id", "name", "department", "salary").show()

# --- XML with attributes ---
print("\n--- 3. XML with attributes ---")
xml_attr = """<products>
  <product id="1" category="electronics"><name>Laptop</name><price>999.99</price></product>
  <product id="2" category="clothing"><name>Shirt</name><price>29.99</price></product>
</products>"""
attr_path = "/tmp/format_demo/products.xml"
dbutils.fs.put(attr_path, xml_attr, overwrite=True)

df_attr = spark.read.format("xml").option("rowTag", "product").load(attr_path)
df_attr.show(truncate=False)
df_attr.printSchema()
print("Attributes become columns prefixed with '_' (e.g., _id, _category)")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Delta Advanced Features
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Delta Advanced Features
# ═══════════════════════════════════════════════════════

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, when, current_timestamp

print("=== Delta Advanced: MERGE, VACUUM, OPTIMIZE ===")
print()

# Create a Delta table for CDC (Change Data Capture) demo
delta_path = "/tmp/format_demo/customers_delta"
data = [(1,"Alice","NYC","active"), (2,"Bob","LA","active"), (3,"Charlie","CHI","active")]
df = spark.createDataFrame(data, ["id","name","city","status"])
df.write.format("delta").mode("overwrite").save(delta_path)

# --- MERGE (Upsert) ---
print("--- 1. MERGE (Upsert: insert + update in one operation) ---")
updates = spark.createDataFrame([
    (2, "Bob", "SF", "active"),      # Bob moved to SF (UPDATE)
    (4, "Diana", "BOS", "active"),   # New customer (INSERT)
    (3, "Charlie", "CHI", "inactive")  # Charlie deactivated (UPDATE)
], ["id", "name", "city", "status"])

dt = DeltaTable.forPath(spark, delta_path)
dt.alias("target").merge(
    updates.alias("source"),
    "target.id = source.id"  # Match condition
).whenMatchedUpdateAll(  # If row exists: update all columns
).whenNotMatchedInsertAll(  # If row is new: insert all columns
).execute()

print("After MERGE:")
spark.read.format("delta").load(delta_path).show()

# --- OPTIMIZE (compact small files) ---
print("--- 2. OPTIMIZE (compact small files for performance) ---")
spark.sql(f"OPTIMIZE delta.`{delta_path}`")
print("Small files merged into larger ones (better read performance)")
detail = spark.sql(f"DESCRIBE DETAIL delta.`{delta_path}")
detail.select("numFiles").show()

# --- VACUUM (remove old versions' files) ---
print("--- 3. VACUUM (cleanup old files) ---")
print("  spark.sql(f'VACUUM delta.`{path}` RETAIN 168 HOURS')")
print("  Default retention: 7 days (168 hours)")
print("  WARNING: After VACUUM, time travel before retained period is GONE!")
print("  NEVER set retention below 7 days in production!")

# --- Schema enforcement ---
print("\n--- 4. Schema Enforcement (Delta rejects bad data!) ---")
try:
    bad_data = spark.createDataFrame([(5, "Eve", "DAL", "active", 99.9)],
                                     ["id", "name", "city", "status", "score"])  # Extra column!
    bad_data.write.format("delta").mode("append").save(delta_path)
    print("This should NOT succeed!")
except Exception as e:
    print(f"  REJECTED! Error: {str(e)[:100]}...")
    print("  Delta enforces schema by default (unlike Parquet!)")
    print("  Use mergeSchema=true to allow new columns")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Reading Excel Files
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Reading Excel Files
# ═══════════════════════════════════════════════════════

import pandas as pd

print("=== Reading Excel Files ===")
print()

# --- Method 1: Via Pandas (most common approach) ---
print("--- 1. Excel via Pandas (recommended) ---")
# Create a sample Excel file using Pandas
pd_df = pd.DataFrame({
    "Product": ["Laptop", "Phone", "Tablet", "Watch"],
    "Price": [999.99, 699.99, 449.99, 299.99],
    "Stock": [50, 200, 75, 300],
    "Category": ["Electronics", "Electronics", "Electronics", "Wearable"]
})

excel_path = "/tmp/format_demo/products.xlsx"
pd_df.to_excel(f"/dbfs{excel_path}", index=False)  # Write with Pandas
print(f"Excel written to: {excel_path}")

# Read Excel with Pandas, then convert to Spark DataFrame
pd_read = pd.read_excel(f"/dbfs{excel_path}", engine="openpyxl")
df_excel = spark.createDataFrame(pd_read)
df_excel.show()
df_excel.printSchema()
print("Pattern: Pandas reads Excel → convert to Spark DF")

# --- Method 2: Multiple sheets ---
print("\n--- 2. Reading specific Excel sheets ---")
# Create multi-sheet Excel
with pd.ExcelWriter(f"/dbfs/tmp/format_demo/multi_sheet.xlsx") as writer:
    pd_df.to_excel(writer, sheet_name="Products", index=False)
    pd.DataFrame({"Region": ["US", "EU"], "Revenue": [1000000, 750000]}).to_excel(
        writer, sheet_name="Revenue", index=False)

print("  Reading sheet 'Products':")
df_products = spark.createDataFrame(pd.read_excel(
    "/dbfs/tmp/format_demo/multi_sheet.xlsx", sheet_name="Products"))
df_products.show()

print("  Reading sheet 'Revenue':")
df_revenue = spark.createDataFrame(pd.read_excel(
    "/dbfs/tmp/format_demo/multi_sheet.xlsx", sheet_name="Revenue"))
df_revenue.show()

# --- Best practices ---
print("--- 3. Excel reading best practices ---")
print("  1. Use Pandas as intermediary (most reliable)")
print("  2. Specify dtypes to avoid inference issues")
print("  3. Skip header rows: pd.read_excel(path, skiprows=2)")
print("  4. Handle merged cells: they become NaN in Pandas")
print("  5. For large files (>100MB): convert to CSV first, then use Spark CSV reader")
print("  6. Alternative: com.crealytics:spark-excel library for native Spark reading")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Binary Files as DataFrames
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Binary Files (Images, PDFs)
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, length, regexp_extract, split

print("=== Reading Binary Files ===")
print()

# Create sample binary files (simulating images)
for i in range(5):
    content = f"FAKE_IMAGE_DATA_{i}_" + "x" * (1000 * (i + 1))  # Varying sizes
    dbutils.fs.put(f"/tmp/format_demo/images/img_{i:03d}.png", content, overwrite=True)
print("Created 5 sample 'image' files")

# --- Read binary files ---
print("\n--- 1. Read binary files ---")
df_binary = spark.read.format("binaryFile").load("/tmp/format_demo/images/")
df_binary.show(truncate=False)
print("\nSchema:")
df_binary.printSchema()

# --- Analyze file metadata ---
print("--- 2. File metadata analysis ---")
df_meta = df_binary.select(
    regexp_extract("path", r"([^/]+)$", 1).alias("filename"),  # Extract filename
    col("length").alias("size_bytes"),  # File size
    col("modificationTime").alias("modified"),  # Last modified
)
df_meta.show(truncate=False)

# --- Filter by size or extension ---
print("--- 3. Filter by file properties ---")
df_large = df_binary.filter(col("length") > 3000)  # Files > 3KB
print(f"  Files > 3KB: {df_large.count()}")

df_png = df_binary.filter(col("path").endswith(".png"))  # Only PNGs
print(f"  PNG files: {df_png.count()}")

# --- Glob pattern for selective reading ---
print("\n--- 4. Glob patterns ---")
# Read only specific files
df_subset = spark.read.format("binaryFile").option(
    "pathGlobFilter", "img_00[0-2].*"  # Only img_000, img_001, img_002
).load("/tmp/format_demo/images/")
print(f"  Glob pattern match: {df_subset.count()} files")

# --- Recursive directory reading ---
print("\n--- 5. Recursive reading ---")
print("  spark.read.format('binaryFile')")
print("       .option('recursiveFileLookup', 'true')")
print("       .load('/base/path/')")
print("  Reads ALL files in ALL subdirectories")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Delta as a Table vs Path
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Delta Table vs Path-Based Access
# ═══════════════════════════════════════════════════════

print("=== Delta: Table Access Patterns ===")
print()

# --- Three ways to access Delta ---
print("--- 1. Three ways to read Delta data ---")
print()
print("  # Method A: By path (raw files)")
print("  df = spark.read.format('delta').load('/path/to/delta')")
print()
print("  # Method B: SQL table (registered in metastore)")
print("  df = spark.table('catalog.schema.table_name')")
print()
print("  # Method C: SQL query")
print("  df = spark.sql('SELECT * FROM catalog.schema.table_name')")

# --- Demonstrate table registration ---
print("\n--- 2. Path-based vs Table-based ---")
delta_path = "/tmp/format_demo/employees_delta"

# Path-based
df_path = spark.read.format("delta").load(delta_path)
print(f"  Path-based: {df_path.count()} rows")

# Register as temp table
spark.sql(f"CREATE OR REPLACE TEMP VIEW emp_view USING DELTA LOCATION '{delta_path}'")
df_view = spark.sql("SELECT * FROM emp_view WHERE status = 'active'")
print(f"  SQL view:   {df_view.count()} active rows")

# --- Unity Catalog table (production pattern) ---
print("\n--- 3. Unity Catalog pattern (production) ---")
print("  # Best practice: Use 3-level namespace")
print("  df = spark.table('my_catalog.my_schema.my_table')")
print()
print("  # Behind the scenes, UC maps this to a managed Delta location")
print("  # Benefits:")
print("  #   - Access control (GRANT/REVOKE)")
print("  #   - Lineage tracking")
print("  #   - Discovery (search, tags, comments)")
print("  #   - Cross-workspace sharing")

# --- Delta properties ---
print("\n--- 4. Key Delta table properties ---")
props = [
    ("delta.autoOptimize.optimizeWrite", "true", "Auto-compact files on write"),
    ("delta.autoOptimize.autoCompact", "true", "Auto-run OPTIMIZE"),
    ("delta.logRetentionDuration", "interval 30 days", "Keep 30 days of history"),
    ("delta.deletedFileRetentionDuration", "interval 7 days", "VACUUM after 7 days"),
    ("delta.enableChangeDataFeed", "true", "Track row-level changes"),
]
print(f"  {'Property':<45} {'Value':<15} {'Purpose'}")
print(f"  {'-'*85}")
for prop, val, purpose in props:
    print(f"  {prop:<45} {val:<15} {purpose}")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Complex XML with Namespaces
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Complex XML Parsing
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, explode, explode_outer
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

print("=== Complex XML: Nested Structures & Explicit Schema ===")
print()

# --- Deeply nested XML ---
print("--- 1. Deeply nested XML ---")
complex_xml = """<orders>
  <order>
    <id>1001</id>
    <customer>
      <name>Alice Corp</name>
      <tier>premium</tier>
    </customer>
    <items>
      <item><sku>A1</sku><qty>5</qty><price>10.99</price></item>
      <item><sku>B2</sku><qty>2</qty><price>25.50</price></item>
    </items>
    <shipping>
      <method>express</method>
      <cost>15.00</cost>
    </shipping>
  </order>
  <order>
    <id>1002</id>
    <customer>
      <name>Bob LLC</name>
      <tier>standard</tier>
    </customer>
    <items>
      <item><sku>C3</sku><qty>10</qty><price>5.00</price></item>
    </items>
    <shipping>
      <method>standard</method>
      <cost>5.00</cost>
    </shipping>
  </order>
</orders>"""

dbutils.fs.put("/tmp/format_demo/orders.xml", complex_xml, overwrite=True)

df_orders = spark.read.format("xml").option("rowTag", "order").load("/tmp/format_demo/orders.xml")
df_orders.show(truncate=False)
df_orders.printSchema()

# --- Flatten nested structures ---
print("\n--- 2. Flattening nested XML ---")
df_flat = df_orders.select(
    col("id"),
    col("customer.name").alias("customer_name"),
    col("customer.tier").alias("customer_tier"),
    col("shipping.method").alias("ship_method"),
    col("shipping.cost").alias("ship_cost"),
    explode("items.item").alias("item")  # Explode array of items
).select("id", "customer_name", "customer_tier", "ship_method", "ship_cost",
         col("item.sku"), col("item.qty"), col("item.price"))
df_flat.show()
print("Nested XML flattened into a tabular DataFrame!")

# --- Reading XML with explicit schema ---
print("\n--- 3. XML with explicit schema (recommended) ---")
xml_schema = StructType([
    StructField("id", IntegerType()),
    StructField("customer", StructType([
        StructField("name", StringType()),
        StructField("tier", StringType()),
    ])),
    StructField("items", StructType([
        StructField("item", ArrayType(StructType([
            StructField("sku", StringType()),
            StructField("qty", IntegerType()),
            StructField("price", StringType()),  # Deliberately string to show type control
        ])))
    ])),
])

df_typed = spark.read.format("xml").option("rowTag", "order").schema(xml_schema).load("/tmp/format_demo/orders.xml")
df_typed.printSchema()
print("Explicit schema gives you full control over types")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production Multi-Format Ingestion
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Production Multi-Format Ingestion
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import input_file_name, current_timestamp, lit, col
import json

print("=== Production: Multi-Format Ingest to Delta ===")
print()

def ingest_to_delta(spark, source_path, source_format, target_path,
                    schema=None, options=None, partition_cols=None):
    """
    Production ingestion: Read any format -> validate -> write as Delta.
    
    Args:
        source_path: Input file/folder path
        source_format: json/parquet/orc/avro/csv/xml
        target_path: Delta output path
        schema: Optional explicit schema
        options: Dict of format-specific options
        partition_cols: Optional list of partition columns
    """
    options = options or {}
    
    # Build reader with format-specific configuration
    reader = spark.read.format(source_format)
    if schema:
        reader = reader.schema(schema)
    for k, v in options.items():
        reader = reader.option(k, str(v))
    
    # Read source data
    df = reader.load(source_path)
    
    # Add audit columns (standard for all ingestion)
    df = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_source_format", lit(source_format))
    )
    
    # Validate: check for empty DataFrame
    row_count = df.count()
    if row_count == 0:
        raise ValueError(f"No data found at {source_path}!")
    
    # Write to Delta
    writer = df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(target_path)
    
    return {"rows": row_count, "columns": len(df.columns), "target": target_path}

# --- Demo: Ingest from multiple formats into Delta ---
print("--- Ingesting multiple formats to Delta ---")

# Ingest JSON
result = ingest_to_delta(
    spark, "/tmp/format_demo/users.json", "json",
    "/tmp/format_demo/delta_lake/users"
)
print(f"  JSON → Delta: {result}")

# Ingest Parquet
result = ingest_to_delta(
    spark, "/tmp/format_demo/users.parquet", "parquet",
    "/tmp/format_demo/delta_lake/products"
)
print(f"  Parquet → Delta: {result}")

# Ingest XML
result = ingest_to_delta(
    spark, "/tmp/format_demo/employees.xml", "xml",
    "/tmp/format_demo/delta_lake/employees",
    options={"rowTag": "employee"}
)
print(f"  XML → Delta: {result}")

# Verify everything is Delta now
print("\n--- All data now in Delta format ---")
for table in ["users", "products", "employees"]:
    df = spark.read.format("delta").load(f"/tmp/format_demo/delta_lake/{table}")
    print(f"  {table}: {df.count()} rows, {len(df.columns)} columns")

print("\n--- Key takeaway: Raw formats land once, then ALWAYS Delta ---")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Not Using Delta for Your Data Lake
# MAGIC **Problem:** Storing everything as Parquet/CSV means no transactions, no time travel, no schema enforcement.  
# MAGIC **Fix:** Always write final data as Delta. Use `df.write.format("delta")` everywhere.
# MAGIC
# MAGIC ### Mistake #2: VACUUM with Too Short Retention
# MAGIC **Problem:** `VACUUM RETAIN 0 HOURS` deletes ALL history immediately — time travel stops working.  
# MAGIC **Fix:** Keep minimum 7 days (168 hours). Never go below unless you're absolutely sure.
# MAGIC
# MAGIC ### Mistake #3: Wrong rowTag for XML
# MAGIC **Problem:** Using the root element as rowTag gives you 1 row with everything nested inside.  
# MAGIC **Fix:** rowTag should be the repeating element (e.g., `<employee>` not `<employees>`).
# MAGIC
# MAGIC ### Mistake #4: Reading Excel Files Directly with Spark
# MAGIC **Problem:** Spark has no built-in Excel reader — using `spark.read.excel()` throws errors.  
# MAGIC **Fix:** Use Pandas as intermediary: `spark.createDataFrame(pd.read_excel(path))`.
# MAGIC
# MAGIC ### Mistake #5: Ignoring Delta Schema Enforcement
# MAGIC **Problem:** Appending data with extra/missing columns fails silently or throws errors.  
# MAGIC **Fix:** Use `mergeSchema=true` when intentionally evolving schema, or fix upstream data.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC **Level 1:** Create a Delta table and read it back. Print its schema.
# MAGIC
# MAGIC **Level 2:** Make 3 changes to a Delta table. Use DESCRIBE HISTORY to see all versions.
# MAGIC
# MAGIC **Level 3:** Use time travel to read version 0 of your Delta table. Compare with the latest version.
# MAGIC
# MAGIC **Level 4:** Perform a MERGE (upsert) on a Delta table with new + updated records.
# MAGIC
# MAGIC **Level 5:** Create an XML file with nested elements. Read it and flatten into a tabular format.
# MAGIC
# MAGIC **Level 6:** Read an Excel file with multiple sheets. Combine them into a single DataFrame.
# MAGIC
# MAGIC **Level 7:** Read binary files from a folder. Filter by size > 5KB and compute total size.
# MAGIC
# MAGIC **Level 8:** Build an ingestion pipeline: read JSON → validate schema → write Delta → run OPTIMIZE.
# MAGIC
# MAGIC **Level 9:** Implement Change Data Feed on a Delta table. Track all inserts, updates, and deletes.
# MAGIC
# MAGIC **Level 10:** Design a data lakehouse landing zone: raw format → Bronze Delta → Silver Delta with full audit trail.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, sum as _sum, current_timestamp

# Level 2: History demo
print("=== Level 2: Delta History ===")
hw_path = "/tmp/format_demo/hw_delta"
df1 = spark.createDataFrame([(1,"A",100),(2,"B",200)], ["id","name","val"])
df1.write.format("delta").mode("overwrite").save(hw_path)

# Change 1: Update
dt = DeltaTable.forPath(spark, hw_path)
dt.update(col("id")==1, {"val": lit(150)})
# Change 2: Insert
spark.createDataFrame([(3,"C",300)],["id","name","val"]).write.format("delta").mode("append").save(hw_path)
# Change 3: Delete
dt.delete(col("id")==2)

spark.sql(f"DESCRIBE HISTORY delta.`{hw_path}`").select("version","operation").show()

# Level 3: Time travel
print("\n=== Level 3: Time Travel ===")
df_v0 = spark.read.format("delta").option("versionAsOf",0).load(hw_path)
df_latest = spark.read.format("delta").load(hw_path)
print("Version 0:")
df_v0.show()
print("Latest:")
df_latest.show()

# Level 4: MERGE
print("\n=== Level 4: MERGE ===")
merge_data = spark.createDataFrame([(1,"A",175),(4,"D",400)],["id","name","val"])
dt = DeltaTable.forPath(spark, hw_path)
dt.alias("t").merge(merge_data.alias("s"),"t.id=s.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
spark.read.format("delta").load(hw_path).show()

# Level 7: Binary file analysis
print("\n=== Level 7: Binary Files ===")
df_bin = spark.read.format("binaryFile").load("/tmp/format_demo/images/")
df_large = df_bin.filter(col("length") > 3000)  # > 3KB (our simulated threshold)
total_size = df_large.agg(_sum("length")).collect()[0][0]
print(f"Files > 3KB: {df_large.count()}, Total size: {total_size:,} bytes")

# Level 9: Change Data Feed
print("\n=== Level 9: Change Data Feed ===")
cdf_path = "/tmp/format_demo/cdf_demo"
spark.createDataFrame([(1,"X"),(2,"Y")],["id","val"]).write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed","true").save(cdf_path)
# Make a change
dt_cdf = DeltaTable.forPath(spark, cdf_path)
dt_cdf.update(col("id")==1, {"val": lit("Z")})
# Read changes
changes = spark.read.format("delta").option("readChangeFeed","true").option("startingVersion",1).load(cdf_path)
changes.show()
print("Change Data Feed tracks: _change_type, _commit_version, _commit_timestamp")

print("\n\u2705 All homework complete!")