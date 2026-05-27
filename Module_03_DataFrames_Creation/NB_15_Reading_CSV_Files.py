# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 15: Reading CSV Files (Every Option)
# MAGIC # Module: DataFrames — Creation & Basics
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 45 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: Opening a Spreadsheet with Messy Formatting
# MAGIC
# MAGIC Reading a CSV is like opening a spreadsheet someone emailed you:
# MAGIC - Does the first row have column names? (header)
# MAGIC - Are values separated by commas, semicolons, or tabs? (delimiter)
# MAGIC - Are numbers formatted as text? (type inference)
# MAGIC - Are dates written as "01/15/2024" or "2024-01-15"? (date format)
# MAGIC - What happens when a row is broken? (error handling)
# MAGIC
# MAGIC Spark's CSV reader handles ALL of these variations with **options**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Basic Pattern
# MAGIC
# MAGIC ```python
# MAGIC df = spark.read.csv("path/to/file.csv")
# MAGIC # OR with all options:
# MAGIC df = (
# MAGIC     spark.read
# MAGIC     .format("csv")
# MAGIC     .option("header", "true")
# MAGIC     .option("inferSchema", "true")
# MAGIC     .option("delimiter", ",")
# MAGIC     .load("path/to/file.csv")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### All CSV Options Reference
# MAGIC
# MAGIC | Option | Default | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | `header` | false | First row = column names? |
# MAGIC | `inferSchema` | false | Guess column types? |
# MAGIC | `delimiter` / `sep` | `,` | Field separator |
# MAGIC | `quote` | `"` | Quote character for fields containing delimiter |
# MAGIC | `escape` | `\\` | Escape char inside quoted fields |
# MAGIC | `multiLine` | false | Can a field span multiple lines? |
# MAGIC | `nullValue` | (empty) | String that represents null |
# MAGIC | `dateFormat` | yyyy-MM-dd | How to parse date columns |
# MAGIC | `timestampFormat` | yyyy-MM-dd'T'HH:mm:ss | How to parse timestamps |
# MAGIC | `encoding` | UTF-8 | File character encoding |
# MAGIC | `mode` | PERMISSIVE | Error handling mode |
# MAGIC | `columnNameOfCorruptRecord` | _corrupt_record | Column for bad rows |
# MAGIC | `ignoreLeadingWhiteSpace` | false | Trim leading spaces |
# MAGIC | `ignoreTrailingWhiteSpace` | false | Trim trailing spaces |
# MAGIC | `comment` | (none) | Character that marks comment lines |

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### What Happens When You Read a CSV
# MAGIC
# MAGIC ```
# MAGIC   spark.read.option("header", "true").option("inferSchema", "true").csv(path)
# MAGIC        │
# MAGIC        ├─ 1. Locate file(s): DBFS, ADLS, S3, local path
# MAGIC        │      Supports wildcards: "/data/2024/*.csv"
# MAGIC        │
# MAGIC        ├─ 2. Header detection:
# MAGIC        │      header=true → first row becomes column names
# MAGIC        │      header=false → columns named _c0, _c1, _c2...
# MAGIC        │
# MAGIC        ├─ 3. Schema:
# MAGIC        │      inferSchema=true → Spark reads data TWICE (once to guess types)
# MAGIC        │      explicit schema → Spark reads data ONCE (faster!)
# MAGIC        │      neither → all columns are StringType
# MAGIC        │
# MAGIC        ├─ 4. Parsing: Each row split by delimiter, types applied
# MAGIC        │
# MAGIC        ├─ 5. Error handling:
# MAGIC        │      PERMISSIVE → bad rows get nulls + _corrupt_record
# MAGIC        │      DROPMALFORMED → bad rows silently dropped
# MAGIC        │      FAILFAST → first bad row throws exception
# MAGIC        │
# MAGIC        └─ 6. Returns DataFrame (lazy — actual reading on first action)
# MAGIC ```
# MAGIC
# MAGIC ### The inferSchema Double-Read Problem
# MAGIC
# MAGIC ```
# MAGIC   Without inferSchema: ONE pass through data
# MAGIC   With inferSchema:    TWO passes through data!
# MAGIC     Pass 1: Read sample to guess types
# MAGIC     Pass 2: Actually parse data with guessed types
# MAGIC   
# MAGIC   For a 100GB CSV:
# MAGIC     Without: ~5 minutes
# MAGIC     With inferSchema: ~10 minutes (2x slower!)
# MAGIC   
# MAGIC   FIX: Provide explicit schema (zero extra passes)
# MAGIC ```
# MAGIC
# MAGIC ### Error Handling Modes
# MAGIC
# MAGIC | Mode | Behavior | Use Case |
# MAGIC |------|----------|----------|
# MAGIC | PERMISSIVE (default) | Keep row, put raw text in _corrupt_record column | Debug, investigate |
# MAGIC | DROPMALFORMED | Silently drop bad rows | Quick-and-dirty, don't care about losses |
# MAGIC | FAILFAST | Throw exception on first bad row | Strict validation, CI/CD |

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Basic CSV Read
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: Basic CSV Reading
# ═══════════════════════════════════════════════════════

print("=== Basic CSV Reading ===")
print()

# First, create a sample CSV file to read
csv_content = """name,age,city,salary
Alice,30,London,95000.50
Bob,25,New York,72000.00
Charlie,35,Paris,110000.75
Diana,28,Tokyo,68000.25
Eve,32,Berlin,85000.00"""

# Write the CSV to a temp path
csv_path = "/tmp/csv_demo/basic.csv"
dbutils.fs.put(csv_path, csv_content, overwrite=True)  # Write file
print(f"CSV written to: {csv_path}")

# --- Read 1: Minimal (no options) --- 
print("\n--- Read 1: No options (worst) ---")
df_raw = spark.read.csv(csv_path)  # No header, no type inference
df_raw.show()  # Column names are _c0, _c1, _c2, _c3
df_raw.printSchema()  # All types are string
print("Problem: Header row is data, columns are _c0/_c1, all strings")

# --- Read 2: With header --- 
print("\n--- Read 2: header=true ---")
df_header = spark.read.option("header", "true").csv(csv_path)
df_header.show()  # Column names are correct now!
df_header.printSchema()  # But types are still all strings
print("Better: Column names correct, but still all strings")

# --- Read 3: With header + inferSchema ---
print("\n--- Read 3: header=true + inferSchema=true ---")
df_infer = (
    spark.read
    .option("header", "true")       # First row = column names
    .option("inferSchema", "true")  # Guess column types
    .csv(csv_path)
)
df_infer.show()  # Correct names AND types!
df_infer.printSchema()  # age=int, salary=double, etc.
print("Best for exploration: Correct names and types")
print("But SLOW for production (reads data twice)")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Explicit Schema (Production)
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: Reading with Explicit Schema
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

print("=== Reading CSV with Explicit Schema (Production Way) ===")
print()

# Define EXACTLY what we expect
employee_schema = StructType([
    StructField("name", StringType(), True),       # Text, can be null
    StructField("age", IntegerType(), True),       # Whole number
    StructField("city", StringType(), True),       # Text
    StructField("salary", DoubleType(), True),     # Decimal number
])

# Read with our explicit schema
csv_path = "/tmp/csv_demo/basic.csv"  # Same file as before
df = (
    spark.read
    .option("header", "true")    # Skip the header row
    .schema(employee_schema)      # Use OUR types (no inference scan!)
    .csv(csv_path)
)

df.show()
df.printSchema()

# Why this is better:
print("\n--- Why explicit schema is better ---")
print("  1. FASTER: No extra scan to infer types")
print("  2. CORRECT: You decide the types, not Spark's guesswork")
print("  3. SAFE: Schema doesn't change if data changes")
print("  4. DOCUMENTED: Schema IS the documentation")

# DDL shorthand (same result, less code)
print("\n--- DDL shorthand alternative ---")
ddl = "name STRING, age INT, city STRING, salary DOUBLE"
df_ddl = spark.read.option("header", "true").schema(ddl).csv(csv_path)
df_ddl.printSchema()  # Same result!
print("DDL string produces identical schema with less code")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Common Options
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: Delimiter, Quote, Null Handling
# ═══════════════════════════════════════════════════════

print("=== CSV Options: Delimiter, Quote, Null ===")
print()

# --- Semicolon-delimited CSV (European style) ---
print("--- 1. Semicolon delimiter (European format) ---")
euro_csv = """name;amount;date
Alice;1.234,56;15.01.2024
Bob;2.345,67;20.03.2024
Charlie;NULL;01.06.2024"""

euro_path = "/tmp/csv_demo/european.csv"
dbutils.fs.put(euro_path, euro_csv, overwrite=True)

df_euro = (
    spark.read
    .option("header", "true")
    .option("delimiter", ";")      # Semicolon separator (not comma!)
    .option("nullValue", "NULL")   # Treat string "NULL" as actual null
    .csv(euro_path)
)
df_euro.show(truncate=False)
print("Note: 'NULL' string became actual null value")

# --- Quoted fields (fields containing the delimiter) ---
print("\n--- 2. Quoted fields (names with commas) ---")
quoted_csv = '''id,name,description
1,"Smith, John","A description with a, comma"
2,"O'Brien, Mary","She said ""hello"""
3,"Normal Name",Simple description'''

quoted_path = "/tmp/csv_demo/quoted.csv"
dbutils.fs.put(quoted_path, quoted_csv, overwrite=True)

df_quoted = (
    spark.read
    .option("header", "true")
    .option("quote", '"')          # Double-quote encloses fields
    .option("escape", '"')         # Doubled quotes inside = escaped
    .csv(quoted_path)
)
df_quoted.show(truncate=False)
print("Commas inside quotes are preserved, not treated as delimiters")

# --- Tab-delimited (TSV) ---
print("\n--- 3. Tab-delimited (TSV) ---")
tsv_content = "name\tage\tcity\nAlice\t30\tLondon\nBob\t25\tParis"
tsv_path = "/tmp/csv_demo/data.tsv"
dbutils.fs.put(tsv_path, tsv_content, overwrite=True)

df_tsv = spark.read.option("header", "true").option("delimiter", "\t").csv(tsv_path)
df_tsv.show()
print("Tab-separated values read correctly")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Date/Timestamp Formats
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Date and Timestamp Parsing
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, DoubleType

print("=== CSV Date and Timestamp Parsing ===")
print()

# Different date formats around the world:
# US: MM/dd/yyyy     Europe: dd.MM.yyyy     ISO: yyyy-MM-dd
# Timestamps: dd/MM/yyyy HH:mm:ss, yyyy-MM-dd'T'HH:mm:ss

# --- US-format dates ---
print("--- 1. US-format dates (MM/dd/yyyy) ---")
us_csv = """event,event_date,amount
Purchase,01/15/2024,99.99
Refund,02/28/2024,45.50
Subscription,12/01/2023,120.00"""
us_path = "/tmp/csv_demo/us_dates.csv"
dbutils.fs.put(us_path, us_csv, overwrite=True)

us_schema = StructType([
    StructField("event", StringType()),
    StructField("event_date", DateType()),
    StructField("amount", DoubleType()),
])

df_us = (
    spark.read
    .option("header", "true")
    .option("dateFormat", "MM/dd/yyyy")  # Tell Spark the date format!
    .schema(us_schema)
    .csv(us_path)
)
df_us.show()
df_us.printSchema()  # event_date is DateType, properly parsed!

# --- European-format dates ---
print("--- 2. European dates (dd.MM.yyyy) ---")
eu_csv = """sensor,reading_time,value
temp_01,15.01.2024 10:30:45,22.5
temp_02,20.03.2024 14:15:00,25.1"""
eu_path = "/tmp/csv_demo/eu_dates.csv"
dbutils.fs.put(eu_path, eu_csv, overwrite=True)

eu_schema = StructType([
    StructField("sensor", StringType()),
    StructField("reading_time", TimestampType()),
    StructField("value", DoubleType()),
])

df_eu = (
    spark.read
    .option("header", "true")
    .option("timestampFormat", "dd.MM.yyyy HH:mm:ss")  # European timestamp!
    .schema(eu_schema)
    .csv(eu_path)
)
df_eu.show(truncate=False)
df_eu.printSchema()

# --- What happens with WRONG format ---
print("--- 3. Wrong format = null values! ---")
df_wrong = (
    spark.read
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd")  # Wrong format for US dates!
    .schema(us_schema)
    .csv(us_path)
)
df_wrong.show()  # Dates are NULL because format didn't match!
print("⚠️  Wrong dateFormat silently produces nulls!")
print("  Always verify dates are not null after reading.")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Error Handling Modes
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Handling Bad/Corrupt CSV Records
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col

print("=== Handling Bad Records in CSV ===")
print()

# Create a CSV with intentionally BAD records
bad_csv = """id,name,age,salary
1,Alice,30,95000.50
2,Bob,twenty-five,72000.00
3,Charlie,35,not_a_number
this_line_has_wrong_column_count
5,Eve,32,85000.00
6,,,-"""

bad_path = "/tmp/csv_demo/bad_data.csv"
dbutils.fs.put(bad_path, bad_csv, overwrite=True)
print(f"Written CSV with bad records to: {bad_path}")

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True),
    StructField("_corrupt_record", StringType(), True),  # Catch bad rows here
])

# --- Mode 1: PERMISSIVE (default) ---
print("\n--- Mode 1: PERMISSIVE (keep everything, flag bad rows) ---")
df_perm = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(schema)
    .csv(bad_path)
)
df_perm.show(truncate=False)

# Separate good and bad
print("Good records:")
df_perm.filter(col("_corrupt_record").isNull()).drop("_corrupt_record").show()
print("Bad records:")
df_perm.filter(col("_corrupt_record").isNotNull()).select("_corrupt_record").show(truncate=False)

# --- Mode 2: DROPMALFORMED ---
print("\n--- Mode 2: DROPMALFORMED (silently drop bad rows) ---")
schema_no_corrupt = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True),
])
df_drop = (
    spark.read
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .schema(schema_no_corrupt)
    .csv(bad_path)
)
df_drop.show()
print(f"Only {df_drop.count()} rows survived (bad ones silently dropped)")

print("\n--- Production Pattern ---")
print("  1. Use PERMISSIVE + _corrupt_record column")
print("  2. Write corrupt records to a 'quarantine' table")
print("  3. Alert on quarantine count > threshold")
print("  4. Process only clean records downstream")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Reading Multiple Files and Wildcards
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Multiple Files, Wildcards, Folders
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import input_file_name

print("=== Reading Multiple CSV Files ===")
print()

# Create multiple CSV files (simulating daily data)
jan_csv = "date,product,revenue\n2024-01-01,Widget,100\n2024-01-02,Gadget,200"
feb_csv = "date,product,revenue\n2024-02-01,Widget,150\n2024-02-02,Tool,300"
mar_csv = "date,product,revenue\n2024-03-01,Gadget,250\n2024-03-02,Widget,175"

base_path = "/tmp/csv_demo/multi"
dbutils.fs.put(f"{base_path}/sales_jan.csv", jan_csv, overwrite=True)
dbutils.fs.put(f"{base_path}/sales_feb.csv", feb_csv, overwrite=True)
dbutils.fs.put(f"{base_path}/sales_mar.csv", mar_csv, overwrite=True)
print(f"Created 3 CSV files in {base_path}/")

# --- Method 1: Read entire folder ---
print("\n--- 1. Read entire folder (all CSVs) ---")
df_all = spark.read.option("header", "true").csv(f"{base_path}/")  # Trailing slash = all files
df_all.show()
print(f"Total rows from all files: {df_all.count()}")

# --- Method 2: Wildcard pattern ---
print("\n--- 2. Wildcard pattern (sales_*.csv) ---")
df_wild = spark.read.option("header", "true").csv(f"{base_path}/sales_*.csv")  # Glob pattern
df_wild.show()

# --- Method 3: List of specific paths ---
print("\n--- 3. Specific file list ---")
df_list = spark.read.option("header", "true").csv(
    f"{base_path}/sales_jan.csv",
    f"{base_path}/sales_mar.csv"  # Skip February!
)
df_list.show()
print("Only Jan + Mar (skipped Feb)")

# --- Method 4: Track source file ---
print("\n--- 4. Track which file each row came from ---")
df_tracked = (
    spark.read.option("header", "true").csv(f"{base_path}/")
    .withColumn("source_file", input_file_name())  # Add source tracking
)
df_tracked.show(truncate=50)
print("input_file_name() shows the origin of each row")

print("\n--- Wildcard Patterns ---")
print("  /data/*.csv            = all CSVs in folder")
print("  /data/2024-*.csv       = files starting with 2024-")
print("  /data/sales_??.csv     = sales_ + exactly 2 chars")
print("  /data/year=*/month=*/  = partitioned folders")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Production-Grade CSV Reader
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Production CSV Reader Function
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, current_timestamp, input_file_name

print("=== Production-Grade CSV Reader ===")
print()

def read_csv_production(spark, path, schema, **options):
    """
    Production-grade CSV reader with:
    - Explicit schema (no inference)
    - Corrupt record capture
    - Source file tracking
    - Audit columns (load timestamp)
    - Configurable options
    """
    # Add _corrupt_record to schema for error capture
    full_schema = StructType(
        schema.fields + [StructField("_corrupt_record", StringType(), True)]
    )
    
    # Default options (can be overridden)
    default_options = {
        "header": "true",
        "mode": "PERMISSIVE",
        "columnNameOfCorruptRecord": "_corrupt_record",
        "ignoreLeadingWhiteSpace": "true",
        "ignoreTrailingWhiteSpace": "true",
    }
    default_options.update(options)  # Override with user options
    
    # Build the reader
    reader = spark.read.schema(full_schema)
    for key, value in default_options.items():
        reader = reader.option(key, value)
    
    # Read the data
    df = reader.csv(path)
    
    # Add audit columns
    df = (
        df
        .withColumn("_source_file", input_file_name())      # Which file
        .withColumn("_load_timestamp", current_timestamp()) # When loaded
    )
    
    # Separate clean and corrupt
    df_clean = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
    df_corrupt = df.filter(col("_corrupt_record").isNotNull()).select(
        "_corrupt_record", "_source_file", "_load_timestamp"
    )
    
    # Report
    total = df.count()
    clean_count = df_clean.count()
    corrupt_count = df_corrupt.count()
    
    print(f"  Total rows read: {total}")
    print(f"  Clean rows: {clean_count}")
    print(f"  Corrupt rows: {corrupt_count}")
    if corrupt_count > 0:
        print(f"  ⚠️  {corrupt_count} corrupt records found!")
    
    return df_clean, df_corrupt

# --- Use the production reader ---
from pyspark.sql.types import IntegerType, DoubleType

my_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("salary", DoubleType()),
])

print("Reading with production reader:")
df_clean, df_corrupt = read_csv_production(
    spark, "/tmp/csv_demo/bad_data.csv", my_schema
)

print("\nClean data:")
df_clean.show()
print("\nCorrupt records (for investigation):")
df_corrupt.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Performance Comparison
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Performance of Schema Approaches
# ═══════════════════════════════════════════════════════

import time
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.functions import col, concat, lit

print("=== CSV Read Performance: inferSchema vs Explicit ===")
print()

# Create a larger CSV for meaningful timing
print("Creating test data (50K rows)...")
df_big = (
    spark.range(50000)
    .withColumn("name", concat(lit("user_"), col("id").cast("string")))
    .withColumn("value", (col("id") * 1.5))
    .withColumn("category", (col("id") % 10).cast("string"))
)
big_path = "/tmp/csv_demo/big_data.csv"
df_big.write.mode("overwrite").option("header", "true").csv(big_path)
print(f"Written 50K rows to: {big_path}")

# --- Method 1: inferSchema (slowest) ---
print("\n--- Method 1: inferSchema=true ---")
start = time.time()
df1 = spark.read.option("header", "true").option("inferSchema", "true").csv(big_path)
df1.count()  # Force execution
t_infer = time.time() - start
print(f"  Time: {t_infer:.3f}s")

# --- Method 2: Explicit schema (fastest) ---
print("\n--- Method 2: Explicit schema ---")
explicit = StructType([
    StructField("id", LongType()), StructField("name", StringType()),
    StructField("value", DoubleType()), StructField("category", StringType()),
])
start = time.time()
df2 = spark.read.option("header", "true").schema(explicit).csv(big_path)
df2.count()  # Force execution
t_explicit = time.time() - start
print(f"  Time: {t_explicit:.3f}s")

# --- Method 3: No schema (all strings, fastest read but useless) ---
print("\n--- Method 3: No schema (all strings) ---")
start = time.time()
df3 = spark.read.option("header", "true").csv(big_path)
df3.count()  # Force execution
t_raw = time.time() - start
print(f"  Time: {t_raw:.3f}s")

# Comparison
print(f"\n{'=' * 50}")
print(f"{'Method':<25} {'Time (s)':<10} {'Types Correct'}")
print(f"{'-' * 50}")
print(f"{'inferSchema=true':<25} {t_infer:<10.3f} {'Yes (guessed)'}")
print(f"{'Explicit schema':<25} {t_explicit:<10.3f} {'Yes (defined)'}")
print(f"{'No schema (strings)':<25} {t_raw:<10.3f} {'No (all string)'}")
print(f"{'=' * 50}")
print(f"\n  Winner: Explicit schema (fast + correct)")
print(f"  Loser: inferSchema (correct but slow)")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: All CSV Options Reference
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Complete Options Demo
# ═══════════════════════════════════════════════════════

print("=== Complete CSV Options: The Full Kitchen Sink ===")
print()

# Create a CSV that needs EVERY option
tricky_csv = """# This is a comment line
id|name|amount|hire_date|notes
1|"Smith, John"|$1,234.56|01/15/2024|Good employee
2|O'Brien|$987.00|03/20/2024|Has special chars: <>&
3|NULL|$0.00|06/01/2024|This name is null
4|"Williams"|$45,678.90||Missing date
5|"Davis"|N/A|12/01/2023|Amount is N/A"""

tricky_path = "/tmp/csv_demo/tricky.csv"
dbutils.fs.put(tricky_path, tricky_csv, overwrite=True)

# Read with ALL relevant options
df = (
    spark.read
    .option("header", "true")              # First non-comment row = headers
    .option("delimiter", "|")              # Pipe-separated
    .option("quote", '"')                  # Double-quote for escaping
    .option("escape", '"')                 # Escape character
    .option("nullValue", "NULL")           # String "NULL" = null
    .option("nanValue", "N/A")             # String "N/A" = NaN
    .option("comment", "#")                # Lines starting with # are comments
    .option("ignoreLeadingWhiteSpace", "true")   # Trim leading spaces
    .option("ignoreTrailingWhiteSpace", "true")  # Trim trailing spaces
    .option("inferSchema", "true")         # For demo (use explicit in prod)
    .csv(tricky_path)
)

print("Tricky CSV read with all options:")
df.show(truncate=False)
df.printSchema()

# Complete options reference
print("\n" + "=" * 60)
print("COMPLETE CSV OPTIONS REFERENCE")
print("=" * 60)
options_ref = [
    ("header", "true/false", "First row has column names"),
    ("inferSchema", "true/false", "Auto-detect types (slow!)"),
    ("schema", "StructType/DDL", "Explicit schema (fast!)"),
    ("delimiter/sep", "char", "Field separator (default: ,)"),
    ("quote", "char", "Quote char (default: \")"),
    ("escape", "char", "Escape in quotes (default: \\)"),
    ("comment", "char", "Comment line prefix"),
    ("nullValue", "string", "String = null"),
    ("nanValue", "string", "String = NaN"),
    ("emptyValue", "string", "String for empty fields"),
    ("dateFormat", "pattern", "Date parsing (Java SimpleDateFormat)"),
    ("timestampFormat", "pattern", "Timestamp parsing"),
    ("multiLine", "true/false", "Fields can span lines"),
    ("encoding", "charset", "File encoding (default: UTF-8)"),
    ("mode", "PERMISSIVE/DROP/FAIL", "Error handling"),
    ("columnNameOfCorruptRecord", "string", "Column for bad rows"),
    ("ignoreLeadingWhiteSpace", "true/false", "Trim leading"),
    ("ignoreTrailingWhiteSpace", "true/false", "Trim trailing"),
    ("maxColumns", "int", "Max columns (default: 20480)"),
    ("maxCharsPerColumn", "int", "Max chars per field (default: -1=unlimited)"),
]
print(f"{'Option':<30} {'Values':<20} {'Description'}")
print("-" * 80)
for opt, vals, desc in options_ref:
    print(f"{opt:<30} {vals:<20} {desc}")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Forgetting header=true
# MAGIC **Problem:** Column names become `_c0`, `_c1`, `_c2` instead of actual headers.  
# MAGIC **Fix:** Always set `option("header", "true")` when your CSV has a header row.
# MAGIC
# MAGIC ### Mistake #2: Using inferSchema in Production
# MAGIC **Problem:** Schema can change between runs if data changes. Also 2x slower.  
# MAGIC **Fix:** Define explicit schema with `StructType` or DDL string for production.
# MAGIC
# MAGIC ### Mistake #3: Wrong dateFormat Produces Silent Nulls
# MAGIC **Problem:** If `dateFormat` doesn't match actual data, dates become null (no error!).  
# MAGIC **Example:** Data has `01/15/2024` but you set `dateFormat=yyyy-MM-dd` → all nulls!  
# MAGIC **Fix:** Always verify date columns are not null after reading. Test format on sample first.
# MAGIC
# MAGIC ### Mistake #4: Not Handling Bad Records
# MAGIC **Problem:** PERMISSIVE mode silently puts null in columns for bad rows. You miss data quality issues.  
# MAGIC **Fix:** Add `_corrupt_record` column, filter and log/alert on non-null values.
# MAGIC
# MAGIC ### Mistake #5: Reading CSV When Parquet/Delta Exists
# MAGIC **Problem:** Reading raw CSV is 10-100x slower than Parquet for the same data.  
# MAGIC **Fix:** Convert CSV to Delta/Parquet once (ETL), then always read the optimized format.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC **Level 1 (Run it):** Run Example 1 and compare the output of all three reads (no option, header only, header+inferSchema).
# MAGIC
# MAGIC **Level 2 (Tiny change):** Change the delimiter in Example 3 from semicolon to pipe (|). Create matching test data.
# MAGIC
# MAGIC **Level 3 (Combine):** Read a CSV with both a custom delimiter AND a custom dateFormat in one read.
# MAGIC
# MAGIC **Level 4 (New data):** Create a CSV with product data (name, price, weight, in_stock boolean). Read it with explicit schema.
# MAGIC
# MAGIC **Level 5 (Project):** Create 3 CSV files in a folder. Read all at once with wildcards. Add a column showing which file each row came from.
# MAGIC
# MAGIC **Level 6 (Design):** Design a CSV reader function that accepts: path, schema, date_format, delimiter. Returns clean DataFrame with audit columns.
# MAGIC
# MAGIC **Level 7 (Error handling):** Create a CSV with 5 good rows and 3 bad rows. Read with PERMISSIVE mode, separate good from bad, count each.
# MAGIC
# MAGIC **Level 8 (Edge cases):** Handle a CSV where: some rows have too many columns, some have too few, some have the delimiter inside quoted fields.
# MAGIC
# MAGIC **Level 9 (Production):** Build a full ingestion function: read CSV → validate schema → quarantine bad records → write clean to Delta → log statistics.
# MAGIC
# MAGIC **Level 10 (Teach):** Write a 200-word guide explaining CSV read options to a colleague who knows Excel but not Spark.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import col, input_file_name, current_timestamp, lit
from datetime import date

# Level 4: Product data with explicit schema
print("=== Level 4 ===")
product_csv = """name,price,weight_kg,in_stock
Widget Pro,29.99,0.5,true
Gadget X,49.99,1.2,true
Tool Kit,99.99,3.5,false
Mega Device,199.99,0.8,true"""
product_path = "/tmp/csv_demo/products.csv"
dbutils.fs.put(product_path, product_csv, overwrite=True)

product_schema = StructType([
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("weight_kg", DoubleType(), True),
    StructField("in_stock", BooleanType(), True),
])
df_products = spark.read.option("header", "true").schema(product_schema).csv(product_path)
df_products.show()
df_products.printSchema()

# Level 5: Multiple files with source tracking
print("\n=== Level 5 ===")
for month in ["jan", "feb", "mar"]:
    content = f"product,qty\nWidget,{10}\nGadget,{20}"
    dbutils.fs.put(f"/tmp/csv_demo/monthly/sales_{month}.csv", content, overwrite=True)

df_multi = (
    spark.read.option("header", "true").csv("/tmp/csv_demo/monthly/sales_*.csv")
    .withColumn("source", input_file_name())
)
df_multi.show(truncate=50)

# Level 6: Reusable reader function
print("\n=== Level 6 ===")
def smart_csv_reader(spark, path, schema, date_fmt=None, delimiter=","):
    """Reusable CSV reader with common production options."""
    reader = (
        spark.read
        .option("header", "true")
        .option("delimiter", delimiter)
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .schema(schema)
    )
    if date_fmt:
        reader = reader.option("dateFormat", date_fmt)
    df = reader.csv(path)
    df = df.withColumn("_loaded_at", current_timestamp())
    return df

result = smart_csv_reader(spark, product_path, product_schema)
result.show()
print("Reusable function with audit column added")

print("\n\u2705 All homework solutions complete!")