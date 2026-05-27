# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 13: Creating DataFrames From Collections
# MAGIC # Module: DataFrames — Creation & Basics
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 35 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: Building a Spreadsheet from Scratch
# MAGIC
# MAGIC Creating a DataFrame from collections is like:
# MAGIC - Taking data from your **notebook** (Python lists, dicts) and typing it into a **spreadsheet**
# MAGIC - You decide the column names, data types, and layout
# MAGIC - Perfect for testing, prototyping, and small lookup tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Methods to Create DataFrames from In-Memory Data
# MAGIC
# MAGIC | Method | Source | Best For |
# MAGIC |--------|--------|----------|
# MAGIC | `spark.createDataFrame(tuples, cols)` | List of tuples | Quick prototyping |
# MAGIC | `spark.createDataFrame(rows)` | List of Row objects | Named fields |
# MAGIC | `spark.createDataFrame(dicts)` | List of dicts | JSON-like data |
# MAGIC | `spark.createDataFrame(pandas_df)` | Pandas DataFrame | Pandas → Spark |
# MAGIC | `spark.createDataFrame(rdd, schema)` | RDD + schema | Legacy migration |
# MAGIC | `spark.range(n)` | Sequential IDs | Large test data |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use This
# MAGIC
# MAGIC 1. **Unit testing** — create small test DataFrames to verify logic
# MAGIC 2. **Lookup tables** — small reference data (country codes, config)
# MAGIC 3. **Prototyping** — test your logic before connecting to real data
# MAGIC 4. **Constants** — create DataFrames for broadcasting or joining

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### What Happens Under the Hood
# MAGIC
# MAGIC ```
# MAGIC   spark.createDataFrame(data, schema)
# MAGIC        │
# MAGIC        ├─ 1. Schema inference (if not provided):
# MAGIC        │      - Scans data to determine types
# MAGIC        │      - Strings → StringType, ints → LongType, floats → DoubleType
# MAGIC        │
# MAGIC        ├─ 2. Data conversion:
# MAGIC        │      - Python objects → Spark internal Row format
# MAGIC        │      - Validates against schema (type checking)
# MAGIC        │
# MAGIC        ├─ 3. Distribution:
# MAGIC        │      - Data is parallelized across partitions
# MAGIC        │      - Default partitions = spark.sparkContext.defaultParallelism
# MAGIC        │
# MAGIC        └─ 4. Returns DataFrame object (lazy — no computation yet)
# MAGIC ```
# MAGIC
# MAGIC ### Schema Inference Rules
# MAGIC
# MAGIC | Python Type | Inferred Spark Type |
# MAGIC |-------------|--------------------|
# MAGIC | `int` | `LongType` (64-bit) |
# MAGIC | `float` | `DoubleType` (64-bit) |
# MAGIC | `str` | `StringType` |
# MAGIC | `bool` | `BooleanType` |
# MAGIC | `datetime` | `TimestampType` |
# MAGIC | `date` | `DateType` |
# MAGIC | `list` | `ArrayType` |
# MAGIC | `dict` | `MapType` |
# MAGIC | `None` | Column becomes nullable |
# MAGIC
# MAGIC ### Explicit Schema vs Inferred Schema
# MAGIC
# MAGIC - **Inferred** (default): Spark samples data → guesses types (may be wrong!)
# MAGIC - **Explicit** (StructType): You define exact types (production-grade, never wrong)

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: From Tuples
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: From List of Tuples
# ═══════════════════════════════════════════════════════

print("=== Creating DataFrames from Tuples ===")
print()

# Method 1: List of tuples + column names (most common)
data = [
    ("Alice", 30, "Engineering", 95000.0),   # Each tuple = one row
    ("Bob", 25, "Marketing", 72000.0),       # Values match column order
    ("Charlie", 35, "Engineering", 110000.0),
    ("Diana", 28, "Sales", 68000.0),
    ("Eve", 32, "Marketing", 85000.0),
]

# Pass data + column names
df = spark.createDataFrame(data, ["name", "age", "department", "salary"])

print("1. DataFrame from tuples + column names:")
df.show()  # Display the table
print("   Schema (inferred automatically):")
df.printSchema()  # name: string, age: long, department: string, salary: double

# Method 2: Single column (just a list wrapped in tuples)
print("\n2. Single-column DataFrame:")
numbers = spark.createDataFrame([(i,) for i in range(1, 6)], ["number"])
numbers.show()  # Column 'number' with values 1-5

# Method 3: spark.range() for sequential IDs (very efficient!)
print("\n3. spark.range() — fast sequential data:")
ids = spark.range(5)  # Creates 'id' column with 0,1,2,3,4
ids.show()  # Built-in, no Python overhead

# Method 4: Empty DataFrame (for schema-only use)
print("\n4. Empty DataFrame (schema only):")
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
empty_schema = StructType([
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True),
])
empty_df = spark.createDataFrame([], empty_schema)  # No data, just schema
print(f"   Rows: {empty_df.count()}")  # 0
empty_df.printSchema()  # Schema exists even with no data

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: From Row Objects and Dicts
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: Row Objects and Dicts
# ═══════════════════════════════════════════════════════

from pyspark.sql import Row  # Import Row class

print("=== Row Objects and Dictionaries ===")
print()

# Method 1: List of Row objects (named fields, more explicit)
print("--- Method 1: Row objects ---")
rows = [
    Row(product="Laptop", price=999.99, category="Electronics"),   # Named fields
    Row(product="Headphones", price=149.99, category="Electronics"),
    Row(product="Desk Chair", price=299.99, category="Furniture"),
    Row(product="Mouse", price=29.99, category="Electronics"),
    Row(product="Bookshelf", price=189.99, category="Furniture"),
]

df_rows = spark.createDataFrame(rows)  # Schema inferred from Row field names
df_rows.show()
print(f"Columns: {df_rows.columns}")  # ['category', 'price', 'product'] - alphabetical!
print("Note: Row fields are sorted alphabetically!")

# Method 2: List of dictionaries (most flexible, JSON-like)
print("\n--- Method 2: Dictionaries ---")
dicts = [
    {"city": "Tokyo", "country": "Japan", "population": 14000000},
    {"city": "London", "country": "UK", "population": 9000000},
    {"city": "Paris", "country": "France", "population": 2200000},
    {"city": "NYC", "country": "USA", "population": 8300000},
]

df_dicts = spark.createDataFrame(dicts)  # Keys become column names
df_dicts.show()

# Method 3: Dicts with missing keys (creates nulls)
print("--- Method 3: Dicts with missing keys ---")
mixed = [
    {"name": "Alice", "age": 30, "email": "alice@test.com"},
    {"name": "Bob", "age": 25},  # Missing 'email' → null
    {"name": "Charlie", "email": "charlie@test.com"},  # Missing 'age' → null
]
df_mixed = spark.createDataFrame(mixed)  # Handles missing keys gracefully
df_mixed.show()  # nulls where keys are missing

print("--- Key: Dicts are flexible but schema inference may be inconsistent ---")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: From Pandas DataFrame
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: From Pandas DataFrame
# ═══════════════════════════════════════════════════════

import pandas as pd  # Import pandas
import numpy as np   # Import numpy for data generation

print("=== Pandas ↔ Spark Conversion ===")
print()

# Create a Pandas DataFrame
print("--- Step 1: Create Pandas DataFrame ---")
pdf = pd.DataFrame({
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "age": [30, 25, 35, 28, 32],
    "score": [95.5, 87.3, 72.1, 91.8, 88.4],
    "passed": [True, True, False, True, True],
})
print(pdf)
print(f"\nPandas type: {type(pdf)}")

# Convert Pandas → Spark DataFrame
print("\n--- Step 2: Pandas → Spark ---")
spark_df = spark.createDataFrame(pdf)  # Simple conversion!
spark_df.show()
spark_df.printSchema()  # Check how types were mapped

# Convert Spark → Pandas (bring to driver — only for small data!)
print("--- Step 3: Spark → Pandas ---")
pdf_back = spark_df.toPandas()  # Collects ALL data to driver!
print(f"Back to Pandas: {type(pdf_back)}")
print(pdf_back.head())

# Type mapping between Pandas and Spark
print("\n--- Type Mapping ---")
print("  Pandas int64    → Spark LongType")
print("  Pandas float64  → Spark DoubleType")
print("  Pandas object   → Spark StringType")
print("  Pandas bool     → Spark BooleanType")
print("  Pandas datetime → Spark TimestampType")

print("\n--- ⚠️  Warning ---")
print("  toPandas() collects ALL data to driver!")
print("  Only use on small DataFrames (< few million rows)")
print("  For large data: use .limit(1000).toPandas()")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Explicit Schema (Production Pattern)
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 1: Explicit Schema
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType, DateType,
    ArrayType, MapType
)
from datetime import datetime, date

print("=== Explicit Schema: Production-Grade DataFrames ===")
print()

# Why explicit schema?
# 1. Type safety: catches errors early
# 2. Performance: no schema inference scan
# 3. Documentation: schema IS the documentation
# 4. Consistency: same types every time

# Define a complex schema
order_schema = StructType([
    StructField("order_id", StringType(), nullable=False),      # Required
    StructField("customer_name", StringType(), nullable=True),  # Optional
    StructField("amount", DoubleType(), nullable=False),        # Required
    StructField("quantity", IntegerType(), nullable=False),     # Required
    StructField("is_premium", BooleanType(), nullable=True),    # Optional
    StructField("order_date", DateType(), nullable=False),      # Required
    StructField("tags", ArrayType(StringType()), nullable=True), # Array of strings
])

# Create data matching the schema
order_data = [
    ("ORD001", "Alice Johnson", 299.99, 2, True, date(2024, 1, 15), ["electronics", "premium"]),
    ("ORD002", "Bob Smith", 49.99, 1, False, date(2024, 1, 16), ["books"]),
    ("ORD003", None, 150.00, 3, True, date(2024, 1, 16), None),  # Nullable fields = None
    ("ORD004", "Diana Prince", 89.99, 1, None, date(2024, 1, 17), ["clothing", "sale"]),
]

# Create DataFrame with explicit schema
df = spark.createDataFrame(order_data, schema=order_schema)

print("DataFrame with explicit schema:")
df.show(truncate=False)
df.printSchema()

# Schema as DDL string (alternative compact syntax)
print("\n--- Alternative: DDL String Schema ---")
ddl_schema = "id INT, name STRING, score DOUBLE, active BOOLEAN"
df_ddl = spark.createDataFrame(
    [(1, "Alice", 95.5, True), (2, "Bob", 87.3, False)],
    schema=ddl_schema  # DDL string instead of StructType
)
df_ddl.show()
df_ddl.printSchema()

print("--- Rule: ALWAYS use explicit schema in production code ---")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Complex and Nested Types
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 2: Nested and Complex Types
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType, DoubleType
from pyspark.sql.functions import col, explode, map_keys, map_values, size

print("=== Complex Types: Arrays, Maps, Nested Structs ===")
print()

# 1. ArrayType — list of values in a column
print("--- 1. ArrayType (list of values) ---")
array_schema = StructType([
    StructField("name", StringType()),
    StructField("skills", ArrayType(StringType())),  # Array of strings
    StructField("scores", ArrayType(IntegerType())), # Array of ints
])
array_data = [
    ("Alice", ["Python", "SQL", "Spark"], [95, 88, 92]),
    ("Bob", ["Java", "Scala"], [82, 79]),
    ("Charlie", ["Python"], [90]),
]
df_array = spark.createDataFrame(array_data, array_schema)
df_array.show(truncate=False)

# Access array elements
print("First skill per person:")
df_array.select("name", col("skills")[0].alias("first_skill")).show()  # Index access
print(f"Array size: ")
df_array.select("name", size("skills").alias("num_skills")).show()  # Array length

# 2. MapType — key-value pairs in a column
print("--- 2. MapType (key-value dictionary) ---")
map_schema = StructType([
    StructField("id", IntegerType()),
    StructField("properties", MapType(StringType(), StringType())),  # Dict in a column
])
map_data = [
    (1, {"color": "red", "size": "large", "material": "cotton"}),
    (2, {"color": "blue", "size": "medium"}),
    (3, {"color": "green", "weight": "2kg"}),
]
df_map = spark.createDataFrame(map_data, map_schema)
df_map.show(truncate=False)

# Access map values
print("Map access:")
df_map.select("id", col("properties")["color"].alias("color")).show()  # Key lookup

# 3. Nested StructType — struct within struct
print("--- 3. Nested StructType (struct within struct) ---")
nested_schema = StructType([
    StructField("emp_id", IntegerType()),
    StructField("name", StringType()),
    StructField("address", StructType([  # Nested struct!
        StructField("street", StringType()),
        StructField("city", StringType()),
        StructField("zip", StringType()),
    ])),
])
nested_data = [
    (1, "Alice", ("123 Main St", "NYC", "10001")),
    (2, "Bob", ("456 Oak Ave", "LA", "90001")),
]
df_nested = spark.createDataFrame(nested_data, nested_schema)
df_nested.show(truncate=False)

# Access nested fields with dot notation
print("Nested access (dot notation):")
df_nested.select("name", col("address.city"), col("address.zip")).show()

print("--- Key: Complex types model real-world nested data naturally ---")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: spark.range() for Large Test Data
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 3: Generating Test Data
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col, rand, randn, floor, concat, lit, expr, array

print("=== Generating Large Test DataFrames ===")
print()

# spark.range() — fastest way to create large DataFrames
print("--- spark.range() basics ---")
df1 = spark.range(10)  # 0 to 9 (single column 'id')
df1.show()

df2 = spark.range(0, 100, 10)  # start=0, end=100, step=10
df2.show()

df3 = spark.range(1000000)  # 1 million rows (instant!)
print(f"1M rows created: {df3.count():,} rows")

# Build realistic test data using spark.range() + expressions
print("\n--- Building realistic test data ---")
test_data = (
    spark.range(100)  # Start with 100 IDs
    .withColumn("name", concat(lit("user_"), col("id").cast("string")))  # user_0, user_1...
    .withColumn("age", (rand() * 50 + 18).cast("int"))  # Random age 18-68
    .withColumn("salary", (rand() * 100000 + 40000).cast("int"))  # Salary 40K-140K
    .withColumn("department", 
        expr("CASE WHEN id % 4 = 0 THEN 'Engineering' "
             "WHEN id % 4 = 1 THEN 'Marketing' "
             "WHEN id % 4 = 2 THEN 'Sales' "
             "ELSE 'Operations' END"))  # 4 departments
    .withColumn("rating", (rand() * 4 + 1).cast("double"))  # Rating 1.0-5.0
)

print("Generated test data (first 10 rows):")
test_data.show(10)
print(f"Total rows: {test_data.count()}")
print(f"Schema:")
test_data.printSchema()

# Generate time-series test data
print("\n--- Time-series test data ---")
from pyspark.sql.functions import date_add, current_date
ts_data = (
    spark.range(365)  # 365 days
    .withColumn("date", date_add(lit("2024-01-01"), col("id").cast("int")))  # Daily dates
    .withColumn("temperature", randn() * 10 + 20)  # Normal dist ~20°C
    .withColumn("humidity", rand() * 60 + 30)  # Uniform 30-90%
    .drop("id")
)
ts_data.show(5)

print("--- Key: spark.range() + withColumn = unlimited realistic test data ---")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Schema Evolution and Enforcement
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 1: Schema Validation
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

print("=== Advanced: Schema Validation and Enforcement ===")
print()

# Production pattern: Validate incoming data against expected schema

# Define the expected schema (contract)
expected_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), False),
])

# Function to validate DataFrame schema
def validate_schema(df, expected, strict=True):
    """Validate that a DataFrame matches the expected schema."""
    actual_fields = {f.name: f for f in df.schema.fields}  # Actual fields
    expected_fields = {f.name: f for f in expected.fields}  # Expected fields
    
    errors = []  # Collect errors
    
    # Check for missing columns
    for name, field in expected_fields.items():
        if name not in actual_fields:
            errors.append(f"MISSING column: '{name}' ({field.dataType.simpleString()})")
        elif actual_fields[name].dataType != field.dataType:
            errors.append(
                f"TYPE MISMATCH: '{name}' expected {field.dataType.simpleString()}, "
                f"got {actual_fields[name].dataType.simpleString()}"
            )
        elif not field.nullable and actual_fields[name].nullable:
            errors.append(f"NULLABLE MISMATCH: '{name}' should be non-nullable")
    
    # Check for extra columns (strict mode)
    if strict:
        extra = set(actual_fields.keys()) - set(expected_fields.keys())
        if extra:
            errors.append(f"EXTRA columns: {extra}")
    
    return errors

# Test 1: Good data (matches schema)
print("--- Test 1: Valid data ---")
df_good = spark.createDataFrame([(1, "Alice", 99.99), (2, "Bob", 45.50)], expected_schema)
errors = validate_schema(df_good, expected_schema)
print(f"  Errors: {errors if errors else 'None ✔️'}")

# Test 2: Wrong types
print("\n--- Test 2: Wrong types ---")
df_bad_types = spark.createDataFrame(
    [("1", "Alice", "99.99")],  # All strings!
    ["id", "name", "amount"]
)
errors = validate_schema(df_bad_types, expected_schema)
for e in errors:
    print(f"  ❌ {e}")

# Test 3: Missing columns
print("\n--- Test 3: Missing columns ---")
df_missing = spark.createDataFrame([(1, "Alice")], ["id", "name"])  # No 'amount'
errors = validate_schema(df_missing, expected_schema)
for e in errors:
    print(f"  ❌ {e}")

print("\n--- Key: Schema validation catches data issues before expensive processing ---")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Factory Pattern for DataFrame Creation
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 2: DataFrame Factory Pattern
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import date

print("=== Advanced: DataFrame Factory for Reusable Creation ===")
print()

# Pattern: Create a factory function for consistent DataFrame creation
# Used in production ETL pipelines for test fixtures and lookup tables

class DataFrameFactory:
    """Factory class for creating well-typed DataFrames."""
    
    def __init__(self, spark_session):
        self.spark = spark_session  # Store reference
    
    def create_from_dict_list(self, data, schema=None, add_metadata=False):
        """Create DataFrame from list of dicts with optional metadata."""
        df = self.spark.createDataFrame(data, schema=schema)  # Create base DF
        if add_metadata:  # Add audit columns
            df = (
                df
                .withColumn("_created_at", current_timestamp())  # When created
                .withColumn("_source", lit("manual"))             # Data source
            )
        return df
    
    def create_lookup_table(self, mapping, key_col="code", value_col="description"):
        """Create a lookup/reference DataFrame from a Python dict."""
        data = [(k, v) for k, v in mapping.items()]  # Dict to tuple list
        schema = StructType([
            StructField(key_col, StringType(), False),
            StructField(value_col, StringType(), False),
        ])
        return self.spark.createDataFrame(data, schema)  # Return typed DF
    
    def create_empty_like(self, reference_df):
        """Create an empty DataFrame with the same schema as another."""
        return self.spark.createDataFrame([], reference_df.schema)  # Empty, same schema

# Use the factory
factory = DataFrameFactory(spark)  # Initialize

# Create a lookup table
print("--- 1. Lookup Table ---")
country_lookup = factory.create_lookup_table({
    "US": "United States", "UK": "United Kingdom",
    "DE": "Germany", "FR": "France", "JP": "Japan"
})
country_lookup.show()

# Create a DataFrame with metadata
print("--- 2. Data with Metadata ---")
orders = factory.create_from_dict_list(
    [{"order_id": "A001", "amount": 99.99}, {"order_id": "A002", "amount": 45.50}],
    add_metadata=True  # Adds _created_at and _source columns
)
orders.show(truncate=False)

# Create empty DataFrame for accumulation
print("--- 3. Empty DataFrame (schema only) ---")
empty = factory.create_empty_like(orders)  # Same schema, no data
print(f"  Rows: {empty.count()}, Columns: {empty.columns}")

print("\n--- Key: Factory pattern ensures consistency across your codebase ---")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Performance of Creation Methods
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 3: Creation Performance
# ═══════════════════════════════════════════════════════

import time
import pandas as pd
from pyspark.sql.types import StructType, StructField, LongType, StringType

print("=== Performance: Which Creation Method is Fastest? ===")
print()

num_rows = 100000  # 100K rows for benchmarking

# Method 1: spark.range() (native, no Python overhead)
print("--- Method 1: spark.range() ---")
start = time.time()
df1 = spark.range(num_rows)
df1.count()  # Force materialization
t1 = time.time() - start
print(f"  Time: {t1:.3f}s")

# Method 2: createDataFrame from Python tuples with schema
print("\n--- Method 2: createDataFrame(tuples, schema) ---")
schema = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), True),
])
data = [(i, f"user_{i}") for i in range(num_rows)]  # Generate tuples
start = time.time()
df2 = spark.createDataFrame(data, schema=schema)
df2.count()  # Force materialization
t2 = time.time() - start
print(f"  Time: {t2:.3f}s")

# Method 3: createDataFrame from Pandas
print("\n--- Method 3: createDataFrame(pandas_df) ---")
pdf = pd.DataFrame({"id": range(num_rows), "name": [f"user_{i}" for i in range(num_rows)]})
start = time.time()
df3 = spark.createDataFrame(pdf)
df3.count()  # Force materialization
t3 = time.time() - start
print(f"  Time: {t3:.3f}s")

# Method 4: createDataFrame from RDD
print("\n--- Method 4: createDataFrame(rdd, schema) ---")
sc = spark.sparkContext
rdd = sc.parallelize([(i, f"user_{i}") for i in range(num_rows)], 8)
start = time.time()
df4 = spark.createDataFrame(rdd, schema=schema)
df4.count()  # Force materialization
t4 = time.time() - start
print(f"  Time: {t4:.3f}s")

# Summary
print(f"\n{'=' * 50}")
print(f"{'Method':<30} {'Time (s)':<10} {'Relative'}")
print(f"{'-' * 50}")
min_time = min(t1, t2, t3, t4)
for name, t in [("spark.range()", t1), ("tuples + schema", t2), ("Pandas DF", t3), ("RDD + schema", t4)]:
    relative = t / min_time
    bar = '█' * int(relative * 5)
    print(f"{name:<30} {t:<10.3f} {relative:.1f}x {bar}")

print(f"\nRecommendation:")
print(f"  1. spark.range() for sequential/synthetic data (fastest)")
print(f"  2. tuples + explicit schema for small datasets")
print(f"  3. Pandas conversion for data already in Pandas")
print(f"  4. Avoid: large Python collections without schema (slow inference)")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Not Specifying Schema (Slow Inference)
# MAGIC **Issue:** `spark.createDataFrame(data)` scans ALL data to infer types — slow for large collections.  
# MAGIC **Fix:** Always pass `schema=StructType(...)` for production code.
# MAGIC
# MAGIC ### Mistake #2: Integer Overflow with Default LongType
# MAGIC **Issue:** Python `int` maps to Spark `LongType` (64-bit). If you need `IntegerType`, specify schema.  
# MAGIC **Fix:** Use explicit schema with `IntegerType()` when you know values fit in 32 bits.
# MAGIC
# MAGIC ### Mistake #3: Null Handling Without Nullable Schema
# MAGIC **Issue:** Data has `None` values but schema says `nullable=False` → runtime error.  
# MAGIC **Fix:** Set `nullable=True` for any column that might have nulls.
# MAGIC
# MAGIC ### Mistake #4: Row Field Ordering Surprise
# MAGIC **Issue:** `Row(name="Alice", age=30)` creates fields in ALPHABETICAL order, not insertion order.  
# MAGIC **Fix:** Use tuples + explicit column names, or be aware of Row's alphabetical sorting.
# MAGIC
# MAGIC ### Mistake #5: Creating Large DataFrames from Python Lists
# MAGIC **Issue:** `spark.createDataFrame(million_item_list)` is slow — all data goes through driver.  
# MAGIC **Fix:** For large data, use `spark.range()` + `withColumn()`, or read from files directly.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC ### Level 1: Create a DataFrame from a list of 5 tuples with 3 columns.
# MAGIC ### Level 2: Create the same data using Row objects. Note the column ordering.
# MAGIC ### Level 3: Create a DataFrame from a Pandas DataFrame. Convert it back.
# MAGIC ### Level 4: Define an explicit StructType schema with 4 different data types.
# MAGIC ### Level 5: Create a DataFrame with ArrayType and MapType columns.
# MAGIC ### Level 6: Use spark.range() + withColumn to generate 1M rows of realistic test data.
# MAGIC ### Level 7: Write a schema validation function that checks column names AND types.
# MAGIC ### Level 8: Create a nested schema (struct inside struct) and access nested fields.
# MAGIC ### Level 9: Benchmark: compare creation time for tuples vs Pandas vs RDD with 500K rows.
# MAGIC ### Level 10: Build a reusable DataFrameFactory class that creates validated, audited DataFrames.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import col, rand, lit, concat, expr, current_timestamp
import pandas as pd
import time

# Level 1: Basic tuples
print("=== Level 1 ===")
df1 = spark.createDataFrame([
    ("Tokyo", "Japan", 14000000),
    ("London", "UK", 9000000),
    ("Paris", "France", 2200000),
    ("NYC", "USA", 8300000),
    ("Mumbai", "India", 20700000),
], ["city", "country", "population"])
df1.show()

# Level 2: Row objects
print("\n=== Level 2 ===")
df2 = spark.createDataFrame([
    Row(city="Tokyo", country="Japan", population=14000000),
    Row(city="London", country="UK", population=9000000),
])
df2.show()
print(f"Column order: {df2.columns}")  # Alphabetical!

# Level 4: Multiple types
print("\n=== Level 4 ===")
schema4 = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("active", BooleanType(), True),
])
df4 = spark.createDataFrame([(1, "Alice", 95000.0, True)], schema4)
df4.printSchema()

# Level 5: Complex types
print("\n=== Level 5 ===")
df5 = spark.createDataFrame([
    (1, ["a", "b"], {"x": "1"}),
    (2, ["c"], {"y": "2", "z": "3"}),
], StructType([
    StructField("id", IntegerType()),
    StructField("tags", ArrayType(StringType())),
    StructField("meta", MapType(StringType(), StringType())),
]))
df5.show(truncate=False)

# Level 6: spark.range() test data
print("\n=== Level 6 ===")
big_df = (
    spark.range(1000000)
    .withColumn("name", concat(lit("user_"), col("id").cast("string")))
    .withColumn("score", (rand() * 100).cast("int"))
    .withColumn("dept", expr("CASE WHEN id%3=0 THEN 'A' WHEN id%3=1 THEN 'B' ELSE 'C' END"))
)
print(f"Generated {big_df.count():,} rows")
big_df.show(3)

print("\n\u2705 All homework complete!")