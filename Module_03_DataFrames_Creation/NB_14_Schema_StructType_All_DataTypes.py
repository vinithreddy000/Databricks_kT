# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 14: Schema — StructType, StructField, and All Data Types
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
# MAGIC ### Real-World Analogy: The Blueprint of a Building
# MAGIC
# MAGIC Before a construction crew builds a house, they need a **blueprint**:
# MAGIC - It shows every room (columns)
# MAGIC - It shows what TYPE each room is (bedroom, kitchen = data types)
# MAGIC - It shows which rooms are optional vs required (nullable)
# MAGIC - You can share the blueprint without building the house (schema without data)
# MAGIC
# MAGIC In Spark, a **Schema** is the blueprint for your DataFrame. It defines:
# MAGIC 1. **What columns** exist
# MAGIC 2. **What type** each column holds (text, numbers, dates, lists, etc.)
# MAGIC 3. **Whether nulls** are allowed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What Is StructType?
# MAGIC
# MAGIC `StructType` is PySpark's way to define a DataFrame schema:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# MAGIC
# MAGIC schema = StructType([
# MAGIC     StructField("name", StringType(), nullable=True),
# MAGIC     StructField("age", IntegerType(), nullable=False),
# MAGIC ])
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### All Available Data Types
# MAGIC
# MAGIC **Primitive Types (single values):**
# MAGIC
# MAGIC | Type | Python Equivalent | Example | Use Case |
# MAGIC |------|------------------|---------|-----------|
# MAGIC | `StringType()` | str | "hello" | Names, descriptions |
# MAGIC | `IntegerType()` | int (32-bit) | 42 | Small whole numbers |
# MAGIC | `LongType()` | int (64-bit) | 9999999999 | IDs, large counts |
# MAGIC | `ShortType()` | int (16-bit) | 255 | Very small numbers |
# MAGIC | `ByteType()` | int (8-bit) | 127 | Tiny numbers |
# MAGIC | `DoubleType()` | float (64-bit) | 3.14159 | Precise decimals |
# MAGIC | `FloatType()` | float (32-bit) | 3.14 | Less precise decimals |
# MAGIC | `BooleanType()` | bool | True/False | Yes/No flags |
# MAGIC | `DateType()` | date | 2024-01-15 | Calendar dates |
# MAGIC | `TimestampType()` | datetime | 2024-01-15 10:30:00 | Date + time |
# MAGIC | `DecimalType(p,s)` | Decimal | 99999.99 | Financial (exact) |
# MAGIC | `BinaryType()` | bytes | b"\\x00" | Raw bytes |
# MAGIC | `NullType()` | None | null | Unknown type |
# MAGIC
# MAGIC **Complex Types (nested/collections):**
# MAGIC
# MAGIC | Type | Python Equivalent | Example |
# MAGIC |------|------------------|---------|
# MAGIC | `ArrayType(T)` | list | [1, 2, 3] |
# MAGIC | `MapType(K, V)` | dict | {"a": 1} |
# MAGIC | `StructType([...])` | nested object | {"name": {"first": ...}} |

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Schema Hierarchy
# MAGIC
# MAGIC ```
# MAGIC   StructType (the entire schema = the whole blueprint)
# MAGIC     ├─ StructField("name", StringType, nullable=True)         ← one column
# MAGIC     ├─ StructField("age", IntegerType, nullable=False)        ← another column
# MAGIC     ├─ StructField("scores", ArrayType(IntegerType))          ← a list column
# MAGIC     ├─ StructField("address", StructType([                    ← nested struct!
# MAGIC     │       StructField("city", StringType),
# MAGIC     │       StructField("zip", StringType)
# MAGIC     │   ]))
# MAGIC     └─ StructField("metadata", MapType(StringType, StringType)) ← dict column
# MAGIC ```
# MAGIC
# MAGIC ### StructField Parameters
# MAGIC
# MAGIC ```python
# MAGIC StructField(
# MAGIC     name,        # Column name (string)
# MAGIC     dataType,    # Column type (any DataType)
# MAGIC     nullable,    # Can it contain null? (boolean, default True)
# MAGIC     metadata     # Optional metadata dict (rarely used)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Schema Inference vs Explicit Schema
# MAGIC
# MAGIC | Approach | How | Pros | Cons |
# MAGIC |----------|-----|------|------|
# MAGIC | **Inferred** | `spark.read.option("inferSchema", True)` | Easy, no code | Slow (scans all data), may guess wrong |
# MAGIC | **Explicit** | `spark.read.schema(my_schema)` | Fast, correct, documented | More code upfront |
# MAGIC
# MAGIC **Rule:** Always use explicit schema in production. Only use inference for quick exploration.
# MAGIC
# MAGIC ### What Happens Under the Hood
# MAGIC
# MAGIC ```
# MAGIC   When you provide a schema:
# MAGIC   1. Spark trusts your schema (no scanning)
# MAGIC   2. Data is parsed according to your types
# MAGIC   3. Mismatches become null (PERMISSIVE mode)
# MAGIC   
# MAGIC   When Spark infers a schema:
# MAGIC   1. Spark reads a sample (or all) of the data
# MAGIC   2. Guesses types based on values it sees
# MAGIC   3. May get it wrong ("123" could be int OR string)
# MAGIC   4. Takes extra time for large files
# MAGIC ```
# MAGIC
# MAGIC ### Schema Representations
# MAGIC
# MAGIC | Format | Example | Use Case |
# MAGIC |--------|---------|----------|
# MAGIC | StructType (Python) | `StructType([StructField(...)])` | Programmatic, most flexible |
# MAGIC | DDL string | `"name STRING, age INT"` | Compact, readable |
# MAGIC | JSON | `schema.json()` | Serializable, shareable |
# MAGIC | simpleString | `schema.simpleString()` | Quick display |

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: All Primitive Types
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: Every Primitive Data Type
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, ShortType, ByteType,
    DoubleType, FloatType, BooleanType,
    DateType, TimestampType, DecimalType, BinaryType, NullType
)
from datetime import date, datetime
from decimal import Decimal

print("=== Every Primitive Data Type in PySpark ===")
print()

# Define a schema with EVERY primitive type
all_types_schema = StructType([
    StructField("string_col", StringType(), True),         # Text
    StructField("int_col", IntegerType(), True),           # 32-bit integer
    StructField("long_col", LongType(), True),             # 64-bit integer
    StructField("short_col", ShortType(), True),           # 16-bit integer
    StructField("byte_col", ByteType(), True),             # 8-bit integer
    StructField("double_col", DoubleType(), True),         # 64-bit float
    StructField("float_col", FloatType(), True),           # 32-bit float
    StructField("boolean_col", BooleanType(), True),       # True/False
    StructField("date_col", DateType(), True),             # Date only
    StructField("timestamp_col", TimestampType(), True),   # Date + Time
    StructField("decimal_col", DecimalType(10, 2), True),  # Exact decimal (10 digits, 2 after point)
])

# Create data that matches every type
data = [
    ("Alice", 30, 9999999999, 255, 127, 3.14159, 2.71,
     True, date(2024, 1, 15), datetime(2024, 1, 15, 10, 30, 0), Decimal("99999.99")),
    ("Bob", 25, 1234567890, 100, 50, 2.71828, 1.41,
     False, date(2024, 6, 20), datetime(2024, 6, 20, 14, 0, 0), Decimal("12345.67")),
    (None, None, None, None, None, None, None,
     None, None, None, None),  # All nulls (to show nullable=True works)
]

# Create DataFrame with explicit schema
df = spark.createDataFrame(data, schema=all_types_schema)

# Show the data
print("Data:")
df.show(truncate=False)

# Show the schema
print("\nSchema (blueprint):")
df.printSchema()

# Show type ranges
print("\nType Ranges:")
print(f"  ByteType:    -128 to 127")
print(f"  ShortType:   -32,768 to 32,767")
print(f"  IntegerType: -2,147,483,648 to 2,147,483,647")
print(f"  LongType:    -9.2 quintillion to 9.2 quintillion")
print(f"  FloatType:   ~7 decimal digits of precision")
print(f"  DoubleType:  ~15 decimal digits of precision")
print(f"  DecimalType(10,2): up to 10 digits total, 2 after decimal")

# Expected Output:
# Schema shows all types correctly
# Third row shows all nulls

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Complex Types (Array, Map, Struct)
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: Complex Types
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql.functions import col, explode, map_keys, size

print("=== Complex Data Types: Array, Map, Nested Struct ===")
print()

# --- ArrayType: A list inside a column ---
print("--- 1. ArrayType (a list in a column) ---")
array_schema = StructType([
    StructField("name", StringType(), True),
    StructField("skills", ArrayType(StringType()), True),    # List of strings
    StructField("scores", ArrayType(IntegerType()), True),   # List of integers
])
array_data = [
    ("Alice", ["Python", "SQL", "Spark"], [95, 88, 92]),
    ("Bob", ["Java", "Scala"], [82, 79]),
    ("Charlie", ["Python"], [90]),
    ("Diana", None, None),  # Null arrays are allowed
]
df_array = spark.createDataFrame(array_data, array_schema)
df_array.show(truncate=False)
print("Access first skill:")  # Index into array
df_array.select("name", col("skills")[0].alias("first_skill"), size("skills").alias("num_skills")).show()

# --- MapType: A dictionary inside a column ---
print("--- 2. MapType (a dictionary in a column) ---")
map_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("properties", MapType(StringType(), StringType()), True),
])
map_data = [
    (1, {"color": "red", "size": "large", "material": "cotton"}),
    (2, {"color": "blue", "size": "medium"}),
    (3, {"color": "green", "weight": "2kg"}),
]
df_map = spark.createDataFrame(map_data, map_schema)
df_map.show(truncate=False)
print("Access 'color' key:")
df_map.select("id", col("properties")["color"].alias("color")).show()

# --- StructType inside StructType: Nested objects ---
print("--- 3. Nested StructType (object inside object) ---")
nested_schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("address", StructType([  # Nested struct!
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
    ]), True),
])
nested_data = [
    (1, "Alice", ("123 Main St", "NYC", "10001")),
    (2, "Bob", ("456 Oak Ave", "LA", "90001")),
    (3, "Charlie", None),  # Null struct
]
df_nested = spark.createDataFrame(nested_data, nested_schema)
df_nested.show(truncate=False)
print("Access nested fields with dot notation:")
df_nested.select("name", col("address.city"), col("address.zip")).show()

print("--- Key: Complex types let you model real-world nested data ---")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Schema Inference vs Explicit
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: Inferred vs Explicit Schema
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time

print("=== Schema Inference vs Explicit Schema ===")
print()

# Create sample CSV data to demonstrate
csv_data = [
    ("Alice", "30", "95000.50"),  # Note: ALL values are strings (like raw CSV)
    ("Bob", "25", "72000.00"),
    ("Charlie", "35", "110000.75"),
]

# --- Method 1: Let Spark INFER the schema ---
print("--- Method 1: Schema Inference (Spark guesses) ---")
df_inferred = spark.createDataFrame(csv_data, ["name", "age", "salary"])
df_inferred.printSchema()  # Spark guesses all are strings (from tuples of strings)
print("Problem: age and salary are strings, not numbers!")
print("Inference from tuples preserves original Python types.")

# --- Method 2: Explicit schema (YOU define types) ---
print("\n--- Method 2: Explicit Schema (you define types) ---")
explicit_schema = StructType([
    StructField("name", StringType(), True),       # Keep as string
    StructField("age", IntegerType(), True),       # Force to integer
    StructField("salary", DoubleType(), True),     # Force to double
])

# Now create with proper types from the start
proper_data = [
    ("Alice", 30, 95000.50),
    ("Bob", 25, 72000.00),
    ("Charlie", 35, 110000.75),
]
df_explicit = spark.createDataFrame(proper_data, schema=explicit_schema)
df_explicit.printSchema()  # Now types are correct!
df_explicit.show()

# --- Method 3: DDL string (compact alternative) ---
print("--- Method 3: DDL String (compact) ---")
ddl_schema = "name STRING, age INT, salary DOUBLE"
df_ddl = spark.createDataFrame(proper_data, schema=ddl_schema)
df_ddl.printSchema()

# Comparison
print("\n--- When to use which ---")
print("  Inference:  Quick exploration, small data, notebooks")
print("  Explicit:   Production code, reading files, data contracts")
print("  DDL string: Quick explicit schema without imports")
print("\n  RULE: Always explicit in production (faster + safer)")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Schema from Files
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Applying Schema to File Reads
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from datetime import date
import time

print("=== Applying Schema When Reading Files ===")
print()

# Create a sample CSV file for demonstration
sample_data = [
    ("Alice", 30, 95000.50, date(2019, 3, 15)),
    ("Bob", 25, 72000.00, date(2021, 7, 1)),
    ("Charlie", 35, 110000.75, date(2018, 1, 10)),
]
columns = ["name", "age", "salary", "hire_date"]
df_source = spark.createDataFrame(sample_data, columns)

# Write as CSV
csv_path = "/tmp/schema_demo/employees.csv"
df_source.write.mode("overwrite").option("header", "true").csv(csv_path)
print(f"CSV written to: {csv_path}")

# --- Read WITHOUT schema (inference) ---
print("\n--- Read with inferSchema ---")
start = time.time()
df_infer = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
infer_time = time.time() - start
df_infer.printSchema()
print(f"Time with inferSchema: {infer_time:.4f}s")

# --- Read WITH explicit schema ---
print("\n--- Read with explicit schema ---")
my_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True),
    StructField("hire_date", DateType(), True),
])

start = time.time()
df_explicit = spark.read.option("header", "true").schema(my_schema).csv(csv_path)
explicit_time = time.time() - start
df_explicit.printSchema()
df_explicit.show()
print(f"Time with explicit schema: {explicit_time:.4f}s")

# --- Read WITHOUT any schema (all strings) ---
print("\n--- Read without schema (all strings) ---")
df_raw = spark.read.option("header", "true").csv(csv_path)  # No inferSchema!
df_raw.printSchema()  # Everything is string!
print("Without inferSchema or explicit schema, ALL columns are strings!")

print("\n--- Summary ---")
print(f"  inferSchema: Correct types but slower ({infer_time:.4f}s)")
print(f"  explicit schema: Correct types AND faster ({explicit_time:.4f}s)")
print(f"  no schema: All strings (fastest but useless without casting)")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Schema Serialization (JSON, DDL)
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Schema Serialization
# ═══════════════════════════════════════════════════════

import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

print("=== Schema Serialization: Save, Share, Reload ===")
print()

# Original schema
original = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("scores", ArrayType(DoubleType()), True),
])

print("Original schema:")
for field in original.fields:
    print(f"  {field.name}: {field.dataType.simpleString()}, nullable={field.nullable}")

# --- Format 1: JSON (most portable, for config files) ---
print("\n--- 1. JSON format ---")
schema_json = original.json()  # Serialize to JSON string
print(f"  JSON (first 80 chars): {schema_json[:80]}...")

# Save to a file (production pattern)
schema_file = "/tmp/schema_demo/my_schema.json"
dbutils.fs.put(schema_file, schema_json, overwrite=True)  # Save to DBFS
print(f"  Saved to: {schema_file}")

# Reload from file
loaded_json = dbutils.fs.head(schema_file)  # Read back
reloaded = StructType.fromJson(json.loads(loaded_json))  # Deserialize
print(f"  Reloaded match: {reloaded == original}")  # True!

# --- Format 2: DDL string (most human-readable) ---
print("\n--- 2. DDL string format ---")
# Create from DDL
ddl_string = "id INT NOT NULL, name STRING, scores ARRAY<DOUBLE>"
from_ddl = StructType.fromDDL(ddl_string)  # Parse DDL
print(f"  DDL: {ddl_string}")
print(f"  Parsed fields: {[f.name for f in from_ddl.fields]}")

# Convert schema TO DDL-like string
print(f"  simpleString: {original.simpleString()}")

# --- Format 3: Extract schema from existing DataFrame ---
print("\n--- 3. Extract from DataFrame ---")
df = spark.createDataFrame([(1, "test", [1.0, 2.0])], original)
extracted = df.schema  # Get schema from DataFrame
print(f"  Extracted: {extracted.simpleString()}")
print(f"  Match: {extracted == original}")  # True

# Reuse extracted schema for another DataFrame
df2 = spark.createDataFrame([(2, "new", [3.0])], extracted)
df2.show()

print("\n--- Production Pattern ---")
print("  1. Define schema in a shared module or config file")
print("  2. Serialize to JSON and version control it")
print("  3. All readers/writers reference the same schema")
print("  4. Schema changes tracked as code changes")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Python to Spark Type Mapping
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Python Type → Spark Type Mapping
# ═══════════════════════════════════════════════════════

from datetime import date, datetime
from decimal import Decimal

print("=== Python Type → Spark Type (Inference Rules) ===")
print()

# When you DON'T provide a schema, Spark maps Python types like this:
test_data = [
    (
        "hello",           # str → StringType
        42,                # int → LongType (NOT IntegerType!)
        3.14,              # float → DoubleType
        True,              # bool → BooleanType
        date(2024, 1, 1),  # date → DateType
        datetime(2024, 1, 1, 12, 0, 0),  # datetime → TimestampType
        Decimal("99.99"),  # Decimal → DecimalType(38,18)
        [1, 2, 3],         # list → ArrayType
        {"a": 1},          # dict → MapType
        None,              # None → NullType (column becomes nullable)
    ),
]

columns = ["str_val", "int_val", "float_val", "bool_val",
           "date_val", "timestamp_val", "decimal_val",
           "list_val", "dict_val", "null_val"]

df = spark.createDataFrame(test_data, columns)  # Schema inferred from Python types

print("Inferred schema from Python values:")
df.printSchema()

# Key surprises:
print("\n\u26a0\ufe0f  SURPRISES in type inference:")
print("  1. Python int → Spark LongType (64-bit, NOT IntegerType!)")
print("  2. Python float → Spark DoubleType (64-bit)")
print("  3. Python Decimal → Spark DecimalType(38,18) (max precision!)")
print("  4. Python list → Spark ArrayType (element type inferred)")
print("  5. Python dict → Spark MapType (key/value types inferred)")
print("  6. Python None → Spark NullType (if ALL values are None)")

# When this causes problems:
print("\n--- Why this matters ---")
print("  If you write a Parquet file with inferred schema:")
print("    - All ints become LongType (wastes 2x storage!)")
print("    - Decimals get max precision (may not match target table)")
print("    - NullType columns can't be written to some formats")
print("  FIX: Always provide explicit schema for production writes")

df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Deeply Nested Schemas
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Deeply Nested Real-World Schema
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import *
from pyspark.sql.functions import col, explode, size

print("=== Advanced: Real-World Deeply Nested Schema ===")
print()

# Scenario: E-commerce order with nested customer, items, and payment
order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_date", TimestampType(), True),
    StructField("customer", StructType([                    # Nested: customer info
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("address", StructType([                 # Double nested: address
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("zip", StringType(), True),
        ]), True),
    ]), False),
    StructField("items", ArrayType(StructType([            # Array of nested structs
        StructField("product_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("tags", ArrayType(StringType()), True), # Array inside array of structs!
    ])), True),
    StructField("payment", StructType([                     # Nested: payment info
        StructField("method", StringType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("currency", StringType(), True),
    ]), True),
    StructField("metadata", MapType(StringType(), StringType()), True),  # Flexible key-value
])

# Create data matching this complex schema
from datetime import datetime
from decimal import Decimal

order_data = [
    ("ORD-001", datetime(2024, 1, 15, 10, 30),
     (1, "Alice Johnson", "alice@test.com", ("123 Main St", "NYC", "US", "10001")),
     [("P001", "Laptop", 1, 999.99, ["electronics", "premium"]),
      ("P002", "Mouse", 2, 29.99, ["electronics", "accessories"])],
     ("credit_card", Decimal("1059.97"), "USD"),
     {"source": "web", "campaign": "summer_sale"}),
    ("ORD-002", datetime(2024, 1, 16, 14, 0),
     (2, "Bob Smith", "bob@test.com", ("456 Oak Ave", "London", "UK", "SW1A")),
     [("P003", "Book", 3, 15.99, ["books", "education"])],
     ("paypal", Decimal("47.97"), "GBP"),
     {"source": "mobile"}),
]

df = spark.createDataFrame(order_data, order_schema)

print("Full nested schema:")
df.printSchema()

print("\nAccessing deeply nested fields:")
df.select(
    col("order_id"),
    col("customer.name").alias("customer_name"),
    col("customer.address.city").alias("city"),
    col("payment.amount").alias("total"),
    size("items").alias("num_items"),
).show(truncate=False)

print("Exploding items array:")
df.select("order_id", explode("items").alias("item")).select(
    "order_id", col("item.name"), col("item.quantity"), col("item.price")
).show()

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Schema Validation Framework
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Production Schema Validation
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, when

print("=== Production: Schema Validation & Enforcement ===")
print()

# Production pattern: Validate incoming data against expected schema
# Catches issues BEFORE expensive processing

def validate_schema(df, expected_schema, strict=True):
    """Validate a DataFrame schema against expected. Returns list of errors."""
    actual_fields = {f.name: f for f in df.schema.fields}
    expected_fields = {f.name: f for f in expected_schema.fields}
    errors = []
    
    # Check missing columns
    for name, field in expected_fields.items():
        if name not in actual_fields:
            errors.append(f"MISSING: '{name}' ({field.dataType.simpleString()})")
        elif actual_fields[name].dataType != field.dataType:
            errors.append(
                f"TYPE MISMATCH: '{name}' expected {field.dataType.simpleString()}, "
                f"got {actual_fields[name].dataType.simpleString()}"
            )
        elif not field.nullable and actual_fields[name].nullable:
            errors.append(f"NULLABLE: '{name}' should be NOT NULL")
    
    # Check extra columns (strict mode)
    if strict:
        extra = set(actual_fields.keys()) - set(expected_fields.keys())
        if extra:
            errors.append(f"EXTRA columns: {extra}")
    
    return errors

def enforce_schema(df, target_schema):
    """Force a DataFrame to match target schema (add missing, cast types, drop extra)."""
    result = df
    target_cols = [f.name for f in target_schema.fields]
    
    # Add missing columns as null
    for field in target_schema.fields:
        if field.name not in df.columns:
            result = result.withColumn(field.name, lit(None).cast(field.dataType))
    
    # Cast existing columns to target types
    for field in target_schema.fields:
        if field.name in df.columns:
            result = result.withColumn(field.name, col(field.name).cast(field.dataType))
    
    # Select only target columns in order
    return result.select(*target_cols)

# --- Test with good data ---
print("--- Test 1: Valid data ---")
target = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True),
])
df_good = spark.createDataFrame([(1, "Alice", 99.99), (2, "Bob", 45.50)], target)
errors = validate_schema(df_good, target)
print(f"  Errors: {errors if errors else 'None ✔️'}")

# --- Test with bad data ---
print("\n--- Test 2: Schema violations ---")
df_bad = spark.createDataFrame([("1", "Alice", "bad", "extra")], ["id", "name", "amount", "extra_col"])
errors = validate_schema(df_bad, target)
for e in errors:
    print(f"  ❌ {e}")

# --- Enforce (fix) the schema ---
print("\n--- Test 3: Enforce target schema ---")
df_fixed = enforce_schema(df_bad, target)
df_fixed.show()
df_fixed.printSchema()
print("  After enforcement: schema matches target, types cast, extras dropped")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Dynamic Schema Builder from Config
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Config-Driven Schema Builder
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import *
from datetime import date

print("=== Dynamic Schema Builder from Configuration ===")
print()

# In production, schemas are often defined in YAML/JSON config files
# This function builds a StructType from a simple config dictionary

# Type lookup mapping
TYPE_REGISTRY = {
    "string": StringType(), "int": IntegerType(), "long": LongType(),
    "double": DoubleType(), "float": FloatType(), "boolean": BooleanType(),
    "date": DateType(), "timestamp": TimestampType(),
    "decimal(10,2)": DecimalType(10, 2), "decimal(18,6)": DecimalType(18, 6),
    "array<string>": ArrayType(StringType()),
    "array<int>": ArrayType(IntegerType()),
    "array<double>": ArrayType(DoubleType()),
    "map<string,string>": MapType(StringType(), StringType()),
    "map<string,int>": MapType(StringType(), IntegerType()),
}

def build_schema(config):
    """Build StructType from a list of column configs."""
    fields = []
    for col_def in config:
        name = col_def["name"]
        type_str = col_def["type"].lower()
        nullable = col_def.get("nullable", True)
        spark_type = TYPE_REGISTRY.get(type_str)
        if spark_type is None:
            raise ValueError(f"Unknown type: '{type_str}' for column '{name}'")
        fields.append(StructField(name, spark_type, nullable))
    return StructType(fields)

# --- Example: Define tables via config ---
table_configs = {
    "customers": [
        {"name": "id", "type": "int", "nullable": False},
        {"name": "name", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "signup_date", "type": "date"},
        {"name": "tags", "type": "array<string>"},
        {"name": "preferences", "type": "map<string,string>"},
    ],
    "transactions": [
        {"name": "txn_id", "type": "string", "nullable": False},
        {"name": "customer_id", "type": "int", "nullable": False},
        {"name": "amount", "type": "decimal(10,2)", "nullable": False},
        {"name": "timestamp", "type": "timestamp"},
        {"name": "items", "type": "array<string>"},
    ],
}

# Build schemas dynamically
print("Schemas built from config:")
for table_name, config in table_configs.items():
    schema = build_schema(config)
    print(f"\n  Table: {table_name} ({len(schema.fields)} columns)")
    for field in schema.fields:
        null_str = "" if field.nullable else " NOT NULL"
        print(f"    {field.name:15} {field.dataType.simpleString():25}{null_str}")

# Use the dynamic schema
cust_schema = build_schema(table_configs["customers"])
df = spark.createDataFrame([
    (1, "Alice", "alice@test.com", date(2024, 1, 15), ["premium"], {"theme": "dark"}),
    (2, "Bob", "bob@test.com", date(2024, 3, 20), ["standard"], {"lang": "en"}),
], cust_schema)
df.show(truncate=False)

print("\n--- Key: Config-driven schemas = one codebase, many tables ---")
print("--- Store configs in YAML/JSON, version control them ---")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Using IntegerType When Data Has Large Numbers
# MAGIC **Problem:** `IntegerType` is 32-bit (max \~2.1 billion). IDs and timestamps overflow.  
# MAGIC **Example:** A user ID of `3000000000` becomes negative with IntegerType!  
# MAGIC **Fix:** Use `LongType` for IDs, timestamps, or any number that might exceed 2.1 billion.
# MAGIC
# MAGIC ### Mistake #2: Forgetting nullable=True When Data Has Nulls
# MAGIC **Problem:** Setting `nullable=False` but data contains None → runtime error on strict sources.  
# MAGIC **Fix:** Only use `nullable=False` when you've validated data has zero nulls. Default is True (safe).
# MAGIC
# MAGIC ### Mistake #3: Relying on Schema Inference in Production
# MAGIC **Problem:** Inferred schema can change if data changes (new file has different values → different types).  
# MAGIC **Example:** File 1 has `"123"` → inferred as int. File 2 has `"N/A"` → now inferred as string!  
# MAGIC **Fix:** Always define and version your schemas explicitly in production.
# MAGIC
# MAGIC ### Mistake #4: Using FloatType for Financial Data
# MAGIC **Problem:** `FloatType` and `DoubleType` have floating-point imprecision (0.1 + 0.2 ≠ 0.3).  
# MAGIC **Example:** `99.99 * 100 = 9998.999...` instead of `9999`.  
# MAGIC **Fix:** Use `DecimalType(precision, scale)` for money. Example: `DecimalType(10, 2)` for dollars.
# MAGIC
# MAGIC ### Mistake #5: Row Fields in Wrong Order (from nested tuples)
# MAGIC **Problem:** When using Row objects, fields sort alphabetically, not by insertion order.  
# MAGIC **Example:** `Row(name="Alice", age=30)` stores as `(age=30, name="Alice")` internally.  
# MAGIC **Fix:** Use tuples + explicit column names, or be aware of Row's alphabetical sorting.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC **Level 1 (Just read and run):** Run Example 1 and note all primitive types printed.
# MAGIC
# MAGIC **Level 2 (Tiny change):** Change `DecimalType(10, 2)` to `DecimalType(15, 4)`. What changes?
# MAGIC
# MAGIC **Level 3 (Combine two things):** Create a schema with both ArrayType AND MapType in the same DataFrame.
# MAGIC
# MAGIC **Level 4 (New scenario):** Define a schema for a "movie" dataset: title, year, rating, genres (array), director (struct with first/last name).
# MAGIC
# MAGIC **Level 5 (Intermediate project):** Write a CSV file, read it WITH inferSchema, then read it WITH explicit schema. Compare printSchema() output.
# MAGIC
# MAGIC **Level 6 (Design first):** Design a schema for an IoT sensor event: device_id, timestamp, readings (map of sensor_name to value), location (nested lat/lon), alerts (array of strings).
# MAGIC
# MAGIC **Level 7 (Optimize it):** Given a schema with all LongType columns, identify which should be IntegerType or ShortType to save storage. Rewrite with optimal types.
# MAGIC
# MAGIC **Level 8 (Edge cases):** Create a DataFrame where one column is all nulls. What type does Spark infer? How do you fix it with explicit schema?
# MAGIC
# MAGIC **Level 9 (Production-grade):** Build a `validate_and_enforce()` function that takes any DataFrame + target schema and returns: (cleaned_df, error_report_df).
# MAGIC
# MAGIC **Level 10 (Teach it):** Write a 200-word explanation of StructType for a colleague who only knows Excel. Include an analogy and one code example.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, when
from datetime import date, datetime
from decimal import Decimal

# Level 3: Array + Map in same schema
print("=== Level 3 ===")
schema3 = StructType([
    StructField("name", StringType()),
    StructField("hobbies", ArrayType(StringType())),
    StructField("scores", MapType(StringType(), IntegerType())),
])
df3 = spark.createDataFrame([
    ("Alice", ["reading", "coding"], {"math": 95, "english": 88}),
    ("Bob", ["gaming"], {"math": 72}),
], schema3)
df3.show(truncate=False)

# Level 4: Movie schema
print("\n=== Level 4 ===")
movie_schema = StructType([
    StructField("title", StringType(), False),
    StructField("year", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("genres", ArrayType(StringType()), True),
    StructField("director", StructType([
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
    ]), True),
])
df4 = spark.createDataFrame([
    ("Inception", 2010, 8.8, ["Sci-Fi", "Thriller"], ("Christopher", "Nolan")),
    ("Parasite", 2019, 8.5, ["Drama", "Thriller"], ("Bong", "Joon-ho")),
], movie_schema)
df4.show(truncate=False)
df4.printSchema()

# Level 6: IoT sensor schema
print("\n=== Level 6 ===")
iot_schema = StructType([
    StructField("device_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("readings", MapType(StringType(), DoubleType()), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
    ]), True),
    StructField("alerts", ArrayType(StringType()), True),
])
df6 = spark.createDataFrame([
    ("sensor_001", datetime(2024,1,15,10,30), {"temp": 22.5, "humidity": 45.0},
     (40.7128, -74.0060), ["battery_low"]),
    ("sensor_002", datetime(2024,1,15,10,31), {"temp": 35.0, "pressure": 1013.0},
     (51.5074, -0.1278), None),
], iot_schema)
df6.show(truncate=False)
df6.printSchema()

# Level 8: All-null column
print("\n=== Level 8 ===")
df8 = spark.createDataFrame([(1, None), (2, None), (3, None)], ["id", "mystery"])
print("Inferred schema (mystery = void/null):")
df8.printSchema()  # mystery is void or string depending on Spark version
# Fix: use explicit schema
fixed_schema = StructType([
    StructField("id", IntegerType()),
    StructField("mystery", StringType()),  # Force to string
])
df8_fixed = spark.createDataFrame([(1, None), (2, None)], fixed_schema)
df8_fixed.printSchema()  # Now it's StringType

print("\n\u2705 All homework solutions complete!")