# Databricks notebook source
# DBTITLE 1,NB_36 Header
# MAGIC %md
# MAGIC # NB_36 — Struct Functions
# MAGIC
# MAGIC **Module 5: Built-in Functions** | Notebook 36 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Creating structs: struct(), named_struct(), col("outer.inner")
# MAGIC * Accessing fields: dot notation, getField(), col("struct.field")
# MAGIC * Modifying: withField(), dropFields()
# MAGIC * Flattening: inline(), select("struct.*")
# MAGIC * Nested structs: multi-level access and modification
# MAGIC * Struct arrays: arrays of structs patterns
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Essential for nested/semi-structured data)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are Struct Functions?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are Struct Functions? (Real-World Analogy)
# MAGIC
# MAGIC ### 🏠 The Nested Folder System
# MAGIC
# MAGIC A Struct is like a folder containing named sub-files:
# MAGIC
# MAGIC | File System | PySpark Struct | Example |
# MAGIC |---|---|---|
# MAGIC | Folder with files | `StructType` | Address struct with street, city, zip |
# MAGIC | Open folder | `col("address.city")` | Access nested field |
# MAGIC | Add file to folder | `withField()` | Add "country" to address |
# MAGIC | Remove file | `dropFields()` | Remove "zip" from address |
# MAGIC | Dump all files out | `select("struct.*")` | Flatten to top-level columns |
# MAGIC | Create new folder | `struct()` | Bundle columns into struct |
# MAGIC
# MAGIC ### Where Structs Appear
# MAGIC * **JSON data:** Every nested object becomes a struct
# MAGIC * **API responses:** `{"user": {"name": "Alice", "age": 30}}`
# MAGIC * **Addresses:** `{"street": ..., "city": ..., "zip": ...}`
# MAGIC * **Audit fields:** `{"created_by": ..., "created_at": ..., "modified_at": ...}`
# MAGIC * **Composite keys:** Grouping related fields together

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Struct Functions Work
# MAGIC %md
# MAGIC ## SECTION 2 — How Struct Functions Work (Internal Mechanics)
# MAGIC
# MAGIC ### Struct vs Map vs Array
# MAGIC ```
# MAGIC ┌─────────────┬──────────────────┬──────────────────┬──────────────────┐
# MAGIC │             │ Struct           │ Map              │ Array            │
# MAGIC ├─────────────┼──────────────────┼──────────────────┼──────────────────┤
# MAGIC │ Schema      │ Fixed fields    │ Dynamic keys     │ No keys          │
# MAGIC │ Types       │ Mixed per field │ Same type        │ Same type        │
# MAGIC │ Access      │ By field name   │ By key value     │ By index         │
# MAGIC │ Best for    │ Known structure │ Variable KV      │ Ordered list     │
# MAGIC └─────────────┴──────────────────┴──────────────────┴──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Access Patterns
# MAGIC ```
# MAGIC df.schema:
# MAGIC   root
# MAGIC    |-- user: struct
# MAGIC    |    |-- name: string
# MAGIC    |    |-- address: struct
# MAGIC    |    |    |-- city: string
# MAGIC    |    |    |-- zip: string
# MAGIC    |    |-- scores: array<int>
# MAGIC
# MAGIC Access:
# MAGIC   col("user.name")              → "Alice"
# MAGIC   col("user.address.city")      → "NYC"
# MAGIC   col("user").getField("name")  → "Alice"
# MAGIC   df.select("user.*")           → Flatten one level
# MAGIC ```
# MAGIC
# MAGIC ### Key Rules
# MAGIC 1. Struct fields are accessed with DOT notation: `col("struct.field")`
# MAGIC 2. `withField()` ADDS or REPLACES a field inside a struct
# MAGIC 3. `dropFields()` REMOVES fields from a struct
# MAGIC 4. `select("struct.*")` flattens one level of nesting
# MAGIC 5. `inline()` explodes an array of structs into columns

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Creating and Accessing Structs
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Creating and Accessing Structs
# ============================================================
# Real-world: Bundling related fields into logical groups.

from pyspark.sql import SparkSession  # Import SparkSession.
from pyspark.sql.functions import col, struct, lit  # Import struct functions.
from pyspark.sql.types import (  # Import types for schema.
    StructType, StructField, StringType, IntegerType, DoubleType
)  # End type imports.

spark = SparkSession.builder.getOrCreate()  # Get active session.

# Method 1: Create struct from existing columns.
print("=== struct() — Bundle Columns ===")  # Print heading.
people = spark.createDataFrame([
    (1, "Alice", 30, "123 Main St", "New York", "10001"),
    (2, "Bob", 25, "456 Oak Ave", "Chicago", "60601"),
    (3, "Charlie", 35, "789 Pine Rd", "Seattle", "98101"),
], ["id", "name", "age", "street", "city", "zip"])  # Flat data.

# Bundle address fields into a struct.
with_struct = people.select(
    col("id"),  # Keep id.
    col("name"),  # Keep name.
    col("age"),  # Keep age.
    struct(  # Create address struct.
        col("street"),  # Field 1.
        col("city"),  # Field 2.
        col("zip"),  # Field 3.
    ).alias("address"),  # Name the struct.
)

with_struct.show(truncate=False)  # Display with struct.
with_struct.printSchema()  # Show nested schema.

# Method 2: Data with pre-existing nested structure.
print("\n=== Accessing Struct Fields ===")  # Print heading.
# Access struct fields with DOT notation.
with_struct.select(
    col("name"),  # Top-level field.
    col("address.city").alias("city"),  # Dot notation access.
    col("address.zip").alias("zip_code"),  # Another field.
    col("address").getField("street").alias("street"),  # getField() alternative.
).show(truncate=False)  # Display accessed fields.

# Flatten struct with star notation.
print("=== select('struct.*') — Flatten ===")  # Print heading.
with_struct.select(
    col("name"),  # Keep name.
    col("address.*"),  # Flatten all address fields to top level.
).show(truncate=False)  # Display flattened.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Nested JSON as Structs
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Nested JSON as Structs
# ============================================================
# Real-world: JSON APIs naturally produce nested struct data.

from pyspark.sql.functions import col, struct, from_json, to_json, schema_of_json  # Imports.
from pyspark.sql.types import *  # All types.

# Simulate nested JSON structure.
print("=== Nested Structs from Schema ===")  # Print heading.
schema = StructType([  # Define nested schema.
    StructField("order_id", IntegerType()),  # Order ID.
    StructField("customer", StructType([  # Nested customer.
        StructField("name", StringType()),  # Customer name.
        StructField("email", StringType()),  # Customer email.
        StructField("address", StructType([  # Double-nested address.
            StructField("city", StringType()),  # City.
            StructField("country", StringType()),  # Country.
        ])),
    ])),
    StructField("total", DoubleType()),  # Order total.
])  # End schema.

orders = spark.createDataFrame([
    (1, ("Alice", "alice@co.com", ("NYC", "US")), 99.99),
    (2, ("Bob", "bob@co.com", ("London", "UK")), 149.50),
    (3, ("Charlie", "charlie@co.com", ("Berlin", "DE")), 75.00),
], schema)  # Create with schema.

orders.printSchema()  # Show full nested schema.
orders.show(truncate=False)  # Display data.

# Access multi-level nested fields.
print("=== Multi-Level Access ===")  # Print heading.
orders.select(
    col("order_id"),  # Top level.
    col("customer.name").alias("customer_name"),  # Level 1.
    col("customer.address.city").alias("city"),  # Level 2.
    col("customer.address.country").alias("country"),  # Level 2.
    col("total"),  # Top level.
).show(truncate=False)  # Display accessed fields.

# Convert struct to JSON string.
print("=== Struct → JSON String ===")  # Print heading.
orders.select(
    col("order_id"),  # Keep id.
    to_json(col("customer")).alias("customer_json"),  # Struct to JSON string.
).show(truncate=False)  # Display JSON strings.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: withField and dropFields
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: withField and dropFields
# ============================================================
# Real-world: Modifying nested structures without full reconstruction.

from pyspark.sql.functions import col, struct, lit, upper, concat  # Imports.

# Create data with struct.
employees = spark.createDataFrame([
    (1, "Alice", ("Engineering", "Senior", 95000)),
    (2, "Bob", ("Marketing", "Junior", 55000)),
    (3, "Charlie", ("Engineering", "Lead", 120000)),
], "id INT, name STRING, job STRUCT<dept: STRING, level: STRING, salary: INT>")  # Struct data.

print("=== Original Schema ===")  # Print heading.
employees.printSchema()  # Show schema.
employees.show(truncate=False)  # Show data.

# withField() — add or replace a field inside a struct.
print("=== withField() — Add 'bonus' to job struct ===")  # Print heading.
with_bonus = employees.withColumn(
    "job",  # Target the struct column.
    col("job").withField("bonus", (col("job.salary") * 0.1).cast("int")),  # Add bonus field.
)
with_bonus.printSchema()  # Show modified schema.
with_bonus.show(truncate=False)  # Display with bonus.

# withField() to modify an existing field.
print("=== withField() — Modify existing field ===")  # Print heading.
modified = employees.withColumn(
    "job",  # Target the struct.
    col("job").withField("dept", upper(col("job.dept"))),  # Uppercase dept.
)
modified.show(truncate=False)  # Display modified.

# dropFields() — remove fields from struct.
print("=== dropFields() — Remove 'salary' from struct ===")  # Print heading.
no_salary = employees.withColumn(
    "job",  # Target the struct.
    col("job").dropFields("salary"),  # Remove salary field.
)
no_salary.printSchema()  # Schema without salary.
no_salary.show(truncate=False)  # Display without salary.

# Chain withField and dropFields.
print("=== Chain: Add display_name, drop level ===")  # Print heading.
chained = employees.withColumn(
    "job",
    col("job")
        .withField("display", concat(col("job.level"), lit(" "), col("job.dept")))  # Add display.
        .dropFields("level"),  # Remove level.
)
chained.printSchema()  # Show final schema.
chained.show(truncate=False)  # Display chained result.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: inline() and arrays of structs
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: inline() and Arrays of Structs
# ============================================================
# Real-world: Order line items, nested JSON arrays.

from pyspark.sql.functions import (  # Import functions.
    col, inline, explode, struct, collect_list, array, size
)  # End imports.

# Orders with line items (array of structs).
order_data = spark.createDataFrame([
    (1, "Alice", [("Laptop", 1, 999.99), ("Mouse", 2, 29.99)]),
    (2, "Bob", [("Book", 3, 15.99), ("Pen", 10, 2.99), ("Notebook", 5, 8.99)]),
    (3, "Charlie", [("Headphones", 1, 149.99)]),
], "order_id INT, customer STRING, items ARRAY<STRUCT<product: STRING, qty: INT, price: DOUBLE>>")  # Nested orders.

print("=== Original: Array of Structs ===")  # Print heading.
order_data.printSchema()  # Show nested schema.
order_data.show(truncate=False)  # Display nested data.

# inline() — explode array of structs into columns directly.
print("=== inline() — Explode Struct Array into Columns ===")  # Print heading.
order_data.select(
    col("order_id"), col("customer"),  # Keep context.
    inline(col("items")),  # Explodes into product, qty, price columns!
).show(truncate=False)  # Display inlined result.

# Compare with explode() — keeps struct as single column.
print("=== explode() — Keeps Struct as Column ===")  # Print heading.
order_data.select(
    col("order_id"),  # Keep context.
    explode(col("items")).alias("item"),  # Single struct column.
).select(
    col("order_id"),  # Keep context.
    col("item.product"),  # Access struct field.
    col("item.qty"),  # Access struct field.
    col("item.price"),  # Access struct field.
    (col("item.qty") * col("item.price")).alias("line_total"),  # Compute.
).show(truncate=False)  # Display exploded + accessed.

# Build arrays of structs from flat data.
print("=== Building Array of Structs from Flat Data ===")  # Print heading.
flat_items = spark.createDataFrame([
    (1, "Laptop", 1, 999.99),
    (1, "Mouse", 2, 29.99),
    (2, "Book", 3, 15.99),
    (2, "Pen", 10, 2.99),
], ["order_id", "product", "qty", "price"])  # Flat line items.

# Aggregate into array of structs.
nested_orders = flat_items.groupBy("order_id").agg(
    collect_list(struct("product", "qty", "price")).alias("items"),  # Build nested.
    size(collect_list(struct("product", "qty", "price"))).alias("item_count"),  # Count.
)

nested_orders.show(truncate=False)  # Display rebuilt nested structure.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Deep nesting patterns
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Deep Nesting Patterns
# ============================================================
# Real-world: Multi-level JSON from APIs, CRM systems.

from pyspark.sql.functions import col, struct, lit, when, coalesce  # Imports.
from pyspark.sql.types import *  # All types.

# Triple-nested structure: Company > Department > Employee.
schema = StructType([
    StructField("company", StringType()),
    StructField("details", StructType([
        StructField("founded", IntegerType()),
        StructField("hq", StructType([
            StructField("city", StringType()),
            StructField("country", StringType()),
            StructField("coords", StructType([
                StructField("lat", DoubleType()),
                StructField("lon", DoubleType()),
            ])),
        ])),
        StructField("employees", IntegerType()),
    ])),
])  # 4-level nesting.

companies = spark.createDataFrame([
    ("TechCorp", (2010, ("San Francisco", "US", (37.7749, -122.4194)), 5000)),
    ("DataInc", (2015, ("Berlin", "DE", (52.5200, 13.4050)), 1200)),
    ("CloudLtd", (2018, ("London", "UK", (51.5074, -0.1278)), 800)),
], schema)  # Companies.

print("=== 4-Level Nested Schema ===")  # Print heading.
companies.printSchema()  # Show deep nesting.

# Access deeply nested fields.
print("=== Deep Access ===")  # Print heading.
companies.select(
    col("company"),  # Level 0.
    col("details.founded").alias("year_founded"),  # Level 1.
    col("details.hq.city").alias("hq_city"),  # Level 2.
    col("details.hq.country").alias("hq_country"),  # Level 2.
    col("details.hq.coords.lat").alias("latitude"),  # Level 3.
    col("details.hq.coords.lon").alias("longitude"),  # Level 3.
    col("details.employees").alias("employee_count"),  # Level 1.
).show(truncate=False)  # Display deep access.

# Modify deeply nested field.
print("=== Modify Deep Nested Field ===")  # Print heading.
updated = companies.withColumn(
    "details",
    col("details").withField(
        "hq",
        col("details.hq").withField("timezone", lit("UTC"))  # Add timezone to hq.
    )
)
updated.printSchema()  # Show added field.
updated.select("company", "details.hq.timezone").show()  # Display new field.

# Full flattening of nested structure.
print("=== Full Flattening ===")  # Print heading.
companies.select(
    col("company"),  # Top level.
    col("details.*"),  # Flatten level 1.
).select(
    col("company"),  # Keep.
    col("founded"),  # Flattened.
    col("hq.*"),  # Flatten level 2.
    col("employees"),  # Flattened.
).select(
    col("company"), col("founded"), col("employees"),  # Keep.
    col("city"), col("country"),  # Flattened.
    col("coords.*"),  # Flatten level 3.
).show(truncate=False)  # Display fully flat.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Struct comparison and merging
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Struct Comparison and Merging
# ============================================================
# Real-world: Comparing old vs new records, MERGE operations.

from pyspark.sql.functions import (  # Import functions.
    col, struct, when, lit, coalesce, concat_ws, hash as spark_hash
)  # End imports.

# Current and updated records.
current = spark.createDataFrame([
    (1, ("Alice", "alice@old.com", "NYC")),
    (2, ("Bob", "bob@co.com", "Chicago")),
    (3, ("Charlie", "charlie@co.com", "Seattle")),
], "id INT, info STRUCT<name: STRING, email: STRING, city: STRING>")  # Current.

updates = spark.createDataFrame([
    (1, ("Alice", "alice@new.com", "NYC")),  # Email changed.
    (2, ("Bob", "bob@co.com", "Chicago")),  # No change.
    (3, ("Charlie", "charlie@co.com", "Portland")),  # City changed.
], "id INT, info STRUCT<name: STRING, email: STRING, city: STRING>")  # Updates.

# Compare structs — structs support equality comparison!
print("=== Struct Equality Comparison ===")  # Print heading.
joined = current.alias("c").join(updates.alias("u"), "id")

joined.select(
    col("id"),  # Keep id.
    col("c.info").alias("current_info"),  # Current struct.
    col("u.info").alias("updated_info"),  # Updated struct.
    (col("c.info") == col("u.info")).alias("unchanged"),  # Struct equality!
).show(truncate=False)  # Display comparison.

# Field-level diff.
print("=== Field-Level Diff ===")  # Print heading.
joined.select(
    col("id"),  # Keep id.
    when(col("c.info.name") != col("u.info.name"), "CHANGED").otherwise("same").alias("name_status"),
    when(col("c.info.email") != col("u.info.email"), "CHANGED").otherwise("same").alias("email_status"),
    when(col("c.info.city") != col("u.info.city"), "CHANGED").otherwise("same").alias("city_status"),
).show(truncate=False)  # Display field-level changes.

# Merge: take updated value if changed, current otherwise.
print("=== Selective Merge ===")  # Print heading.
merged = joined.select(
    col("id"),  # Keep id.
    struct(  # Build merged struct.
        coalesce(col("u.info.name"), col("c.info.name")).alias("name"),  # Prefer update.
        coalesce(col("u.info.email"), col("c.info.email")).alias("email"),  # Prefer update.
        coalesce(col("u.info.city"), col("c.info.city")).alias("city"),  # Prefer update.
    ).alias("merged_info"),  # Merged struct.
)
merged.show(truncate=False)  # Display merged.

# Hash struct for change detection.
print("=== Struct Hashing for CDC ===")  # Print heading.
current.select(
    col("id"),  # Keep id.
    col("info"),  # Keep struct.
    spark_hash(col("info.name"), col("info.email"), col("info.city")).alias("row_hash"),  # Hash for CDC.
).show(truncate=False)  # Display hashed structs.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Dynamic struct flattening
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Dynamic Struct Flattening
# ============================================================
# Real-world: Recursively flatten any nested JSON structure.

from pyspark.sql.functions import col  # Import col.
from pyspark.sql.types import StructType, ArrayType  # Type checking.

# Utility: Recursively flatten all nested structs.
def flatten_struct(df, separator="_"):
    """Recursively flatten all struct columns in a DataFrame."""
    flat_cols = []  # Accumulator for flat columns.
    
    def _flatten(schema, prefix=""):
        """Recursive helper to traverse schema."""
        for field in schema.fields:  # Iterate fields.
            col_name = f"{prefix}{field.name}" if prefix else field.name  # Build path.
            col_path = f"{prefix}{field.name}" if prefix else field.name  # Dot path.
            
            if isinstance(field.dataType, StructType):  # Nested struct.
                _flatten(field.dataType, f"{col_name}{separator}")  # Recurse.
            else:
                # Replace separator in path for column access.
                access_path = col_name.replace(separator, ".") if prefix else col_name
                flat_cols.append(col(access_path).alias(col_name))  # Add flat column.
    
    _flatten(df.schema)  # Start recursion.
    return df.select(flat_cols)  # Select flat columns.

# Test with deeply nested data.
from pyspark.sql.types import *  # All types.
nested_schema = StructType([
    StructField("id", IntegerType()),
    StructField("user", StructType([
        StructField("name", StringType()),
        StructField("contact", StructType([
            StructField("email", StringType()),
            StructField("phone", StringType()),
        ])),
        StructField("address", StructType([
            StructField("street", StringType()),
            StructField("city", StringType()),
        ])),
    ])),
    StructField("score", DoubleType()),
])  # 3-level nesting.

deep_df = spark.createDataFrame([
    (1, ("Alice", ("alice@co.com", "555-0101"), ("123 Main", "NYC")), 95.5),
    (2, ("Bob", ("bob@co.com", "555-0202"), ("456 Oak", "LA")), 87.3),
], nested_schema)  # Nested data.

print("=== Before Flattening ===")  # Print heading.
deep_df.printSchema()  # Nested schema.

# Note: The simple flatten above works for schemas without duplicate leaf names.
# For production, use prefix-based approach.
print("=== After Flattening (manual for control) ===")  # Print heading.
flat = deep_df.select(
    col("id"),  # Top level.
    col("user.name").alias("user_name"),  # Level 1.
    col("user.contact.email").alias("user_contact_email"),  # Level 2.
    col("user.contact.phone").alias("user_contact_phone"),  # Level 2.
    col("user.address.street").alias("user_address_street"),  # Level 2.
    col("user.address.city").alias("user_address_city"),  # Level 2.
    col("score"),  # Top level.
)
flat.printSchema()  # Flat schema.
flat.show(truncate=False)  # Display flat data.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Struct evolution patterns
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Struct Evolution Patterns
# ============================================================
# Real-world: Schema evolution - adding/removing fields over time.

from pyspark.sql.functions import (  # Import functions.
    col, struct, lit, when, coalesce, current_timestamp,
    to_json, from_json, schema_of_json
)  # End imports.
from pyspark.sql.types import *  # All types.

# Schema V1: Basic user info.
v1_data = spark.createDataFrame([
    (1, ("Alice", "alice@co.com")),
    (2, ("Bob", "bob@co.com")),
], "id INT, profile STRUCT<name: STRING, email: STRING>")  # V1 schema.

print("=== Schema V1 ===")  # Print heading.
v1_data.printSchema()  # V1 schema.

# Evolve to V2: Add phone and preferences struct.
print("=== Evolve V1 → V2: Add fields ===")  # Print heading.
v2_data = v1_data.withColumn(
    "profile",
    col("profile")
        .withField("phone", lit(None).cast("string"))  # Add nullable phone.
        .withField("preferences", struct(  # Add nested preferences.
            lit("en").alias("language"),  # Default language.
            lit("dark").alias("theme"),  # Default theme.
        ))
)

v2_data.printSchema()  # V2 schema.
v2_data.show(truncate=False)  # Display V2 data.

# Evolve to V3: Remove email, add verified flag.
print("=== Evolve V2 → V3: Remove email, add verified ===")  # Print heading.
v3_data = v2_data.withColumn(
    "profile",
    col("profile")
        .dropFields("email")  # Remove email (GDPR compliance).
        .withField("verified", lit(False))  # Add verified flag.
)

v3_data.printSchema()  # V3 schema.
v3_data.show(truncate=False)  # Display V3 data.

# === Pattern: Add audit struct to any DataFrame ===
print("=== Add Audit Struct ===")  # Print heading.
def add_audit(df, created_by="system"):
    """Add audit struct with timestamps."""
    return df.withColumn(
        "_audit",
        struct(  # Audit metadata.
            lit(created_by).alias("created_by"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
            lit(1).alias("version"),
        )
    )  # Return with audit.

audited = add_audit(v1_data, "admin")  # Add audit.
audited.printSchema()  # Show with audit.
audited.show(truncate=False)  # Display audited data.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production struct utilities
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Production Struct Utilities
# ============================================================
# Real-world: Schema validation, null handling in structs.

from pyspark.sql.functions import (  # Import functions.
    col, struct, when, lit, coalesce, isnull, greatest, expr,
    concat, to_json, size, array
)  # End imports.
from pyspark.sql.types import *  # All types.

# === Pattern: NULL-safe struct access ===
print("=== NULL-Safe Struct Access ===")  # Print heading.
nullable_df = spark.createDataFrame([
    (1, ("Alice", "NYC")),
    (2, None),  # NULL struct!
    (3, ("Charlie", None)),  # NULL field inside struct.
], "id INT, info STRUCT<name: STRING, city: STRING>")  # With NULLs.

nullable_df.select(
    col("id"),  # Keep id.
    col("info"),  # Original (may be NULL).
    col("info.name").alias("name_direct"),  # NULL if struct is NULL.
    # Safe access with fallback.
    coalesce(col("info.name"), lit("Unknown")).alias("name_safe"),
    coalesce(col("info.city"), lit("N/A")).alias("city_safe"),
    col("info").isNull().alias("struct_is_null"),  # Check whole struct.
).show(truncate=False)  # Display null-safe access.

# === Pattern: Struct completeness check ===
print("=== Struct Completeness Check ===")  # Print heading.
records = spark.createDataFrame([
    (1, ("Alice", "alice@co.com", "555-0101")),  # Complete.
    (2, ("Bob", None, "555-0202")),  # Missing email.
    (3, ("Charlie", "charlie@co.com", None)),  # Missing phone.
    (4, (None, None, None)),  # All missing.
], "id INT, contact STRUCT<name: STRING, email: STRING, phone: STRING>")  # Contact data.

records.select(
    col("id"),  # Keep id.
    col("contact"),  # Keep struct.
    # Count non-null fields.
    (col("contact.name").isNotNull().cast("int") +
     col("contact.email").isNotNull().cast("int") +
     col("contact.phone").isNotNull().cast("int")
    ).alias("filled_fields"),  # Completeness score.
    # Completeness percentage.
    ((col("contact.name").isNotNull().cast("int") +
      col("contact.email").isNotNull().cast("int") +
      col("contact.phone").isNotNull().cast("int")
     ) / 3.0 * 100).alias("completeness_pct"),  # Percentage.
).show(truncate=False)  # Display completeness.

# === Pattern: Struct to flat columns with prefix ===
print("=== Flatten with Prefix ===")  # Print heading.
def flatten_with_prefix(df, struct_col, prefix=None):
    """Flatten a struct column, optionally adding a prefix."""
    prefix = prefix or struct_col  # Default prefix = struct name.
    struct_fields = [f.name for f in df.schema[struct_col].dataType.fields]  # Get field names.
    other_cols = [col(c) for c in df.columns if c != struct_col]  # Non-struct columns.
    flat_cols = [col(f"{struct_col}.{f}").alias(f"{prefix}_{f}") for f in struct_fields]  # Prefixed.
    return df.select(other_cols + flat_cols)  # Return flat.

result = flatten_with_prefix(records, "contact", "c")  # Flatten with prefix.
result.show(truncate=False)  # Display prefixed flat columns.
result.printSchema()  # Show flat schema.

print("✅ Struct functions mastery complete!")  # Done.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Struct Functions
# MAGIC
# MAGIC ### Mistake 1: Using col("struct")["field"] instead of dot notation
# MAGIC ```python
# MAGIC # WRONG (Python dict syntax doesn't work on struct columns).
# MAGIC df.select(col("address")["city"])  # May not work as expected!
# MAGIC
# MAGIC # CORRECT — Use dot notation or getField().
# MAGIC df.select(col("address.city"))  # Dot notation.
# MAGIC df.select(col("address").getField("city"))  # getField method.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Expecting withField to create a new column
# MAGIC ```python
# MAGIC # WRONG — withField modifies INSIDE a struct, doesn't add top-level column.
# MAGIC df.withColumn("new_col", col("struct").withField("x", lit(1)))  # Modifies struct!
# MAGIC
# MAGIC # If you want a new top-level column:
# MAGIC df.withColumn("new_col", col("struct.field"))  # Extract to top level.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Forgetting that NULL struct makes all fields NULL
# MAGIC ```python
# MAGIC # If struct column is NULL, ALL nested access returns NULL.
# MAGIC # col("info.name") where info is NULL → NULL (no error, just NULL)
# MAGIC # Always check: col("info").isNull() first if needed.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Not handling arrays of structs properly
# MAGIC ```python
# MAGIC # WRONG — Can't use dot notation directly on array of structs.
# MAGIC df.select(col("items.product"))  # Returns ARRAY of values, not single value!
# MAGIC
# MAGIC # CORRECT — Explode first, then access.
# MAGIC df.select(explode(col("items")).alias("item")).select(col("item.product"))
# MAGIC # Or use inline() for direct expansion.
# MAGIC df.select(inline(col("items")))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Schema mismatch when creating structs
# MAGIC ```python
# MAGIC # All struct fields must have consistent types across rows.
# MAGIC # Mixing ("Alice", 30) and ("Bob", "thirty") in same struct column = error.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Struct Function Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Create a struct from 3 columns. Access each field using dot notation.
# MAGIC 2. Use `select("struct.*")` to flatten a struct.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Use `withField()` to add a computed field inside a struct.
# MAGIC 4. Use `dropFields()` to remove sensitive data from a nested struct.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Create a DataFrame with array of structs, use `inline()` to flatten, then re-aggregate with `collect_list(struct(...))`.
# MAGIC 6. Combine `to_json()` and `from_json()` to serialize/deserialize a struct.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Build a user profile system: struct with `{personal: {name, email}, settings: {theme, lang}, audit: {created, modified}}`.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Implement a schema evolution pipeline: V1→V2→V3 with backward-compatible changes.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design a recursive flatten function that handles any depth of nesting.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Compare: accessing deeply nested field via dot notation vs pre-flattening on 1M rows.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Test: NULL structs, structs with all-NULL fields, empty strings in struct fields.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build a struct completeness scorer: for any struct, compute % of non-null fields.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create decision guide: "Struct vs Map vs JSON string — when to use which."

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all for solutions.
from pyspark.sql.types import *  # All types.

# --- Level 1: Basic struct operations ---
print("=== Level 1: Struct Creation and Access ===")  # Print heading.
people = spark.createDataFrame([
    ("Alice", 30, "NYC"), ("Bob", 25, "LA")
], ["name", "age", "city"])  # Flat data.

with_struct = people.select(
    struct(col("name"), col("age"), col("city")).alias("person"),  # Create struct.
)
with_struct.select(
    col("person.name"),  # Access with dot.
    col("person.age"),  # Access with dot.
    col("person.city"),  # Access with dot.
).show()  # Display.

# Flatten.
with_struct.select("person.*").show()  # Star notation flattens.

# --- Level 5: Schema evolution ---
print("=== Level 5: Schema Evolution ===")  # Print heading.
# V1: Basic.
v1 = spark.createDataFrame([
    (1, ("Alice", "alice@co.com")),
], "id INT, user STRUCT<name: STRING, email: STRING>")  # V1.

# V2: Add phone.
v2 = v1.withColumn("user", col("user").withField("phone", lit(None).cast("string")))
print("V2 schema:")  # Label.
v2.printSchema()  # Show V2.

# V3: Add preferences, drop email.
v3 = v2.withColumn("user",
    col("user")
        .withField("preferences", struct(lit("en").alias("lang"), lit("dark").alias("theme")))
        .dropFields("email")
)
print("V3 schema:")  # Label.
v3.printSchema()  # Show V3.
v3.show(truncate=False)  # Display V3.

# --- Level 8: Edge cases ---
print("=== Level 8: NULL Struct Edge Cases ===")  # Print heading.
edge = spark.createDataFrame([
    (1, ("Alice", "NYC")),
    (2, None),  # NULL struct.
    (3, ("", "")),  # Empty strings.
    (4, (None, None)),  # NULL fields.
], "id INT, info STRUCT<name: STRING, city: STRING>")  # Edge cases.

edge.select(
    col("id"),  # Keep id.
    col("info").isNull().alias("struct_null"),  # Whole struct NULL?
    col("info.name").alias("name"),  # NULL if struct NULL.
    col("info.name").isNull().alias("name_null"),  # Field NULL?
    (col("info.name") == "").alias("name_empty"),  # Empty string?
).show(truncate=False)  # Display edge case handling.

print("✅ All homework solutions complete!")  # Completion message.