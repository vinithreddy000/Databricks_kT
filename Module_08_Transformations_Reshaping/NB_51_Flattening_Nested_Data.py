# Databricks notebook source
# DBTITLE 1,NB_51 Header
# MAGIC %md
# MAGIC # NB_51 — Flattening Nested Data
# MAGIC
# MAGIC **Module 8: Transformations & Reshaping** | Notebook 51 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Explode arrays: explode(), explode_outer(), posexplode()
# MAGIC * Flatten structs: col("struct.field"), struct("*")
# MAGIC * Nested JSON flattening (recursive)
# MAGIC * Map columns: explode, map_keys(), map_values()
# MAGIC * Multi-level nesting (arrays of structs of arrays)
# MAGIC * Dynamic schema flattening
# MAGIC * Handling heterogeneous nested structures
# MAGIC * Performance considerations for large nested datasets
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐⭐ (Critical for JSON/API data ingestion)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — Why Flatten Nested Data
# MAGIC %md
# MAGIC ## SECTION 1 — Why Flatten Nested Data? (Real-World Analogy)
# MAGIC
# MAGIC ### 🏭 The Unboxing Assembly Line
# MAGIC
# MAGIC Nested data is like packages within packages:
# MAGIC
# MAGIC ```
# MAGIC Order Box (Struct)
# MAGIC ├── Customer Info (Struct)
# MAGIC │   ├── name: "Alice"
# MAGIC │   └── address (Struct)
# MAGIC │       ├── city: "Seattle"
# MAGIC │       └── zip: "98101"
# MAGIC ├── Items (Array of Structs)
# MAGIC │   ├── {sku: "A1", qty: 2, price: 10.00}
# MAGIC │   └── {sku: "B2", qty: 1, price: 25.00}
# MAGIC └── Tags (Map)
# MAGIC     ├── priority -> "high"
# MAGIC     └── channel -> "web"
# MAGIC ```
# MAGIC
# MAGIC Flattening = opening all boxes and laying items flat on the table.
# MAGIC
# MAGIC ### The Three Nesting Types
# MAGIC | Type | Access Pattern | Flatten Method |
# MAGIC |---|---|---|
# MAGIC | Struct | `col("parent.child")` | Select individual fields |
# MAGIC | Array | `explode()` | One row per element |
# MAGIC | Map | `explode()` or `map_keys/values` | Key-value pairs to rows |
# MAGIC
# MAGIC ### When to Flatten
# MAGIC * **Always** for SQL queries (SQL doesn't handle nested well)
# MAGIC * **Always** for ML feature extraction
# MAGIC * **Often** for reporting and dashboards
# MAGIC * **Sometimes** keep nested for storage efficiency
# MAGIC
# MAGIC ### Warning: Row Multiplication
# MAGIC ```
# MAGIC explode() multiplies rows!
# MAGIC A table with 1M rows and arrays of avg size 5 = 5M rows after explode.
# MAGIC Always check cardinality impact.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 2 — Flattening Mechanics
# MAGIC %md
# MAGIC ## SECTION 2 — Flattening Mechanics in Spark
# MAGIC
# MAGIC ### Struct Access
# MAGIC ```python
# MAGIC # Dot notation for struct fields
# MAGIC df.select("customer.name", "customer.address.city")
# MAGIC
# MAGIC # Expand all struct fields to top-level
# MAGIC df.select("customer.*")  # All fields of customer struct
# MAGIC
# MAGIC # Nested struct
# MAGIC df.select("order.customer.address.*")
# MAGIC ```
# MAGIC
# MAGIC ### Array Flattening
# MAGIC ```python
# MAGIC # explode: one row per array element (skips nulls/empty)
# MAGIC explode(col("items"))  # NULL/[] rows removed
# MAGIC
# MAGIC # explode_outer: keeps nulls/empty as NULL row
# MAGIC explode_outer(col("items"))  # NULL rows preserved
# MAGIC
# MAGIC # posexplode: adds position index
# MAGIC posexplode(col("items"))  # (pos, col) columns
# MAGIC
# MAGIC # flatten: merge nested arrays
# MAGIC flatten(array(array(1,2), array(3,4)))  # [1,2,3,4]
# MAGIC ```
# MAGIC
# MAGIC ### Map Flattening
# MAGIC ```python
# MAGIC # Explode map to key-value rows
# MAGIC df.select(explode(col("tags")))  # (key, value) columns
# MAGIC
# MAGIC # Access specific keys
# MAGIC df.select(col("tags")["priority"])  # Single value
# MAGIC df.select(map_keys(col("tags")))    # All keys as array
# MAGIC df.select(map_values(col("tags")))  # All values as array
# MAGIC ```
# MAGIC
# MAGIC ### Recursive Flattening Pattern
# MAGIC ```python
# MAGIC def flatten_df(df):
# MAGIC     """Recursively flatten all struct/array columns."""
# MAGIC     # 1. Expand structs: col("s.field") for each struct field
# MAGIC     # 2. Explode arrays: one row per element
# MAGIC     # 3. Repeat until no complex types remain
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Struct flattening
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Struct Flattening
# ============================================================
# Real-world: API JSON response with nested objects.

from pyspark.sql import SparkSession  # Import.
from pyspark.sql.functions import col  # Functions.
from pyspark.sql.types import (  # Types.
    StructType, StructField, StringType, IntegerType, DoubleType
)

spark = SparkSession.builder.getOrCreate()  # Session.

# Nested struct data (simulating JSON API response).
data = [
    (1, ("Alice", "Smith", ("Seattle", "WA", "98101")), "Engineering"),
    (2, ("Bob", "Jones", ("Portland", "OR", "97201")), "Marketing"),
    (3, ("Carol", "Lee", ("Denver", "CO", "80201")), "Sales"),
    (4, ("Dave", "Kim", ("Austin", "TX", "73301")), "Engineering"),
]

# Define nested schema.
address_schema = StructType([
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("zip", StringType()),
])  # Address struct.

person_schema = StructType([
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("address", address_schema),
])  # Person struct.

full_schema = StructType([
    StructField("emp_id", IntegerType()),
    StructField("person", person_schema),
    StructField("department", StringType()),
])  # Full schema.

df = spark.createDataFrame(data, full_schema)  # Create.

print("=== Nested Schema ===")  # Heading.
df.printSchema()  # Show nesting.
df.show(truncate=False)  # Display.

# Method 1: Access specific nested fields with dot notation.
print("=== Dot Notation Access ===")  # Heading.
df.select(
    col("emp_id"),  # Top-level.
    col("person.first_name"),  # First level struct.
    col("person.last_name"),  # First level struct.
    col("person.address.city"),  # Second level struct.
    col("person.address.state"),  # Second level.
    col("department"),  # Top-level.
).show()  # Display.

# Method 2: Expand struct with .* notation.
print("=== Expand with .* ===")  # Heading.
df.select("emp_id", "person.*", "department").show(truncate=False)  # First level.

# Method 3: Fully flatten to flat columns.
print("=== Fully Flattened ===")  # Heading.
flat = df.select(
    col("emp_id"),  # Keep.
    col("person.first_name").alias("first_name"),  # Rename.
    col("person.last_name").alias("last_name"),  # Rename.
    col("person.address.city").alias("city"),  # Flatten.
    col("person.address.state").alias("state"),  # Flatten.
    col("person.address.zip").alias("zip_code"),  # Flatten.
    col("department"),  # Keep.
)
flat.show()  # Display.
flat.printSchema()  # All flat now.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Array explode patterns
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Array Explode Patterns
# ============================================================
# Real-world: E-commerce orders with line items array.

from pyspark.sql.functions import (
    col, explode, explode_outer, posexplode, size, array_contains
)  # Imports.
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType  # Types.

# Orders with array of items.
orders_data = [
    ("ORD-001", "Alice", ["Widget", "Gadget", "Doohickey"]),
    ("ORD-002", "Bob", ["Widget"]),
    ("ORD-003", "Carol", []),         # Empty array.
    ("ORD-004", "Dave", None),        # NULL array.
    ("ORD-005", "Eve", ["Gadget", "Gadget", "Widget"]),  # Duplicates.
]

orders = spark.createDataFrame(orders_data, ["order_id", "customer", "items"])  # Create.

print("=== Orders with Array Column ===")  # Heading.
orders.show(truncate=False)  # Display.
print(f"Row count before explode: {orders.count()}")  # Count.

# explode(): removes NULL and empty arrays.
print("\n=== explode() — skips NULL/empty ===")  # Heading.
exploded = orders.select("order_id", "customer", explode("items").alias("item"))  # Explode.
exploded.show()  # Display.
print(f"Row count after explode: {exploded.count()}")  # ORD-003/004 gone!

# explode_outer(): keeps NULL/empty as NULL row.
print("\n=== explode_outer() — preserves all rows ===")  # Heading.
exploded_outer = orders.select("order_id", "customer", explode_outer("items").alias("item"))  # Outer.
exploded_outer.show()  # Display.
print(f"Row count after explode_outer: {exploded_outer.count()}")  # Keeps all.

# posexplode(): adds position index.
print("\n=== posexplode() — with position ===")  # Heading.
pos_exploded = orders.select("order_id", posexplode("items").alias("position", "item"))  # With pos.
pos_exploded.show()  # Display.

# Practical: Array size and contains.
print("\n=== Array Operations ===")  # Heading.
orders.select(
    "order_id",
    size("items").alias("item_count"),  # Array length.
    array_contains("items", "Widget").alias("has_widget"),  # Check membership.
).show()  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Map column flattening
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Map Column Flattening
# ============================================================
# Real-world: Configuration/metadata stored as key-value maps.

from pyspark.sql.functions import (
    col, explode, map_keys, map_values, element_at, create_map, lit
)  # Imports.
from pyspark.sql.types import MapType, StringType  # Types.

# Data with map columns (common in event/config data).
config_data = spark.createDataFrame([
    ("app_1", {"env": "prod", "region": "us-east", "version": "2.1"}),
    ("app_2", {"env": "staging", "region": "eu-west", "version": "2.0", "debug": "true"}),
    ("app_3", {"env": "prod", "region": "ap-south"}),  # Fewer keys.
    ("app_4", None),  # NULL map.
], ["app_id", "config"])  # Schema.

print("=== Map Column Data ===")  # Heading.
config_data.printSchema()  # Show map type.
config_data.show(truncate=False)  # Display.

# Method 1: Access specific keys.
print("=== Access Specific Keys ===")  # Heading.
config_data.select(
    "app_id",  # Keep.
    col("config")["env"].alias("environment"),  # Key access.
    col("config")["region"].alias("region"),  # Key access.
    col("config")["version"].alias("version"),  # May be NULL.
    element_at(col("config"), "debug").alias("debug_mode"),  # element_at syntax.
).show()  # Display.

# Method 2: Explode map to key-value rows.
print("=== Explode Map to Rows ===")  # Heading.
exploded_map = config_data.select(
    "app_id",  # Keep.
    explode("config").alias("config_key", "config_value"),  # Key-value pairs.
)
exploded_map.show()  # Display.

# Method 3: Get keys and values as arrays.
print("=== Map Keys and Values ===")  # Heading.
config_data.select(
    "app_id",  # Keep.
    map_keys(col("config")).alias("all_keys"),  # Keys array.
    map_values(col("config")).alias("all_values"),  # Values array.
).show(truncate=False)  # Display.

# Method 4: Pivot map to columns (dynamic).
print("=== Map Pivoted to Columns ===")  # Heading.
# Get all unique keys across all rows.
all_keys = (
    config_data.select(explode("config").alias("k", "v"))
    .select("k").distinct().rdd.flatMap(lambda x: x).collect()
)  # Unique keys.
all_keys.sort()  # Sort.
print(f"All config keys: {all_keys}")  # Show.

# Select each key as a column.
pivoted_config = config_data.select(
    "app_id",  # Keep.
    *[col("config")[k].alias(k) for k in all_keys]  # Each key -> column.
)
pivoted_config.show()  # Fully flat.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Arrays of structs
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Arrays of Structs
# ============================================================
# Real-world: E-commerce order with line items (array of structs).

from pyspark.sql.functions import (
    col, explode, explode_outer, posexplode, sum as spark_sum, size
)  # Imports.
from pyspark.sql.types import *  # All types.

# Schema: order with items array of structs.
item_schema = StructType([
    StructField("sku", StringType()),
    StructField("name", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
])  # Item struct.

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer", StringType()),
    StructField("order_date", StringType()),
    StructField("items", ArrayType(item_schema)),  # Array of item structs.
    StructField("shipping", StructType([  # Nested struct.
        StructField("method", StringType()),
        StructField("cost", DoubleType()),
    ])),
])  # Full schema.

# Create complex nested data.
orders_data = [
    ("ORD-001", "Alice", "2024-01-15",
     [{"sku": "W01", "name": "Widget", "quantity": 3, "unit_price": 10.0},
      {"sku": "G01", "name": "Gadget", "quantity": 1, "unit_price": 25.0}],
     {"method": "express", "cost": 15.0}),
    ("ORD-002", "Bob", "2024-01-16",
     [{"sku": "W01", "name": "Widget", "quantity": 5, "unit_price": 10.0},
      {"sku": "D01", "name": "Doohickey", "quantity": 2, "unit_price": 5.0},
      {"sku": "G01", "name": "Gadget", "quantity": 2, "unit_price": 25.0}],
     {"method": "standard", "cost": 5.0}),
    ("ORD-003", "Carol", "2024-01-17",
     [{"sku": "D01", "name": "Doohickey", "quantity": 10, "unit_price": 5.0}],
     {"method": "express", "cost": 15.0}),
]

orders = spark.createDataFrame(orders_data, order_schema)  # Create.

print("=== Nested Order Data ===")  # Heading.
orders.printSchema()  # Show nesting.
orders.show(truncate=False)  # Display.

# Step 1: Explode items array.
print("=== Exploded Items ===")  # Heading.
exploded = orders.select(
    col("order_id"),  # Keep.
    col("customer"),  # Keep.
    col("order_date"),  # Keep.
    col("shipping.method").alias("ship_method"),  # Flatten struct.
    col("shipping.cost").alias("ship_cost"),  # Flatten struct.
    posexplode("items").alias("line_num", "item"),  # Explode with position.
)
exploded.show(truncate=False)  # Display.

# Step 2: Flatten the item struct.
print("=== Fully Flattened ===")  # Heading.
flat_orders = exploded.select(
    col("order_id"),  # Keep.
    col("customer"),  # Keep.
    col("order_date").cast("date"),  # Cast.
    col("ship_method"),  # Already flat.
    col("ship_cost"),  # Already flat.
    (col("line_num") + 1).alias("line_number"),  # 1-based.
    col("item.sku"),  # Struct field.
    col("item.name").alias("product_name"),  # Struct field.
    col("item.quantity"),  # Struct field.
    col("item.unit_price"),  # Struct field.
    (col("item.quantity") * col("item.unit_price")).alias("line_total"),  # Computed.
)
flat_orders.show()  # Display.

# Step 3: Aggregate from flattened data.
print("=== Order Totals ===")  # Heading.
flat_orders.groupBy("order_id", "customer", "ship_cost").agg(
    spark_sum("line_total").alias("items_total"),  # Sum items.
).withColumn(
    "grand_total", col("items_total") + col("ship_cost")  # With shipping.
).show()  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Multi-level nesting
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Multi-Level Nesting
# ============================================================
# Real-world: Deeply nested JSON from social media API.

from pyspark.sql.functions import (
    col, explode, explode_outer, size, collect_list, struct
)  # Imports.

# Simulate social media post data with deep nesting.
posts_json = """
[
  {"post_id": "P1", "author": {"name": "Alice", "followers": 1500},
   "content": "Great day!", "tags": ["life", "happy"],
   "comments": [
     {"user": "Bob", "text": "Agree!", "reactions": ["like", "heart"]},
     {"user": "Carol", "text": "Same here", "reactions": ["like"]}
   ]},
  {"post_id": "P2", "author": {"name": "Bob", "followers": 800},
   "content": "New project", "tags": ["tech", "code", "python"],
   "comments": [
     {"user": "Alice", "text": "Cool!", "reactions": ["like", "fire", "heart"]}
   ]},
  {"post_id": "P3", "author": {"name": "Carol", "followers": 2200},
   "content": "Weekend vibes", "tags": ["life"],
   "comments": []}
]
"""

# Read JSON string as DataFrame.
from pyspark.sql import Row  # Import.
import json  # Import.

rdd = spark.sparkContext.parallelize([posts_json])  # Create RDD.
posts = spark.read.json(rdd)  # Infer schema from JSON.

print("=== Deeply Nested Post Data ===")  # Heading.
posts.printSchema()  # Show full nesting.
posts.show(truncate=False)  # Display.

# Level 1: Flatten author struct and tags array.
print("=== Level 1: Author + Tags ===")  # Heading.
level1 = posts.select(
    col("post_id"),  # Keep.
    col("author.name").alias("author_name"),  # Flatten struct.
    col("author.followers").alias("author_followers"),  # Flatten struct.
    col("content"),  # Keep.
    size(col("tags")).alias("tag_count"),  # Array size.
    size(col("comments")).alias("comment_count"),  # Array size.
)
level1.show()  # Display.

# Level 2: Explode comments (array of structs).
print("=== Level 2: Explode Comments ===")  # Heading.
with_comments = posts.select(
    col("post_id"),  # Keep.
    col("author.name").alias("author"),  # Flatten.
    col("content"),  # Keep.
    explode_outer("comments").alias("comment"),  # Explode.
)
with_comments.show(truncate=False)  # Display.

# Level 3: Flatten comment struct AND explode nested reactions.
print("=== Level 3: Full Flatten (posts -> comments -> reactions) ===")  # Heading.
fully_flat = with_comments.select(
    col("post_id"),  # Keep.
    col("author"),  # Keep.
    col("comment.user").alias("commenter"),  # Struct field.
    col("comment.text").alias("comment_text"),  # Struct field.
    explode_outer("comment.reactions").alias("reaction"),  # Nested array explode.
)
fully_flat.show()  # Display.
print(f"Original: {posts.count()} rows -> Fully flattened: {fully_flat.count()} rows")  # Show expansion.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Recursive schema flattener
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Recursive Schema Flattener
# ============================================================
# Real-world: Generic utility to flatten ANY nested DataFrame.

from pyspark.sql.functions import col, explode_outer  # Imports.
from pyspark.sql.types import StructType, ArrayType  # Types.
from pyspark.sql import DataFrame  # Type.

def flatten_schema(df: DataFrame, separator: str = "_") -> DataFrame:
    """Recursively flatten all struct columns (not arrays)."""
    complex_cols = True  # Loop flag.
    result = df  # Start.
    
    while complex_cols:  # Keep flattening.
        complex_cols = False  # Assume done.
        new_columns = []  # Build column list.
        
        for field in result.schema.fields:  # Each column.
            if isinstance(field.dataType, StructType):  # Struct?
                complex_cols = True  # Need another pass.
                for sub_field in field.dataType.fields:  # Sub-fields.
                    new_name = f"{field.name}{separator}{sub_field.name}"  # Flat name.
                    new_columns.append(
                        col(f"`{field.name}`.`{sub_field.name}`").alias(new_name)  # Flatten.
                    )
            else:
                new_columns.append(col(f"`{field.name}`"))  # Keep as-is.
        
        result = result.select(new_columns)  # Apply.
    
    return result  # Return flat.


def flatten_all(df: DataFrame, separator: str = "_", max_depth: int = 5) -> DataFrame:
    """Flatten structs AND explode arrays (full recursive flatten)."""
    result = df  # Start.
    depth = 0  # Track depth.
    
    while depth < max_depth:  # Safety limit.
        depth += 1  # Increment.
        has_complex = False  # Flag.
        
        # Step 1: Flatten all structs.
        result = flatten_schema(result, separator)  # Flatten structs.
        
        # Step 2: Check for remaining arrays.
        for field in result.schema.fields:  # Each field.
            if isinstance(field.dataType, ArrayType):  # Array?
                has_complex = True  # Found one.
                result = result.withColumn(
                    field.name, explode_outer(col(f"`{field.name}`"))  # Explode.
                )
                break  # Re-check schema after explode.
        
        # Check if any complex types remain.
        remaining = [f for f in result.schema.fields
                     if isinstance(f.dataType, (StructType, ArrayType))]  # Complex?
        if not remaining:  # All flat?
            break  # Done.
    
    return result  # Return.

# Demo: Apply to nested data.
print("=== Recursive Flattener Demo ===")  # Heading.

# Create deeply nested structure.
nested_data = spark.createDataFrame([
    (1, ("Alice", ("Seattle", "WA")), ["tag1", "tag2"]),
    (2, ("Bob", ("Portland", "OR")), ["tag3"]),
    (3, ("Carol", ("Denver", "CO")), ["tag1", "tag4", "tag5"]),
], StructType([
    StructField("id", IntegerType()),
    StructField("person", StructType([
        StructField("name", StringType()),
        StructField("location", StructType([
            StructField("city", StringType()),
            StructField("state", StringType()),
        ])),
    ])),
    StructField("tags", ArrayType(StringType())),
]))  # Schema.

print("Before flattening:")  # Heading.
nested_data.printSchema()  # Nested.
nested_data.show(truncate=False)  # Display.

# Flatten structs only.
print("\n=== Structs Flattened (arrays kept) ===")  # Heading.
struct_flat = flatten_schema(nested_data)  # Structs only.
struct_flat.printSchema()  # Check.
struct_flat.show(truncate=False)  # Display.

# Flatten everything (structs + arrays).
print("\n=== Fully Flattened (structs + arrays) ===")  # Heading.
fully_flat = flatten_all(nested_data)  # Everything.
fully_flat.printSchema()  # Check.
fully_flat.show()  # Display.
print(f"Rows: {nested_data.count()} -> {fully_flat.count()} (array explosion)")  # Impact.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Production JSON ingestion pipeline
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Production JSON Ingestion Pipeline
# ============================================================
# Real-world: Ingest complex JSON, flatten, and write to Delta.

from pyspark.sql.functions import (
    col, explode_outer, posexplode_outer, size, when, lit,
    current_timestamp, input_file_name, from_json, schema_of_json
)  # Imports.
from pyspark.sql.types import *  # Types.

class JSONFlattener:
    """Production JSON flattening with metadata and audit."""
    
    def __init__(self, df, id_col=None):
        """Initialize with DataFrame and optional ID column."""
        self.original = df  # Keep original.
        self.df = df  # Working copy.
        self.id_col = id_col  # Track lineage.
        self.explosion_log = []  # Track explosions.
    
    def get_complex_columns(self):
        """Identify all complex type columns."""
        structs = []  # Struct columns.
        arrays = []  # Array columns.
        maps = []  # Map columns.
        
        for field in self.df.schema.fields:  # Each field.
            if isinstance(field.dataType, StructType):  # Struct.
                structs.append(field.name)  # Add.
            elif isinstance(field.dataType, ArrayType):  # Array.
                arrays.append(field.name)  # Add.
            elif isinstance(field.dataType, MapType):  # Map.
                maps.append(field.name)  # Add.
        
        return {"structs": structs, "arrays": arrays, "maps": maps}  # Return.
    
    def flatten_structs(self, separator="_"):
        """Flatten all struct columns in one pass."""
        complex_info = self.get_complex_columns()  # Check.
        
        while complex_info["structs"]:  # While structs exist.
            new_cols = []  # Build.
            for field in self.df.schema.fields:  # Each field.
                if isinstance(field.dataType, StructType):  # Struct?
                    for sub in field.dataType.fields:  # Sub-fields.
                        alias = f"{field.name}{separator}{sub.name}"  # Name.
                        new_cols.append(col(f"`{field.name}`.`{sub.name}`").alias(alias))  # Add.
                else:
                    new_cols.append(col(f"`{field.name}`"))  # Keep.
            
            self.df = self.df.select(new_cols)  # Apply.
            complex_info = self.get_complex_columns()  # Re-check.
        
        return self  # Chain.
    
    def explode_array(self, col_name, keep_position=True):
        """Explode a specific array column with metadata."""
        before_count = self.df.count()  # Before.
        
        if keep_position:  # With position?
            self.df = self.df.select(
                "*",  # All existing.
                posexplode_outer(col(col_name)).alias(f"{col_name}_pos", f"{col_name}_elem")  # Explode.
            ).drop(col_name)  # Remove original array.
        else:
            self.df = self.df.withColumn(col_name, explode_outer(col(col_name)))  # In-place.
        
        after_count = self.df.count()  # After.
        self.explosion_log.append({  # Log.
            "column": col_name,
            "before": before_count,
            "after": after_count,
            "multiplier": round(after_count / max(before_count, 1), 2)
        })
        
        return self  # Chain.
    
    def report(self):
        """Print flattening report."""
        print("=== Flattening Report ===")  # Heading.
        print(f"Columns: {len(self.original.columns)} -> {len(self.df.columns)}")  # Columns.
        print(f"Rows: {self.original.count()} -> {self.df.count()}")  # Rows.
        if self.explosion_log:  # If any.
            print("\nArray Explosions:")  # Header.
            for log in self.explosion_log:  # Each.
                print(f"  {log['column']}: {log['before']} -> {log['after']} (x{log['multiplier']})")  # Detail.
        remaining = self.get_complex_columns()  # Check.
        if any(remaining.values()):  # Still complex?
            print(f"\nRemaining complex: {remaining}")  # Warn.
        else:
            print("\nAll columns are now scalar types!")  # Done.

# Demo with complex nested data.
print("=== JSON Flattening Pipeline ===")  # Heading.

# Simulate complex API response.
api_data = spark.createDataFrame([
    ("evt_1", "click", ("user_1", "Alice"), [{"key": "page", "value": "/home"}, {"key": "duration", "value": "5s"}]),
    ("evt_2", "purchase", ("user_2", "Bob"), [{"key": "amount", "value": "$50"}, {"key": "item", "value": "Widget"}]),
    ("evt_3", "view", ("user_1", "Alice"), [{"key": "page", "value": "/products"}]),
], StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user", StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
    ])),
    StructField("metadata", ArrayType(StructType([
        StructField("key", StringType()),
        StructField("value", StringType()),
    ]))),
]))  # Complex schema.

print("Original schema:")  # Heading.
api_data.printSchema()  # Nested.

# Use the pipeline.
flattener = JSONFlattener(api_data, id_col="event_id")  # Init.
flattener.flatten_structs()  # Flatten structs.
flattener.explode_array("metadata", keep_position=True)  # Explode array.
flattener.flatten_structs()  # Flatten exploded structs.

print("\nFlattened schema:")  # Heading.
flattener.df.printSchema()  # Flat.
flattener.df.show(truncate=False)  # Display.
flattener.report()  # Audit.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Schema-driven flattening
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Schema-Driven Flattening
# ============================================================
# Real-world: Flatten based on configurable schema mapping.

from pyspark.sql.functions import (
    col, explode_outer, when, lit, coalesce, concat_ws, collect_list
)  # Imports.
from pyspark.sql.types import *  # Types.
from typing import Dict, List, Optional  # Typing.

def schema_report(df, prefix="", depth=0):
    """Generate a report of all fields with their paths and types."""
    report = []  # Build list.
    for field in df.schema.fields:  # Each field.
        path = f"{prefix}{field.name}" if not prefix else f"{prefix}.{field.name}"  # Path.
        type_name = field.dataType.simpleString()  # Type.
        report.append({"path": path, "type": type_name, "depth": depth})  # Add.
        
        if isinstance(field.dataType, StructType):  # Recurse struct.
            sub_df = spark.createDataFrame([], field.dataType)  # Temp DF.
            sub_report = schema_report(sub_df, path, depth + 1)  # Recurse.
            report.extend(sub_report)  # Add.
        elif isinstance(field.dataType, ArrayType):  # Array.
            if isinstance(field.dataType.elementType, StructType):  # Array of struct.
                sub_df = spark.createDataFrame([], field.dataType.elementType)  # Temp.
                sub_report = schema_report(sub_df, f"{path}[]", depth + 1)  # Recurse.
                report.extend(sub_report)  # Add.
    
    return report  # Return.

# Configurable flattening with field selection.
def selective_flatten(df, field_mappings: Dict[str, str]) -> "DataFrame":
    """Flatten only selected fields with custom aliases."""
    select_exprs = []  # Build.
    
    for source_path, target_name in field_mappings.items():  # Each mapping.
        select_exprs.append(col(source_path).alias(target_name))  # Map.
    
    return df.select(select_exprs)  # Apply.

# Demo: Complex nested structure.
print("=== Schema-Driven Flattening ===")  # Heading.

# Create complex data.
complex_data = spark.createDataFrame([
    ("TX-001", "2024-01-15",
     {"id": "C1", "name": "Alice", "tier": "gold"},
     [{"product": "Widget", "qty": 3, "price": 10.0},
      {"product": "Gadget", "qty": 1, "price": 25.0}],
     {"method": "credit", "status": "approved"}),
    ("TX-002", "2024-01-16",
     {"id": "C2", "name": "Bob", "tier": "silver"},
     [{"product": "Doohickey", "qty": 5, "price": 5.0}],
     {"method": "debit", "status": "approved"}),
], StructType([
    StructField("tx_id", StringType()),
    StructField("date", StringType()),
    StructField("customer", StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("tier", StringType()),
    ])),
    StructField("items", ArrayType(StructType([
        StructField("product", StringType()),
        StructField("qty", IntegerType()),
        StructField("price", DoubleType()),
    ]))),
    StructField("payment", StructType([
        StructField("method", StringType()),
        StructField("status", StringType()),
    ])),
]))  # Schema.

print("Original schema:")  # Heading.
complex_data.printSchema()  # Nested.

# Generate schema report.
print("\n=== Schema Report ===")  # Heading.
report = schema_report(complex_data)  # Generate.
for r in report:  # Print.
    indent = "  " * r["depth"]  # Indent.
    print(f"{indent}{r['path']} : {r['type']}")  # Show.

# Selective flatten: only fields we need.
print("\n=== Selective Flatten (header info only) ===")  # Heading.
header = selective_flatten(complex_data, {
    "tx_id": "transaction_id",
    "date": "transaction_date",
    "customer.name": "customer_name",
    "customer.tier": "customer_tier",
    "payment.method": "payment_method",
    "payment.status": "payment_status",
})  # Select.
header.show()  # Display.

# Full flatten with line items.
print("=== Full Flatten with Line Items ===")  # Heading.
with_items = complex_data.select(
    col("tx_id"),  # Keep.
    col("date"),  # Keep.
    col("customer.id").alias("customer_id"),  # Flatten.
    col("customer.name").alias("customer_name"),  # Flatten.
    col("payment.method").alias("pay_method"),  # Flatten.
    explode_outer("items").alias("item"),  # Explode.
).select(
    "tx_id", "date", "customer_id", "customer_name", "pay_method",  # Keep.
    col("item.product").alias("product"),  # Item field.
    col("item.qty").alias("quantity"),  # Item field.
    col("item.price").alias("unit_price"),  # Item field.
)
with_items.show()  # Display.
print("Schema-driven approach lets you control exactly what gets flattened!")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Re-nesting flat data
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Re-Nesting Flat Data
# ============================================================
# Real-world: Reconstruct nested JSON from flat table for API output.

from pyspark.sql.functions import (
    col, struct, array, collect_list, first, create_map,
    map_from_entries, to_json
)  # Imports.

# Flat order line items (from a relational DB).
flat_lines = spark.createDataFrame([
    ("ORD-001", "Alice", "2024-01-15", "express", "Widget", 3, 10.0),
    ("ORD-001", "Alice", "2024-01-15", "express", "Gadget", 1, 25.0),
    ("ORD-002", "Bob", "2024-01-16", "standard", "Widget", 5, 10.0),
    ("ORD-002", "Bob", "2024-01-16", "standard", "Doohickey", 2, 5.0),
    ("ORD-002", "Bob", "2024-01-16", "standard", "Gadget", 2, 25.0),
    ("ORD-003", "Carol", "2024-01-17", "express", "Doohickey", 10, 5.0),
], ["order_id", "customer", "date", "shipping", "product", "qty", "price"])  # Flat.

print("=== Flat Line Items ===")  # Heading.
flat_lines.show()  # Display.

# Re-nest: group items back into arrays of structs.
print("=== Re-Nested Orders ===")  # Heading.
nested_orders = flat_lines.groupBy("order_id").agg(
    first("customer").alias("customer"),  # Order-level.
    first("date").alias("date"),  # Order-level.
    first("shipping").alias("shipping"),  # Order-level.
    collect_list(
        struct(  # Array of structs.
            col("product"),
            col("qty").alias("quantity"),
            col("price").alias("unit_price"),
            (col("qty") * col("price")).alias("line_total"),
        )
    ).alias("items"),  # Nested items array.
)
nested_orders.printSchema()  # Show nesting.
nested_orders.show(truncate=False)  # Display.

# Create full nested structure for JSON output.
print("=== Full Nested Structure for API ===")  # Heading.
api_output = flat_lines.groupBy("order_id").agg(
    struct(  # Customer struct.
        first("customer").alias("name"),
    ).alias("customer"),
    struct(  # Order info struct.
        first("date").alias("date"),
        first("shipping").alias("method"),
    ).alias("order_info"),
    collect_list(  # Items array.
        struct(
            col("product").alias("name"),
            col("qty").alias("quantity"),
            col("price").alias("unit_price"),
        )
    ).alias("line_items"),
)
api_output.printSchema()  # Nested schema.
api_output.show(truncate=False)  # Display.

# Convert to JSON strings (ready for API response).
print("=== As JSON Strings ===")  # Heading.
json_output = api_output.withColumn(
    "json_payload", to_json(struct("*"))  # Full row to JSON.
)
json_output.select("order_id", "json_payload").show(truncate=False)  # Display.

print("Flat -> Nested -> JSON: Full round-trip complete!")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Key Takeaways
# MAGIC %md
# MAGIC ## SECTION 6 — Key Takeaways
# MAGIC
# MAGIC ### Struct Flattening
# MAGIC 1. **Dot notation** `col("parent.child")` for specific fields
# MAGIC 2. **Star expansion** `select("struct.*")` for all fields of one struct
# MAGIC 3. **Recursive flatten** when you have unknown nesting depth
# MAGIC 4. **Alias always** — flattened names get long fast
# MAGIC
# MAGIC ### Array Flattening
# MAGIC 1. **explode()** removes NULL/empty rows — data loss risk!
# MAGIC 2. **explode_outer()** preserves all rows — safer for data integrity
# MAGIC 3. **posexplode()** when you need element position (e.g., line number)
# MAGIC 4. **Check row multiplication** before exploding large arrays
# MAGIC
# MAGIC ### Map Flattening
# MAGIC 1. **Key access** `col("map")["key"]` for known keys
# MAGIC 2. **explode()** for dynamic key-value exploration
# MAGIC 3. **Pivot after explode** for maps → columns transformation
# MAGIC
# MAGIC ### Re-Nesting
# MAGIC 1. **struct()** creates structs from flat columns
# MAGIC 2. **collect_list(struct(...))** creates arrays of structs
# MAGIC 3. **to_json()** for API/file output
# MAGIC
# MAGIC ### Performance
# MAGIC | Pattern | Cost | Mitigation |
# MAGIC |---|---|---|
# MAGIC | explode large arrays | Row multiplication | Filter before explode |
# MAGIC | Recursive flatten | Multiple passes | Limit depth |
# MAGIC | collect_list for re-nesting | Shuffle + memory | Partition by group key |

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Practice Exercises
# MAGIC %md
# MAGIC ## SECTION 7 — Practice Exercises
# MAGIC
# MAGIC ### Exercise 1: Struct Flatten
# MAGIC Given employee data with nested `address(street, city, state, zip)` struct, flatten to individual columns.
# MAGIC
# MAGIC ### Exercise 2: Array Explode
# MAGIC Given orders with an `items` array column, explode and calculate total revenue per product.
# MAGIC
# MAGIC ### Exercise 3: Recursive Flatten
# MAGIC Given a 3-level nested JSON structure, use the recursive flattener to produce a fully flat DataFrame.
# MAGIC
# MAGIC ### Exercise 4: Re-Nest
# MAGIC Given flat transaction data (customer_id, product, amount), re-nest into `{customer_id, purchases: [{product, amount}]}`.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Solutions
# ============================================================
# SECTION 7 — EXERCISE SOLUTIONS
# ============================================================

from pyspark.sql.functions import col, explode, struct, collect_list, sum as spark_sum  # Imports.
from pyspark.sql.types import *  # Types.

# --- Exercise 1: Struct Flatten ---
print("=== Exercise 1: Struct Flatten ===")  # Heading.
emp = spark.createDataFrame([
    (1, "Alice", ("123 Main St", "Seattle", "WA", "98101")),
    (2, "Bob", ("456 Oak Ave", "Portland", "OR", "97201")),
], StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("address", StructType([
        StructField("street", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("zip", StringType()),
    ])),
]))  # Nested.

emp.select("id", "name", "address.*").show()  # Flatten.

# --- Exercise 2: Array Explode + Revenue ---
print("=== Exercise 2: Revenue per Product ===")  # Heading.
orders = spark.createDataFrame([
    ("O1", [{"product": "A", "qty": 2, "price": 10.0}, {"product": "B", "qty": 1, "price": 20.0}]),
    ("O2", [{"product": "A", "qty": 3, "price": 10.0}]),
], StructType([
    StructField("order_id", StringType()),
    StructField("items", ArrayType(StructType([
        StructField("product", StringType()),
        StructField("qty", IntegerType()),
        StructField("price", DoubleType()),
    ]))),
]))  # With items array.

orders.select("order_id", explode("items").alias("item")).select(
    "order_id", "item.product",
    (col("item.qty") * col("item.price")).alias("revenue")
).groupBy("product").agg(spark_sum("revenue").alias("total_revenue")).show()  # Revenue.

# --- Exercise 3: Recursive (uses flatten_all from above) ---
print("=== Exercise 3: Recursive Flatten ===")  # Heading.
deep = spark.createDataFrame([
    (1, ("A", ("X", "Y")), ["t1", "t2"]),
], StructType([
    StructField("id", IntegerType()),
    StructField("info", StructType([
        StructField("label", StringType()),
        StructField("nested", StructType([
            StructField("x", StringType()),
            StructField("y", StringType()),
        ])),
    ])),
    StructField("tags", ArrayType(StringType())),
]))  # 3 levels.
flatten_all(deep).show()  # Fully flat.

# --- Exercise 4: Re-Nest ---
print("=== Exercise 4: Re-Nest ===")  # Heading.
flat_tx = spark.createDataFrame([
    ("C1", "Widget", 50.0), ("C1", "Gadget", 25.0), ("C2", "Widget", 30.0),
], ["customer_id", "product", "amount"])  # Flat.

flat_tx.groupBy("customer_id").agg(
    collect_list(struct("product", "amount")).alias("purchases")  # Re-nest.
).show(truncate=False)  # Display.

print("All exercises completed!")