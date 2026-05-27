# Databricks notebook source
# DBTITLE 1,NB_35 Header
# MAGIC %md
# MAGIC # NB_35 — Map Functions (Every One)
# MAGIC
# MAGIC **Module 5: Built-in Functions** | Notebook 35 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Creating maps: create_map(), map_from_arrays(), map_from_entries()
# MAGIC * Inspection: map_keys(), map_values(), size(), element_at()
# MAGIC * Transformation: map_concat(), map_filter(), map_zip_with(), transform_keys(), transform_values()
# MAGIC * Explosion: explode(), posexplode(), map_entries()
# MAGIC * Practical patterns: Config parsing, KV stores, pivot-like operations
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Key-value patterns in semi-structured data)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are Map Functions?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are Map Functions? (Real-World Analogy)
# MAGIC
# MAGIC ### 📚 The Dictionary
# MAGIC
# MAGIC A Map is like a dictionary: every entry has a **key** and a **value**.
# MAGIC
# MAGIC | Real-World Dictionary | PySpark Map | Example |
# MAGIC |---|---|---|
# MAGIC | Look up a word | `element_at(map, key)` | Get value for key "color" |
# MAGIC | List all words | `map_keys(map)` | Get ["color", "size", "weight"] |
# MAGIC | List all definitions | `map_values(map)` | Get ["red", "large", "5kg"] |
# MAGIC | Merge two dictionaries | `map_concat(map1, map2)` | Combine properties |
# MAGIC | Count entries | `size(map)` | How many key-value pairs |
# MAGIC | Unpack all entries | `explode(map)` | One row per key-value pair |
# MAGIC
# MAGIC ### Where Maps Appear in Real Data
# MAGIC * **Config/Properties:** `{"env": "prod", "region": "eu", "version": "2.1"}`
# MAGIC * **Tags/Labels:** Kubernetes labels, resource tags
# MAGIC * **Metrics:** `{"cpu": 85.5, "memory": 72.1, "disk": 45.0}`
# MAGIC * **Flexible schemas:** When columns vary per row
# MAGIC * **Parsed JSON:** Arbitrary key-value structures

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Map Functions Work
# MAGIC %md
# MAGIC ## SECTION 2 — How Map Functions Work (Internal Mechanics)
# MAGIC
# MAGIC ### Map Type Structure
# MAGIC ```
# MAGIC MapType(keyType, valueType, valueContainsNull)
# MAGIC │
# MAGIC ├─ Keys: MUST be non-null, unique within the map
# MAGIC ├─ Values: CAN be null
# MAGIC └─ Ordering: NOT guaranteed (use sort before display)
# MAGIC ```
# MAGIC
# MAGIC ### Function Categories
# MAGIC ```
# MAGIC ┌───────────────────┬───────────────────┬───────────────────┐
# MAGIC │ CREATE             │ INSPECT            │ TRANSFORM          │
# MAGIC │                   │                    │                    │
# MAGIC │ create_map()      │ map_keys()         │ map_concat()       │
# MAGIC │ map_from_arrays() │ map_values()       │ map_filter()       │
# MAGIC │ map_from_entries()│ element_at()       │ map_zip_with()     │
# MAGIC │ str_to_map()      │ size()             │ transform_keys()   │
# MAGIC │                   │ map_contains_key() │ transform_values() │
# MAGIC ├───────────────────┴───────────────────┴───────────────────┤
# MAGIC │                  EXPLODE                                     │
# MAGIC │  explode(map) → key, value columns (one row per entry)       │
# MAGIC │  posexplode(map) → pos, key, value                           │
# MAGIC │  map_entries(map) → array of structs [{key, value}, ...]     │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Critical Rules
# MAGIC 1. Map keys MUST be non-null and unique (duplicates = error or last-wins)
# MAGIC 2. `element_at(map, key)` returns NULL if key not found
# MAGIC 3. `explode(map)` creates "key" and "value" columns automatically
# MAGIC 4. `map_concat()` with duplicate keys: last map wins
# MAGIC 5. Maps are NOT ordered — don't rely on insertion order

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Creating Maps
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Creating Maps
# ============================================================
# Real-world: Storing flexible key-value attributes per row.

from pyspark.sql import SparkSession  # Import SparkSession.
from pyspark.sql.functions import (  # Import map functions.
    col, create_map, map_from_arrays, lit, array, map_keys,
    map_values, size, element_at
)  # End imports.
from pyspark.sql.types import MapType, StringType  # Types.

spark = SparkSession.builder.getOrCreate()  # Get active session.

# Method 1: create_map() from alternating key-value columns.
print("=== create_map() — From Columns ===")  # Print heading.
products = spark.createDataFrame([
    (1, "Laptop", "electronics", 999.99, "silver"),
    (2, "Book", "education", 29.99, "blue"),
    (3, "Headphones", "audio", 149.99, "black"),
], ["id", "name", "category", "price", "color"])  # Product data.

products.select(
    col("id"), col("name"),  # Keep id and name.
    # create_map takes alternating key, value, key, value...
    create_map(
        lit("category"), col("category"),  # Key: "category", Value: from column.
        lit("price"), col("price").cast("string"),  # Key: "price", Value: as string.
        lit("color"), col("color"),  # Key: "color", Value: from column.
    ).alias("properties"),  # Map column.
).show(truncate=False)  # Display map creation.

# Method 2: map_from_arrays() from key array + value array.
print("=== map_from_arrays() — From Two Arrays ===")  # Print heading.
kv_df = spark.createDataFrame([
    (1, ["env", "region", "version"], ["prod", "us-east", "2.1"]),
    (2, ["env", "region"], ["dev", "eu-west"]),
], "id INT, keys ARRAY<STRING>, vals ARRAY<STRING>")  # Parallel arrays.

kv_df.select(
    col("id"),  # Keep id.
    map_from_arrays(col("keys"), col("vals")).alias("config_map"),  # Create map.
).show(truncate=False)  # Display map from arrays.

# Method 3: Direct map literals in DataFrame creation.
print("=== Direct Map in Schema ===")  # Print heading.
config_df = spark.createDataFrame([
    (1, {"host": "db.prod.com", "port": "5432", "ssl": "true"}),
    (2, {"host": "db.dev.com", "port": "5432", "ssl": "false"}),
], "id INT, config MAP<STRING, STRING>")  # Map type directly.

config_df.show(truncate=False)  # Display direct maps.
config_df.printSchema()  # Show schema with MapType.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Inspecting Maps
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Inspecting Maps
# ============================================================
# Real-world: Querying configuration, extracting specific values.

from pyspark.sql.functions import (  # Import inspection functions.
    col, map_keys, map_values, size, element_at, lit, explode
)  # End imports.

# Config data as maps.
servers = spark.createDataFrame([
    ("web-01", {"cpu": "85", "memory": "72", "disk": "45", "network": "30"}),
    ("web-02", {"cpu": "92", "memory": "88", "disk": "60"}),
    ("db-01", {"cpu": "45", "memory": "95", "disk": "80", "connections": "150"}),
], "server STRING, metrics MAP<STRING, STRING>")  # Server metrics.

# Inspect map contents.
print("=== Map Inspection ===")  # Print heading.
servers.select(
    col("server"),  # Keep server name.
    col("metrics"),  # Original map.
    map_keys(col("metrics")).alias("metric_names"),  # Get all keys.
    map_values(col("metrics")).alias("metric_values"),  # Get all values.
    size(col("metrics")).alias("num_metrics"),  # Count entries.
).show(truncate=False)  # Display inspection.

# Access specific keys.
print("=== Accessing Specific Keys ===")  # Print heading.
servers.select(
    col("server"),  # Keep server.
    element_at(col("metrics"), "cpu").alias("cpu_usage"),  # Get CPU value.
    element_at(col("metrics"), "memory").alias("mem_usage"),  # Get memory value.
    element_at(col("metrics"), "disk").alias("disk_usage"),  # Get disk value.
    element_at(col("metrics"), "gpu").alias("gpu_usage"),  # NULL — key not found.
).show(truncate=False)  # Display accessed values.

# Explode map into rows.
print("=== explode(map) — One Row Per Entry ===")  # Print heading.
servers.select(
    col("server"),  # Keep server context.
    explode(col("metrics")),  # Creates "key" and "value" columns automatically.
).show(truncate=False)  # Display exploded map.

# Alternative: using map_entries to get array of structs.
from pyspark.sql.functions import map_entries  # Import.
print("=== map_entries() — Array of Structs ===")  # Print heading.
servers.select(
    col("server"),  # Keep server.
    map_entries(col("metrics")).alias("entries_array"),  # [{key, value}, ...]
).show(truncate=False)  # Display entries.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Map Concatenation and str_to_map
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Map Concatenation and str_to_map
# ============================================================
# Real-world: Merging config maps, parsing query strings.

from pyspark.sql.functions import (  # Import functions.
    col, map_concat, create_map, lit, expr
)  # End imports.

# Merge multiple maps.
print("=== map_concat() — Merge Maps ===")  # Print heading.
merge_df = spark.createDataFrame([
    (1, {"a": "1", "b": "2"}, {"c": "3", "d": "4"}),
    (2, {"x": "10"}, {"x": "20", "y": "30"}),  # Duplicate key "x"!
], "id INT, map1 MAP<STRING,STRING>, map2 MAP<STRING,STRING>")  # Two maps.

merge_df.select(
    col("id"),  # Keep id.
    col("map1"), col("map2"),  # Original maps.
    map_concat(col("map1"), col("map2")).alias("merged"),  # Merge (last wins for dupes).
).show(truncate=False)  # Display merged maps.

# Add a literal entry to existing map.
print("=== Adding Entries to Existing Map ===")  # Print heading.
config = spark.createDataFrame([
    (1, {"env": "prod", "region": "us"}),
    (2, {"env": "dev", "region": "eu"}),
], "id INT, config MAP<STRING,STRING>")  # Config data.

config.select(
    col("id"),  # Keep id.
    col("config"),  # Original.
    # Add new entry using map_concat + create_map.
    map_concat(col("config"), create_map(lit("version"), lit("3.0"))).alias("with_version"),
).show(truncate=False)  # Display with added entry.

# str_to_map() — parse delimited strings into maps.
print("=== str_to_map() — Parse Key-Value Strings ===")  # Print heading.
query_strings = spark.createDataFrame([
    ("page=home&user=alice&theme=dark",),
    ("page=products&sort=price&order=asc",),
    ("page=checkout&coupon=SAVE20",),
], ["query_string"])  # URL query strings.

query_strings.select(
    col("query_string"),  # Original string.
    # str_to_map(string, pairDelimiter, kvDelimiter).
    expr("str_to_map(query_string, '&', '=')").alias("params_map"),  # Parse.
).show(truncate=False)  # Display parsed maps.

# Access parsed params.
query_strings.select(
    expr("str_to_map(query_string, '&', '=')['page']").alias("page"),  # Get page value.
    expr("str_to_map(query_string, '&', '=')['user']").alias("user"),  # Get user value.
).show(truncate=False)  # Display specific params.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Higher-order map functions
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Higher-Order Map Functions
# ============================================================
# Real-world: Transform, filter, and combine maps dynamically.

from pyspark.sql.functions import col, expr, lit  # Import basics.

# Server metrics data.
metrics = spark.createDataFrame([
    ("web-01", {"cpu": 85.0, "memory": 72.0, "disk": 45.0, "network": 30.0}),
    ("web-02", {"cpu": 92.0, "memory": 88.0, "disk": 60.0, "network": 15.0}),
    ("db-01", {"cpu": 45.0, "memory": 95.0, "disk": 80.0, "network": 5.0}),
], "server STRING, metrics MAP<STRING, DOUBLE>")  # Numeric metrics.

# transform_keys() — modify all keys.
print("=== transform_keys() ===")  # Print heading.
metrics.select(
    col("server"),  # Keep server.
    # Uppercase all keys.
    expr("transform_keys(metrics, (k, v) -> upper(k))").alias("upper_keys"),
    # Prefix all keys.
    expr("transform_keys(metrics, (k, v) -> concat('sys_', k))").alias("prefixed_keys"),
).show(truncate=False)  # Display transformed keys.

# transform_values() — modify all values.
print("=== transform_values() ===")  # Print heading.
metrics.select(
    col("server"),  # Keep server.
    col("metrics"),  # Original.
    # Convert percentages to fractions (divide by 100).
    expr("transform_values(metrics, (k, v) -> v / 100.0)").alias("as_fraction"),
    # Round to integers.
    expr("transform_values(metrics, (k, v) -> cast(round(v) as double))").alias("rounded"),
).show(truncate=False)  # Display transformed values.

# map_filter() — keep only entries matching condition.
print("=== map_filter() — Keep High Metrics Only ===")  # Print heading.
metrics.select(
    col("server"),  # Keep server.
    col("metrics"),  # Original.
    # Keep only metrics above 70%.
    expr("map_filter(metrics, (k, v) -> v > 70)").alias("high_usage"),
    # Keep only cpu and memory.
    expr("map_filter(metrics, (k, v) -> k IN ('cpu', 'memory'))").alias("cpu_mem_only"),
).show(truncate=False)  # Display filtered maps.

# map_zip_with() — combine two maps by key.
print("=== map_zip_with() — Compare Two Maps ===")  # Print heading.
compare = spark.createDataFrame([
    ("web-01", {"cpu": 85.0, "memory": 72.0}, {"cpu": 80.0, "memory": 70.0}),
    ("web-02", {"cpu": 92.0, "memory": 88.0}, {"cpu": 60.0, "memory": 50.0}),
], "server STRING, current MAP<STRING,DOUBLE>, previous MAP<STRING,DOUBLE>")  # Current vs previous.

compare.select(
    col("server"),  # Keep server.
    # Compute difference: current - previous.
    expr("map_zip_with(current, previous, (k, v1, v2) -> v1 - v2)").alias("change"),
).show(truncate=False)  # Display differences.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Map to columns pattern
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Map-to-Columns Pattern
# ============================================================
# Real-world: Dynamically extracting map keys into separate columns.

from pyspark.sql.functions import (  # Import functions.
    col, element_at, explode, map_keys, collect_set, array_sort, lit
)  # End imports.

# Data with varying attributes stored as maps.
events = spark.createDataFrame([
    (1, "click", {"page": "home", "element": "button", "duration": "5"}),
    (2, "view", {"page": "products", "category": "electronics"}),
    (3, "click", {"page": "checkout", "element": "submit", "value": "99.99"}),
    (4, "scroll", {"page": "home", "depth": "75", "duration": "12"}),
], "id INT, event_type STRING, properties MAP<STRING, STRING>")  # Event data.

# Extract known keys as columns.
print("=== Extract Known Keys as Columns ===")  # Print heading.
events.select(
    col("id"),  # Keep id.
    col("event_type"),  # Keep event type.
    element_at(col("properties"), "page").alias("page"),  # Always present.
    element_at(col("properties"), "element").alias("element"),  # May be NULL.
    element_at(col("properties"), "duration").alias("duration"),  # May be NULL.
    element_at(col("properties"), "value").alias("value"),  # May be NULL.
).show(truncate=False)  # Display columns.

# Dynamic: Discover ALL unique keys across all rows.
print("=== Discover All Unique Keys ===")  # Print heading.
all_keys = events.select(
    explode(map_keys(col("properties"))).alias("key"),  # Explode all keys.
).distinct().orderBy("key")  # Unique keys sorted.

all_keys.show()  # Display discovered keys.

# Dynamic pivot: Explode map, then pivot.
print("=== Dynamic Pivot: Map → Columns ===")  # Print heading.
exploded = events.select(
    col("id"), col("event_type"),  # Keep context.
    explode(col("properties")),  # Creates "key" and "value" columns.
)

# Pivot: one column per unique key.
pivoted = exploded.groupBy("id", "event_type").pivot("key").agg(
    expr("first(value)")  # Take first value per key.
)

pivoted.show(truncate=False)  # Display pivoted result.
print("Schema after pivot:")  # Schema heading.
pivoted.printSchema()  # Show resulting schema.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Building maps from groups
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Building Maps from Groups
# ============================================================
# Real-world: Aggregating data into maps for compact representation.

from pyspark.sql.functions import (  # Import functions.
    col, create_map, map_from_entries, collect_list, struct,
    expr, count, sum as spark_sum, avg, lit
)  # End imports.

# Sales data.
sales = spark.createDataFrame([
    ("Alice", "Q1", 5000), ("Alice", "Q2", 7000), ("Alice", "Q3", 6000), ("Alice", "Q4", 8000),
    ("Bob", "Q1", 3000), ("Bob", "Q2", 4000), ("Bob", "Q3", 3500),
    ("Charlie", "Q1", 9000), ("Charlie", "Q2", 8500), ("Charlie", "Q3", 9500), ("Charlie", "Q4", 10000),
], ["rep", "quarter", "revenue"])  # Sales by quarter.

# Build map from grouped data: {quarter -> revenue}.
print("=== Build Map from Aggregation ===")  # Print heading.
rev_map = sales.groupBy("rep").agg(
    map_from_entries(collect_list(struct("quarter", "revenue"))).alias("quarterly_revenue"),
)

rev_map.show(truncate=False)  # Display revenue maps.

# Access specific quarters from the map.
print("=== Access Map Entries ===")  # Print heading.
rev_map.select(
    col("rep"),  # Keep rep.
    element_at(col("quarterly_revenue"), "Q1").alias("Q1_revenue"),  # Q1 value.
    element_at(col("quarterly_revenue"), "Q4").alias("Q4_revenue"),  # Q4 value (NULL if missing).
).show(truncate=False)  # Display specific quarters.

# Build config map per environment.
print("=== Config Map per Environment ===")  # Print heading.
settings = spark.createDataFrame([
    ("prod", "host", "db.prod.com"), ("prod", "port", "5432"), ("prod", "ssl", "true"),
    ("dev", "host", "localhost"), ("dev", "port", "5432"), ("dev", "ssl", "false"),
    ("staging", "host", "db.staging.com"), ("staging", "port", "5432"),
], ["env", "key", "value"])  # Config entries.

env_config = settings.groupBy("env").agg(
    map_from_entries(collect_list(struct("key", "value"))).alias("config"),  # Build map.
)

env_config.show(truncate=False)  # Display environment configs.

# Extract host from each environment's config.
env_config.select(
    col("env"),  # Keep environment.
    element_at(col("config"), "host").alias("db_host"),  # Get host.
    element_at(col("config"), "ssl").alias("ssl_enabled"),  # Get ssl.
).show(truncate=False)  # Display extracted values.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Map-based flexible schema
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Map-Based Flexible Schema
# ============================================================
# Real-world: Handling events with varying attributes (schema-on-read).

from pyspark.sql.functions import (  # Import functions.
    col, expr, element_at, map_keys, map_values, size,
    when, lit, map_filter, map_concat, create_map, explode
)  # End imports.

# IoT events with different sensor types having different attributes.
iot_events = spark.createDataFrame([
    (1, "temperature", "sensor-01", {"value": "23.5", "unit": "celsius", "location": "floor1"}),
    (2, "humidity", "sensor-02", {"value": "65", "unit": "percent", "location": "floor1"}),
    (3, "pressure", "sensor-03", {"value": "1013", "unit": "hPa", "altitude": "100m"}),
    (4, "temperature", "sensor-04", {"value": "19.8", "unit": "celsius", "location": "floor2", "battery": "85"}),
    (5, "motion", "sensor-05", {"detected": "true", "location": "entrance", "confidence": "0.95"}),
], "id INT, sensor_type STRING, sensor_id STRING, attributes MAP<STRING, STRING>")  # IoT data.

# Query across heterogeneous schemas.
print("=== Flexible Schema Queries ===")  # Print heading.
iot_events.select(
    col("id"), col("sensor_type"), col("sensor_id"),  # Keep context.
    element_at(col("attributes"), "value").cast("double").alias("reading"),  # Numeric value.
    element_at(col("attributes"), "unit").alias("unit"),  # Unit of measurement.
    element_at(col("attributes"), "location").alias("location"),  # Location (if present).
    size(col("attributes")).alias("num_attrs"),  # Attribute count.
).show(truncate=False)  # Display flexible query.

# Find sensors with specific attributes.
print("=== Filter by Attribute Existence ===")  # Print heading.
iot_events.filter(
    expr("map_contains_key(attributes, 'battery')")  # Only sensors reporting battery.
).select("sensor_id", "sensor_type", "attributes").show(truncate=False)

# Add default attributes.
print("=== Add Default Attributes ===")  # Print heading.
default_attrs = create_map(lit("status"), lit("active"), lit("version"), lit("1.0"))  # Defaults.

iot_events.select(
    col("sensor_id"),  # Keep id.
    map_concat(col("attributes"), default_attrs).alias("enriched_attrs"),  # Merge with defaults.
).show(truncate=False)  # Display enriched attributes.

# Summarize: which attributes exist across all events?
print("=== Attribute Usage Summary ===")  # Print heading.
from pyspark.sql.functions import collect_set, array_sort, flatten  # Imports.
iot_events.select(map_keys(col("attributes")).alias("keys")).select(
    explode(col("keys")).alias("attr_name"),  # One row per attribute.
).groupBy("attr_name").count().orderBy("attr_name").show()  # Frequency.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Map diff and versioning
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Map Diff and Versioning
# ============================================================
# Real-world: Tracking configuration changes, audit trails.

from pyspark.sql.functions import (  # Import functions.
    col, expr, map_keys, map_values, element_at, map_filter,
    array_union, array_except, explode, lit, when, create_map,
    map_concat, array_intersect, size
)  # End imports.

# Configuration versions.
config_changes = spark.createDataFrame([
    (1, "v1", {"host": "db.prod.com", "port": "5432", "ssl": "true", "pool_size": "10"}),
    (1, "v2", {"host": "db.prod.com", "port": "5433", "ssl": "true", "pool_size": "20", "timeout": "30"}),
], "config_id INT, version STRING, settings MAP<STRING, STRING>")  # Versioned configs.

# Compute map diff between versions.
print("=== Map Diff: What Changed Between Versions ===")  # Print heading.
from pyspark.sql.functions import array_contains  # Import.

v1 = config_changes.filter(col("version") == "v1").select(col("settings").alias("v1_settings")).first()[0]
v2 = config_changes.filter(col("version") == "v2").select(col("settings").alias("v2_settings")).first()[0]

# Compare as DataFrames.
v1_df = spark.createDataFrame([(k, v) for k, v in v1.items()], ["key", "v1_value"])  # V1 entries.
v2_df = spark.createDataFrame([(k, v) for k, v in v2.items()], ["key", "v2_value"])  # V2 entries.

# Full outer join to find all changes.
diff = v1_df.join(v2_df, "key", "full_outer").select(
    col("key"),  # Setting name.
    col("v1_value"),  # Old value.
    col("v2_value"),  # New value.
    when(col("v1_value").isNull(), "ADDED")  # New key.
        .when(col("v2_value").isNull(), "REMOVED")  # Deleted key.
        .when(col("v1_value") != col("v2_value"), "MODIFIED")  # Changed value.
        .otherwise("UNCHANGED").alias("change_type"),  # No change.
)

diff.show(truncate=False)  # Display diff results.

# === Audit log with map-based changes ===
print("=== Audit Log Pattern ===")  # Print heading.
audit = spark.createDataFrame([
    ("2024-01-01", "admin", "user_settings", {"theme": "dark", "lang": "en"}),
    ("2024-01-05", "admin", "user_settings", {"theme": "light", "lang": "en", "timezone": "UTC"}),
    ("2024-01-10", "user1", "user_settings", {"theme": "light", "lang": "de", "timezone": "CET"}),
], "timestamp STRING, changed_by STRING, setting_name STRING, new_values MAP<STRING, STRING>")  # Audit log.

# Show all changes to a setting over time.
audit.select(
    col("timestamp"),  # When.
    col("changed_by"),  # Who.
    map_keys(col("new_values")).alias("changed_keys"),  # What keys.
    col("new_values"),  # New values.
).orderBy("timestamp").show(truncate=False)  # Display audit trail.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production map utilities
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Production Map Utilities
# ============================================================
# Real-world: Reusable map operations for data pipelines.

from pyspark.sql.functions import (  # Import production helpers.
    col, expr, element_at, map_keys, map_values, size,
    create_map, map_concat, lit, when, explode, map_from_entries,
    collect_list, struct, array_sort
)  # End imports.
from pyspark.sql import DataFrame  # Type hint.
from pyspark.sql import Column  # Column type.

# === Utility: Safe map access with default value ===
def map_get_or_default(map_col: Column, key: str, default: str) -> Column:
    """Get value from map, return default if key not found."""
    return when(
        element_at(map_col, key).isNull(), lit(default)  # Default if NULL.
    ).otherwise(element_at(map_col, key))  # Value if exists.

# === Utility: Map size categorization ===
def map_complexity(map_col: Column) -> Column:
    """Categorize map by number of entries."""
    return when(size(map_col) == 0, "empty").when(
        size(map_col) <= 3, "simple"
    ).when(size(map_col) <= 7, "moderate").otherwise("complex")

# === Apply utilities ===
print("=== Production Map Pipeline ===")  # Print heading.
app_config = spark.createDataFrame([
    ("app1", {"env": "prod", "region": "us", "version": "2.1", "debug": "false"}),
    ("app2", {"env": "dev", "debug": "true"}),
    ("app3", {"env": "staging", "region": "eu", "version": "3.0", "replicas": "3",
              "cpu_limit": "4", "mem_limit": "8Gi", "gpu": "1"}),
    ("app4", {}),  # Empty config!
], "app_name STRING, config MAP<STRING, STRING>")  # App configurations.

result = app_config.select(
    col("app_name"),  # Keep name.
    col("config"),  # Original.
    map_get_or_default(col("config"), "env", "unknown").alias("environment"),  # Safe access.
    map_get_or_default(col("config"), "region", "global").alias("region"),  # With default.
    map_get_or_default(col("config"), "debug", "false").alias("debug_mode"),  # With default.
    size(col("config")).alias("num_settings"),  # Count.
    map_complexity(col("config")).alias("complexity"),  # Categorize.
)

result.show(truncate=False)  # Display production results.

# === Flatten nested map data for reporting ===
print("=== Flatten Maps for Reporting ===")  # Print heading.
app_config.filter(size(col("config")) > 0).select(
    col("app_name"),  # Keep app.
    explode(col("config")),  # One row per setting.
).show(truncate=False)  # Display flattened report.

print("✅ Map functions mastery complete!")  # Done.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Map Functions
# MAGIC
# MAGIC ### Mistake 1: NULL keys in maps
# MAGIC ```python
# MAGIC # WRONG — Map keys cannot be NULL!
# MAGIC create_map(lit(None), lit("value"))  # Runtime error!
# MAGIC
# MAGIC # CORRECT — Always ensure keys are non-null.
# MAGIC create_map(lit("key"), lit("value"))  # Valid.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Assuming element_at raises error on missing key
# MAGIC ```python
# MAGIC # element_at(map, "nonexistent_key") returns NULL, not error.
# MAGIC # Always handle NULL with coalesce or when/otherwise.
# MAGIC coalesce(element_at(col("map"), "key"), lit("default_value"))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Duplicate keys in map_concat
# MAGIC ```python
# MAGIC # map_concat({"a": 1}, {"a": 2}) → {"a": 2} (last wins!)
# MAGIC # If you need first-wins, reverse the argument order.
# MAGIC map_concat(map2, map1)  # map1 values take priority.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Confusing map_from_arrays with create_map
# MAGIC ```python
# MAGIC # create_map: takes alternating key, value, key, value literals/columns.
# MAGIC create_map(lit("k1"), col("v1"), lit("k2"), col("v2"))
# MAGIC
# MAGIC # map_from_arrays: takes two array columns (keys[], values[]).
# MAGIC map_from_arrays(col("keys_array"), col("values_array"))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Forgetting size() returns -1 for NULL maps
# MAGIC ```python
# MAGIC # size(NULL) = -1, size({}) = 0
# MAGIC # Always check for NULL first:
# MAGIC when(col("map_col").isNull(), 0).otherwise(size(col("map_col")))
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Map Function Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Create a map with `create_map()`. Extract keys, values, and a specific element.
# MAGIC 2. Explode a map into key-value rows.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Use `str_to_map()` to parse semicolon-separated key=value pairs.
# MAGIC 4. Merge two maps and show which value wins for duplicate keys.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Use `map_filter()` to keep only entries where value > threshold.
# MAGIC 6. Use `transform_values()` to convert all string values to uppercase.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Build a feature flag system: store flags as map, query by flag name, provide defaults.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Parse HTTP headers (key:value pairs) into maps, extract content-type, user-agent.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design a change-detection system: compare old and new map versions, classify each key as ADDED/REMOVED/MODIFIED/UNCHANGED.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Compare: explode+pivot vs direct element_at for extracting 10 keys from maps on 1M rows.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Test: NULL maps, empty maps, maps with NULL values, duplicate keys in concat.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build a config merge pipeline: base config + env overlay + app overlay with proper precedence.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create a reference: "Map vs Struct vs JSON string: when to use which?"

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all for solutions.

# --- Level 1: Basic map operations ---
print("=== Level 1: Basic Map Operations ===")  # Print heading.
basic_map = spark.createDataFrame([
    (1, "Alice", "engineer", "senior")
], ["id", "name", "role", "level"])  # Sample data.

basic_map.select(
    col("name"),  # Keep name.
    create_map(lit("role"), col("role"), lit("level"), col("level")).alias("info_map"),  # Create map.
    map_keys(create_map(lit("role"), col("role"), lit("level"), col("level"))).alias("keys"),  # Get keys.
    element_at(create_map(lit("role"), col("role"), lit("level"), col("level")), "role").alias("role_val"),  # Get value.
).show(truncate=False)  # Display results.

# --- Level 6: Change detection ---
print("=== Level 6: Change Detection System ===")  # Print heading.
changes = spark.createDataFrame([
    ("cfg1", {"a": "1", "b": "2", "c": "3"}, {"a": "1", "b": "5", "d": "4"}),
], "id STRING, old_map MAP<STRING,STRING>, new_map MAP<STRING,STRING>")  # Changes.

# Explode both maps and compare.
old_entries = changes.select("id", explode(col("old_map")).alias("key", "old_val"))  # Old entries.
new_entries = changes.select("id", explode(col("new_map")).alias("key", "new_val"))  # New entries.

change_report = old_entries.join(new_entries, ["id", "key"], "full_outer").select(
    col("id"), col("key"),  # Context.
    col("old_val"), col("new_val"),  # Values.
    when(col("old_val").isNull(), "ADDED")
        .when(col("new_val").isNull(), "REMOVED")
        .when(col("old_val") != col("new_val"), "MODIFIED")
        .otherwise("UNCHANGED").alias("status"),  # Change type.
)

change_report.show(truncate=False)  # Display change report.

# --- Level 9: Config merge with precedence ---
print("=== Level 9: Config Merge Pipeline ===")  # Print heading.
# Precedence: app > env > base (later map_concat wins).
base = {"timeout": "30", "retries": "3", "log_level": "INFO"}  # Base.
env_override = {"timeout": "60", "ssl": "true"}  # Env overrides timeout.
app_override = {"log_level": "DEBUG"}  # App overrides log_level.

merge_df = spark.createDataFrame([
    ("myapp", base, env_override, app_override)
], "app STRING, base MAP<STRING,STRING>, env MAP<STRING,STRING>, app_cfg MAP<STRING,STRING>")  # Configs.

# map_concat: last wins, so order = base, env, app.
merge_df.select(
    col("app"),  # Keep app.
    map_concat(col("base"), col("env"), col("app_cfg")).alias("final_config"),  # Merged.
).show(truncate=False)  # Display final config.

print("✅ All homework solutions complete!")  # Completion message.