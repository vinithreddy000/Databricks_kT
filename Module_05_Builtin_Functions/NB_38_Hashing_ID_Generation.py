# Databricks notebook source
# DBTITLE 1,NB_38 Header
# MAGIC %md
# MAGIC # NB_38 — Hashing, ID Generation & Utility Functions
# MAGIC
# MAGIC **Module 5: Built-in Functions** | Notebook 38 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Hashing: hash(), xxhash64(), md5(), sha1(), sha2(), crc32()
# MAGIC * ID generation: monotonically_increasing_id(), uuid()
# MAGIC * Metadata: spark_partition_id(), input_file_name()
# MAGIC * Assertions: assert_true(), raise_error()
# MAGIC * Use cases: Change Data Capture, deduplication, surrogate keys, bucketing
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Essential for data engineering)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are Hashing Functions?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are Hashing Functions? (Real-World Analogy)
# MAGIC
# MAGIC ### 🔐 The Fingerprint Machine
# MAGIC
# MAGIC Hashing creates a "fingerprint" of data — a fixed-size identifier from variable-size input:
# MAGIC
# MAGIC | Real World | PySpark Function | Use Case |
# MAGIC |---|---|---|
# MAGIC | Fingerprint | `md5()`, `sha2()` | Data integrity check |
# MAGIC | Quick stamp | `hash()`, `xxhash64()` | Bucketing, partitioning |
# MAGIC | Serial number | `monotonically_increasing_id()` | Row numbering |
# MAGIC | UUID badge | `uuid()` | Globally unique identifier |
# MAGIC | Assembly line number | `spark_partition_id()` | Debug partition distribution |
# MAGIC | Source stamp | `input_file_name()` | Track data origin |
# MAGIC
# MAGIC ### Key Properties of Hash Functions
# MAGIC * **Deterministic:** Same input always produces same output
# MAGIC * **Fixed size:** Output length is constant regardless of input size
# MAGIC * **One-way:** Cannot reverse a hash to get original data
# MAGIC * **Collision-resistant:** Different inputs rarely produce same hash (varies by algorithm)

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Hashing Works
# MAGIC %md
# MAGIC ## SECTION 2 — How Hashing Works (Internal Mechanics)
# MAGIC
# MAGIC ### Hash Function Comparison
# MAGIC ```
# MAGIC ┌─────────────┬──────────────┬──────────────┬──────────────────┐
# MAGIC │ Function    │ Output       │ Speed        │ Best For           │
# MAGIC ├─────────────┼──────────────┼──────────────┼──────────────────┤
# MAGIC │ hash()      │ 32-bit int   │ ⭐⭐⭐⭐⭐    │ Bucketing, joins   │
# MAGIC │ xxhash64()  │ 64-bit long  │ ⭐⭐⭐⭐⭐    │ Better hash(), less collision │
# MAGIC │ crc32()     │ 32-bit int   │ ⭐⭐⭐⭐⭐    │ Checksums          │
# MAGIC │ md5()       │ 128-bit hex  │ ⭐⭐⭐       │ Data fingerprints  │
# MAGIC │ sha1()      │ 160-bit hex  │ ⭐⭐        │ Legacy systems     │
# MAGIC │ sha2(x,256) │ 256-bit hex  │ ⭐         │ Cryptographic need │
# MAGIC └─────────────┴──────────────┴──────────────┴──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### ID Generation
# MAGIC ```
# MAGIC monotonically_increasing_id():
# MAGIC   │
# MAGIC   ├─ Guaranteed unique within a single DataFrame operation
# MAGIC   ├─ NOT consecutive (gaps between partitions)
# MAGIC   ├─ NOT stable across re-runs (may change!)
# MAGIC   └─ Format: (partition_id << 33) + row_within_partition
# MAGIC
# MAGIC uuid():
# MAGIC   ├─ Globally unique (128-bit random)
# MAGIC   ├─ Different every execution (non-deterministic)
# MAGIC   └─ Format: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Hash functions
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Hash Functions
# ============================================================
# Real-world: Data fingerprinting, change detection, bucketing.

from pyspark.sql import SparkSession  # Import SparkSession.
from pyspark.sql.functions import (  # Import hash functions.
    col, hash as spark_hash, xxhash64, md5, sha1, sha2, crc32,
    concat_ws, lit
)  # End imports.

spark = SparkSession.builder.getOrCreate()  # Get active session.

# Sample data.
data = spark.createDataFrame([
    (1, "Alice", "alice@co.com", "NYC"),
    (2, "Bob", "bob@co.com", "Chicago"),
    (3, "Charlie", "charlie@co.com", "Seattle"),
    (4, "Alice", "alice@co.com", "NYC"),  # Duplicate of row 1!
], ["id", "name", "email", "city"])  # User data.

# All hash functions comparison.
print("=== Hash Function Comparison ===")  # Print heading.
data.select(
    col("name"),  # Keep name.
    spark_hash(col("name")).alias("hash_32bit"),  # 32-bit integer hash.
    xxhash64(col("name")).alias("xxhash_64bit"),  # 64-bit long hash.
    md5(col("name")).alias("md5_128bit"),  # 128-bit hex string.
    sha1(col("name")).alias("sha1_160bit"),  # 160-bit hex string.
    sha2(col("name"), 256).alias("sha256"),  # 256-bit hex string.
    crc32(col("name")).alias("crc32"),  # 32-bit checksum.
).show(truncate=40)  # Display hash comparison.

# Multi-column hash (composite key fingerprint).
print("=== Multi-Column Hash for CDC ===")  # Print heading.
data.select(
    col("id"),  # Keep id.
    col("name"), col("email"), col("city"),  # Keep data.
    # Hash multiple columns for change detection.
    md5(concat_ws("|", col("name"), col("email"), col("city"))).alias("row_hash"),  # Row fingerprint.
    xxhash64(col("name"), col("email"), col("city")).alias("row_xxhash"),  # Alternative.
).show(truncate=40)  # Display multi-column hash.

# Verify: same data produces same hash.
print("=== Deterministic: Same Input = Same Hash ===")  # Print heading.
data.select(
    col("id"), col("name"),  # Keep context.
    md5(concat_ws("|", col("name"), col("email"), col("city"))).alias("hash"),  # Compute.
).show(truncate=False)  # Rows 1 and 4 have SAME hash!

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: ID generation
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: ID Generation
# ============================================================
# Real-world: Generating unique identifiers for rows.

from pyspark.sql.functions import (  # Import ID functions.
    col, monotonically_increasing_id, expr, lit
)  # End imports.

# Create data without IDs.
products = spark.createDataFrame([
    ("Laptop", 999.99),
    ("Mouse", 29.99),
    ("Keyboard", 59.99),
    ("Monitor", 299.99),
    ("Headphones", 149.99),
], ["product", "price"])  # Products without IDs.

# monotonically_increasing_id: unique but NOT consecutive.
print("=== monotonically_increasing_id() ===")  # Print heading.
with_id = products.withColumn(
    "row_id",
    monotonically_increasing_id()  # Unique ID per row.
)
with_id.show()  # Display (IDs may have gaps!).

# uuid(): globally unique, random.
print("=== uuid() — Globally Unique ===")  # Print heading.
with_uuid = products.withColumn(
    "unique_id",
    expr("uuid()")  # Random UUID per row.
)
with_uuid.show(truncate=False)  # Display UUIDs.

# Practical: Create consecutive IDs using row_number.
print("=== Consecutive IDs with row_number() ===")  # Print heading.
from pyspark.sql.window import Window  # Import Window.
from pyspark.sql.functions import row_number  # Import row_number.

w = Window.orderBy("product")  # Order alphabetically.
consecutive = products.withColumn(
    "seq_id",
    row_number().over(w)  # Consecutive 1, 2, 3...
)
consecutive.show()  # Display consecutive IDs.

print("""IMPORTANT NOTES:
- monotonically_increasing_id(): Unique but NOT consecutive (gaps between partitions)
- uuid(): Random, different every run (non-deterministic)
- row_number(): Consecutive but requires a Window (expensive for large data)
- For surrogate keys: prefer hash-based approaches""")  # Notes.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Metadata functions
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Metadata Functions
# ============================================================
# Real-world: Understanding data distribution and origin.

from pyspark.sql.functions import (  # Import metadata functions.
    col, spark_partition_id, input_file_name, current_timestamp, lit
)  # End imports.

# spark_partition_id: which partition is each row on?
print("=== spark_partition_id() ===")  # Print heading.
df = spark.range(20)  # 20 rows.

df.select(
    col("id"),  # Keep id.
    spark_partition_id().alias("partition"),  # Which partition.
).show(20)  # Display partition assignment.

# Count rows per partition.
print("=== Rows Per Partition ===")  # Print heading.
df.select(
    spark_partition_id().alias("partition"),  # Get partition.
).groupBy("partition").count().orderBy("partition").show()  # Distribution.

# Practical: Check for data skew.
print("=== Skew Detection ===")  # Print heading.
from pyspark.sql.functions import count, min as spark_min, max as spark_max  # Imports.

skewed = spark.createDataFrame(
    [(i % 3, f"val_{i}") for i in range(100)],  # 100 rows, 3 groups.
    ["key", "value"]
).repartition(5, col("key"))  # Repartition by key.

skewed.select(
    spark_partition_id().alias("partition"),  # Get partition.
).groupBy("partition").count().orderBy("partition").show()  # Check for skew.

# input_file_name: track data source.
print("=== input_file_name() (shows source file path) ===")  # Print heading.
print("Note: input_file_name() returns empty string for in-memory DataFrames.")  # Note.
print("It works with read operations: spark.read.csv(...).withColumn('source', input_file_name())")

# Demo with in-memory data (returns empty).
df.select(
    col("id"),  # Keep id.
    input_file_name().alias("source_file"),  # Empty for in-memory.
).show(5, truncate=False)  # Display (empty source).

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: CDC with hashing
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: CDC with Hashing
# ============================================================
# Real-world: Change Data Capture — detect which rows changed.

from pyspark.sql.functions import (  # Import functions.
    col, md5, concat_ws, xxhash64, when, lit, coalesce
)  # End imports.

# Yesterday's snapshot.
yesterday = spark.createDataFrame([
    (1, "Alice", "alice@co.com", "NYC", 95000),
    (2, "Bob", "bob@co.com", "Chicago", 75000),
    (3, "Charlie", "charlie@co.com", "Seattle", 85000),
    (4, "Diana", "diana@co.com", "Boston", 70000),
], ["id", "name", "email", "city", "salary"])  # Previous data.

# Today's snapshot.
today = spark.createDataFrame([
    (1, "Alice", "alice@new.com", "NYC", 95000),  # Email changed.
    (2, "Bob", "bob@co.com", "Chicago", 80000),  # Salary changed.
    (3, "Charlie", "charlie@co.com", "Seattle", 85000),  # No change.
    (5, "Eve", "eve@co.com", "Denver", 90000),  # New employee.
], ["id", "name", "email", "city", "salary"])  # Current data.

# Step 1: Compute row hash for both snapshots.
def add_row_hash(df):
    """Add a hash column for change detection."""
    return df.withColumn(
        "row_hash",
        md5(concat_ws("|", col("name"), col("email"), col("city"), col("salary").cast("string")))  # Hash all non-key columns.
    )  # Return with hash.

yesterday_h = add_row_hash(yesterday)  # Add hash to yesterday.
today_h = add_row_hash(today)  # Add hash to today.

# Step 2: Full outer join on business key (id).
print("=== CDC Detection ===")  # Print heading.
cdc = today_h.alias("t").join(
    yesterday_h.alias("y"),
    col("t.id") == col("y.id"),
    "full_outer"  # Catch inserts and deletes.
).select(
    coalesce(col("t.id"), col("y.id")).alias("id"),  # Business key.
    col("t.name").alias("current_name"),  # Current value.
    col("t.row_hash").alias("current_hash"),  # Current hash.
    col("y.row_hash").alias("previous_hash"),  # Previous hash.
    # Classify change type.
    when(col("y.id").isNull(), "INSERT")  # New in today.
        .when(col("t.id").isNull(), "DELETE")  # Gone from today.
        .when(col("t.row_hash") != col("y.row_hash"), "UPDATE")  # Hash changed.
        .otherwise("NO_CHANGE").alias("change_type"),  # No change.
)

cdc.show(truncate=False)  # Display CDC results.

# Summary.
print("=== Change Summary ===")  # Print heading.
cdc.groupBy("change_type").count().show()  # Count by type.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Surrogate keys and bucketing
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Surrogate Keys and Bucketing
# ============================================================
# Real-world: Creating stable surrogate keys, distributing data evenly.

from pyspark.sql.functions import (  # Import functions.
    col, md5, concat_ws, xxhash64, hash as spark_hash, abs as spark_abs, lit, expr
)  # End imports.

# Surrogate key from business key (deterministic!).
print("=== Surrogate Key Generation ===")  # Print heading.
customers = spark.createDataFrame([
    ("CUST-001", "Alice Smith", "2020-01-15"),
    ("CUST-002", "Bob Jones", "2020-03-22"),
    ("CUST-003", "Charlie Brown", "2020-07-10"),
], ["customer_id", "name", "signup_date"])  # Customer data.

customers.select(
    col("customer_id"),  # Business key.
    col("name"),  # Keep name.
    # MD5 surrogate key (stable, deterministic).
    md5(col("customer_id")).alias("surrogate_key_md5"),
    # Integer surrogate key (for join optimization).
    xxhash64(col("customer_id")).alias("surrogate_key_int"),
).show(truncate=False)  # Display surrogate keys.

# Bucketing: distribute rows evenly across N buckets.
print("=== Hash Bucketing (Distribute Evenly) ===")  # Print heading.
from pyspark.sql.functions import abs as spark_abs  # Import abs.

num_buckets = 4  # 4 buckets.

events = spark.createDataFrame(
    [(f"user_{i}", f"event_{i}") for i in range(20)],
    ["user_id", "event"]  # 20 events.
)

bucketed = events.withColumn(
    "bucket",
    spark_abs(spark_hash(col("user_id"))) % lit(num_buckets)  # Hash mod N.
)

bucketed.show(20, truncate=False)  # Display with buckets.

# Verify even distribution.
print("=== Bucket Distribution ===")  # Print heading.
bucketed.groupBy("bucket").count().orderBy("bucket").show()  # Count per bucket.

# A/B test assignment (deterministic!).
print("=== A/B Test Assignment ===")  # Print heading.
users = spark.createDataFrame(
    [(f"user_{i}",) for i in range(10)], ["user_id"]  # 10 users.
)

users.select(
    col("user_id"),  # Keep.
    (spark_abs(spark_hash(col("user_id"))) % 100).alias("hash_pct"),  # 0-99.
    expr("CASE WHEN abs(hash(user_id)) % 100 < 50 THEN 'A' ELSE 'B' END").alias("test_group"),  # 50/50.
).show()  # Display assignments.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: assert_true and raise_error
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: assert_true and raise_error
# ============================================================
# Real-world: Data quality assertions in pipelines.

from pyspark.sql.functions import (  # Import assertion functions.
    col, expr, when, lit, count, sum as spark_sum
)  # End imports.

# Data with potential quality issues.
orders = spark.createDataFrame([
    (1, "Alice", 99.99, 2),
    (2, "Bob", 149.99, 1),
    (3, "Charlie", -5.00, 1),  # Negative price!
    (4, "Diana", 0.00, 0),  # Zero quantity!
    (5, "Eve", 50.00, 3),
], ["id", "customer", "price", "quantity"])  # Order data.

# assert_true: raises error if condition is false.
print("=== assert_true() — Data Validation ===")  # Print heading.
print("assert_true raises an error when condition is FALSE.")
print("Use in production pipelines to halt on bad data.\n")

# Safe way: check first, then assert.
print("=== Pre-check Before Assert ===")  # Print heading.
quality_check = orders.select(
    count("*").alias("total_rows"),  # Total.
    spark_sum(when(col("price") < 0, 1).otherwise(0)).alias("negative_prices"),  # Bad prices.
    spark_sum(when(col("quantity") <= 0, 1).otherwise(0)).alias("zero_qty"),  # Bad quantities.
)
quality_check.show()  # Display quality metrics.

# Conditional assertions (won't fail here, just demonstrate).
print("=== Safe Assertions (filter bad rows first) ===")  # Print heading.
clean_orders = orders.filter(
    (col("price") > 0) & (col("quantity") > 0)  # Keep only valid rows.
)

# Now it's safe to assert.
clean_orders.select(
    col("id"),  # Keep.
    col("price"),  # Keep.
    col("quantity"),  # Keep.
    expr("assert_true(price > 0, 'Price must be positive')").alias("price_check"),  # Assert.
    expr("assert_true(quantity > 0, 'Quantity must be positive')").alias("qty_check"),  # Assert.
).show()  # Display (passes because we filtered!).

# raise_error for custom error messages.
print("=== raise_error() with CASE WHEN ===")  # Print heading.
print("Pattern: Use CASE WHEN to conditionally raise errors.")
print("""Example:
  df.select(
      CASE WHEN price < 0 THEN raise_error('Negative price found!')
           ELSE price
      END as validated_price
  )""")

# Practical: Validation report instead of failing.
print("\n=== Validation Report Pattern ===")  # Print heading.
orders.select(
    col("id"), col("customer"),  # Context.
    col("price"), col("quantity"),  # Values.
    when(col("price") <= 0, "INVALID: negative/zero price")
        .when(col("quantity") <= 0, "INVALID: zero quantity")
        .otherwise("VALID").alias("validation_status"),  # Status.
).show(truncate=False)  # Display validation report.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Deduplication with hashing
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Deduplication with Hashing
# ============================================================
# Real-world: Removing exact and near-duplicate records at scale.

from pyspark.sql.functions import (  # Import functions.
    col, md5, concat_ws, row_number, count, xxhash64, first
)  # End imports.
from pyspark.sql.window import Window  # Import Window.

# Data with duplicates.
records = spark.createDataFrame([
    (1, "Alice", "alice@co.com", "NYC", "2024-01-01"),
    (2, "Alice", "alice@co.com", "NYC", "2024-01-02"),  # Same person, different date.
    (3, "Bob", "bob@co.com", "Chicago", "2024-01-01"),
    (4, "Bob", "bob@co.com", "Chicago", "2024-01-01"),  # Exact duplicate!
    (5, "Charlie", "charlie@co.com", "Seattle", "2024-01-03"),
    (6, "Alice", "alice@new.com", "NYC", "2024-01-05"),  # Same person, different email.
], ["id", "name", "email", "city", "date"])  # Records.

# Strategy 1: Hash all columns for exact-duplicate detection.
print("=== Strategy 1: Exact Duplicate Detection ===")  # Print heading.
with_hash = records.withColumn(
    "full_hash",
    md5(concat_ws("|", col("name"), col("email"), col("city"), col("date")))  # Hash all cols.
)

# Find duplicates.
dupes = with_hash.groupBy("full_hash").agg(
    count("*").alias("count"),  # How many duplicates.
    first("id").alias("first_id"),  # First occurrence.
    first("name").alias("name"),  # Name.
).filter(col("count") > 1)  # Only duplicates.

print("Exact duplicates found:")  # Label.
dupes.show(truncate=False)  # Display duplicates.

# Strategy 2: Business key dedup (keep latest).
print("=== Strategy 2: Business Key Dedup (Keep Latest) ===")  # Print heading.
business_hash = records.withColumn(
    "business_key_hash",
    md5(concat_ws("|", col("name"), col("city")))  # Hash business key fields only.
)

# Keep latest record per business key.
w = Window.partitionBy("business_key_hash").orderBy(col("date").desc())  # Latest first.

deduped = business_hash.withColumn(
    "rn", row_number().over(w)  # Rank by date.
).filter(col("rn") == 1).drop("rn", "business_key_hash")  # Keep first only.

print("After dedup (latest per business key):")
deduped.show(truncate=False)  # Display deduped.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Data masking with hashing
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Data Masking with Hashing
# ============================================================
# Real-world: PII anonymization, tokenization for analytics.

from pyspark.sql.functions import (  # Import functions.
    col, md5, sha2, concat, lit, substring, upper, regexp_replace
)  # End imports.

# PII data that needs anonymization.
pii_data = spark.createDataFrame([
    (1, "Alice Smith", "alice.smith@company.com", "555-0101", "123-45-6789"),
    (2, "Bob Jones", "bob.jones@company.com", "555-0202", "987-65-4321"),
    (3, "Charlie Brown", "charlie@company.com", "555-0303", "456-78-9012"),
], ["id", "name", "email", "phone", "ssn"])  # Sensitive data.

# Anonymization strategies.
print("=== PII Anonymization Strategies ===")  # Print heading.
masked = pii_data.select(
    col("id"),  # Keep ID (not PII).
    # Strategy 1: Full hash (irreversible, consistent).
    sha2(col("name"), 256).alias("name_hash"),  # Fully hashed.
    # Strategy 2: Partial masking (keep domain).
    concat(
        md5(col("email")),  # Hash the email.
        lit("@masked.com")  # Replace domain.
    ).alias("email_masked"),
    # Strategy 3: Partial reveal (last 4 of phone).
    concat(
        lit("***-"),  # Mask first part.
        substring(regexp_replace(col("phone"), "-", ""), -4, 4)  # Last 4 digits.
    ).alias("phone_masked"),
    # Strategy 4: Full redaction of SSN.
    lit("***-**-****").alias("ssn_redacted"),  # Completely hidden.
    # Strategy 5: Consistent pseudonym (same hash = same pseudonym).
    concat(lit("USER_"), upper(substring(md5(col("name")), 1, 8))).alias("pseudonym"),
)

masked.show(truncate=False)  # Display masked data.

# Verify: consistent pseudonyms (same name = same pseudonym).
print("=== Consistency Check ===")  # Print heading.
verify = spark.createDataFrame([
    ("Alice Smith",), ("Bob Jones",), ("Alice Smith",)  # Alice appears twice.
], ["name"])  # Verification data.

verify.select(
    col("name"),  # Original.
    concat(lit("USER_"), upper(substring(md5(col("name")), 1, 8))).alias("pseudonym"),  # Same pseudonym.
).show(truncate=False)  # Both "Alice Smith" rows get same pseudonym.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production hashing pipeline
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Production Hashing Pipeline
# ============================================================
# Real-world: Reusable hashing utilities for data engineering.

from pyspark.sql.functions import (  # Import functions.
    col, md5, xxhash64, concat_ws, hash as spark_hash,
    abs as spark_abs, lit, when, current_timestamp, expr
)  # End imports.
from pyspark.sql import DataFrame, Column  # Types.

# === Utility: Composite hash key ===
def surrogate_key(*cols):
    """Generate MD5 surrogate key from multiple columns."""
    return md5(concat_ws("|", *[col(c) if isinstance(c, str) else c for c in cols]))

# === Utility: Bucket assignment ===
def assign_bucket(key_col, num_buckets):
    """Assign rows to evenly distributed buckets."""
    return spark_abs(spark_hash(key_col)) % lit(num_buckets)

# === Utility: Row change hash (exclude audit columns) ===
def row_change_hash(df, exclude_cols=None):
    """Compute hash of all columns except excluded ones."""
    exclude = exclude_cols or []  # Default empty.
    hash_cols = [col(c) for c in df.columns if c not in exclude]  # All except excluded.
    return md5(concat_ws("|", *hash_cols))  # Hash remaining.

# Apply pipeline.
print("=== Production Hashing Pipeline ===")  # Print heading.
raw_data = spark.createDataFrame([
    ("CUST-001", "Alice", "NYC", 95000, "2024-01-01"),
    ("CUST-002", "Bob", "Chicago", 75000, "2024-01-02"),
    ("CUST-003", "Charlie", "Seattle", 85000, "2024-01-03"),
    ("CUST-004", "Diana", "Boston", 70000, "2024-01-04"),
], ["customer_id", "name", "city", "salary", "load_date"])  # Raw data.

result = raw_data.select(
    col("customer_id"),  # Business key.
    col("name"), col("city"), col("salary"),  # Data columns.
    # Surrogate key from business key.
    surrogate_key("customer_id").alias("sk"),
    # Row hash for CDC (exclude load_date).
    md5(concat_ws("|", col("name"), col("city"), col("salary").cast("string"))).alias("row_hash"),
    # Bucket assignment for parallel processing.
    assign_bucket(col("customer_id"), 4).alias("processing_bucket"),
    # Load metadata.
    current_timestamp().alias("loaded_at"),
)

result.show(truncate=40)  # Display pipeline result.
result.printSchema()  # Show schema.

print("✅ Hashing and ID Generation mastery complete!")  # Done.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Hashing & IDs
# MAGIC
# MAGIC ### Mistake 1: Using monotonically_increasing_id() as a stable key
# MAGIC ```python
# MAGIC # WRONG — IDs change across runs! NOT stable for joins!
# MAGIC df1 = df.withColumn("id", monotonically_increasing_id())  # Run 1: 0, 1, 2
# MAGIC df2 = df.withColumn("id", monotonically_increasing_id())  # Run 2: might be different!
# MAGIC
# MAGIC # CORRECT — Use hash-based surrogate key for stability.
# MAGIC df.withColumn("sk", md5(concat_ws("|", col("biz_key1"), col("biz_key2"))))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Expecting consecutive IDs from monotonically_increasing_id
# MAGIC ```python
# MAGIC # monotonically_increasing_id() has GAPS between partitions!
# MAGIC # Example: 0, 1, 2, 8589934592, 8589934593 (gap = partition boundary)
# MAGIC # For consecutive: use row_number() over a Window.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Using hash() for cryptographic purposes
# MAGIC ```python
# MAGIC # WRONG — hash()/xxhash64() are NOT cryptographically secure!
# MAGIC # They're fast but have higher collision probability.
# MAGIC
# MAGIC # For security: use sha2(col, 256) or sha2(col, 512).
# MAGIC # For performance: use hash() or xxhash64().
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Forgetting NULL handling in hashes
# MAGIC ```python
# MAGIC # md5(NULL) = NULL, not a hash of empty string!
# MAGIC # concat_ws("|", "a", NULL, "c") = "a|c" (NULLs skipped!)
# MAGIC # This means ("a", NULL, "c") hashes same as ("a", "c")!
# MAGIC
# MAGIC # Fix: use coalesce or nvl to replace NULLs.
# MAGIC md5(concat_ws("|", coalesce(col("a"), lit("NULL")), ...))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: uuid() is non-deterministic
# MAGIC ```python
# MAGIC # uuid() generates DIFFERENT values every time you read the column!
# MAGIC # Calling df.show() twice may show different UUIDs!
# MAGIC # Fix: materialize immediately with .cache() or write to table.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Hashing Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Hash a column with md5(), sha1(), sha2(), xxhash64(). Compare outputs.
# MAGIC 2. Generate IDs with monotonically_increasing_id() and uuid().
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Create a multi-column hash key. Show that same data = same hash.
# MAGIC 4. Assign rows to 8 buckets using hash mod.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Use hashing + row_number to deduplicate (keep latest per hash).
# MAGIC 6. Combine spark_partition_id() with count to detect data skew.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Build a PII anonymization pipeline: hash emails, mask phones, redact SSNs.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Implement full CDC: compare two snapshots using row hashing, classify as INSERT/UPDATE/DELETE/UNCHANGED.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design a surrogate key generator that handles: composite business keys, NULL values, type casting.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Benchmark: hash() vs xxhash64() vs md5() vs sha2() on 10M rows.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Test: NULL columns in hash, empty strings, numeric precision, unicode.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build incremental loading: use row hashes to only process changed records.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create a guide: "Which hash for which use case?" (speed vs security vs size).

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all.

# --- Level 1: Hash comparison ---
print("=== Level 1: Hash Comparison ===")  # Print heading.
sample = spark.createDataFrame([("hello world",), ("PySpark",)], ["text"])  # Sample.
sample.select(
    col("text"),  # Original.
    md5(col("text")).alias("md5"),  # 32 hex chars.
    sha1(col("text")).alias("sha1"),  # 40 hex chars.
    sha2(col("text"), 256).alias("sha256"),  # 64 hex chars.
    xxhash64(col("text")).alias("xxhash64"),  # 64-bit integer.
).show(truncate=30)  # Display comparison.

# --- Level 4: Bucket assignment ---
print("=== Level 4: 8-Bucket Assignment ===")  # Print heading.
users = spark.createDataFrame([(f"user_{i}",) for i in range(20)], ["uid"])  # Users.
users.withColumn(
    "bucket", abs(hash(col("uid"))) % lit(8)  # 8 buckets.
).groupBy("bucket").count().orderBy("bucket").show()  # Distribution.

# --- Level 5: CDC ---
print("=== Level 5: CDC Implementation ===")  # Print heading.
old = spark.createDataFrame([
    (1, "A", "x"), (2, "B", "y"), (3, "C", "z")
], ["id", "name", "val"])  # Old.
new = spark.createDataFrame([
    (1, "A", "x"), (2, "B", "CHANGED"), (4, "D", "w")
], ["id", "name", "val"])  # New.

old_h = old.withColumn("h", md5(concat_ws("|", col("name"), col("val"))))  # Hash old.
new_h = new.withColumn("h", md5(concat_ws("|", col("name"), col("val"))))  # Hash new.

cdc = new_h.alias("n").join(old_h.alias("o"), col("n.id") == col("o.id"), "full_outer").select(
    coalesce(col("n.id"), col("o.id")).alias("id"),  # Key.
    when(col("o.id").isNull(), "INSERT")
        .when(col("n.id").isNull(), "DELETE")
        .when(col("n.h") != col("o.h"), "UPDATE")
        .otherwise("NO_CHANGE").alias("action"),  # CDC type.
)
cdc.show()  # Display CDC.

print("✅ All homework solutions complete!")  # Completion message.