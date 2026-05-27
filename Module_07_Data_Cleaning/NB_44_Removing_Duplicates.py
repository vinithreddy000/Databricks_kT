# Databricks notebook source
# DBTITLE 1,NB_44 Header
# MAGIC %md
# MAGIC # NB_44 — Removing and Identifying Duplicates
# MAGIC
# MAGIC **Module 7: Data Cleaning & Quality** | Notebook 44 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Exact duplicates: distinct(), dropDuplicates()
# MAGIC * Subset duplicates: dropDuplicates(subset)
# MAGIC * Identifying duplicates: groupBy count, window row_number
# MAGIC * Keeping first/last by timestamp (dedup with ordering)
# MAGIC * Counting and reporting duplicates before removal
# MAGIC * dropDuplicatesWithinWatermark() for streaming
# MAGIC * Performance: distinct vs dropDuplicates
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Critical for data quality)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are Duplicates?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are Duplicates? (Real-World Analogy)
# MAGIC
# MAGIC ### 📦 The Mail Room
# MAGIC
# MAGIC Duplicates are like receiving the same letter multiple times:
# MAGIC
# MAGIC | Mail Scenario | Data Equivalent | Strategy |
# MAGIC |---|---|---|
# MAGIC | Same letter, same everything | Exact duplicate row | `distinct()` |
# MAGIC | Same sender, different dates | Subset duplicate | `dropDuplicates(["sender"])` |
# MAGIC | Same letter, keep latest | Ordered dedup | `row_number() + filter` |
# MAGIC | Same customer, slight typos | Fuzzy duplicate | Levenshtein/soundex matching |
# MAGIC
# MAGIC ### Types of Duplicates
# MAGIC 1. **Exact duplicates:** Every column matches perfectly
# MAGIC 2. **Key-based duplicates:** Same business key, different other values
# MAGIC 3. **Temporal duplicates:** Same entity at different timestamps (keep latest)
# MAGIC 4. **Fuzzy duplicates:** Near-matches due to typos or format differences
# MAGIC
# MAGIC ### Why Duplicates Happen
# MAGIC * Re-processing of source files (most common!)
# MAGIC * Retry logic in pipelines creating extra records
# MAGIC * JOIN operations producing unexpected multiplication
# MAGIC * Multiple source systems with overlapping data

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Duplicate Removal Works
# MAGIC %md
# MAGIC ## SECTION 2 — How Duplicate Removal Works
# MAGIC
# MAGIC ### Methods Comparison
# MAGIC ```
# MAGIC ┌────────────────────┬───────────────────┬────────────────────┐
# MAGIC │ Method             │ What It Does        │ When to Use          │
# MAGIC ├────────────────────┼───────────────────┼────────────────────┤
# MAGIC │ distinct()         │ Remove exact dupes  │ All columns match    │
# MAGIC │ dropDuplicates()   │ Same as distinct()  │ Alias for distinct   │
# MAGIC │ dropDuplicates([]) │ Dedup on subset     │ Business key dedup   │
# MAGIC │ row_number + filter│ Keep first/last     │ Ordered dedup        │
# MAGIC │ groupBy + agg      │ Merge duplicates    │ Aggregate approach   │
# MAGIC └────────────────────┴───────────────────┴────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Rule: distinct() vs dropDuplicates()
# MAGIC ```
# MAGIC distinct()              = compares ALL columns
# MAGIC dropDuplicates()        = same as distinct() (no args)
# MAGIC dropDuplicates([cols])  = compares ONLY specified columns
# MAGIC                           keeps FIRST occurrence (non-deterministic!)
# MAGIC ```
# MAGIC
# MAGIC ### When Order Matters: Window Dedup Pattern
# MAGIC ```python
# MAGIC w = Window.partitionBy("business_key").orderBy(col("timestamp").desc())
# MAGIC df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
# MAGIC # Keeps the LATEST record per business key
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Exact duplicates
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Exact Duplicates
# ============================================================
# Real-world: Data loaded twice from same source file.

from pyspark.sql import SparkSession  # Import SparkSession.
from pyspark.sql.functions import col, count, lit  # Import functions.

spark = SparkSession.builder.getOrCreate()  # Get active session.

# Data with exact duplicates.
orders = spark.createDataFrame([
    (1, "Alice", "Laptop", 999.99, "2024-01-15"),
    (2, "Bob", "Mouse", 29.99, "2024-01-15"),
    (3, "Alice", "Keyboard", 59.99, "2024-01-16"),
    (1, "Alice", "Laptop", 999.99, "2024-01-15"),  # Exact duplicate of row 1.
    (2, "Bob", "Mouse", 29.99, "2024-01-15"),  # Exact duplicate of row 2.
    (4, "Charlie", "Monitor", 299.99, "2024-01-17"),
    (1, "Alice", "Laptop", 999.99, "2024-01-15"),  # Triple! 3rd copy.
], ["order_id", "customer", "product", "price", "date"])  # Order data.

print(f"=== Before Dedup: {orders.count()} rows ===")  # Print count.
orders.show()  # Display all rows.

# Method 1: distinct() — remove exact duplicates.
print(f"=== After distinct(): {orders.distinct().count()} rows ===")  # Count.
orders.distinct().show()  # Display.

# Method 2: dropDuplicates() — same as distinct().
print(f"=== After dropDuplicates(): {orders.dropDuplicates().count()} rows ===")  # Count.

# Count duplicates BEFORE removing.
print("=== Duplicate Count Report ===")  # Print heading.
total = orders.count()  # Total.
unique = orders.distinct().count()  # Unique.
duplicates = total - unique  # Duplicates.
print(f"Total rows: {total}")  # Display.
print(f"Unique rows: {unique}")  # Display.
print(f"Duplicate rows: {duplicates} ({round(duplicates/total*100, 1)}%)")  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Subset deduplication
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Subset Deduplication
# ============================================================
# Real-world: Same customer appears multiple times, keep one record.

from pyspark.sql.functions import col, count, desc  # Imports.

# Customer data with business-key duplicates.
customers = spark.createDataFrame([
    (1, "alice@co.com", "Alice Smith", "NYC", "2024-01-01"),
    (2, "bob@co.com", "Bob Jones", "Chicago", "2024-01-05"),
    (3, "alice@co.com", "Alice S.", "New York", "2024-02-01"),  # Same email, different name/city.
    (4, "charlie@co.com", "Charlie Brown", "Seattle", "2024-01-10"),
    (5, "bob@co.com", "Robert Jones", "Chicago", "2024-03-01"),  # Same email, different name.
    (6, "diana@co.com", "Diana Prince", "Boston", "2024-01-15"),
], ["id", "email", "name", "city", "last_seen"])  # Customer data.

print(f"=== Original: {customers.count()} rows ===")  # Count.
customers.show(truncate=False)  # Display.

# dropDuplicates on subset: keep first occurrence by email.
print("=== dropDuplicates(['email']): Keep First Per Email ===")  # Print heading.
deduped = customers.dropDuplicates(["email"])  # Dedup on email only.
print(f"After dedup: {deduped.count()} rows")  # Count.
deduped.show(truncate=False)  # Display.
print("WARNING: dropDuplicates keeps an ARBITRARY row (not guaranteed first/last!)")

# Identify duplicates: show which emails appear multiple times.
print("\n=== Identifying Duplicates ===")  # Print heading.
customers.groupBy("email").agg(
    count("*").alias("occurrence_count"),  # How many times.
).filter(col("occurrence_count") > 1).show(truncate=False)  # Only duplicates.

# Show all rows for duplicate emails.
print("=== Duplicate Records Detail ===")  # Print heading.
from pyspark.sql.window import Window  # Import Window.
from pyspark.sql.functions import row_number, count as w_count  # Imports.

w = Window.partitionBy("email")  # Window by email.
customers.withColumn(
    "group_count", w_count("*").over(w)  # Count per group.
).filter(col("group_count") > 1).drop("group_count").show(truncate=False)  # Show dupes.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Ordered dedup (keep latest)
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Ordered Dedup (Keep Latest)
# ============================================================
# Real-world: Multiple records per entity, keep the most recent.

from pyspark.sql.functions import col, row_number, desc  # Imports.
from pyspark.sql.window import Window  # Import Window.

# Keep the LATEST record per customer email (by last_seen date).
print("=== Ordered Dedup: Keep Latest Record ===")  # Print heading.

# Window: partition by business key, order by recency.
w = Window.partitionBy("email").orderBy(col("last_seen").desc())  # Latest first.

latest = customers.withColumn(
    "rn", row_number().over(w)  # Rank within group.
).filter(
    col("rn") == 1  # Keep only the latest.
).drop("rn")  # Remove helper column.

print(f"Keeping latest per email: {latest.count()} rows")
latest.show(truncate=False)  # Display latest records.

# Alternative: keep EARLIEST record.
print("=== Ordered Dedup: Keep Earliest Record ===")  # Print heading.
w_asc = Window.partitionBy("email").orderBy(col("last_seen").asc())  # Earliest first.

earliest = customers.withColumn(
    "rn", row_number().over(w_asc)  # Rank.
).filter(col("rn") == 1).drop("rn")  # Keep first.

earliest.show(truncate=False)  # Display earliest.

# Comparison: what we kept vs what we dropped.
print("=== Summary ===")  # Print heading.
print(f"Original rows: {customers.count()}")  # Original.
print(f"After keep-latest: {latest.count()}")  # Latest.
print(f"Rows removed: {customers.count() - latest.count()}")  # Removed.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Multi-key dedup with priority
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Multi-Key Dedup with Priority
# ============================================================
# Real-world: Dedup using composite keys and source priority.

from pyspark.sql.functions import (  # Import functions.
    col, row_number, when, lit, desc, asc, coalesce
)  # End imports.
from pyspark.sql.window import Window  # Import Window.

# Data from multiple sources with overlapping records.
multi_source = spark.createDataFrame([
    ("CRM", "C001", "Alice Smith", "alice@co.com", "NYC", "2024-03-01"),
    ("ERP", "C001", "Alice S", "alice@company.com", "New York", "2024-02-15"),
    ("WEB", "C001", "alice smith", None, "ny", "2024-03-10"),  # Most recent but incomplete.
    ("CRM", "C002", "Bob Jones", "bob@co.com", "Chicago", "2024-01-20"),
    ("WEB", "C002", "Bob J", "bob@co.com", "Chicago", "2024-02-01"),
    ("CRM", "C003", "Charlie", "charlie@co.com", "Seattle", "2024-03-15"),
], ["source", "customer_id", "name", "email", "city", "updated_at"])  # Multi-source.

print("=== Original Multi-Source Data ===")  # Print heading.
multi_source.show(truncate=False)  # Display.

# Strategy: Prioritize by source (CRM > ERP > WEB), then by recency.
print("=== Dedup: Source Priority + Recency ===")  # Print heading.

# Assign source priority.
prioritized = multi_source.withColumn(
    "source_priority",
    when(col("source") == "CRM", 1)  # CRM highest priority.
        .when(col("source") == "ERP", 2)  # ERP second.
        .otherwise(3)  # WEB lowest.
)

# Window: partition by customer_id, order by priority then recency.
w = Window.partitionBy("customer_id").orderBy(
    col("source_priority").asc(),  # Best source first.
    col("updated_at").desc(),  # Most recent first within same source.
)

result = prioritized.withColumn(
    "rn", row_number().over(w)  # Rank.
).filter(col("rn") == 1).drop("rn", "source_priority")  # Keep best.

result.show(truncate=False)  # Display deduped.

# Alternative: Merge best fields from all sources (golden record).
print("=== Golden Record: Best Fields from All Sources ===")  # Print heading.
from pyspark.sql.functions import first, collect_list, array_distinct  # Imports.

golden = multi_source.groupBy("customer_id").agg(
    first("name", ignorenulls=True).alias("name"),  # First non-null name.
    first("email", ignorenulls=True).alias("email"),  # First non-null email.
    first("city", ignorenulls=True).alias("city"),  # First non-null city.
    collect_list("source").alias("sources"),  # All sources.
)
golden.show(truncate=False)  # Display golden records.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Duplicate reporting and audit
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Duplicate Reporting and Audit
# ============================================================
# Real-world: Generate audit report before and after dedup.

from pyspark.sql.functions import (  # Import functions.
    col, count, sum as spark_sum, when, row_number, desc, lit,
    current_timestamp, concat_ws, md5
)  # End imports.
from pyspark.sql.window import Window  # Import Window.

# Transaction data with duplicates.
transactions = spark.createDataFrame([
    ("T001", "Alice", 100.0, "2024-01-15 10:00:00"),
    ("T002", "Bob", 200.0, "2024-01-15 11:00:00"),
    ("T001", "Alice", 100.0, "2024-01-15 10:00:00"),  # Exact dupe.
    ("T003", "Charlie", 300.0, "2024-01-15 12:00:00"),
    ("T002", "Bob", 200.0, "2024-01-15 11:00:00"),  # Exact dupe.
    ("T004", "Alice", 150.0, "2024-01-15 14:00:00"),  # Different transaction.
    ("T001", "Alice", 100.0, "2024-01-15 10:00:00"),  # Triple!
], ["txn_id", "customer", "amount", "timestamp"])  # Transactions.

# Duplicate audit report.
print("=== Duplicate Audit Report ===")  # Print heading.
audit = transactions.groupBy(transactions.columns).agg(
    count("*").alias("occurrence_count"),  # How many copies.
).filter(col("occurrence_count") > 1)  # Only duplicates.

print(f"Duplicate groups found: {audit.count()}")  # Count.
audit.show(truncate=False)  # Display duplicates.

# Detailed: mark each row as original or duplicate.
print("=== Mark Originals vs Duplicates ===")  # Print heading.
w = Window.partitionBy("txn_id", "customer", "amount", "timestamp").orderBy(lit(1))

marked = transactions.withColumn(
    "row_num", row_number().over(w)  # Number within duplicate group.
).withColumn(
    "status",
    when(col("row_num") == 1, "ORIGINAL").otherwise("DUPLICATE")  # Label.
)

marked.show(truncate=False)  # Display marked.

# Summary.
print("=== Dedup Summary ===")  # Print heading.
marked.groupBy("status").count().show()  # Count by status.

# Keep originals only.
clean = marked.filter(col("status") == "ORIGINAL").drop("row_num", "status")  # Keep.
print(f"Clean dataset: {clean.count()} rows")  # Final count.
clean.show(truncate=False)  # Display clean data.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Dedup with hash fingerprint
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Dedup with Hash Fingerprint
# ============================================================
# Real-world: Efficient dedup using row hashing for large datasets.

from pyspark.sql.functions import (  # Import functions.
    col, md5, concat_ws, row_number, count, desc
)  # End imports.
from pyspark.sql.window import Window  # Import Window.

# Large-ish dataset with subtle duplicates.
events = spark.createDataFrame([
    (1, "click", "page_home", "2024-01-15 10:00:01", "user_1"),
    (2, "click", "page_home", "2024-01-15 10:00:01", "user_1"),  # Exact dupe (different id).
    (3, "view", "page_products", "2024-01-15 10:01:00", "user_1"),
    (4, "click", "page_home", "2024-01-15 10:00:01", "user_1"),  # Triple.
    (5, "click", "page_home", "2024-01-15 10:05:00", "user_1"),  # Different time = NOT dupe.
    (6, "view", "page_products", "2024-01-15 10:01:00", "user_2"),  # Different user.
], ["event_id", "event_type", "page", "timestamp", "user_id"])  # Events.

# Step 1: Create hash fingerprint (exclude surrogate key).
print("=== Hash-Based Dedup ===")  # Print heading.
business_cols = ["event_type", "page", "timestamp", "user_id"]  # Business columns.

hashed = events.withColumn(
    "row_hash",
    md5(concat_ws("|", *[col(c) for c in business_cols]))  # Hash business columns.
)

hashed.select("event_id", "row_hash", *business_cols).show(truncate=False)  # Show hashes.

# Step 2: Dedup on hash (keep lowest event_id as "original").
w = Window.partitionBy("row_hash").orderBy(col("event_id").asc())  # Ordered by id.

deduped = hashed.withColumn(
    "rn", row_number().over(w)  # Rank.
).filter(col("rn") == 1).drop("rn", "row_hash")  # Keep first.

print(f"Before: {events.count()} rows")  # Before.
print(f"After:  {deduped.count()} rows")  # After.
deduped.show(truncate=False)  # Display.

# Step 3: Report which rows were duplicates.
print("=== Duplicate Groups ===")  # Print heading.
hashed.groupBy("row_hash").agg(
    count("*").alias("copies"),  # Count copies.
).filter(col("copies") > 1).show()  # Show groups with dupes.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Production dedup pipeline
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Production Dedup Pipeline
# ============================================================
# Real-world: Complete dedup workflow with audit trail.

from pyspark.sql.functions import (  # Import functions.
    col, row_number, count, md5, concat_ws, current_timestamp,
    lit, when, desc, monotonically_increasing_id
)  # End imports.
from pyspark.sql.window import Window  # Import Window.
from pyspark.sql import DataFrame  # Type.

def dedup_pipeline(df, business_keys, order_col=None, order_desc=True, audit=True):
    """Production dedup: deduplicate on business keys with optional ordering."""
    
    # Step 1: Add row hash for tracking.
    df_hashed = df.withColumn(
        "_row_hash", md5(concat_ws("|", *[col(c).cast("string") for c in business_keys]))
    )
    
    # Step 2: Count duplicates before.
    total_before = df_hashed.count()  # Before count.
    dup_groups = df_hashed.groupBy("_row_hash").count().filter(col("count") > 1).count()  # Groups.
    
    # Step 3: Dedup.
    if order_col:  # Ordered dedup.
        order = col(order_col).desc() if order_desc else col(order_col).asc()  # Direction.
        w = Window.partitionBy("_row_hash").orderBy(order)  # Window.
        result = df_hashed.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn", "_row_hash")
    else:  # Simple dedup (arbitrary first).
        result = df_hashed.dropDuplicates(["_row_hash"]).drop("_row_hash")
    
    total_after = result.count()  # After count.
    
    # Step 4: Audit report.
    if audit:
        print(f"\n{'='*50}")
        print(f"  DEDUP AUDIT REPORT")
        print(f"{'='*50}")
        print(f"  Business keys: {business_keys}")
        print(f"  Order column: {order_col} ({'DESC' if order_desc else 'ASC'})")
        print(f"  Rows before: {total_before:,}")
        print(f"  Rows after: {total_after:,}")
        print(f"  Rows removed: {total_before - total_after:,}")
        print(f"  Duplicate groups: {dup_groups:,}")
        print(f"  Dedup rate: {round((total_before-total_after)/total_before*100, 1)}%")
        print(f"{'='*50}\n")
    
    return result  # Return clean DataFrame.

# Apply pipeline.
print("=== Production Dedup Pipeline ===")  # Print heading.

# Test with customer data.
test_data = spark.createDataFrame([
    ("C001", "Alice", "alice@co.com", "2024-01-01"),
    ("C001", "Alice Smith", "alice@co.com", "2024-02-01"),  # Updated name.
    ("C001", "Alice S.", "alice@new.com", "2024-03-01"),  # Latest.
    ("C002", "Bob", "bob@co.com", "2024-01-15"),
    ("C003", "Charlie", "charlie@co.com", "2024-01-20"),
    ("C003", "Charlie B.", "charlie@co.com", "2024-02-20"),  # Updated.
], ["customer_id", "name", "email", "last_updated"])  # Test data.

# Dedup keeping latest per customer_id.
clean = dedup_pipeline(
    test_data,
    business_keys=["customer_id"],  # Dedup key.
    order_col="last_updated",  # Keep latest.
    order_desc=True,  # Descending = latest first.
)
clean.show(truncate=False)  # Display result.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Incremental dedup pattern
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Incremental Dedup Pattern
# ============================================================
# Real-world: Dedup new batch against existing data.

from pyspark.sql.functions import (  # Import functions.
    col, lit, when, row_number, desc, current_timestamp
)  # End imports.
from pyspark.sql.window import Window  # Import Window.

# Existing data (already clean).
existing = spark.createDataFrame([
    ("C001", "Alice", "alice@co.com", "2024-01-01", "existing"),
    ("C002", "Bob", "bob@co.com", "2024-01-05", "existing"),
    ("C003", "Charlie", "charlie@co.com", "2024-01-10", "existing"),
], ["customer_id", "name", "email", "updated_at", "source"])  # Existing.

# New batch (may contain duplicates of existing + internal dupes).
new_batch = spark.createDataFrame([
    ("C001", "Alice Smith", "alice@new.com", "2024-02-01", "batch_2"),  # Update to existing.
    ("C004", "Diana", "diana@co.com", "2024-02-01", "batch_2"),  # New record.
    ("C004", "Diana P.", "diana@co.com", "2024-02-05", "batch_2"),  # Internal dupe.
    ("C002", "Bob Jones", "bob@co.com", "2024-02-10", "batch_2"),  # Update to existing.
], ["customer_id", "name", "email", "updated_at", "source"])  # New batch.

# Incremental dedup: union all, then keep latest per key.
print("=== Incremental Dedup: Existing + New Batch ===")  # Print heading.
print(f"Existing records: {existing.count()}")  # Existing count.
print(f"New batch records: {new_batch.count()}")  # New count.

# Union existing + new.
combined = existing.unionByName(new_batch)  # Combine.
print(f"Combined: {combined.count()}")  # Combined.

# Dedup: keep latest per customer_id.
w = Window.partitionBy("customer_id").orderBy(col("updated_at").desc())  # Latest first.

final = combined.withColumn(
    "rn", row_number().over(w)  # Rank.
).filter(col("rn") == 1).drop("rn")  # Keep latest.

print(f"After incremental dedup: {final.count()}")  # Final count.
final.show(truncate=False)  # Display.

# Identify what happened to each record.
print("=== Change Classification ===")  # Print heading.
final.select(
    col("customer_id"),  # Key.
    col("name"),  # Current name.
    col("source"),  # Where it came from.
    when(col("source") == "existing", "UNCHANGED")
        .otherwise("UPDATED/NEW").alias("status"),  # Classification.
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Performance-optimized dedup
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Performance-Optimized Dedup
# ============================================================
# Real-world: Efficient dedup strategies for large datasets.

from pyspark.sql.functions import (  # Import functions.
    col, row_number, count, md5, concat_ws, desc,
    monotonically_increasing_id, spark_partition_id
)  # End imports.
from pyspark.sql.window import Window  # Import Window.
import time  # Timing.

# Generate test data.
print("=== Performance Tips for Dedup ===")  # Print heading.

# Create 50K rows with ~20% duplicates.
import random  # Random.
random.seed(42)  # Seed.
perf_data = [(f"key_{i % 40000}", f"name_{i}", random.random() * 1000) for i in range(50000)]  # 50K rows.
perf_df = spark.createDataFrame(perf_data, ["key", "name", "value"])  # Create.
perf_df.cache()  # Cache for benchmarking.
perf_df.count()  # Materialize.

print(f"Test data: {perf_df.count()} rows")  # Display.
print(f"Distinct keys: {perf_df.select('key').distinct().count()}")  # Distinct.

# Method 1: dropDuplicates (simplest).
start = time.time()  # Timer.
m1 = perf_df.dropDuplicates(["key"]).count()  # Dedup.
t1 = time.time() - start  # Elapsed.
print(f"\nMethod 1 - dropDuplicates([key]): {m1} rows, {t1:.3f}s")

# Method 2: Window row_number (ordered).
start = time.time()  # Timer.
w = Window.partitionBy("key").orderBy(col("value").desc())  # Window.
m2 = perf_df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn").count()
t2 = time.time() - start  # Elapsed.
print(f"Method 2 - Window row_number: {m2} rows, {t2:.3f}s")

# Method 3: groupBy + first (aggregate approach).
start = time.time()  # Timer.
from pyspark.sql.functions import first  # Import.
m3 = perf_df.groupBy("key").agg(first("name").alias("name"), first("value").alias("value")).count()
t3 = time.time() - start  # Elapsed.
print(f"Method 3 - groupBy+first: {m3} rows, {t3:.3f}s")

print(f"\n=== Recommendations ===")
print("- dropDuplicates: Fastest for simple dedup (no ordering needed)")
print("- Window row_number: Use when you need ordered dedup (keep latest/first)")
print("- groupBy+first: Use when you also need to aggregate other columns")
print("- For very large data: repartition by dedup key first to minimize shuffles")

perf_df.unpersist()  # Cleanup.
print("\n✅ Duplicate removal mastery complete!")  # Done.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Duplicate Removal
# MAGIC
# MAGIC ### Mistake 1: Assuming dropDuplicates keeps first/last
# MAGIC ```python
# MAGIC # WRONG assumption — dropDuplicates keeps an ARBITRARY row!
# MAGIC df.dropDuplicates(["email"])  # Which row survives? UNPREDICTABLE!
# MAGIC
# MAGIC # CORRECT — use Window + row_number for deterministic dedup.
# MAGIC w = Window.partitionBy("email").orderBy(col("date").desc())
# MAGIC df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1)
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Using distinct() when subset dedup is needed
# MAGIC ```python
# MAGIC # distinct() compares ALL columns!
# MAGIC # Two rows with same email but different timestamps are NOT duplicates to distinct().
# MAGIC
# MAGIC # Use dropDuplicates with subset for business-key dedup:
# MAGIC df.dropDuplicates(["email"])  # Dedup on email only.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Not counting duplicates before removing
# MAGIC ```python
# MAGIC # Always measure before removing!
# MAGIC total = df.count()
# MAGIC unique = df.dropDuplicates(["key"]).count()
# MAGIC print(f"Removing {total - unique} duplicates ({(total-unique)/total*100:.1f}%)")
# MAGIC # If >50% are duplicates, something is WRONG with the source!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Dedup destroying valid data
# MAGIC ```python
# MAGIC # Same customer buying same product twice is NOT a duplicate!
# MAGIC # Include timestamp or transaction_id in dedup key.
# MAGIC df.dropDuplicates(["customer", "product", "timestamp"])  # Include time!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not handling NULL in dedup keys
# MAGIC ```python
# MAGIC # Two rows with NULL email are NOT considered duplicates by dropDuplicates!
# MAGIC # NULL != NULL in Spark (same as SQL).
# MAGIC # Filter NULLs separately or coalesce before dedup.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Dedup Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Use `distinct()` and `dropDuplicates()` on a DataFrame with exact dupes.
# MAGIC 2. Count duplicates before and after removal.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Change from full-row dedup to subset dedup on a business key.
# MAGIC 4. Use `row_number()` to keep the latest record.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Mark rows as ORIGINAL or DUPLICATE before removing.
# MAGIC 6. Combine hash fingerprint + window dedup for efficient processing.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Build an incremental dedup: new batch vs existing data.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build a complete dedup pipeline with audit report and metrics.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design a golden record builder: merge best fields from duplicate groups.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Compare performance: distinct vs dropDuplicates vs window on 1M rows.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Handle: NULL in dedup keys, case-sensitive matching, whitespace differences.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build idempotent pipeline: re-running produces same result regardless of duplicates in source.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create guide: "Which dedup method for which scenario?"

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all.
from pyspark.sql.window import Window  # Window.

# --- Level 1: Basic dedup ---
print("=== Level 1: Basic Dedup ===")  # Print heading.
test = spark.createDataFrame([
    (1, "A", 10), (2, "B", 20), (1, "A", 10), (3, "C", 30), (2, "B", 20)
], ["id", "name", "value"])  # With dupes.

print(f"Before: {test.count()}, After distinct: {test.distinct().count()}")  # Count.
print(f"Duplicates removed: {test.count() - test.distinct().count()}")  # Removed.

# --- Level 3: Mark + hash dedup ---
print("\n=== Level 3: Mark Originals ===")  # Print heading.
w = Window.partitionBy(md5(concat_ws("|", col("id"), col("name"), col("value")))).orderBy(lit(1))
test.withColumn("rn", row_number().over(w)).withColumn(
    "status", when(col("rn") == 1, "KEEP").otherwise("DUPLICATE")
).show()  # Display marked.

# --- Level 6: Golden record ---
print("\n=== Level 6: Golden Record ===")  # Print heading.
dupes = spark.createDataFrame([
    ("C1", "Alice", None, "NYC"),
    ("C1", None, "alice@co.com", "New York"),
    ("C1", "Alice Smith", "alice@co.com", None),
], ["id", "name", "email", "city"])  # Fragmented.

# Golden: first non-null per field.
dupes.groupBy("id").agg(
    first("name", ignorenulls=True).alias("name"),
    first("email", ignorenulls=True).alias("email"),
    first("city", ignorenulls=True).alias("city"),
).show(truncate=False)  # Best of each.

# --- Level 8: NULL handling ---
print("\n=== Level 8: NULL in Dedup Keys ===")  # Print heading.
null_test = spark.createDataFrame([
    (None, "A"), (None, "B"), (1, "C"), (1, "D")
], "key INT, value STRING")  # With NULL keys.

print("dropDuplicates on key (NULLs NOT treated as equal):")
null_test.dropDuplicates(["key"]).show()  # Both NULL rows may survive!

print("Safe dedup (coalesce NULL):")
null_test.withColumn("safe_key", coalesce(col("key"), lit(-999))).dropDuplicates(["safe_key"]).drop("safe_key").show()

print("✅ All homework solutions complete!")  # Done.