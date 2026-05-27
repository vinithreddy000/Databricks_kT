# Databricks notebook source
# DBTITLE 1,NB_52 Header
# MAGIC %md
# MAGIC # NB_52 — Slowly Changing Dimensions (SCD)
# MAGIC
# MAGIC **Module 8: Transformations & Reshaping** | Notebook 52 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * SCD Type 0: Fixed/retain original
# MAGIC * SCD Type 1: Overwrite (no history)
# MAGIC * SCD Type 2: Add new row (full history)
# MAGIC * SCD Type 3: Add new column (limited history)
# MAGIC * SCD Type 6: Hybrid (1+2+3 combined)
# MAGIC * Delta Lake MERGE for SCD implementation
# MAGIC * Effective dating and surrogate keys
# MAGIC * Production SCD pipeline patterns
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐⭐ (Core data warehousing skill)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What are SCDs
# MAGIC %md
# MAGIC ## SECTION 1 — What are Slowly Changing Dimensions? (Real-World Analogy)
# MAGIC
# MAGIC ### 🏠 The Address Book Problem
# MAGIC
# MAGIC When a customer moves, what do you do with their old address?
# MAGIC
# MAGIC | SCD Type | Strategy | Analogy |
# MAGIC |---|---|---|
# MAGIC | Type 0 | Never change | Birth certificate (original data preserved) |
# MAGIC | Type 1 | Overwrite | Phone contacts (only current info) |
# MAGIC | Type 2 | Add new row | Journal/diary (complete history) |
# MAGIC | Type 3 | Add column | Sticky note on old record (previous + current) |
# MAGIC | Type 6 | Hybrid | Full audit trail + current flag |
# MAGIC
# MAGIC ### When to Use Each
# MAGIC ```
# MAGIC Type 1: Don't need history (names, phone numbers)
# MAGIC Type 2: MUST have history (addresses, prices, statuses)
# MAGIC Type 3: Need only one previous value
# MAGIC Type 6: Need both full history AND quick current lookup
# MAGIC ```
# MAGIC
# MAGIC ### SCD Type 2 — The Gold Standard
# MAGIC ```
# MAGIC Customer: Alice
# MAGIC ┌─────┬─────────┬───────────┬───────────┬─────────┐
# MAGIC │ SK  │ City    │ Valid_From │ Valid_To   │ Current │
# MAGIC ├─────┼─────────┼───────────┼───────────┼─────────┤
# MAGIC │ 101 │ Seattle │ 2020-01-01│ 2023-06-30│ false   │
# MAGIC │ 102 │ Denver  │ 2023-07-01│ 9999-12-31│ true    │
# MAGIC └─────┴─────────┴───────────┴───────────┴─────────┘
# MAGIC ```
# MAGIC
# MAGIC ### The Delta Lake Advantage
# MAGIC Delta MERGE makes SCD implementation declarative and ACID-safe.

# COMMAND ----------

# DBTITLE 1,SECTION 2 — SCD Implementation Patterns
# MAGIC %md
# MAGIC ## SECTION 2 — SCD Implementation Patterns
# MAGIC
# MAGIC ### SCD Type 1 (Overwrite)
# MAGIC ```python
# MAGIC # Simple MERGE: update matching rows
# MAGIC target.merge(source, "target.id = source.id")
# MAGIC   .whenMatchedUpdateAll()   # Overwrite
# MAGIC   .whenNotMatchedInsertAll() # New rows
# MAGIC   .execute()
# MAGIC ```
# MAGIC
# MAGIC ### SCD Type 2 (Full History)
# MAGIC ```python
# MAGIC # Step 1: Find changed records
# MAGIC # Step 2: Close old records (set valid_to, is_current=false)
# MAGIC # Step 3: Insert new version (set valid_from=today, is_current=true)
# MAGIC # Step 4: Insert brand new records
# MAGIC ```
# MAGIC
# MAGIC ### SCD Type 2 Key Columns
# MAGIC | Column | Purpose |
# MAGIC |---|---|
# MAGIC | surrogate_key | Unique row identifier |
# MAGIC | natural_key | Business key (customer_id) |
# MAGIC | valid_from | When this version became effective |
# MAGIC | valid_to | When this version was superseded |
# MAGIC | is_current | Boolean flag for latest version |
# MAGIC | hash_diff | Hash of tracked columns for change detection |
# MAGIC
# MAGIC ### Change Detection
# MAGIC ```python
# MAGIC # Hash-based change detection (efficient)
# MAGIC md5(concat_ws('|', col1, col2, col3))  # Fast comparison
# MAGIC
# MAGIC # Column-by-column comparison (explicit)
# MAGIC source.col1 != target.col1 OR source.col2 != target.col2
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: SCD Type 1
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: SCD Type 1 (Overwrite)
# ============================================================
# Real-world: Customer contact info where history isn't needed.

from pyspark.sql import SparkSession  # Import.
from pyspark.sql.functions import col, lit, current_timestamp  # Functions.

spark = SparkSession.builder.getOrCreate()  # Session.

# Current dimension table (target).
print("=== SCD Type 1: Overwrite Strategy ===")  # Heading.

customers_current = spark.createDataFrame([
    (1, "Alice", "alice@old.com", "555-0001", "Seattle"),
    (2, "Bob", "bob@work.com", "555-0002", "Portland"),
    (3, "Carol", "carol@home.com", "555-0003", "Denver"),
    (4, "Dave", "dave@co.com", "555-0004", "Austin"),
], ["customer_id", "name", "email", "phone", "city"])  # Current data.

print("BEFORE update:")  # Heading.
customers_current.show()  # Display.

# Incoming changes (source).
changes = spark.createDataFrame([
    (1, "Alice", "alice@new.com", "555-0001", "Seattle"),  # Email changed.
    (2, "Bob", "bob@work.com", "555-9999", "Portland"),   # Phone changed.
    (5, "Eve", "eve@new.com", "555-0005", "Miami"),       # New customer.
], ["customer_id", "name", "email", "phone", "city"])  # Changes.

print("Incoming changes:")  # Heading.
changes.show()  # Display.

# SCD Type 1: Simple overwrite using DataFrame operations.
# (In production, use Delta MERGE — shown here as concept.)
from pyspark.sql.functions import coalesce  # Import.

# Left anti join to find unchanged records.
unchanged = customers_current.join(
    changes, "customer_id", "left_anti"  # Records NOT in changes.
)

# Union unchanged + all changes (upsert simulation).
result_type1 = unchanged.unionByName(changes)  # Combine.

print("AFTER SCD Type 1 (overwrite):")  # Heading.
result_type1.orderBy("customer_id").show()  # Display.
print("Notice: Alice's email and Bob's phone updated. Eve is new. No history kept!")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: SCD Type 2 basic
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: SCD Type 2 (Add Row)
# ============================================================
# Real-world: Customer address history for order attribution.

from pyspark.sql.functions import (
    col, lit, when, current_date, to_date, monotonically_increasing_id
)  # Imports.

# Current SCD2 dimension table.
print("=== SCD Type 2: Full History ===")  # Heading.

scd2_current = spark.createDataFrame([
    (101, 1, "Alice", "Seattle", "2020-01-01", "9999-12-31", True),
    (102, 2, "Bob", "Portland", "2019-06-01", "9999-12-31", True),
    (103, 3, "Carol", "Denver", "2021-03-15", "9999-12-31", True),
], ["sk", "customer_id", "name", "city", "valid_from", "valid_to", "is_current"])  # SCD2.

scd2_current = scd2_current.withColumn("valid_from", to_date("valid_from")).withColumn("valid_to", to_date("valid_to"))  # Cast dates.

print("Current Dimension (all current=True):")  # Heading.
scd2_current.show()  # Display.

# Incoming changes: Alice moved, new customer Dave.
changes = spark.createDataFrame([
    (1, "Alice", "Denver"),    # Moved from Seattle to Denver.
    (4, "Dave", "Austin"),     # Brand new customer.
], ["customer_id", "name", "city"])  # Changes.

print("Incoming changes:")  # Heading.
changes.show()  # Display.

# Step 1: Identify changes (join current records with incoming).
current_records = scd2_current.filter(col("is_current") == True)  # Only current.
changed = current_records.join(
    changes, "customer_id", "inner"  # Matching records.
).filter(
    current_records["city"] != changes["city"]  # Actually changed.
).select(current_records["sk"], current_records["customer_id"])  # Changed IDs.

print(f"Records that changed: {changed.count()}")  # Count.

# Step 2: Close old records (set valid_to = today, is_current = false).
today = lit("2024-06-01").cast("date")  # Simulated today.

closed = scd2_current.join(changed, "sk", "left").withColumn(
    "valid_to", when(changed["customer_id"].isNotNull(), today).otherwise(col("valid_to"))  # Close.
).withColumn(
    "is_current", when(changed["customer_id"].isNotNull(), False).otherwise(col("is_current"))  # Flag.
).drop(changed["customer_id"])  # Clean.

print("After closing old records:")  # Heading.
closed.orderBy("sk").show()  # Display.

# Step 3: Create new version rows for changed records.
new_versions = changes.join(
    current_records.select("customer_id"), "customer_id", "inner"  # Only existing.
).select(
    (lit(200) + monotonically_increasing_id()).alias("sk"),  # New surrogate key.
    col("customer_id"),  # Natural key.
    col("name"),  # Keep.
    col("city"),  # New value.
    today.alias("valid_from"),  # Starts today.
    lit("9999-12-31").cast("date").alias("valid_to"),  # Open-ended.
    lit(True).alias("is_current"),  # Current.
)

# Step 4: Create rows for brand new customers.
new_customers = changes.join(
    current_records.select("customer_id"), "customer_id", "left_anti"  # Not existing.
).select(
    (lit(300) + monotonically_increasing_id()).alias("sk"),  # New SK.
    col("customer_id"), col("name"), col("city"),
    today.alias("valid_from"),
    lit("9999-12-31").cast("date").alias("valid_to"),
    lit(True).alias("is_current"),
)

# Final: Union all.
final_scd2 = closed.unionByName(new_versions).unionByName(new_customers)  # Combine.

print("=== Final SCD Type 2 Table ===")  # Heading.
final_scd2.orderBy("customer_id", "valid_from").show()  # Display.
print("Alice has 2 rows: Seattle (closed) and Denver (current). Dave is new.")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: SCD Type 3
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: SCD Type 3 (Add Column)
# ============================================================
# Real-world: Track previous value alongside current (e.g., salary).

from pyspark.sql.functions import col, when, lit, coalesce  # Imports.

# SCD Type 3: previous value stored in a separate column.
print("=== SCD Type 3: Previous + Current ===")  # Heading.

scd3_table = spark.createDataFrame([
    (1, "Alice", "Senior Engineer", None, 95000.00, None),
    (2, "Bob", "Manager", None, 110000.00, None),
    (3, "Carol", "Analyst", None, 75000.00, None),
], ["emp_id", "name", "current_title", "previous_title", "current_salary", "previous_salary"])  # SCD3.

print("Before updates:")  # Heading.
scd3_table.show()  # Display.

# Incoming changes.
changes = spark.createDataFrame([
    (1, "Alice", "Staff Engineer", 120000.00),   # Promotion.
    (2, "Bob", "Director", 140000.00),           # Promotion.
    (4, "Dave", "Junior Dev", 65000.00),         # New hire.
], ["emp_id", "name", "new_title", "new_salary"])  # Changes.

print("Incoming changes:")  # Heading.
changes.show()  # Display.

# Apply SCD Type 3: shift current to previous, insert new.
# For existing employees with changes:
updated = scd3_table.join(changes, "emp_id", "left")  # Join.

result_type3 = updated.select(
    col("emp_id"),  # Keep.
    coalesce(changes["name"], scd3_table["name"]).alias("name"),  # Latest name.
    # Title: current becomes previous, new becomes current.
    when(changes["new_title"].isNotNull(), changes["new_title"])
        .otherwise(scd3_table["current_title"]).alias("current_title"),  # New current.
    when(changes["new_title"].isNotNull(), scd3_table["current_title"])
        .otherwise(scd3_table["previous_title"]).alias("previous_title"),  # Old becomes prev.
    # Salary: same pattern.
    when(changes["new_salary"].isNotNull(), changes["new_salary"])
        .otherwise(scd3_table["current_salary"]).alias("current_salary"),  # New.
    when(changes["new_salary"].isNotNull(), scd3_table["current_salary"])
        .otherwise(scd3_table["previous_salary"]).alias("previous_salary"),  # Previous.
)

# Add new employees (not in original table).
new_emps = changes.join(scd3_table, "emp_id", "left_anti").select(  # Anti join.
    col("emp_id"), col("name"),
    col("new_title").alias("current_title"),
    lit(None).cast("string").alias("previous_title"),
    col("new_salary").alias("current_salary"),
    lit(None).cast("double").alias("previous_salary"),
)

final_type3 = result_type3.unionByName(new_emps)  # Combine.

print("=== After SCD Type 3 Update ===")  # Heading.
final_type3.orderBy("emp_id").show()  # Display.
print("Alice: was Senior Engineer ($95K), now Staff Engineer ($120K).")
print("Limitation: only ONE previous value stored!")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Delta MERGE for SCD2
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Delta MERGE for SCD Type 2
# ============================================================
# Real-world: Production SCD2 using Delta Lake MERGE statement.

from pyspark.sql.functions import (
    col, lit, current_date, when, md5, concat_ws, to_date
)  # Imports.
from delta.tables import DeltaTable  # Delta.

# Create a Delta table for SCD2 demo.
print("=== Delta MERGE SCD Type 2 ===")  # Heading.

# Initial dimension data.
init_data = spark.createDataFrame([
    (1, "Alice", "Engineering", "Seattle", 95000, "2020-01-01", "9999-12-31", True),
    (2, "Bob", "Marketing", "Portland", 85000, "2019-06-01", "9999-12-31", True),
    (3, "Carol", "Sales", "Denver", 90000, "2021-03-15", "9999-12-31", True),
], ["emp_id", "name", "department", "city", "salary", "valid_from", "valid_to", "is_current"])  # Data.

# Write to Delta.
table_path = "/tmp/scd2_demo_employees"  # Path.
init_data.write.format("delta").mode("overwrite").save(table_path)  # Write.

print("Initial dimension table:")  # Heading.
spark.read.format("delta").load(table_path).show()  # Display.

# Incoming source data (with changes).
source_data = spark.createDataFrame([
    (1, "Alice", "Engineering", "Denver", 105000),   # City + salary changed.
    (2, "Bob", "Marketing", "Portland", 85000),      # No change.
    (3, "Carol", "Engineering", "Denver", 95000),    # Department + salary changed.
    (4, "Dave", "Sales", "Austin", 70000),           # New employee.
], ["emp_id", "name", "department", "city", "salary"])  # Source.

print("Source data (incoming):")  # Heading.
source_data.show()  # Display.

# Add hash column for change detection.
tracked_cols = ["department", "city", "salary"]  # Columns to track.
source_hashed = source_data.withColumn(
    "hash_diff", md5(concat_ws("|", *[col(c).cast("string") for c in tracked_cols]))  # Hash.
)

# Load target Delta table.
target_dt = DeltaTable.forPath(spark, table_path)  # Load.
target_df = target_dt.toDF()  # As DataFrame.

# Add hash to target for comparison.
target_hashed = target_df.filter(col("is_current") == True).withColumn(
    "hash_diff", md5(concat_ws("|", *[col(c).cast("string") for c in tracked_cols]))  # Hash.
)

# Identify changed records.
changed_records = source_hashed.join(
    target_hashed.select("emp_id", target_hashed["hash_diff"].alias("target_hash")),
    "emp_id", "inner"  # Match.
).filter(
    col("hash_diff") != col("target_hash")  # Hash differs = changed.
).drop("target_hash")  # Clean.

print(f"Changed records: {changed_records.count()}")  # Count.
changed_records.show()  # Show.

# Identify new records.
new_records = source_data.join(
    target_df.select("emp_id").distinct(), "emp_id", "left_anti"  # Not in target.
)
print(f"New records: {new_records.count()}")  # Count.

# Build staged updates: new rows for changed + new records.
effective_date = "2024-06-01"  # Simulated date.

# New versions of changed records.
new_versions = changed_records.select(
    col("emp_id"), col("name"), col("department"), col("city"), col("salary"),
    lit(effective_date).alias("valid_from"),
    lit("9999-12-31").alias("valid_to"),
    lit(True).alias("is_current"),
)

# Brand new records.
new_inserts = new_records.select(
    col("emp_id"), col("name"), col("department"), col("city"), col("salary"),
    lit(effective_date).alias("valid_from"),
    lit("9999-12-31").alias("valid_to"),
    lit(True).alias("is_current"),
)

# All rows to stage.
staged = new_versions.unionByName(new_inserts)  # Combine.

# MERGE: close old records and insert new versions.
target_dt.alias("t").merge(
    staged.alias("s"),
    "t.emp_id = s.emp_id AND t.is_current = true"  # Match current row.
).whenMatchedUpdate(set={
    "valid_to": lit(effective_date),  # Close.
    "is_current": lit(False),  # No longer current.
}).whenNotMatchedInsertAll().execute()  # Insert new version.

print("\n=== After SCD2 MERGE ===")  # Heading.
result = spark.read.format("delta").load(table_path)  # Read.
result.orderBy("emp_id", "valid_from").show()  # Display.
print("Alice & Carol have history. Bob unchanged. Dave is new.")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Hash-based change detection
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Hash-Based Change Detection
# ============================================================
# Real-world: Efficient comparison for large datasets.

from pyspark.sql.functions import (
    col, md5, sha2, concat_ws, when, lit, count, sum as spark_sum
)  # Imports.

# Simulate large dimension with many columns.
print("=== Hash-Based Change Detection ===")  # Heading.

# Target: current state.
target = spark.createDataFrame([
    (1, "Alice", "Eng", "Seattle", 95000, "alice@co.com", "555-001", "active"),
    (2, "Bob", "Mkt", "Portland", 85000, "bob@co.com", "555-002", "active"),
    (3, "Carol", "Sales", "Denver", 90000, "carol@co.com", "555-003", "active"),
    (4, "Dave", "Eng", "Austin", 70000, "dave@co.com", "555-004", "inactive"),
    (5, "Eve", "HR", "Miami", 80000, "eve@co.com", "555-005", "active"),
], ["id", "name", "dept", "city", "salary", "email", "phone", "status"])  # Target.

# Source: incoming data.
source = spark.createDataFrame([
    (1, "Alice", "Eng", "Denver", 105000, "alice@co.com", "555-001", "active"),  # City+salary.
    (2, "Bob", "Mkt", "Portland", 85000, "bob@new.com", "555-002", "active"),   # Email.
    (3, "Carol", "Sales", "Denver", 90000, "carol@co.com", "555-003", "active"), # No change.
    (4, "Dave", "Eng", "Austin", 70000, "dave@co.com", "555-004", "active"),    # Status.
    (6, "Frank", "Ops", "Boston", 75000, "frank@co.com", "555-006", "active"),  # New.
], ["id", "name", "dept", "city", "salary", "email", "phone", "status"])  # Source.

# Define tracked column groups.
type1_cols = ["email", "phone"]  # Overwrite, no history.
type2_cols = ["dept", "city", "salary", "status"]  # Track history.

# Add hashes for each group.
target_h = target.withColumn(
    "hash_type2", md5(concat_ws("|", *[col(c).cast("string") for c in type2_cols]))  # Type2 hash.
).withColumn(
    "hash_type1", md5(concat_ws("|", *[col(c).cast("string") for c in type1_cols]))  # Type1 hash.
)

source_h = source.withColumn(
    "hash_type2", md5(concat_ws("|", *[col(c).cast("string") for c in type2_cols]))  # Type2 hash.
).withColumn(
    "hash_type1", md5(concat_ws("|", *[col(c).cast("string") for c in type1_cols]))  # Type1 hash.
)

# Compare hashes to classify changes.
print("=== Change Classification ===")  # Heading.
comparison = source_h.alias("s").join(
    target_h.alias("t"), col("s.id") == col("t.id"), "full_outer"  # Full outer.
).select(
    coalesce(col("s.id"), col("t.id")).alias("id"),  # ID.
    col("s.name").alias("source_name"),  # Source.
    when(col("t.id").isNull(), "NEW")  # New record.
    .when(col("s.id").isNull(), "DELETED")  # Deleted.
    .when(col("s.hash_type2") != col("t.hash_type2"), "TYPE2_CHANGE")  # SCD2.
    .when(col("s.hash_type1") != col("t.hash_type1"), "TYPE1_CHANGE")  # SCD1.
    .otherwise("NO_CHANGE").alias("change_type"),  # Same.
)

from pyspark.sql.functions import coalesce  # Import.
comparison.show()  # Display.

# Summary of changes.
print("=== Change Summary ===")  # Heading.
comparison.groupBy("change_type").count().show()  # Counts.
print("Hash comparison avoids comparing every column individually!")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: SCD Type 6 hybrid
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: SCD Type 6 (Hybrid)
# ============================================================
# Real-world: Full history (Type 2) + current value column (Type 3).

from pyspark.sql.functions import (
    col, lit, when, max as spark_max, first
)  # Imports.
from pyspark.sql.window import Window  # Window.

# SCD Type 6 = Type 1 + Type 2 + Type 3 combined.
print("=== SCD Type 6: Hybrid Approach ===")  # Heading.

# Full SCD6 table: history rows + current_* columns on every row.
scd6_table = spark.createDataFrame([
    # Alice: 2 historical records.
    (101, 1, "Alice", "Seattle", "Engineering", "2020-01-01", "2022-12-31", False, "Denver", "Data Science"),
    (102, 1, "Alice", "Portland", "Data Science", "2023-01-01", "2024-05-31", False, "Denver", "Data Science"),
    (103, 1, "Alice", "Denver", "Data Science", "2024-06-01", "9999-12-31", True, "Denver", "Data Science"),
    # Bob: 1 record (never changed).
    (104, 2, "Bob", "Portland", "Marketing", "2019-06-01", "9999-12-31", True, "Portland", "Marketing"),
], ["sk", "emp_id", "name", "hist_city", "hist_dept", "valid_from", "valid_to", "is_current",
    "current_city", "current_dept"])  # SCD6 schema.

print("SCD Type 6 Table:")  # Heading.
scd6_table.show(truncate=False)  # Display.

print("""
Type 6 Benefits:
- hist_city/hist_dept: What was true at that point in time (Type 2)
- current_city/current_dept: Always shows latest value (Type 3/1)
- is_current + valid_from/to: Full temporal tracking (Type 2)

Use cases:
- "Show me Alice's department in 2022" -> hist_dept on matching row
- "Show me everyone's current city" -> current_city (any row works!)
- "Timeline of Alice's moves" -> All rows for emp_id=1
""")

# Query: Point-in-time lookup.
print("=== Point-in-Time Query: Who was where on 2023-06-01? ===")  # Heading.
from pyspark.sql.functions import to_date  # Import.

query_date = "2023-06-01"  # Lookup date.
scd6_table.filter(
    (col("valid_from") <= query_date) & (col("valid_to") >= query_date)  # Active on date.
).select("emp_id", "name", "hist_city", "hist_dept", "current_city", "current_dept").show()  # Display.

# Applying a new change to Type 6.
print("=== Applying Type 6 Update ===")  # Heading.
print("Bob moves to 'Austin' and joins 'Sales':")

# Step 1: Close Bob's current record.
closed = scd6_table.withColumn(
    "valid_to",
    when((col("emp_id") == 2) & (col("is_current") == True), lit("2024-06-01"))
    .otherwise(col("valid_to"))  # Close.
).withColumn(
    "is_current",
    when((col("emp_id") == 2) & (col("is_current") == True), lit(False))
    .otherwise(col("is_current"))  # Flag.
)

# Step 2: Update current_* columns on ALL of Bob's rows.
updated = closed.withColumn(
    "current_city", when(col("emp_id") == 2, lit("Austin")).otherwise(col("current_city"))  # Update all rows.
).withColumn(
    "current_dept", when(col("emp_id") == 2, lit("Sales")).otherwise(col("current_dept"))  # Update all rows.
)

# Step 3: Insert new current row for Bob.
new_bob = spark.createDataFrame([
    (105, 2, "Bob", "Austin", "Sales", "2024-06-01", "9999-12-31", True, "Austin", "Sales"),
], ["sk", "emp_id", "name", "hist_city", "hist_dept", "valid_from", "valid_to", "is_current",
    "current_city", "current_dept"])  # New row.

final_scd6 = updated.unionByName(new_bob)  # Add.
final_scd6.orderBy("emp_id", "valid_from").show(truncate=False)  # Display.
print("Note: ALL of Bob's rows now show current_city='Austin', current_dept='Sales'.")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Production SCD2 pipeline
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Production SCD2 Pipeline
# ============================================================
# Real-world: Complete, reusable SCD2 class with Delta Lake.

from pyspark.sql.functions import (
    col, lit, md5, concat_ws, current_timestamp, when,
    monotonically_increasing_id, row_number
)  # Imports.
from pyspark.sql.window import Window  # Window.
from pyspark.sql import DataFrame  # Type.
from delta.tables import DeltaTable  # Delta.
from typing import List  # Typing.

class SCD2Pipeline:
    """Production SCD Type 2 pipeline with Delta Lake."""
    
    def __init__(self, table_path: str, natural_key: str, tracked_cols: List[str]):
        """Initialize SCD2 pipeline."""
        self.table_path = table_path  # Delta path.
        self.natural_key = natural_key  # Business key column.
        self.tracked_cols = tracked_cols  # Columns that trigger new version.
        self.audit = {}  # Track stats.
    
    def _hash_columns(self, df: DataFrame) -> DataFrame:
        """Add hash_diff column for change detection."""
        return df.withColumn(
            "_hash_diff",
            md5(concat_ws("|", *[col(c).cast("string") for c in self.tracked_cols]))  # Hash.
        )
    
    def initialize(self, df: DataFrame):
        """Create initial SCD2 table from source."""
        scd2_df = df.withColumn(
            "_valid_from", current_timestamp()  # Start now.
        ).withColumn(
            "_valid_to", lit("9999-12-31 23:59:59").cast("timestamp")  # Open.
        ).withColumn(
            "_is_current", lit(True)  # All current.
        ).withColumn(
            "_hash_diff", md5(concat_ws("|", *[col(c).cast("string") for c in self.tracked_cols]))
        )
        
        scd2_df.write.format("delta").mode("overwrite").save(self.table_path)  # Write.
        self.audit["initialized"] = df.count()  # Log.
        print(f"Initialized SCD2 table with {df.count()} records.")  # Info.
    
    def process_changes(self, source_df: DataFrame, effective_date=None):
        """Process incoming changes with full SCD2 logic."""
        if effective_date is None:  # Default.
            effective_date = current_timestamp()  # Now.
        else:
            effective_date = lit(effective_date).cast("timestamp")  # Cast.
        
        # Hash source.
        source_hashed = self._hash_columns(source_df)  # Add hash.
        
        # Load target.
        target_dt = DeltaTable.forPath(spark, self.table_path)  # Load.
        target_df = target_dt.toDF()  # DataFrame.
        target_current = target_df.filter(col("_is_current") == True)  # Current only.
        
        # Classify changes.
        joined = source_hashed.alias("s").join(
            target_current.alias("t"),
            col(f"s.{self.natural_key}") == col(f"t.{self.natural_key}"),
            "full_outer"  # Full comparison.
        )
        
        # Changed records (hash differs).
        changed = joined.filter(
            col("t._hash_diff").isNotNull() &  # Exists in target.
            col("s._hash_diff").isNotNull() &  # Exists in source.
            (col("s._hash_diff") != col("t._hash_diff"))  # Different.
        ).select("s.*")  # Source columns.
        
        # New records (not in target).
        new_records = joined.filter(
            col(f"t.{self.natural_key}").isNull()  # Not in target.
        ).select("s.*")  # Source columns.
        
        changed_count = changed.count()  # Count.
        new_count = new_records.count()  # Count.
        
        print(f"Changes detected: {changed_count} modified, {new_count} new")  # Info.
        
        if changed_count == 0 and new_count == 0:  # Nothing to do.
            print("No changes to process.")  # Info.
            return  # Exit.
        
        # Build staged inserts (new versions + new records).
        staged = changed.unionByName(new_records).select(
            *[col(c) for c in source_df.columns],  # Original columns.
            effective_date.alias("_valid_from"),  # Effective date.
            lit("9999-12-31 23:59:59").cast("timestamp").alias("_valid_to"),  # Open.
            lit(True).alias("_is_current"),  # Current.
            col("_hash_diff"),  # Hash.
        )
        
        # MERGE: close old + insert new.
        target_dt.alias("t").merge(
            staged.alias("s"),
            f"t.{self.natural_key} = s.{self.natural_key} AND t._is_current = true"
        ).whenMatchedUpdate(set={
            "_valid_to": effective_date,
            "_is_current": lit(False),
        }).whenNotMatchedInsertAll().execute()  # Execute.
        
        # Audit.
        self.audit["last_run"] = {"changed": changed_count, "new": new_count}  # Log.
        print(f"SCD2 merge complete. Closed {changed_count}, inserted {changed_count + new_count}.")  # Info.
    
    def get_current(self) -> DataFrame:
        """Get current dimension (latest version of each record)."""
        return spark.read.format("delta").load(self.table_path).filter(col("_is_current") == True)
    
    def get_history(self, key_value) -> DataFrame:
        """Get full history for a specific natural key value."""
        return (
            spark.read.format("delta").load(self.table_path)
            .filter(col(self.natural_key) == key_value)
            .orderBy("_valid_from")
        )

# Demo.
print("=== Production SCD2 Pipeline Demo ===")  # Heading.

# Initialize.
initial = spark.createDataFrame([
    (1, "Alice", "Engineering", "Seattle", 95000),
    (2, "Bob", "Marketing", "Portland", 85000),
    (3, "Carol", "Sales", "Denver", 90000),
], ["emp_id", "name", "department", "city", "salary"])  # Initial.

pipeline = SCD2Pipeline(
    table_path="/tmp/scd2_production_demo",
    natural_key="emp_id",
    tracked_cols=["department", "city", "salary"]
)  # Create pipeline.

pipeline.initialize(initial)  # Load initial.

# Process batch 1.
batch1 = spark.createDataFrame([
    (1, "Alice", "Data Science", "Denver", 110000),  # Dept+city+salary changed.
    (2, "Bob", "Marketing", "Portland", 85000),       # No change.
    (3, "Carol", "Sales", "Denver", 95000),            # Salary changed.
    (4, "Dave", "Engineering", "Austin", 70000),       # New.
], ["emp_id", "name", "department", "city", "salary"])  # Batch 1.

pipeline.process_changes(batch1, effective_date="2024-06-01 00:00:00")  # Process.

# Show results.
print("\n=== Current Dimension ===")  # Heading.
pipeline.get_current().drop("_hash_diff").show()  # Display.

print("=== Alice's History ===")  # Heading.
pipeline.get_history(1).drop("_hash_diff").show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Late-arriving dimensions
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Late-Arriving Dimensions
# ============================================================
# Real-world: Fact arrives before dimension record exists.

from pyspark.sql.functions import (
    col, lit, when, coalesce, current_timestamp, to_timestamp
)  # Imports.

print("=== Late-Arriving Dimension Handling ===")  # Heading.

# Scenario: Order fact references customer_id=99 that doesn't exist yet.
facts = spark.createDataFrame([
    ("F-001", 1, "2024-01-15", 100.00),  # Known customer.
    ("F-002", 2, "2024-01-16", 200.00),  # Known customer.
    ("F-003", 99, "2024-01-17", 150.00), # UNKNOWN customer (late arriving).
    ("F-004", 1, "2024-01-18", 75.00),   # Known customer.
], ["fact_id", "customer_id", "order_date", "amount"])  # Facts.

dimension = spark.createDataFrame([
    (1, "Alice", "Gold", "2020-01-01", "9999-12-31", True),
    (2, "Bob", "Silver", "2019-06-01", "9999-12-31", True),
], ["customer_id", "name", "tier", "valid_from", "valid_to", "is_current"])  # Dim.

print("Facts:")  # Heading.
facts.show()  # Display.
print("Dimension:")  # Heading.
dimension.show()  # Display.

# Strategy 1: Insert placeholder (inferred member).
print("=== Strategy 1: Inferred Member ===")  # Heading.

# Find missing dimension keys.
missing_keys = facts.select("customer_id").distinct().join(
    dimension.select("customer_id").distinct(), "customer_id", "left_anti"  # Not in dim.
)
print(f"Missing dimension keys: {missing_keys.collect()}")  # Show.

# Create placeholder records.
placeholders = missing_keys.select(
    col("customer_id"),  # Key.
    lit("UNKNOWN").alias("name"),  # Placeholder.
    lit("UNKNOWN").alias("tier"),  # Placeholder.
    lit("1900-01-01").alias("valid_from"),  # Placeholder date.
    lit("9999-12-31").alias("valid_to"),  # Open.
    lit(True).alias("is_current"),  # Current.
)

# Insert placeholders into dimension.
dim_with_placeholders = dimension.unionByName(placeholders)  # Add.
print("Dimension with placeholder:")  # Heading.
dim_with_placeholders.show()  # Display.

# Strategy 2: When actual data arrives, update placeholder.
print("\n=== Strategy 2: Update When Real Data Arrives ===")  # Heading.

late_arrival = spark.createDataFrame([
    (99, "Frank", "Bronze"),  # Real data for customer 99.
], ["customer_id", "name", "tier"])  # Late arrival.

print("Late-arriving dimension data:")  # Heading.
late_arrival.show()  # Display.

# Update placeholder with real data (SCD1 on placeholder, SCD2 if real change).
updated_dim = dim_with_placeholders.join(
    late_arrival, "customer_id", "left"  # Join.
).select(
    dim_with_placeholders["customer_id"],  # Keep.
    coalesce(late_arrival["name"], dim_with_placeholders["name"]).alias("name"),  # Update.
    coalesce(late_arrival["tier"], dim_with_placeholders["tier"]).alias("tier"),  # Update.
    dim_with_placeholders["valid_from"],  # Keep.
    dim_with_placeholders["valid_to"],  # Keep.
    dim_with_placeholders["is_current"],  # Keep.
)

print("After late-arriving update:")  # Heading.
updated_dim.show()  # Display.
print("Customer 99 updated from UNKNOWN to Frank/Bronze.")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Temporal query patterns
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Temporal Query Patterns
# ============================================================
# Real-world: Point-in-time joins and temporal analysis.

from pyspark.sql.functions import (
    col, lit, when, to_date, datediff, months_between,
    count, avg, sum as spark_sum, max as spark_max, min as spark_min
)  # Imports.

# SCD2 dimension with full history.
print("=== Temporal Query Patterns ===")  # Heading.

emp_history = spark.createDataFrame([
    (1, "Alice", "Engineering", "Seattle", 85000, "2020-01-01", "2021-06-30"),
    (1, "Alice", "Engineering", "Seattle", 95000, "2021-07-01", "2023-03-31"),
    (1, "Alice", "Data Science", "Denver", 110000, "2023-04-01", "9999-12-31"),
    (2, "Bob", "Marketing", "Portland", 80000, "2019-06-01", "2022-12-31"),
    (2, "Bob", "Marketing", "Portland", 90000, "2023-01-01", "9999-12-31"),
    (3, "Carol", "Sales", "Denver", 90000, "2021-03-15", "9999-12-31"),
], ["emp_id", "name", "department", "city", "salary", "valid_from", "valid_to"])  # History.

emp_history = emp_history.withColumn("valid_from", to_date("valid_from")).withColumn("valid_to", to_date("valid_to"))  # Cast.

print("Employee History (SCD2):")
emp_history.show(truncate=False)  # Display.

# Pattern 1: Point-in-time query.
print("=== Pattern 1: Point-in-Time (as of 2022-01-01) ===")  # Heading.
query_date = "2022-01-01"  # Lookup date.
as_of = emp_history.filter(
    (col("valid_from") <= query_date) & (col("valid_to") >= query_date)  # Active on date.
)
as_of.show()  # Display.

# Pattern 2: Temporal join (fact + dimension at transaction time).
print("=== Pattern 2: Temporal Join ===")  # Heading.
transactions = spark.createDataFrame([
    ("T1", 1, "2020-06-15", 500.0),   # Alice in Seattle/Eng.
    ("T2", 1, "2023-05-01", 800.0),   # Alice in Denver/DS.
    ("T3", 2, "2022-03-01", 300.0),   # Bob at $80K.
    ("T4", 2, "2023-06-01", 600.0),   # Bob at $90K.
], ["tx_id", "emp_id", "tx_date", "amount"])  # Facts.

transactions = transactions.withColumn("tx_date", to_date("tx_date"))  # Cast.

# Join: fact to dimension AT THE TIME of the transaction.
temporal_join = transactions.alias("f").join(
    emp_history.alias("d"),
    (col("f.emp_id") == col("d.emp_id")) &  # Key match.
    (col("f.tx_date") >= col("d.valid_from")) &  # After start.
    (col("f.tx_date") <= col("d.valid_to")),  # Before end.
    "inner"  # Inner join.
).select(
    col("f.tx_id"), col("f.tx_date"), col("f.amount"),
    col("d.name"), col("d.department"), col("d.city"), col("d.salary"),
)
temporal_join.show()  # Display.
print("Each transaction joined to the dimension state AT THAT TIME!")

# Pattern 3: Version duration analysis.
print("\n=== Pattern 3: Version Duration Analysis ===")  # Heading.
duration = emp_history.withColumn(
    "duration_days",
    when(col("valid_to") == to_date(lit("9999-12-31")),
         datediff(lit("2024-06-01").cast("date"), col("valid_from")))  # Active.
    .otherwise(datediff(col("valid_to"), col("valid_from")))  # Historical.
).withColumn(
    "version_num",
    row_number().over(Window.partitionBy("emp_id").orderBy("valid_from"))  # Version #.
)

from pyspark.sql.window import Window  # Import.
from pyspark.sql.functions import row_number  # Import.

duration = emp_history.withColumn(
    "duration_days",
    when(col("valid_to") == to_date(lit("9999-12-31")),
         datediff(lit("2024-06-01").cast("date"), col("valid_from")))
    .otherwise(datediff(col("valid_to"), col("valid_from")))
).withColumn(
    "version_num",
    row_number().over(Window.partitionBy("emp_id").orderBy("valid_from"))
)
duration.select("emp_id", "name", "department", "salary", "valid_from", "valid_to", "duration_days", "version_num").show()
print("Track how long each version lasted!")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Key Takeaways
# MAGIC %md
# MAGIC ## SECTION 6 — Key Takeaways
# MAGIC
# MAGIC ### SCD Type Selection
# MAGIC | Type | History | Complexity | Use When |
# MAGIC |---|---|---|---|
# MAGIC | Type 0 | None (immutable) | Trivial | Birth dates, creation timestamps |
# MAGIC | Type 1 | None (overwrite) | Low | Non-critical attributes |
# MAGIC | Type 2 | Full | Medium-High | Address, status, department |
# MAGIC | Type 3 | Previous only | Low | Need one previous value |
# MAGIC | Type 6 | Full + current | High | Need both history + quick current lookup |
# MAGIC
# MAGIC ### Delta MERGE Pattern for SCD2
# MAGIC 1. **Hash tracked columns** for efficient change detection
# MAGIC 2. **Classify** records: unchanged, changed, new, deleted
# MAGIC 3. **MERGE**: close old records (update valid_to, is_current)
# MAGIC 4. **Insert** new versions and new records
# MAGIC
# MAGIC ### Best Practices
# MAGIC 1. **Use md5/sha2 hashing** — comparing one hash vs. N columns
# MAGIC 2. **Always include valid_from/valid_to** — enables point-in-time queries
# MAGIC 3. **Add is_current flag** — fast filter for current state
# MAGIC 4. **Handle late-arriving data** — placeholder/inferred members
# MAGIC 5. **Track load metadata** — _loaded_at timestamp for debugging
# MAGIC
# MAGIC ### Common Pitfalls
# MAGIC * Forgetting to close old records before inserting new versions
# MAGIC * Not handling NULL hashes (NULL != NULL in comparisons)
# MAGIC * Missing edge case: delete + re-insert same key
# MAGIC * Performance: always filter `is_current = true` for current lookups

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Practice Exercises
# MAGIC %md
# MAGIC ## SECTION 7 — Practice Exercises
# MAGIC
# MAGIC ### Exercise 1: SCD Type 1
# MAGIC Given a product dimension and incoming updates, implement Type 1 overwrite for price and description columns.
# MAGIC
# MAGIC ### Exercise 2: SCD Type 2
# MAGIC Given customer data with address changes, implement Type 2 with valid_from, valid_to, and is_current columns.
# MAGIC
# MAGIC ### Exercise 3: Temporal Join
# MAGIC Join a fact table of sales to the SCD2 customer dimension, matching each sale to the customer's state at that point in time.
# MAGIC
# MAGIC ### Exercise 4: Change Detection
# MAGIC Implement hash-based change detection that classifies records as NEW, CHANGED, UNCHANGED, or DELETED.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Solutions
# ============================================================
# SECTION 7 — EXERCISE SOLUTIONS
# ============================================================

from pyspark.sql.functions import (
    col, lit, when, md5, concat_ws, to_date, coalesce, monotonically_increasing_id
)  # Imports.

# --- Exercise 1: SCD Type 1 (Overwrite) ---
print("=== Exercise 1: SCD Type 1 ===")  # Heading.
products = spark.createDataFrame([
    (1, "Widget", 10.00, "Basic widget"), (2, "Gadget", 25.00, "Standard gadget"),
], ["id", "name", "price", "description"])  # Target.

updates = spark.createDataFrame([
    (1, "Widget", 12.00, "Premium widget"),  # Price + desc changed.
    (3, "Doohickey", 5.00, "New item"),      # New product.
], ["id", "name", "price", "description"])  # Source.

# Type 1: upsert.
result = products.join(updates, "id", "left_anti").unionByName(updates)  # Overwrite.
result.orderBy("id").show()  # Display.

# --- Exercise 2: SCD Type 2 ---
print("=== Exercise 2: SCD Type 2 ===")  # Heading.
cust_dim = spark.createDataFrame([
    (1, "C1", "Alice", "Seattle", "2020-01-01", "9999-12-31", True),
], ["sk", "cust_id", "name", "city", "valid_from", "valid_to", "is_current"])  # SCD2.

# Alice moves to Denver.
closed = cust_dim.withColumn("valid_to", lit("2024-06-01")).withColumn("is_current", lit(False))  # Close.
new_row = spark.createDataFrame([
    (2, "C1", "Alice", "Denver", "2024-06-01", "9999-12-31", True),
], ["sk", "cust_id", "name", "city", "valid_from", "valid_to", "is_current"])  # New.
closed.unionByName(new_row).show()  # Display.

# --- Exercise 3: Temporal Join ---
print("=== Exercise 3: Temporal Join ===")  # Heading.
facts = spark.createDataFrame([
    ("S1", "C1", "2021-06-01", 100.0), ("S2", "C1", "2024-07-01", 200.0),
], ["sale_id", "cust_id", "sale_date", "amount"])  # Facts.
facts = facts.withColumn("sale_date", to_date("sale_date"))  # Cast.

dim_hist = spark.createDataFrame([
    ("C1", "Seattle", "2020-01-01", "2024-05-31"), ("C1", "Denver", "2024-06-01", "9999-12-31"),
], ["cust_id", "city", "valid_from", "valid_to"])  # Dim.
dim_hist = dim_hist.withColumn("valid_from", to_date("valid_from")).withColumn("valid_to", to_date("valid_to"))  # Cast.

facts.alias("f").join(dim_hist.alias("d"),
    (col("f.cust_id") == col("d.cust_id")) &
    (col("f.sale_date").between(col("d.valid_from"), col("d.valid_to")))
).select("sale_id", "f.cust_id", "sale_date", "amount", "city").show()  # Temporal join.

# --- Exercise 4: Change Detection ---
print("=== Exercise 4: Hash Change Detection ===")  # Heading.
target = spark.createDataFrame([(1, "A", 10), (2, "B", 20), (3, "C", 30)], ["id", "name", "val"])  # Target.
source = spark.createDataFrame([(1, "A", 10), (2, "B", 25), (4, "D", 40)], ["id", "name", "val"])  # Source.

t_h = target.withColumn("h", md5(concat_ws("|", *[col(c).cast("string") for c in ["name", "val"]])))  # Hash.
s_h = source.withColumn("h", md5(concat_ws("|", *[col(c).cast("string") for c in ["name", "val"]])))  # Hash.

s_h.alias("s").join(t_h.alias("t"), "id", "full_outer").select(
    coalesce(col("s.id"), col("t.id")).alias("id"),
    when(col("t.id").isNull(), "NEW")
    .when(col("s.id").isNull(), "DELETED")
    .when(col("s.h") != col("t.h"), "CHANGED")
    .otherwise("UNCHANGED").alias("status")
).show()  # Classification.

print("All exercises completed!")