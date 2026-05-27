# Databricks notebook source
# DBTITLE 1,Section 1 - What Is This
# MAGIC %md
# MAGIC # Notebook 55: Delta CRUD — Insert, Update, Delete, Merge
# MAGIC ## Module 09: Delta Lake Deep Dive
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC CRUD stands for **Create, Read, Update, Delete** — the four basic operations you can do with any data.
# MAGIC
# MAGIC With regular Parquet files, you can only Create (write) and Read. You CANNOT update a single row or delete a single row — you have to rewrite the entire file!
# MAGIC
# MAGIC **Delta Lake gives you full CRUD** — just like a real database, but on your data lake.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine a **library catalog system**:
# MAGIC - **INSERT** = Adding a new book card to the catalog
# MAGIC - **UPDATE** = Changing the location of a book (moved from shelf A to shelf B)
# MAGIC - **DELETE** = Removing a book card when the book is permanently gone
# MAGIC - **MERGE (UPSERT)** = A delivery truck arrives with books — some are new (insert them), some are updated editions (update them), some are recalled (delete them) — all handled in ONE operation
# MAGIC
# MAGIC Without Delta (plain Parquet), you'd have to rewrite the ENTIRE catalog every time you change one card. Delta makes it surgical and efficient.

# COMMAND ----------

# DBTITLE 1,Section 2 - How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Text Diagram: Delta CRUD Operations
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                    Delta Table (on disk)                         │
# MAGIC │                                                                 │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐                     │
# MAGIC │  │ file1.pq │  │ file2.pq │  │ file3.pq │  ← Parquet files    │
# MAGIC │  └──────────┘  └──────────┘  └──────────┘                     │
# MAGIC │                                                                 │
# MAGIC │  ┌─────────────────────────────────────────┐                   │
# MAGIC │  │ _delta_log/                             │                   │
# MAGIC │  │   000.json ← CREATE                    │                   │
# MAGIC │  │   001.json ← INSERT (add file4.pq)     │                   │
# MAGIC │  │   002.json ← UPDATE (remove file1,     │                   │
# MAGIC │  │              add file1_new.pq)          │                   │
# MAGIC │  │   003.json ← DELETE (remove file2)     │                   │
# MAGIC │  │   004.json ← MERGE (combo of above)    │                   │
# MAGIC │  └─────────────────────────────────────────┘                   │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### How Each Operation Works Internally:
# MAGIC
# MAGIC | Operation | What Happens on Disk | Transaction Log Entry |
# MAGIC |-----------|---------------------|----------------------|
# MAGIC | INSERT | New parquet file(s) added | `add` action |
# MAGIC | UPDATE | Old file removed, new file with changes added | `remove` + `add` |
# MAGIC | DELETE | Old file removed, new file without deleted rows added | `remove` + `add` |
# MAGIC | MERGE | Combination of insert/update/delete in ONE atomic commit | Multiple `add`/`remove` |
# MAGIC
# MAGIC ### Key Concept: Copy-on-Write
# MAGIC Delta doesn't modify files in-place. It writes NEW files and marks old ones as removed. This is why time travel works — the old files are still there!

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 1: INSERT
# SECTION 3 — BEGINNER EXAMPLE 1: INSERT Operations
# Real-world: Adding new records to a Delta table.

from pyspark.sql.functions import col, lit, current_timestamp  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== INSERT Operations ===")  # Heading.

# Setup: Create a base Delta table.
employees = spark.createDataFrame([
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", "Marketing", 72000),
    (3, "Carol", "Engineering", 88000),
], ["emp_id", "name", "department", "salary"])  # Initial data.

base_path = "/tmp/delta_kt/crud_employees"  # Storage path.
employees.write.format("delta").mode("overwrite").save(base_path)  # Create table.
print("Initial table:")  # Heading.
display(spark.read.format("delta").load(base_path))  # Show 3 rows.

# INSERT Method 1: Append mode (most common).
print("\n--- INSERT Method 1: Append Mode ---")  # Heading.
new_employees = spark.createDataFrame([
    (4, "David", "Sales", 68000),
    (5, "Eve", "Marketing", 75000),
], ["emp_id", "name", "department", "salary"])  # New data.
new_employees.write.format("delta").mode("append").save(base_path)  # Append.
print(f"After append: {spark.read.format('delta').load(base_path).count()} rows")  # 5 rows.

# INSERT Method 2: SQL INSERT INTO.
print("\n--- INSERT Method 2: SQL INSERT INTO ---")  # Heading.
spark.read.format("delta").load(base_path).createOrReplaceTempView("emp_view")  # Temp view.
# Note: For SQL INSERT, we need a registered table. Let's use saveAsTable.
spark.sql("DROP TABLE IF EXISTS delta_crud_emp")  # Clean.
employees_full = spark.read.format("delta").load(base_path)  # Read current.
employees_full.write.format("delta").mode("overwrite").saveAsTable("delta_crud_emp")  # Register.
spark.sql("INSERT INTO delta_crud_emp VALUES (6, 'Frank', 'Engineering', 91000)")  # SQL insert.
print(f"After SQL INSERT: {spark.table('delta_crud_emp').count()} rows")  # 6 rows.
display(spark.table("delta_crud_emp").orderBy("emp_id"))  # Show all.

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 2: UPDATE
# SECTION 3 — BEGINNER EXAMPLE 2: UPDATE Operations
# Real-world: Giving an employee a raise or changing their department.

from pyspark.sql.functions import col, lit, expr  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== UPDATE Operations ===")  # Heading.

# Load existing Delta table.
update_path = "/tmp/delta_kt/crud_employees"  # Path.
dt = DeltaTable.forPath(spark, update_path)  # Load as DeltaTable object.

print("Before updates:")  # Heading.
display(spark.read.format("delta").load(update_path).orderBy("emp_id"))  # Show.

# UPDATE Method 1: Single column, single row.
print("\n--- UPDATE 1: Give Alice a raise ---")  # Heading.
dt.update(
    condition="emp_id = 1",  # WHERE clause: which rows to update.
    set={"salary": lit(105000)}  # SET clause: new value.
)  # Execute.
print("Alice's new salary:")
display(spark.read.format("delta").load(update_path).filter("emp_id = 1"))  # Show.

# UPDATE Method 2: Multiple columns.
print("\n--- UPDATE 2: Bob moves to Sales with raise ---")  # Heading.
dt.update(
    condition="emp_id = 2",  # Find Bob.
    set={
        "department": lit("Sales"),  # New department.
        "salary": lit(80000)  # New salary.
    }
)  # Execute.
print("Bob after transfer:")
display(spark.read.format("delta").load(update_path).filter("emp_id = 2"))  # Show.

# UPDATE Method 3: Expression-based update (10% raise for Engineering).
print("\n--- UPDATE 3: 10% raise for all Engineering ---")  # Heading.
dt.update(
    condition="department = 'Engineering'",  # All engineers.
    set={"salary": expr("salary * 1.10")}  # 10% increase using expression.
)  # Execute.
print("Engineering after raise:")
display(spark.read.format("delta").load(update_path).filter("department = 'Engineering'").orderBy("emp_id"))  # Show.

# UPDATE Method 4: SQL UPDATE.
print("\n--- UPDATE 4: SQL UPDATE ---")  # Heading.
spark.sql("UPDATE delta_crud_emp SET salary = salary + 5000 WHERE department = 'Marketing'")  # SQL.
print("Marketing after SQL update:")
display(spark.sql("SELECT * FROM delta_crud_emp WHERE department = 'Marketing'"))  # Show.

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 3: DELETE
# SECTION 3 — BEGINNER EXAMPLE 3: DELETE Operations
# Real-world: Removing employees who left the company.

from pyspark.sql.functions import col, lit  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== DELETE Operations ===")  # Heading.

# Load Delta table.
delete_path = "/tmp/delta_kt/crud_employees"  # Path.
dt = DeltaTable.forPath(spark, delete_path)  # Load.

print("Before delete:")  # Heading.
display(spark.read.format("delta").load(delete_path).orderBy("emp_id"))  # Show all.
print(f"Row count: {spark.read.format('delta').load(delete_path).count()}")  # Count.

# DELETE Method 1: Delete by condition.
print("\n--- DELETE 1: Remove emp_id = 4 (David left) ---")  # Heading.
dt.delete("emp_id = 4")  # Delete David.
print(f"After delete: {spark.read.format('delta').load(delete_path).count()} rows")  # Count.

# DELETE Method 2: Delete with column expression.
print("\n--- DELETE 2: Remove anyone with salary < 75000 ---")  # Heading.
dt.delete(col("salary") < 75000)  # Delete low salary.
print(f"After salary filter delete: {spark.read.format('delta').load(delete_path).count()} rows")  # Count.
display(spark.read.format("delta").load(delete_path).orderBy("emp_id"))  # Show remaining.

# DELETE Method 3: SQL DELETE.
print("\n--- DELETE 3: SQL DELETE ---")  # Heading.
spark.sql("DELETE FROM delta_crud_emp WHERE emp_id = 6")  # SQL delete.
print("After SQL DELETE:")
display(spark.sql("SELECT * FROM delta_crud_emp ORDER BY emp_id"))  # Show.

# Verify with history — every delete is recorded!
print("\n--- Delete History ---")  # Heading.
dt.history().select("version", "operation", "operationMetrics").show(truncate=False)  # Audit.

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Example 1: MERGE Basic
# SECTION 4 — INTERMEDIATE EXAMPLE 1: MERGE (UPSERT) Basics
# Real-world: A daily feed arrives — insert new records, update existing ones.

from pyspark.sql.functions import col, lit, current_timestamp  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== MERGE (UPSERT) — The Most Powerful Operation ===")  # Heading.

# Setup: Create a clean target table.
target_data = spark.createDataFrame([
    (1, "Alice", "Engineering", 95000, "active"),
    (2, "Bob", "Marketing", 72000, "active"),
    (3, "Carol", "Engineering", 88000, "active"),
    (4, "David", "Sales", 68000, "active"),
], ["emp_id", "name", "department", "salary", "status"])  # Target.

merge_path = "/tmp/delta_kt/crud_merge"  # Path.
target_data.write.format("delta").mode("overwrite").save(merge_path)  # Write target.
print("TARGET table (existing data):")
display(spark.read.format("delta").load(merge_path))  # Show.

# Source: incoming daily feed.
source_data = spark.createDataFrame([
    (2, "Bob", "Sales", 80000, "active"),       # UPDATE: Bob moved to Sales, got raise.
    (3, "Carol", "Engineering", 92000, "active"), # UPDATE: Carol got raise.
    (5, "Eve", "Marketing", 75000, "active"),    # INSERT: New employee.
    (6, "Frank", "Engineering", 91000, "active"), # INSERT: New employee.
], ["emp_id", "name", "department", "salary", "status"])  # Source.
print("\nSOURCE data (incoming feed):")
display(source_data)  # Show.

# MERGE: Update existing + Insert new.
print("\n--- Executing MERGE ---")  # Heading.
dt = DeltaTable.forPath(spark, merge_path)  # Load target.

dt.alias("target").merge(
    source_data.alias("source"),  # Source DataFrame.
    "target.emp_id = source.emp_id"  # Match condition (like a JOIN key).
).whenMatchedUpdateAll(  # If emp_id exists in both → UPDATE all columns.
).whenNotMatchedInsertAll(  # If emp_id only in source → INSERT.
).execute()  # Run the merge.

print("\nAfter MERGE:")
display(spark.read.format("delta").load(merge_path).orderBy("emp_id"))  # Show result.
print("Bob updated (Sales, 80K), Carol updated (92K), Eve & Frank inserted!")

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Example 2: MERGE with Conditions
# SECTION 4 — INTERMEDIATE EXAMPLE 2: MERGE with Specific Conditions
# Real-world: Only update if salary increased, delete if status='terminated'.

from pyspark.sql.functions import col, lit, current_timestamp  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== MERGE with Conditional Logic ===")  # Heading.

# Setup target.
target2 = spark.createDataFrame([
    (1, "Alice", 95000, "active"),
    (2, "Bob", 80000, "active"),
    (3, "Carol", 92000, "active"),
    (4, "David", 68000, "active"),
], ["emp_id", "name", "salary", "status"])  # Target.

merge2_path = "/tmp/delta_kt/crud_merge2"  # Path.
target2.write.format("delta").mode("overwrite").save(merge2_path)  # Write.

# Source with mixed actions.
source2 = spark.createDataFrame([
    (1, "Alice", 90000, "active"),      # Lower salary — should NOT update (only update if higher).
    (2, "Bob", 85000, "active"),        # Higher salary — SHOULD update.
    (3, "Carol", 92000, "terminated"),   # Terminated — SHOULD delete.
    (5, "Eve", 75000, "active"),         # New — SHOULD insert.
], ["emp_id", "name", "salary", "status"])  # Source.

print("Source data:")
display(source2)

# MERGE with conditional logic.
dt2 = DeltaTable.forPath(spark, merge2_path)  # Load.

dt2.alias("t").merge(
    source2.alias("s"),
    "t.emp_id = s.emp_id"  # Match on emp_id.
).whenMatchedUpdate(  # When matched AND salary is higher → update.
    condition="s.salary > t.salary AND s.status = 'active'",
    set={"salary": col("s.salary")}  # Only update salary.
).whenMatchedDelete(  # When matched AND terminated → delete.
    condition="s.status = 'terminated'"
).whenNotMatchedInsert(  # When not matched AND active → insert.
    condition="s.status = 'active'",
    values={
        "emp_id": col("s.emp_id"),
        "name": col("s.name"),
        "salary": col("s.salary"),
        "status": col("s.status")
    }
).execute()  # Run.

print("\nAfter conditional MERGE:")
display(spark.read.format("delta").load(merge2_path).orderBy("emp_id"))  # Show.
print("""Results:
  Alice: salary NOT updated (90K < 95K)
  Bob: salary updated to 85K (higher than 80K)
  Carol: DELETED (terminated)
  David: unchanged (not in source)
  Eve: INSERTED (new active employee)""")

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Example 3: MERGE for SCD Type 1
# SECTION 4 — INTERMEDIATE EXAMPLE 3: MERGE for SCD Type 1 (Overwrite History)
# Real-world: Customer data arrives daily — always keep the latest version.

from pyspark.sql.functions import col, lit, current_timestamp  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== SCD Type 1 with MERGE (Overwrite) ===")  # Heading.
print("SCD Type 1: Always overwrite with the latest data. No history kept.")

# Target: Customer dimension.
customers = spark.createDataFrame([
    (101, "Alice Smith", "alice@old.com", "New York", "2023-01-15"),
    (102, "Bob Jones", "bob@work.com", "Chicago", "2023-03-20"),
    (103, "Carol White", "carol@mail.com", "Denver", "2023-06-10"),
], ["customer_id", "name", "email", "city", "last_updated"])  # Customers.

scd1_path = "/tmp/delta_kt/crud_scd1"  # Path.
customers.write.format("delta").mode("overwrite").save(scd1_path)  # Write.
print("Initial customer table:")
display(spark.read.format("delta").load(scd1_path))  # Show.

# Daily update: some customers changed info.
daily_update = spark.createDataFrame([
    (101, "Alice Smith", "alice@new.com", "Boston", "2024-01-20"),  # Moved + new email.
    (102, "Bob Jones", "bob@work.com", "Chicago", "2024-01-20"),    # No change.
    (104, "David Brown", "david@mail.com", "Miami", "2024-01-20"),  # New customer.
], ["customer_id", "name", "email", "city", "last_updated"])  # Updates.
print("\nDaily update data:")
display(daily_update)

# SCD Type 1 MERGE: overwrite on match, insert if new.
dt_scd1 = DeltaTable.forPath(spark, scd1_path)  # Load.

dt_scd1.alias("target").merge(
    daily_update.alias("source"),
    "target.customer_id = source.customer_id"  # Match key.
).whenMatchedUpdateAll(  # Overwrite all columns with latest.
).whenNotMatchedInsertAll(  # Insert new customers.
).execute()  # Run.

print("\nAfter SCD Type 1 MERGE:")
display(spark.read.format("delta").load(scd1_path).orderBy("customer_id"))  # Show.
print("""Results:
  Alice: overwritten (Boston, new email) — old data GONE
  Bob: overwritten (same data, last_updated changed)
  Carol: unchanged (not in source)
  David: inserted (new customer)""")

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Example 1: SCD Type 2
# SECTION 5 — ADVANCED EXAMPLE 1: SCD Type 2 with MERGE
# Real-world: Keep full history of customer changes for compliance/analytics.

from pyspark.sql.functions import col, lit, current_date, to_date, when  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== SCD Type 2 with MERGE (Full History) ===")  # Heading.
print("SCD Type 2: Keep ALL versions. Old rows get end_date, new row becomes current.\n")

# Target: Customer dimension with SCD2 columns.
scd2_customers = spark.createDataFrame([
    (1, 101, "Alice Smith", "New York", "alice@old.com", "2023-01-01", "9999-12-31", True),
    (2, 102, "Bob Jones", "Chicago", "bob@work.com", "2023-03-01", "9999-12-31", True),
    (3, 103, "Carol White", "Denver", "carol@mail.com", "2023-06-01", "9999-12-31", True),
], ["surrogate_key", "customer_id", "name", "city", "email", "effective_start", "effective_end", "is_current"])

scd2_path = "/tmp/delta_kt/crud_scd2"  # Path.
scd2_customers.write.format("delta").mode("overwrite").save(scd2_path)  # Write.
print("Initial SCD2 table:")
display(spark.read.format("delta").load(scd2_path))  # Show.

# Incoming changes.
changes = spark.createDataFrame([
    (101, "Alice Smith", "Boston", "alice@new.com"),   # Alice moved.
    (104, "David Brown", "Miami", "david@mail.com"),   # New customer.
], ["customer_id", "name", "city", "email"])  # Changes.

print("\nIncoming changes:")
display(changes)

# Step 1: Find which records are actual changes (not just same data resent).
print("\n--- Step 1: Identify actual changes ---")
current_records = spark.read.format("delta").load(scd2_path).filter("is_current = true")  # Current.
actual_changes = changes.alias("s").join(
    current_records.alias("t"),
    "customer_id",
    "left"
).filter(
    (col("t.customer_id").isNull()) |  # New customer.
    (col("s.city") != col("t.city")) |  # City changed.
    (col("s.email") != col("t.email"))  # Email changed.
).select("s.*")  # Only source columns.
print(f"Actual changes to process: {actual_changes.count()}")

# Step 2: Prepare rows to insert (new versions).
from pyspark.sql.functions import monotonically_increasing_id  # Import.
max_key = spark.read.format("delta").load(scd2_path).agg({"surrogate_key": "max"}).collect()[0][0]  # Max key.
new_rows = actual_changes.withColumn("surrogate_key", monotonically_increasing_id() + max_key + 1) \
    .withColumn("effective_start", lit("2024-01-20")) \
    .withColumn("effective_end", lit("9999-12-31")) \
    .withColumn("is_current", lit(True))  # New current rows.

# Step 3: MERGE — close old records + insert new records.
print("\n--- Step 2: Execute SCD2 MERGE ---")
dt_scd2 = DeltaTable.forPath(spark, scd2_path)  # Load.

# Close existing current records for changed customers.
dt_scd2.alias("t").merge(
    actual_changes.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true"  # Match current row.
).whenMatchedUpdate(  # Close the old record.
    set={
        "effective_end": lit("2024-01-19"),  # End yesterday.
        "is_current": lit(False)  # No longer current.
    }
).execute()  # Close old.

# Insert new version rows.
new_rows.select("surrogate_key", "customer_id", "name", "city", "email", "effective_start", "effective_end", "is_current") \
    .write.format("delta").mode("append").save(scd2_path)  # Insert new.

print("\nAfter SCD Type 2 MERGE:")
display(spark.read.format("delta").load(scd2_path).orderBy("customer_id", "effective_start"))  # Show.
print("""Results:
  Alice: OLD record closed (end=2024-01-19, is_current=false)
         NEW record added (Boston, new email, is_current=true)
  David: NEW record inserted (is_current=true)
  Bob, Carol: unchanged""")

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Example 2: Production MERGE Pipeline
# SECTION 5 — ADVANCED EXAMPLE 2: Production-Grade MERGE Pipeline
# Real-world: Idempotent ETL pipeline handling deduplication, audit, error handling.

from pyspark.sql.functions import col, lit, current_timestamp, count, sum as spark_sum  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== Production MERGE Pipeline ===")  # Heading.

# Setup: Target orders table.
orders = spark.createDataFrame([
    ("ORD001", 101, 250.00, "2024-01-10", "shipped", "2024-01-10 08:00:00"),
    ("ORD002", 102, 180.50, "2024-01-11", "delivered", "2024-01-12 14:30:00"),
    ("ORD003", 101, 99.99, "2024-01-12", "processing", "2024-01-12 09:00:00"),
], ["order_id", "customer_id", "amount", "order_date", "status", "last_modified"])  # Target.

prod_path = "/tmp/delta_kt/crud_prod_orders"  # Path.
orders.write.format("delta").mode("overwrite").save(prod_path)  # Write.

# Incoming batch (may have duplicates, updates, new orders).
incoming = spark.createDataFrame([
    ("ORD002", 102, 180.50, "2024-01-11", "delivered", "2024-01-12 14:30:00"),  # Duplicate, no change.
    ("ORD003", 101, 99.99, "2024-01-12", "shipped", "2024-01-13 10:00:00"),    # Updated status.
    ("ORD003", 101, 99.99, "2024-01-12", "shipped", "2024-01-13 10:00:00"),    # Duplicate in source!
    ("ORD004", 103, 320.00, "2024-01-13", "processing", "2024-01-13 08:00:00"), # New order.
    ("ORD005", 101, 45.00, "2024-01-13", "processing", "2024-01-13 09:00:00"),  # New order.
], ["order_id", "customer_id", "amount", "order_date", "status", "last_modified"])  # Incoming.

print("Incoming batch (with duplicates):")
display(incoming)

# Step 1: Deduplicate source — keep latest by last_modified.
print("\n--- Step 1: Deduplicate Source ---")
from pyspark.sql.window import Window  # Import.
from pyspark.sql.functions import row_number  # Import.

window_spec = Window.partitionBy("order_id").orderBy(col("last_modified").desc())  # Latest first.
deduped = incoming.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")  # Keep latest.
print(f"After dedup: {deduped.count()} unique orders (from {incoming.count()} raw rows)")

# Step 2: Execute MERGE with update-only-if-newer logic.
print("\n--- Step 2: MERGE (update only if source is newer) ---")
dt_prod = DeltaTable.forPath(spark, prod_path)  # Load target.

merge_result = dt_prod.alias("t").merge(
    deduped.alias("s"),
    "t.order_id = s.order_id"  # Match on order_id.
).whenMatchedUpdate(  # Update only if source has newer timestamp.
    condition="s.last_modified > t.last_modified",
    set={
        "status": col("s.status"),
        "last_modified": col("s.last_modified")
    }
).whenNotMatchedInsertAll(  # Insert new orders.
).execute()  # Run.

# Step 3: Verify and audit.
print("\n--- Step 3: Results ---")
result = spark.read.format("delta").load(prod_path).orderBy("order_id")  # Read.
display(result)  # Show.

# Audit metrics from history.
print("\n--- Merge Metrics ---")
history = DeltaTable.forPath(spark, prod_path).history(1).select("operationMetrics").collect()[0][0]  # Metrics.
print(f"  Rows updated: {history.get('numTargetRowsUpdated', 0)}")  # Updated.
print(f"  Rows inserted: {history.get('numTargetRowsInserted', 0)}")  # Inserted.
print(f"  Rows unchanged: {history.get('numTargetRowsMatchedNoAction', 0) if 'numTargetRowsMatchedNoAction' in history else 'N/A'}")  # Unchanged.

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Example 3: MERGE with Delete and NotMatchedBySource
# SECTION 5 — ADVANCED EXAMPLE 3: Full MERGE with whenNotMatchedBySource
# Real-world: Sync a dimension table — insert new, update changed, delete removed.

from pyspark.sql.functions import col, lit, current_timestamp  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== Full Sync MERGE (Insert + Update + Delete) ===")  # Heading.
print("Pattern: Source is the 'truth'. Target must match source exactly.\n")

# Target: Current product catalog.
products = spark.createDataFrame([
    ("P001", "Widget", 29.99, "active"),
    ("P002", "Gadget", 49.99, "active"),
    ("P003", "Doohickey", 9.99, "active"),  # Will be discontinued.
    ("P004", "Thingamajig", 19.99, "active"),  # Will be discontinued.
], ["product_id", "name", "price", "status"])  # Target products.

sync_path = "/tmp/delta_kt/crud_sync"  # Path.
products.write.format("delta").mode("overwrite").save(sync_path)  # Write.
print("Current catalog (TARGET):")
display(spark.read.format("delta").load(sync_path))

# Source: Updated product catalog from ERP.
new_catalog = spark.createDataFrame([
    ("P001", "Widget Pro", 34.99, "active"),    # Updated name and price.
    ("P002", "Gadget", 49.99, "active"),        # No change.
    ("P005", "Contraption", 59.99, "active"),   # Brand new product.
], ["product_id", "name", "price", "status"])  # Source (truth).
print("\nUpdated catalog from ERP (SOURCE/truth):")
display(new_catalog)

# Full sync: match source exactly.
print("\n--- Executing Full Sync MERGE ---")
dt_sync = DeltaTable.forPath(spark, sync_path)  # Load.

dt_sync.alias("target").merge(
    new_catalog.alias("source"),
    "target.product_id = source.product_id"  # Match key.
).whenMatchedUpdate(  # Existing products → update.
    set={
        "name": col("source.name"),
        "price": col("source.price"),
        "status": col("source.status")
    }
).whenNotMatchedInsertAll(  # New in source → insert.
).whenNotMatchedBySourceUpdate(  # In target but NOT in source → mark discontinued.
    set={"status": lit("discontinued")}
).execute()  # Run.

print("\nAfter Full Sync MERGE:")
display(spark.read.format("delta").load(sync_path).orderBy("product_id"))  # Show.
print("""Results:
  P001: Updated (Widget Pro, $34.99)
  P002: Unchanged (already matches)
  P003: Discontinued (not in source anymore)
  P004: Discontinued (not in source anymore)
  P005: Inserted (new product)
""")

# Show the complete history.
print("--- Complete Operation History ---")
DeltaTable.forPath(spark, sync_path).history().select("version", "operation", "operationMetrics").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Section 6 - Key Takeaways
# MAGIC %md
# MAGIC ## SECTION 6 — Key Takeaways
# MAGIC
# MAGIC ### CRUD Operations Summary
# MAGIC
# MAGIC | Operation | Python API | SQL | Use Case |
# MAGIC |-----------|-----------|-----|----------|
# MAGIC | INSERT | `df.write.mode("append")` | `INSERT INTO` | Adding new records |
# MAGIC | UPDATE | `dt.update(condition, set)` | `UPDATE ... SET ... WHERE` | Modifying existing records |
# MAGIC | DELETE | `dt.delete(condition)` | `DELETE FROM ... WHERE` | Removing records |
# MAGIC | MERGE | `dt.merge(source, condition).when...` | `MERGE INTO ... USING ...` | All-in-one upsert |
# MAGIC
# MAGIC ### MERGE Clauses
# MAGIC * `whenMatchedUpdateAll()` — Update all columns when keys match
# MAGIC * `whenMatchedUpdate(condition, set)` — Conditional update with specific columns
# MAGIC * `whenMatchedDelete(condition)` — Delete matched rows meeting a condition
# MAGIC * `whenNotMatchedInsertAll()` — Insert all unmatched source rows
# MAGIC * `whenNotMatchedInsert(condition, values)` — Conditional insert with specific columns
# MAGIC * `whenNotMatchedBySourceUpdate(condition, set)` — Handle rows in target not in source
# MAGIC * `whenNotMatchedBySourceDelete(condition)` — Delete target rows not in source
# MAGIC
# MAGIC ### Best Practices
# MAGIC 1. **Always deduplicate source** before MERGE to avoid ambiguous matches
# MAGIC 2. **Use condition in whenMatched** to avoid unnecessary rewrites
# MAGIC 3. **MERGE is atomic** — all changes succeed or none do
# MAGIC 4. **Check history** after MERGE for audit and verification
# MAGIC 5. **Use `whenNotMatchedBySource`** for full sync scenarios (Spark 3.4+)

# COMMAND ----------

# DBTITLE 1,Section 7 - Practice Exercises and Solutions
# SECTION 7 — HOMEWORK & SOLUTIONS

from pyspark.sql.functions import col, lit, expr, current_timestamp  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("="*60)
print("HOMEWORK — Delta CRUD Operations")
print("="*60)

# --- Level 1: Just run it ---
print("\n=== Level 1: Create a Delta table and INSERT 3 rows ===")
l1_data = spark.createDataFrame([(1,"A",10),(2,"B",20),(3,"C",30)], ["id","name","value"])
l1_path = "/tmp/delta_kt/hw55_l1"
l1_data.write.format("delta").mode("overwrite").save(l1_path)  # Create.
spark.createDataFrame([(4,"D",40)], ["id","name","value"]).write.format("delta").mode("append").save(l1_path)  # Insert.
print(f"Count: {spark.read.format('delta').load(l1_path).count()}")  # 4 rows.

# --- Level 2: Tiny change ---
print("\n=== Level 2: UPDATE row where id=2, set value=200 ===")
dt_l2 = DeltaTable.forPath(spark, l1_path)
dt_l2.update("id = 2", {"value": lit(200)})  # Update.
display(spark.read.format("delta").load(l1_path).orderBy("id"))  # Show.

# --- Level 3: Combine two things ---
print("\n=== Level 3: DELETE + verify with history ===")
dt_l2.delete("id = 3")  # Delete.
print(f"After delete: {spark.read.format('delta').load(l1_path).count()} rows")
dt_l2.history().select("version", "operation").show()  # History.

# --- Level 4: New scenario ---
print("\n=== Level 4: MERGE two DataFrames ===")
target = spark.createDataFrame([(1,"X",100),(2,"Y",200)], ["id","code","qty"])
l4_path = "/tmp/delta_kt/hw55_l4"
target.write.format("delta").mode("overwrite").save(l4_path)
source = spark.createDataFrame([(2,"Y",250),(3,"Z",300)], ["id","code","qty"])
DeltaTable.forPath(spark, l4_path).alias("t").merge(
    source.alias("s"), "t.id = s.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
display(spark.read.format("delta").load(l4_path).orderBy("id"))  # id=2 updated, id=3 inserted.

# --- Level 5: Mini project ---
print("\n=== Level 5: Inventory management with MERGE ===")
inventory = spark.createDataFrame([
    ("SKU001", "Widget", 100, 29.99),
    ("SKU002", "Gadget", 50, 49.99),
    ("SKU003", "Thing", 200, 9.99),
], ["sku", "name", "stock", "price"])
l5_path = "/tmp/delta_kt/hw55_l5"
inventory.write.format("delta").mode("overwrite").save(l5_path)
shipment = spark.createDataFrame([
    ("SKU001", "Widget", 50, 29.99),   # Restock.
    ("SKU002", "Gadget", 30, 54.99),   # Restock + price change.
    ("SKU004", "Doohickey", 75, 14.99), # New product.
], ["sku", "name", "stock", "price"])
DeltaTable.forPath(spark, l5_path).alias("t").merge(
    shipment.alias("s"), "t.sku = s.sku"
).whenMatchedUpdate(set={"stock": expr("t.stock + s.stock"), "price": col("s.price")}  # Add stock.
).whenNotMatchedInsertAll().execute()
display(spark.read.format("delta").load(l5_path).orderBy("sku"))

print("\n" + "="*60)
print("All exercises completed!")
print("="*60)