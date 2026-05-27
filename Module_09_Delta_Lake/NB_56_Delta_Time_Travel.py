# Databricks notebook source
# DBTITLE 1,Section 1 - What Is This
# MAGIC %md
# MAGIC # Notebook 56: Delta Time Travel
# MAGIC ## Module 09: Delta Lake Deep Dive
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Time Travel lets you **read your data as it was at any point in the past**. Every change you make to a Delta table is recorded, and you can go back to any previous version.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Think of **Google Docs version history**:
# MAGIC - Every time you edit a document, Google saves a snapshot
# MAGIC - You can click "Version history" and see every change ever made
# MAGIC - You can restore to any previous version instantly
# MAGIC - You never truly lose anything
# MAGIC
# MAGIC Delta Time Travel works the same way but for your data tables — every INSERT, UPDATE, DELETE, and MERGE creates a new version, and you can read or restore ANY previous version.
# MAGIC
# MAGIC ### When You Need Time Travel:
# MAGIC 1. **Accidental DELETE** — "I deleted the wrong rows! Can we undo?"
# MAGIC 2. **Audit** — "What did the data look like last Tuesday?"
# MAGIC 3. **Debugging** — "The report was correct yesterday but wrong today — what changed?"
# MAGIC 4. **Reproducibility** — "Re-run my ML model on last week's data"
# MAGIC 5. **Regulatory** — "Show me the state of customer data on Dec 31st for compliance"

# COMMAND ----------

# DBTITLE 1,Section 2 - How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Timeline of a Delta Table:
# MAGIC
# MAGIC   Version 0       Version 1       Version 2       Version 3
# MAGIC   (CREATE)        (INSERT)        (UPDATE)        (DELETE)
# MAGIC     │               │               │               │
# MAGIC     ▼               ▼               ▼               ▼
# MAGIC ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐
# MAGIC │ 3 rows  │   │ 5 rows  │   │ 5 rows  │   │ 4 rows  │
# MAGIC │         │   │ +2 new  │   │ 1 changed│   │ -1 row  │
# MAGIC └─────────┘   └─────────┘   └─────────┘   └─────────┘
# MAGIC
# MAGIC You can read ANY version:
# MAGIC   spark.read.option("versionAsOf", 0)  → 3 rows (original)
# MAGIC   spark.read.option("versionAsOf", 1)  → 5 rows (after insert)
# MAGIC   spark.read.option("versionAsOf", 2)  → 5 rows (after update)
# MAGIC   spark.read.option("versionAsOf", 3)  → 4 rows (current)
# MAGIC
# MAGIC Or by timestamp:
# MAGIC   spark.read.option("timestampAsOf", "2024-01-15 10:00:00")
# MAGIC ```
# MAGIC
# MAGIC ### How It Works Under the Hood:
# MAGIC 1. **Old Parquet files are never deleted** (until VACUUM runs)
# MAGIC 2. Each version's JSON log says which files are "active" for that version
# MAGIC 3. Reading version N = reading only the files listed as active in version N
# MAGIC 4. RESTORE = writing a new log entry that points back to old files

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 1: Version History
# SECTION 3 — BEGINNER EXAMPLE 1: Creating and Viewing History
# Real-world: Build a table with multiple versions, then explore them.

from pyspark.sql.functions import col, lit  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== Building a Table with History ===")  # Heading.

# Version 0: Create.
tt_path = "/tmp/delta_kt/time_travel"  # Path.
v0 = spark.createDataFrame([
    (1, "Alice", 95000),
    (2, "Bob", 72000),
    (3, "Carol", 88000),
], ["emp_id", "name", "salary"])  # Initial data.
v0.write.format("delta").mode("overwrite").save(tt_path)  # Version 0.
print("Version 0 (CREATE): 3 employees")

# Version 1: Insert new employees.
v1_new = spark.createDataFrame([
    (4, "David", 68000),
    (5, "Eve", 75000),
], ["emp_id", "name", "salary"])  # New.
v1_new.write.format("delta").mode("append").save(tt_path)  # Version 1.
print("Version 1 (INSERT): +2 employees")

# Version 2: Update salary.
dt = DeltaTable.forPath(spark, tt_path)  # Load.
dt.update("emp_id = 1", {"salary": lit(105000)})  # Alice raise. Version 2.
print("Version 2 (UPDATE): Alice salary → 105000")

# Version 3: Delete an employee.
dt.delete("emp_id = 4")  # Remove David. Version 3.
print("Version 3 (DELETE): David removed")

# View the complete history.
print("\n--- DESCRIBE HISTORY ---")
display(dt.history().select("version", "timestamp", "operation", "operationParameters", "operationMetrics"))

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 2: Reading Past Versions
# SECTION 3 — BEGINNER EXAMPLE 2: Reading Past Versions
# Real-world: "What did the data look like before that delete?"

from pyspark.sql.functions import col  # Imports.

print("=== Reading Past Versions ===")  # Heading.
tt_path = "/tmp/delta_kt/time_travel"  # Path.

# Method 1: VERSION AS OF (by version number).
print("--- Version 0 (Original 3 employees) ---")
display(spark.read.format("delta").option("versionAsOf", 0).load(tt_path))  # V0.

print("\n--- Version 1 (After insert, 5 employees) ---")
display(spark.read.format("delta").option("versionAsOf", 1).load(tt_path))  # V1.

print("\n--- Version 2 (After Alice's raise) ---")
display(spark.read.format("delta").option("versionAsOf", 2).load(tt_path).filter("emp_id = 1"))  # V2 Alice.

print("\n--- Current (Version 3, after delete) ---")
display(spark.read.format("delta").load(tt_path).orderBy("emp_id"))  # Current.

# Method 2: TIMESTAMP AS OF.
print("\n--- Reading by Timestamp ---")
from delta.tables import DeltaTable  # Import.
history = DeltaTable.forPath(spark, tt_path).history()  # History.
v1_timestamp = history.filter("version = 1").select("timestamp").collect()[0][0]  # Get V1 time.
print(f"Version 1 was created at: {v1_timestamp}")

# Read at that timestamp.
v1_data = spark.read.format("delta").option("timestampAsOf", str(v1_timestamp)).load(tt_path)  # Read.
print(f"Data at that time: {v1_data.count()} rows")  # 5 rows.

# SQL syntax.
print("\n--- SQL Syntax ---")
spark.sql(f"SELECT * FROM delta.`{tt_path}` VERSION AS OF 0").show()  # SQL version.
print("SQL: SELECT * FROM table VERSION AS OF N")
print("SQL: SELECT * FROM table TIMESTAMP AS OF '2024-01-15'")

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 3: Comparing Versions
# SECTION 3 — BEGINNER EXAMPLE 3: Comparing Two Versions
# Real-world: "What changed between yesterday and today?"

from pyspark.sql.functions import col  # Imports.

print("=== Comparing Versions ===")  # Heading.
tt_path = "/tmp/delta_kt/time_travel"  # Path.

# Read two versions.
v0 = spark.read.format("delta").option("versionAsOf", 0).load(tt_path)  # Before.
v3 = spark.read.format("delta").load(tt_path)  # Current.

# What was added? (in current but not in original)
print("--- Rows ADDED since v0 ---")
added = v3.subtract(v0)  # Set difference.
display(added)

# What was removed? (in original but not in current)
print("\n--- Rows REMOVED since v0 ---")
removed = v0.subtract(v3)  # Set difference.
display(removed)

# What changed? (same emp_id but different values)
print("\n--- Rows CHANGED (salary differences) ---")
v0_alias = v0.alias("old")  # Alias.
v3_alias = v3.alias("new")  # Alias.
changed = v0_alias.join(v3_alias, "emp_id", "inner").filter(
    col("old.salary") != col("new.salary")  # Different salary.
).select(
    col("emp_id"),
    col("old.name"),
    col("old.salary").alias("old_salary"),
    col("new.salary").alias("new_salary")
)  # Show differences.
display(changed)
print("Alice: 95000 → 105000 (raise in version 2)")

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Example 1: RESTORE
# SECTION 4 — INTERMEDIATE EXAMPLE 1: RESTORE to Previous Version
# Real-world: "I accidentally deleted all the data! Roll it back!"

from pyspark.sql.functions import col, lit  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== RESTORE: Undoing Mistakes ===")  # Heading.

# Setup: Create a table and then accidentally delete everything.
restore_path = "/tmp/delta_kt/time_travel_restore"  # Path.
data = spark.createDataFrame([
    (1, "Critical Data A", 1000),
    (2, "Critical Data B", 2000),
    (3, "Critical Data C", 3000),
    (4, "Critical Data D", 4000),
    (5, "Critical Data E", 5000),
], ["id", "description", "value"])  # Important data.
data.write.format("delta").mode("overwrite").save(restore_path)  # Version 0.
print(f"Version 0: {spark.read.format('delta').load(restore_path).count()} rows (good state)")

# Simulate accident: someone runs DELETE without WHERE clause!
dt = DeltaTable.forPath(spark, restore_path)  # Load.
dt.delete("value > 0")  # OOPS! Deleted everything! Version 1.
print(f"Version 1 (ACCIDENT): {spark.read.format('delta').load(restore_path).count()} rows — ALL GONE!")

# PANIC! But we have time travel...
print("\n--- RESTORING to Version 0 ---")

# Method 1: RESTORE using Python.
spark.sql(f"RESTORE TABLE delta.`{restore_path}` TO VERSION AS OF 0")  # Restore!
print(f"After RESTORE: {spark.read.format('delta').load(restore_path).count()} rows — RECOVERED!")
display(spark.read.format("delta").load(restore_path))  # Show recovered data.

# Check history — RESTORE is itself a new version.
print("\n--- History after RESTORE ---")
dt2 = DeltaTable.forPath(spark, restore_path)  # Reload.
display(dt2.history().select("version", "operation", "operationParameters"))
print("""Note: RESTORE creates Version 2 (not overwrites Version 1).
The accidental delete (Version 1) is still in history for audit!""")

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Example 2: Timestamp-Based Travel
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Timestamp-Based Time Travel
# Real-world: "Show me the data as it was at end-of-day yesterday."

from pyspark.sql.functions import col, lit, current_timestamp  # Imports.
from delta.tables import DeltaTable  # Delta API.
import time  # For sleep.

print("=== Timestamp-Based Time Travel ===")  # Heading.

# Create table with timestamps between operations.
ts_path = "/tmp/delta_kt/time_travel_ts"  # Path.

# Version 0.
v0_data = spark.createDataFrame([
    (1, "Product A", 100.0),
    (2, "Product B", 200.0),
], ["id", "product", "price"])  # V0.
v0_data.write.format("delta").mode("overwrite").save(ts_path)  # Write.
print("Version 0 written")

time.sleep(2)  # Wait 2 seconds to get distinct timestamps.

# Version 1: price change.
dt_ts = DeltaTable.forPath(spark, ts_path)  # Load.
dt_ts.update("id = 1", {"price": lit(150.0)})  # Update. V1.
print("Version 1: Product A price → 150")

time.sleep(2)  # Wait.

# Version 2: new product.
spark.createDataFrame([(3, "Product C", 300.0)], ["id", "product", "price"]) \
    .write.format("delta").mode("append").save(ts_path)  # V2.
print("Version 2: Product C added")

# Read history to get timestamps.
print("\n--- Version Timestamps ---")
history = DeltaTable.forPath(spark, ts_path).history()  # History.
display(history.select("version", "timestamp", "operation"))

# Read at specific timestamp.
print("\n--- Reading between versions using timestamp ---")
timestamps = history.orderBy("version").select("timestamp").collect()  # Get all.
v0_ts = timestamps[2][0]  # V0 timestamp (oldest, last in desc order).
v1_ts = timestamps[1][0]  # V1 timestamp.

print(f"\nAt Version 0 time ({v0_ts}):")
display(spark.read.format("delta").option("timestampAsOf", str(v0_ts)).load(ts_path))

print(f"\nAt Version 1 time ({v1_ts}):")
display(spark.read.format("delta").option("timestampAsOf", str(v1_ts)).load(ts_path))

# RESTORE by timestamp.
print("\n--- RESTORE to Version 0 timestamp ---")
spark.sql(f"RESTORE TABLE delta.`{ts_path}` TO TIMESTAMP AS OF '{v0_ts}'")
print("Restored!")
display(spark.read.format("delta").load(ts_path))

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Example 3: Time Travel with SQL
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Time Travel in SQL and Registered Tables
# Real-world: Using time travel in SQL queries and with catalog tables.

from pyspark.sql.functions import col, lit  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== SQL Time Travel ===")  # Heading.

# Create a registered Delta table.
spark.sql("DROP TABLE IF EXISTS tt_demo_products")  # Clean.
spark.sql("""
    CREATE TABLE tt_demo_products (
        product_id INT,
        name STRING,
        price DOUBLE,
        category STRING
    ) USING DELTA
""")  # Create table. V0.

# Insert data.
spark.sql("""
    INSERT INTO tt_demo_products VALUES
    (1, 'Laptop', 999.99, 'Electronics'),
    (2, 'Mouse', 29.99, 'Electronics'),
    (3, 'Desk', 299.99, 'Furniture')
""")  # V1.

# Update price.
spark.sql("UPDATE tt_demo_products SET price = 899.99 WHERE product_id = 1")  # V2.

# Delete a product.
spark.sql("DELETE FROM tt_demo_products WHERE product_id = 2")  # V3.

# SQL Time Travel queries.
print("--- Current State (V3) ---")
display(spark.sql("SELECT * FROM tt_demo_products ORDER BY product_id"))

print("\n--- Version 1 (after initial insert) ---")
display(spark.sql("SELECT * FROM tt_demo_products VERSION AS OF 1 ORDER BY product_id"))

print("\n--- DESCRIBE HISTORY ---")
display(spark.sql("DESCRIBE HISTORY tt_demo_products").select("version", "operation", "timestamp"))

# RESTORE in SQL.
print("\n--- RESTORE via SQL ---")
spark.sql("RESTORE TABLE tt_demo_products TO VERSION AS OF 1")  # Restore.
print("Restored to version 1:")
display(spark.sql("SELECT * FROM tt_demo_products ORDER BY product_id"))

# Count rows in each version.
print("\n--- Row count per version ---")
for v in range(5):  # 0-4 (including restore).
    try:
        cnt = spark.sql(f"SELECT count(*) as cnt FROM tt_demo_products VERSION AS OF {v}").collect()[0][0]
        print(f"  Version {v}: {cnt} rows")
    except:
        break  # No more versions.

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Example 1: Disaster Recovery
# SECTION 5 — ADVANCED EXAMPLE 1: Disaster Recovery Playbook
# Real-world: Step-by-step recovery from accidental data corruption.

from pyspark.sql.functions import col, lit, expr, rand, round as spark_round  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== Disaster Recovery with Time Travel ===")  # Heading.
print("Scenario: A pipeline bug corrupted the sales table overnight.\n")

# Setup: Build a production-like table with history.
dr_path = "/tmp/delta_kt/disaster_recovery"  # Path.

# V0: Initial load (good data).
sales = spark.range(1000).select(
    (col("id") + 1).alias("sale_id"),
    (rand() * 100).cast("int").alias("customer_id"),
    spark_round(rand() * 500 + 10, 2).alias("amount"),
    expr("date_add('2024-01-01', cast(rand()*30 as int))").alias("sale_date")
)  # 1000 sales.
sales.write.format("delta").mode("overwrite").save(dr_path)  # V0.

# V1: Daily append (good).
spark.range(1000, 1200).select(
    (col("id") + 1).alias("sale_id"),
    (rand() * 100).cast("int").alias("customer_id"),
    spark_round(rand() * 500 + 10, 2).alias("amount"),
    expr("date_add('2024-02-01', cast(rand()*28 as int))").alias("sale_date")
).write.format("delta").mode("append").save(dr_path)  # V1.

# V2: Bug! All amounts set to 0 (simulated pipeline bug).
dt_dr = DeltaTable.forPath(spark, dr_path)  # Load.
dt_dr.update(set={"amount": lit(0.0)})  # BUG: All amounts zeroed!
print("After bug (V2): All amounts set to 0!")
print(f"  Total revenue (corrupted): ${spark.read.format('delta').load(dr_path).agg({'amount': 'sum'}).collect()[0][0]}")

# STEP 1: Identify when corruption happened.
print("\n--- Step 1: Check history ---")
history = dt_dr.history()  # Get history.
display(history.select("version", "timestamp", "operation", "operationMetrics"))

# STEP 2: Verify the good version.
print("\n--- Step 2: Verify version 1 is good ---")
v1_total = spark.read.format("delta").option("versionAsOf", 1).load(dr_path).agg({"amount": "sum"}).collect()[0][0]
print(f"  Version 1 total revenue: ${v1_total:,.2f} (looks correct!)")
v1_count = spark.read.format("delta").option("versionAsOf", 1).load(dr_path).count()
print(f"  Version 1 row count: {v1_count}")

# STEP 3: RESTORE to good version.
print("\n--- Step 3: RESTORE to version 1 ---")
spark.sql(f"RESTORE TABLE delta.`{dr_path}` TO VERSION AS OF 1")  # Restore.

# STEP 4: Verify recovery.
print("\n--- Step 4: Verify recovery ---")
recovered_total = spark.read.format("delta").load(dr_path).agg({"amount": "sum"}).collect()[0][0]
recovered_count = spark.read.format("delta").load(dr_path).count()
print(f"  Recovered total revenue: ${recovered_total:,.2f}")
print(f"  Recovered row count: {recovered_count}")
print(f"  Match: {abs(recovered_total - v1_total) < 0.01}")
print("\n✓ DISASTER RECOVERED! Data is back to normal.")

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Example 2: Audit and Compliance
# SECTION 5 — ADVANCED EXAMPLE 2: Audit and Compliance Reporting
# Real-world: "Show the exact state of customer PII on Dec 31, 2023 for the auditor."

from pyspark.sql.functions import col, lit, datediff, current_date  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== Audit & Compliance with Time Travel ===")  # Heading.

# Setup: Customer table with multiple changes over time.
audit_path = "/tmp/delta_kt/audit_compliance"  # Path.

# V0: Initial customer data.
customers_v0 = spark.createDataFrame([
    (1, "Alice Johnson", "alice@email.com", "123-45-6789", "New York"),
    (2, "Bob Smith", "bob@work.com", "987-65-4321", "Chicago"),
    (3, "Carol Davis", "carol@mail.com", "555-12-3456", "Denver"),
], ["id", "name", "email", "ssn", "city"])  # PII data.
customers_v0.write.format("delta").mode("overwrite").save(audit_path)  # V0.

# V1: Address change.
dt_audit = DeltaTable.forPath(spark, audit_path)  # Load.
dt_audit.update("id = 1", {"city": lit("Boston"), "email": lit("alice@newemail.com")})  # V1.

# V2: Customer deletion (GDPR right-to-erasure).
dt_audit.delete("id = 3")  # Carol requested deletion. V2.

# V3: New customer.
spark.createDataFrame([(4, "David Lee", "david@corp.com", "111-22-3333", "Miami")],
    ["id", "name", "email", "ssn", "city"]).write.format("delta").mode("append").save(audit_path)  # V3.

# Audit Function: get state at any version.
def audit_report(path, version, description):
    """Generate an audit report for a specific version."""
    df = spark.read.format("delta").option("versionAsOf", version).load(path)
    print(f"\n{'='*50}")
    print(f"AUDIT REPORT: {description}")
    print(f"Version: {version}")
    history = DeltaTable.forPath(spark, path).history().filter(f"version = {version}")
    ts = history.select("timestamp").collect()[0][0]
    print(f"Timestamp: {ts}")
    print(f"Row Count: {df.count()}")
    print(f"{'='*50}")
    display(df)
    return df

# Generate audit reports.
print("=== Generating Audit Reports ===")
audit_report(audit_path, 0, "Original Customer Data")
audit_report(audit_path, 1, "After Address Change (Alice)")
audit_report(audit_path, 2, "After GDPR Deletion (Carol)")
audit_report(audit_path, 3, "Current State")

# Change log: who changed what and when.
print("\n=== Complete Change Log ===")
dt_audit2 = DeltaTable.forPath(spark, audit_path)  # Reload.
display(dt_audit2.history().select(
    "version", "timestamp", "operation", "operationMetrics",
    "userIdentity", "operationParameters"
))
print("\nEvery change is auditable — meets compliance requirements!")

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Example 3: ML Reproducibility
# SECTION 5 — ADVANCED EXAMPLE 3: ML Reproducibility with Time Travel
# Real-world: Re-train a model using the exact same data snapshot.

from pyspark.sql.functions import col, rand, round as spark_round, expr  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== ML Reproducibility with Time Travel ===")  # Heading.
print("Scenario: Reproduce a model training run from 3 days ago.\n")

# Setup: Feature table that changes daily.
ml_path = "/tmp/delta_kt/ml_features"  # Path.

# V0: Training data snapshot (100 samples).
features_v0 = spark.range(100).select(
    col("id").alias("sample_id"),
    spark_round(rand(42) * 10, 2).alias("feature_1"),
    spark_round(rand(43) * 5, 2).alias("feature_2"),
    (rand(44) > 0.5).cast("int").alias("label")
)  # Training features.
features_v0.write.format("delta").mode("overwrite").save(ml_path)  # V0.

# V1: New training data arrives (appended).
spark.range(100, 150).select(
    col("id").alias("sample_id"),
    spark_round(rand(45) * 10, 2).alias("feature_1"),
    spark_round(rand(46) * 5, 2).alias("feature_2"),
    (rand(47) > 0.5).cast("int").alias("label")
).write.format("delta").mode("append").save(ml_path)  # V1.

# V2: Feature engineering update (recalculate feature_1).
dt_ml = DeltaTable.forPath(spark, ml_path)  # Load.
dt_ml.update("sample_id < 50", {"feature_1": expr("feature_1 * 1.1")})  # V2.

print("Feature table has 3 versions:")
print(f"  V0: {spark.read.format('delta').option('versionAsOf', 0).load(ml_path).count()} samples")
print(f"  V1: {spark.read.format('delta').option('versionAsOf', 1).load(ml_path).count()} samples")
print(f"  V2: {spark.read.format('delta').option('versionAsOf', 2).load(ml_path).count()} samples (current)")

# Reproduce: Load exact data used for model training on V0.
print("\n--- Reproducing Training Run (Version 0) ---")
training_data = spark.read.format("delta").option("versionAsOf", 0).load(ml_path)  # Exact snapshot.
print(f"Training samples: {training_data.count()}")
print(f"Feature 1 stats:")
display(training_data.describe("feature_1", "feature_2", "label"))

# Store version reference for future reproducibility.
print("\n--- Best Practice: Record Data Version with Model ---")
model_metadata = {
    "model_name": "binary_classifier_v1",
    "training_data_path": ml_path,
    "training_data_version": 0,
    "training_samples": training_data.count(),
    "training_date": "2024-01-15",
    "feature_columns": ["feature_1", "feature_2"],
    "label_column": "label"
}
for k, v in model_metadata.items():
    print(f"  {k}: {v}")

print("\n✓ Anyone can reproduce this training run using:")
print(f'  spark.read.format("delta").option("versionAsOf", 0).load("{ml_path}")')

# COMMAND ----------

# DBTITLE 1,Section 6 - Key Takeaways
# MAGIC %md
# MAGIC ## SECTION 6 — Key Takeaways
# MAGIC
# MAGIC ### Time Travel Methods
# MAGIC
# MAGIC | Method | Syntax | Use Case |
# MAGIC |--------|--------|----------|
# MAGIC | Version number | `.option("versionAsOf", N)` | Know exact version |
# MAGIC | Timestamp | `.option("timestampAsOf", "...")` | Know approximate time |
# MAGIC | SQL Version | `SELECT * FROM t VERSION AS OF N` | SQL queries |
# MAGIC | SQL Timestamp | `SELECT * FROM t TIMESTAMP AS OF '...'` | SQL queries |
# MAGIC | RESTORE Version | `RESTORE TABLE t TO VERSION AS OF N` | Undo mistakes |
# MAGIC | RESTORE Timestamp | `RESTORE TABLE t TO TIMESTAMP AS OF '...'` | Undo by time |
# MAGIC
# MAGIC ### Important Limitations
# MAGIC 1. **VACUUM removes old files** — after VACUUM, you can't time travel past the retention period
# MAGIC 2. **Default retention: 7 days** for data files (`delta.deletedFileRetentionDuration`)
# MAGIC 3. **Log retention: 30 days** (`delta.logRetentionDuration`)
# MAGIC 4. **RESTORE creates a NEW version** — it doesn't erase history
# MAGIC 5. **Storage cost** — keeping old versions means keeping old files on disk
# MAGIC
# MAGIC ### Best Practices
# MAGIC * Set `delta.logRetentionDuration` based on your compliance needs
# MAGIC * Document version numbers in ML experiment tracking
# MAGIC * Test RESTORE in dev before using in production
# MAGIC * Use Change Data Feed (CDF) instead of time travel for incremental processing
# MAGIC * Never VACUUM below your compliance retention window

# COMMAND ----------

# DBTITLE 1,Section 7 - Practice Exercises
# SECTION 7 — HOMEWORK & SOLUTIONS

from pyspark.sql.functions import col, lit  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("="*60)
print("HOMEWORK — Delta Time Travel")
print("="*60)

# Level 1: Create table, read version 0.
print("\n=== Level 1: Create and read version 0 ===")
l1 = spark.createDataFrame([(1,"A"),(2,"B"),(3,"C")], ["id","val"])
l1_path = "/tmp/delta_kt/hw56_l1"
l1.write.format("delta").mode("overwrite").save(l1_path)
display(spark.read.format("delta").option("versionAsOf", 0).load(l1_path))

# Level 2: Modify and compare.
print("\n=== Level 2: Update and compare versions ===")
DeltaTable.forPath(spark, l1_path).update("id=1", {"val": lit("Z")})
print("V0:"); display(spark.read.format("delta").option("versionAsOf", 0).load(l1_path))
print("V1:"); display(spark.read.format("delta").load(l1_path))

# Level 3: RESTORE after delete.
print("\n=== Level 3: RESTORE after accidental delete ===")
DeltaTable.forPath(spark, l1_path).delete("id > 0")  # Delete all!
print(f"After delete: {spark.read.format('delta').load(l1_path).count()} rows")
spark.sql(f"RESTORE TABLE delta.`{l1_path}` TO VERSION AS OF 1")
print(f"After RESTORE: {spark.read.format('delta').load(l1_path).count()} rows")

# Level 4: History analysis.
print("\n=== Level 4: Analyze history ===")
history = DeltaTable.forPath(spark, l1_path).history()
display(history.select("version", "operation", "timestamp"))

# Level 5: Build a change report.
print("\n=== Level 5: Generate change report ===")
l5_path = "/tmp/delta_kt/hw56_l5"
spark.createDataFrame([(1,100),(2,200),(3,300)], ["id","amount"]).write.format("delta").mode("overwrite").save(l5_path)
DeltaTable.forPath(spark, l5_path).update("id=2", {"amount": lit(250)})
DeltaTable.forPath(spark, l5_path).delete("id=3")
for v in range(3):
    cnt = spark.read.format("delta").option("versionAsOf", v).load(l5_path).count()
    total = spark.read.format("delta").option("versionAsOf", v).load(l5_path).agg({"amount":"sum"}).collect()[0][0]
    print(f"  V{v}: {cnt} rows, total={total}")

print("\n" + "="*60)
print("All exercises completed!")
print("="*60)