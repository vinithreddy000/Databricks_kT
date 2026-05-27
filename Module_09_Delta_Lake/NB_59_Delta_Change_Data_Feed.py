# Databricks notebook source
# DBTITLE 1,Sections 1-2 Overview
# MAGIC %md
# MAGIC # Notebook 59: Delta Change Data Feed (CDF)
# MAGIC ## Module 09: Delta Lake Deep Dive
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Change Data Feed (CDF) lets you see **exactly which rows were inserted, updated, or deleted** between any two versions of a Delta table. Instead of comparing entire snapshots, you get a precise list of what changed.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Think of **bank statement notifications**:
# MAGIC - Without CDF: You check your full balance every day and try to figure out what changed
# MAGIC - With CDF: Your bank sends you a notification for each transaction: "$50 deposited", "$20 withdrawn"
# MAGIC
# MAGIC CDF is like getting transaction-level notifications for your data.
# MAGIC
# MAGIC ### Use Cases:
# MAGIC 1. **Incremental ETL** — Process only changed rows, not the whole table
# MAGIC 2. **Streaming downstream** — Send changes to Kafka/Event Hub in real time
# MAGIC 3. **Audit trail** — See exactly what changed, when, and the before/after values
# MAGIC 4. **Cache invalidation** — Know which dashboard results are stale
# MAGIC 5. **Data synchronization** — Keep a downstream system in sync
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC When CDF is enabled, Delta records change information:
# MAGIC
# MAGIC   Regular Delta (without CDF):     With CDF enabled:
# MAGIC   Version 1: [full snapshot]        Version 1: [full snapshot]
# MAGIC   Version 2: [full snapshot]        Version 2: [full snapshot] + [changes]
# MAGIC
# MAGIC The _change_type column tells you what happened:
# MAGIC   ┌──────────────────────────────────────────────┐
# MAGIC   │ _change_type      │ Meaning                  │
# MAGIC   ├───────────────────┼──────────────────────────┤
# MAGIC   │ insert            │ New row added             │
# MAGIC   │ update_preimage   │ Row BEFORE update         │
# MAGIC   │ update_postimage  │ Row AFTER update          │
# MAGIC   │ delete            │ Row was removed           │
# MAGIC   └───────────────────┴──────────────────────────┘
# MAGIC
# MAGIC Additional metadata columns:
# MAGIC   _commit_version  → Which version the change occurred in
# MAGIC   _commit_timestamp → When the change was committed
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 1: Enable CDF
# SECTION 3 — BEGINNER EXAMPLE 1: Enabling and Using CDF
# Real-world: Track all changes to a customer table.

from pyspark.sql.functions import col, lit  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== Enabling Change Data Feed ===")  # Heading.

# Create table WITH CDF enabled.
spark.sql("DROP TABLE IF EXISTS cdf_customers")  # Clean.
spark.sql("""
    CREATE TABLE cdf_customers (
        customer_id INT,
        name STRING,
        email STRING,
        city STRING
    ) USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print("✓ Table created with CDF enabled")

# Insert initial data (Version 1).
spark.sql("""
    INSERT INTO cdf_customers VALUES
    (1, 'Alice', 'alice@email.com', 'New York'),
    (2, 'Bob', 'bob@email.com', 'Chicago'),
    (3, 'Carol', 'carol@email.com', 'Denver')
""")
print("✓ Inserted 3 customers (Version 1)")

# Update a customer (Version 2).
spark.sql("UPDATE cdf_customers SET city = 'Boston', email = 'alice@new.com' WHERE customer_id = 1")
print("✓ Updated Alice's city and email (Version 2)")

# Delete a customer (Version 3).
spark.sql("DELETE FROM cdf_customers WHERE customer_id = 3")
print("✓ Deleted Carol (Version 3)")

# Insert new customer (Version 4).
spark.sql("INSERT INTO cdf_customers VALUES (4, 'David', 'david@email.com', 'Miami')")
print("✓ Inserted David (Version 4)")

# Read the Change Data Feed!
print("\n--- Reading Change Data Feed (all changes from v1 to v4) ---")
changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 1) \
    .table("cdf_customers")  # Read CDF.

display(changes.orderBy("_commit_version", "customer_id"))
print("\nNotice: _change_type tells you exactly what happened to each row!")

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 2: Filter by Change Type
# SECTION 3 — BEGINNER EXAMPLE 2: Filtering Changes by Type
# Real-world: Process only inserts for downstream loading.

from pyspark.sql.functions import col  # Imports.

print("=== Filtering Change Data Feed ===")  # Heading.

# Read all changes.
all_changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 1) \
    .table("cdf_customers")

# Filter: Only INSERTS.
print("--- Only INSERTS ---")
inserts = all_changes.filter(col("_change_type") == "insert")  # New rows only.
display(inserts.select("customer_id", "name", "city", "_change_type", "_commit_version"))

# Filter: Only UPDATES (before and after).
print("\n--- Only UPDATES ---")
updates = all_changes.filter(col("_change_type").isin("update_preimage", "update_postimage"))  # Updates.
display(updates.select("customer_id", "name", "email", "city", "_change_type", "_commit_version"))
print("preimage = BEFORE the update, postimage = AFTER the update")

# Filter: Only DELETES.
print("\n--- Only DELETES ---")
deletes = all_changes.filter(col("_change_type") == "delete")  # Deleted rows.
display(deletes.select("customer_id", "name", "_change_type", "_commit_version"))

# Summary.
print("\n--- Change Summary ---")
print(f"  Total inserts: {inserts.count()}")
print(f"  Total updates: {updates.filter(col('_change_type')=='update_postimage').count()}")
print(f"  Total deletes: {deletes.count()}")

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 3: Version Range
# SECTION 3 — BEGINNER EXAMPLE 3: Reading Changes for a Specific Version Range
# Real-world: "Show me only what changed in the last ETL run."

from pyspark.sql.functions import col  # Imports.

print("=== Reading Specific Version Ranges ===")  # Heading.

# Read changes for ONLY version 2 (the update).
print("--- Changes in Version 2 only (Alice's update) ---")
v2_changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 2) \
    .option("endingVersion", 2) \
    .table("cdf_customers")
display(v2_changes)

# Read changes from version 3 onwards.
print("\n--- Changes from Version 3 onwards ---")
v3_plus = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 3) \
    .table("cdf_customers")
display(v3_plus.orderBy("_commit_version"))

# Read by timestamp.
print("\n--- Reading by Timestamp ---")
history = spark.sql("DESCRIBE HISTORY cdf_customers").select("version", "timestamp").collect()
for h in history:
    print(f"  Version {h['version']}: {h['timestamp']}")

# Use timestamp of version 2.
v2_ts = str([h['timestamp'] for h in history if h['version'] == 2][0])
print(f"\nReading changes starting from timestamp: {v2_ts}")
ts_changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", v2_ts) \
    .table("cdf_customers")
print(f"Changes from that timestamp: {ts_changes.count()} rows")

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Example 1: Incremental ETL
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Incremental ETL with CDF
# Real-world: Process only changed rows to update a downstream table.

from pyspark.sql.functions import col, lit, current_timestamp, when  # Imports.
from delta.tables import DeltaTable  # Delta API.

print("=== Incremental ETL with CDF ===")  # Heading.
print("Pattern: Source table changes → CDF captures changes → Apply to target\n")

# Setup: Source table (with CDF).
spark.sql("DROP TABLE IF EXISTS cdf_source_orders")
spark.sql("""
    CREATE TABLE cdf_source_orders (
        order_id INT, customer_id INT, amount DOUBLE, status STRING
    ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
spark.sql("""
    INSERT INTO cdf_source_orders VALUES
    (1, 101, 250.00, 'processing'),
    (2, 102, 180.50, 'processing'),
    (3, 103, 99.99, 'processing')
""")  # V1.
print("Source: 3 orders created (V1)")

# Setup: Target (aggregated) table.
spark.sql("DROP TABLE IF EXISTS cdf_target_summary")
spark.sql("""
    CREATE TABLE cdf_target_summary (
        customer_id INT, total_orders INT, total_amount DOUBLE, last_updated TIMESTAMP
    ) USING DELTA
""")
# Initial load.
spark.sql("""
    INSERT INTO cdf_target_summary
    SELECT customer_id, count(*) as total_orders, sum(amount) as total_amount, current_timestamp()
    FROM cdf_source_orders GROUP BY customer_id
""")
print("Target summary table initialized")
display(spark.sql("SELECT * FROM cdf_target_summary"))

# Simulate changes in source.
print("\n--- Simulating source changes ---")
spark.sql("UPDATE cdf_source_orders SET status = 'shipped', amount = 260.00 WHERE order_id = 1")  # V2.
spark.sql("INSERT INTO cdf_source_orders VALUES (4, 101, 150.00, 'processing')")  # V3.
spark.sql("DELETE FROM cdf_source_orders WHERE order_id = 3")  # V4.
print("  V2: Order 1 shipped (amount changed)")
print("  V3: Order 4 inserted")
print("  V4: Order 3 deleted")

# Incremental ETL: Read only changes since V1.
print("\n--- Reading incremental changes (V2 to V4) ---")
changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 2) \
    .table("cdf_source_orders")
display(changes.orderBy("_commit_version", "order_id"))

# Apply changes to target.
print("\n--- Applying incremental changes to target ---")
# Get affected customer_ids.
affected_customers = changes.select("customer_id").distinct().collect()
affected_ids = [r['customer_id'] for r in affected_customers]
print(f"  Affected customers: {affected_ids}")

# Recalculate only for affected customers.
for cid in affected_ids:
    new_stats = spark.sql(f"""
        SELECT {cid} as customer_id,
               count(*) as total_orders,
               sum(amount) as total_amount,
               current_timestamp() as last_updated
        FROM cdf_source_orders WHERE customer_id = {cid}
    """).collect()[0]
    spark.sql(f"""
        MERGE INTO cdf_target_summary t
        USING (SELECT {cid} as customer_id, {new_stats['total_orders']} as total_orders,
               {new_stats['total_amount']} as total_amount, current_timestamp() as last_updated) s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print("\nTarget after incremental update:")
display(spark.sql("SELECT * FROM cdf_target_summary ORDER BY customer_id"))
print("✓ Only affected customers were recalculated!")

# COMMAND ----------

# DBTITLE 1,Section 4-5 and Exercises
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Streaming CDF
# Real-world: Continuously process changes as they happen.

from pyspark.sql.functions import col, current_timestamp  # Imports.

print("=== Streaming with CDF ===")  # Heading.
print("CDF can be read as a stream for real-time change processing.\n")

print("--- Streaming CDF Pattern ---")
print("""
# Read CDF as a stream:
changes_stream = spark.readStream.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("my_source_table")

# Process and write to target:
changes_stream \
    .filter(col("_change_type") != "update_preimage") \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .trigger(availableNow=True) \
    .toTable("my_target_table")
""")

# Demonstrate batch CDF processing patterns.
print("\n--- Common CDF Processing Patterns ---")

# Pattern 1: Get only the latest state of each changed row.
print("\nPattern 1: Latest state of changed rows")
latest_changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 1) \
    .table("cdf_customers") \
    .filter(col("_change_type").isin("insert", "update_postimage"))  # Only current state.
print("Filter: _change_type IN ('insert', 'update_postimage')")
display(latest_changes.select("customer_id", "name", "city", "_change_type"))

# Pattern 2: Build a change audit log.
print("\nPattern 2: Audit log with before/after")
audit = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 1) \
    .table("cdf_customers")

# Pair pre/post images.
from pyspark.sql.functions import collect_list, struct
pre = audit.filter("_change_type = 'update_preimage'").select(
    col("customer_id"), col("city").alias("old_city"), col("email").alias("old_email"), col("_commit_version")
)
post = audit.filter("_change_type = 'update_postimage'").select(
    col("customer_id"), col("city").alias("new_city"), col("email").alias("new_email"), col("_commit_version")
)
audit_log = pre.join(post, ["customer_id", "_commit_version"])
print("Audit log (before → after):")
display(audit_log)

print("\n" + "="*60)
print("SECTION 7 — HOMEWORK")
print("="*60)

# Level 1: Enable CDF and read it.
print("\n=== Level 1: Basic CDF ===")
spark.sql("DROP TABLE IF EXISTS hw59")
spark.sql("CREATE TABLE hw59 (id INT, val STRING) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
spark.sql("INSERT INTO hw59 VALUES (1,'A'),(2,'B')")
spark.sql("UPDATE hw59 SET val='Z' WHERE id=1")
changes = spark.read.format("delta").option("readChangeFeed","true").option("startingVersion",1).table("hw59")
display(changes)

# Level 2: Filter only inserts.
print("\n=== Level 2: Filter inserts ===")
inserts_only = changes.filter("_change_type = 'insert'")
print(f"Inserts: {inserts_only.count()}")

# Level 3: Count changes by type.
print("\n=== Level 3: Summarize changes ===")
display(changes.groupBy("_change_type").count())

print("\nAll exercises completed!")