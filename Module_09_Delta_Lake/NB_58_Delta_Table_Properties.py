# Databricks notebook source
# DBTITLE 1,Section 1 and 2 - Overview
# MAGIC %md
# MAGIC # Notebook 58: Delta Table Properties and Management
# MAGIC ## Module 09: Delta Lake Deep Dive
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Delta tables have **properties** (settings) that control their behavior — things like how long to keep history, whether to auto-optimize, and whether to track changes. You can also **manage** tables by adding/removing columns, cloning them, and adding comments.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Think of a **car's settings panel**:
# MAGIC - **Table Properties** = Dashboard settings (fuel economy mode, auto headlights, cruise control)
# MAGIC - **ALTER TABLE** = Modifying the car (adding a spoiler, changing tires)
# MAGIC - **CLONE** = Making an exact copy of the car (shallow = shared engine, deep = fully independent)
# MAGIC - **COMMENTS** = Sticky notes on the dashboard explaining what each button does
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Delta Table Management Commands:
# MAGIC
# MAGIC ┌────────────────────────────────────────────────────────────┐
# MAGIC │  DESCRIBE DETAIL       → Full table metadata               │
# MAGIC │  DESCRIBE HISTORY      → Change history                    │
# MAGIC │  ALTER TABLE ADD COLS   → Add new columns                   │
# MAGIC │  ALTER TABLE CHANGE COL → Rename, reorder, retype columns   │
# MAGIC │  ALTER TABLE SET TBLPROPERTIES → Configure behavior          │
# MAGIC │  SHALLOW CLONE          → Fast copy (shared files)          │
# MAGIC │  DEEP CLONE             → Full independent copy             │
# MAGIC │  COMMENT ON             → Add documentation                 │
# MAGIC └────────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC Shallow Clone vs Deep Clone:
# MAGIC
# MAGIC   Source Table:  [file1] [file2] [file3]   (100GB on disk)
# MAGIC
# MAGIC   Shallow Clone: [pointer→file1] [pointer→file2] [pointer→file3]
# MAGIC                  (0GB extra disk, just metadata)
# MAGIC                  ⚠️ Breaks if source is VACUUM'd!
# MAGIC
# MAGIC   Deep Clone:    [copy_file1] [copy_file2] [copy_file3]
# MAGIC                  (100GB extra disk, fully independent)
# MAGIC                  ✓ Safe even if source is deleted
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 1: DESCRIBE
# SECTION 3 — BEGINNER EXAMPLE 1: DESCRIBE DETAIL and DESCRIBE HISTORY
# Real-world: Inspecting table metadata for monitoring.

from pyspark.sql.functions import col, rand, expr, round as spark_round  # Imports.

print("=== DESCRIBE: Inspecting Table Metadata ===")  # Heading.

# Create a sample table.
spark.sql("DROP TABLE IF EXISTS prop_demo_orders")  # Clean.
data = spark.range(10000).select(
    (col("id") + 1).alias("order_id"),
    (rand() * 200).cast("int").alias("customer_id"),
    spark_round(rand() * 500 + 10, 2).alias("amount"),
    expr("date_add('2024-01-01', cast(rand()*180 as int))").alias("order_date"),
    expr("CASE WHEN rand()<0.5 THEN 'online' ELSE 'store' END").alias("channel")
)
data.write.format("delta").mode("overwrite").saveAsTable("prop_demo_orders")

# DESCRIBE DETAIL — everything about the table.
print("--- DESCRIBE DETAIL ---")
print("Shows: format, location, numFiles, sizeInBytes, partitionColumns, properties")
display(spark.sql("DESCRIBE DETAIL prop_demo_orders"))

# DESCRIBE TABLE — schema info.
print("\n--- DESCRIBE TABLE ---")
print("Shows: column names, types, and comments")
display(spark.sql("DESCRIBE TABLE prop_demo_orders"))

# DESCRIBE EXTENDED — schema + table metadata.
print("\n--- DESCRIBE EXTENDED ---")
print("Shows: schema + catalog info, storage, provider, properties")
display(spark.sql("DESCRIBE EXTENDED prop_demo_orders"))

# DESCRIBE HISTORY — change log.
print("\n--- DESCRIBE HISTORY ---")
print("Shows: version, timestamp, operation, who did it, metrics")
display(spark.sql("DESCRIBE HISTORY prop_demo_orders"))

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 2: Table Properties
# SECTION 3 — BEGINNER EXAMPLE 2: Setting and Reading Table Properties
# Real-world: Configure a production table's behavior.

print("=== Setting Table Properties ===")  # Heading.

# Set properties.
print("--- Setting Properties ---")
spark.sql("""
    ALTER TABLE prop_demo_orders SET TBLPROPERTIES (
        'delta.logRetentionDuration' = '30 days',
        'delta.deletedFileRetentionDuration' = '7 days',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.enableChangeDataFeed' = 'true'
    )
""")
print("Properties set!")

# Read properties back.
print("\n--- Reading Properties ---")
props = spark.sql("SHOW TBLPROPERTIES prop_demo_orders")  # Show all.
display(props)

# Key properties explained.
print("\n--- Key Delta Properties Explained ---")
properties_explained = [
    ("delta.logRetentionDuration", "30 days", "How long to keep transaction log history"),
    ("delta.deletedFileRetentionDuration", "7 days", "Minimum age before VACUUM can delete files"),
    ("delta.autoOptimize.optimizeWrite", "true", "Coalesce small partitions at write time"),
    ("delta.autoOptimize.autoCompact", "true", "Auto-compact after writes if too many small files"),
    ("delta.enableChangeDataFeed", "true", "Track row-level changes (insert/update/delete)"),
    ("delta.minReaderVersion", "1-3", "Minimum protocol version for readers"),
    ("delta.minWriterVersion", "2-7", "Minimum protocol version for writers"),
]
for prop, val, desc in properties_explained:
    print(f"  {prop} = {val}")
    print(f"    → {desc}\n")

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 3: ALTER TABLE Columns
# SECTION 3 — BEGINNER EXAMPLE 3: ALTER TABLE (Add, Rename, Reorder Columns)
# Real-world: Schema evolution without rewriting data.

print("=== ALTER TABLE: Column Management ===")  # Heading.

# Show current schema.
print("--- Current Schema ---")
display(spark.sql("DESCRIBE TABLE prop_demo_orders"))

# ADD COLUMNS.
print("\n--- ADD COLUMNS ---")
spark.sql("""
    ALTER TABLE prop_demo_orders ADD COLUMNS (
        discount DOUBLE COMMENT 'Discount percentage applied',
        loyalty_tier STRING COMMENT 'Customer loyalty level'
    )
""")
print("Added: discount (DOUBLE), loyalty_tier (STRING)")
display(spark.sql("SELECT * FROM prop_demo_orders LIMIT 3"))  # New cols are NULL.
print("New columns are NULL for existing rows (backward compatible).")

# RENAME COLUMN.
print("\n--- RENAME COLUMN ---")
spark.sql("ALTER TABLE prop_demo_orders RENAME COLUMN channel TO sales_channel")
print("Renamed: channel → sales_channel")

# ADD COMMENT to column.
print("\n--- ADD COLUMN COMMENTS ---")
spark.sql("ALTER TABLE prop_demo_orders CHANGE COLUMN order_id COMMENT 'Unique order identifier'")
spark.sql("ALTER TABLE prop_demo_orders CHANGE COLUMN amount COMMENT 'Order total in USD'")
print("Comments added to order_id and amount")

# Show updated schema with comments.
print("\n--- Updated Schema with Comments ---")
display(spark.sql("DESCRIBE TABLE prop_demo_orders"))

# TABLE COMMENT.
print("\n--- TABLE-LEVEL COMMENT ---")
spark.sql("COMMENT ON TABLE prop_demo_orders IS 'Production orders table. Contains all customer orders since 2024. Updated daily via ETL pipeline.'")
display(spark.sql("DESCRIBE EXTENDED prop_demo_orders").filter("col_name = 'Comment'"))

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Example 1: Shallow Clone
# SECTION 4 — INTERMEDIATE EXAMPLE 1: SHALLOW CLONE
# Real-world: Create a test copy without duplicating data.

from delta.tables import DeltaTable  # Import.

print("=== SHALLOW CLONE: Zero-Copy Clone ===")  # Heading.
print("A shallow clone shares the source's data files but has its own transaction log.")
print("Use cases: testing, dev environments, experimentation without copying data.\n")

# Create shallow clone.
print("--- Creating Shallow Clone ---")
spark.sql("DROP TABLE IF EXISTS prop_demo_orders_shallow")  # Clean.
spark.sql("CREATE TABLE prop_demo_orders_shallow SHALLOW CLONE prop_demo_orders")
print("Shallow clone created!")

# Compare sizes.
print("\n--- Comparing Source vs Clone ---")
source_detail = spark.sql("DESCRIBE DETAIL prop_demo_orders").collect()[0]
clone_detail = spark.sql("DESCRIBE DETAIL prop_demo_orders_shallow").collect()[0]
print(f"  Source: {source_detail['numFiles']} files, {source_detail['sizeInBytes']/1024:.0f} KB")
print(f"  Clone:  {clone_detail['numFiles']} files, {clone_detail['sizeInBytes']/1024:.0f} KB")
print(f"  Extra storage used by clone: ~0 bytes (just metadata!)")

# Clone has same data.
print(f"\n  Source rows: {spark.table('prop_demo_orders').count()}")
print(f"  Clone rows:  {spark.table('prop_demo_orders_shallow').count()}")

# Modify clone without affecting source.
print("\n--- Modifying Clone (source unaffected) ---")
spark.sql("DELETE FROM prop_demo_orders_shallow WHERE order_id > 9000")
print(f"  Source rows: {spark.table('prop_demo_orders').count()} (unchanged!)")
print(f"  Clone rows:  {spark.table('prop_demo_orders_shallow').count()} (rows deleted)")

# Clone history shows it was cloned.
print("\n--- Clone History ---")
display(spark.sql("DESCRIBE HISTORY prop_demo_orders_shallow").select("version", "operation", "operationParameters").limit(3))

print("\n⚠️ Shallow Clone Warning:")
print("  If you VACUUM the source table, the clone may break!")
print("  The clone's data files are the source's files.")
print("  For safety in production, use DEEP CLONE.")

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Example 2: Deep Clone
# SECTION 4 — INTERMEDIATE EXAMPLE 2: DEEP CLONE
# Real-world: Create a fully independent backup or migrate a table.

from delta.tables import DeltaTable  # Import.

print("=== DEEP CLONE: Full Independent Copy ===")  # Heading.
print("Deep clone copies ALL data files. Takes longer but is fully independent.")
print("Use cases: backups, migrations, archival, cross-region copies.\n")

# Create deep clone.
print("--- Creating Deep Clone ---")
spark.sql("DROP TABLE IF EXISTS prop_demo_orders_deep")  # Clean.
spark.sql("CREATE TABLE prop_demo_orders_deep DEEP CLONE prop_demo_orders")
print("Deep clone created!")

# Compare.
print("\n--- Comparing ---")
src = spark.sql("DESCRIBE DETAIL prop_demo_orders").collect()[0]
deep = spark.sql("DESCRIBE DETAIL prop_demo_orders_deep").collect()[0]
print(f"  Source: {src['numFiles']} files, {src['sizeInBytes']/1024:.0f} KB")
print(f"  Deep Clone: {deep['numFiles']} files, {deep['sizeInBytes']/1024:.0f} KB")
print(f"  → Deep clone uses its OWN storage (independent copy)")

# Deep clone is safe even if source is deleted.
print("\n--- Deep Clone is Safe ---")
print("  Even if source table is dropped or VACUUM'd, deep clone works fine.")
print("  Each table has its own independent set of Parquet files.")

# Incremental deep clone (update clone from source).
print("\n--- Incremental Clone (Sync) ---")
print("If source gets new data, you can sync the clone:")
print("  CREATE OR REPLACE TABLE target DEEP CLONE source")
print("  → Only copies files that changed (incremental!)")

# Add some data to source, then re-clone.
spark.sql("INSERT INTO prop_demo_orders VALUES (99999, 1, 999.99, '2024-12-31', 'online', null, null)")
print(f"\nSource after insert: {spark.table('prop_demo_orders').count()} rows")
print(f"Deep clone (stale): {spark.table('prop_demo_orders_deep').count()} rows")

# Re-sync.
spark.sql("CREATE OR REPLACE TABLE prop_demo_orders_deep DEEP CLONE prop_demo_orders")
print(f"Deep clone (synced): {spark.table('prop_demo_orders_deep').count()} rows")
print("✓ Clone synced with source!")

# COMMAND ----------

# DBTITLE 1,Section 4 - Intermediate Example 3: Column Changes
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Advanced ALTER TABLE Operations
# Real-world: Column type changes, reordering, and dropping.

print("=== Advanced ALTER TABLE ===")  # Heading.

# Create a fresh table for demos.
spark.sql("DROP TABLE IF EXISTS alter_demo")  # Clean.
spark.sql("""
    CREATE TABLE alter_demo (
        id INT,
        name STRING,
        amount FLOAT,
        status STRING,
        created_date STRING
    ) USING DELTA
""")
spark.sql("INSERT INTO alter_demo VALUES (1,'Alice',100.5,'active','2024-01-15'), (2,'Bob',200.75,'inactive','2024-02-20')")
print("Original schema:")
display(spark.sql("DESCRIBE TABLE alter_demo"))

# CHANGE COLUMN TYPE (widen: INT → BIGINT, FLOAT → DOUBLE).
print("\n--- Change Column Type ---")
spark.sql("ALTER TABLE alter_demo CHANGE COLUMN id id BIGINT")  # INT to BIGINT.
spark.sql("ALTER TABLE alter_demo CHANGE COLUMN amount amount DOUBLE")  # FLOAT to DOUBLE.
print("Changed: id INT→BIGINT, amount FLOAT→DOUBLE")

# REORDER COLUMNS.
print("\n--- Reorder Columns ---")
spark.sql("ALTER TABLE alter_demo CHANGE COLUMN status status STRING AFTER id")  # Move status.
print("Moved 'status' column to after 'id'")
display(spark.sql("DESCRIBE TABLE alter_demo"))

# DROP COLUMN (requires column mapping).
print("\n--- Drop Column ---")
# Enable column mapping first (required for drop).
spark.sql("""ALTER TABLE alter_demo SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5'
)""")
spark.sql("ALTER TABLE alter_demo DROP COLUMN created_date")  # Drop.
print("Dropped 'created_date' column")
display(spark.sql("DESCRIBE TABLE alter_demo"))

# Final state.
print("\n--- Final Table ---")
display(spark.sql("SELECT * FROM alter_demo"))

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Examples
# SECTION 5 — ADVANCED EXAMPLE 1: Production Table Setup Template
# Real-world: The complete setup for a production Delta table.

print("=== Production Table Setup Template ===")  # Heading.

spark.sql("DROP TABLE IF EXISTS production_events")  # Clean.

# Create with ALL best practices.
spark.sql("""
    CREATE TABLE production_events (
        event_id BIGINT COMMENT 'Unique event identifier',
        user_id INT COMMENT 'User who triggered the event',
        event_type STRING COMMENT 'Type of event (click, purchase, view)',
        event_data STRING COMMENT 'JSON payload of event details',
        amount DOUBLE COMMENT 'Transaction amount if applicable',
        event_timestamp TIMESTAMP COMMENT 'When the event occurred',
        processing_date DATE COMMENT 'Date event was processed by ETL'
    )
    USING DELTA
    CLUSTER BY (event_type, processing_date)
    COMMENT 'Production event stream table. Updated every 5 minutes via streaming pipeline.'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.logRetentionDuration' = '45 days',
        'delta.deletedFileRetentionDuration' = '14 days',
        'delta.enableChangeDataFeed' = 'true',
        'delta.dataSkippingNumIndexedCols' = '7',
        'quality.team' = 'data-engineering',
        'quality.sla' = '99.9%',
        'quality.owner' = 'data-team@company.com'
    )
""")

print("Production table created with:")
print("  ✓ Column-level comments (documentation)")
print("  ✓ Table-level comment (description)")
print("  ✓ Liquid Clustering (automatic optimization)")
print("  ✓ Auto-optimize (prevent small files)")
print("  ✓ Change Data Feed (track changes)")
print("  ✓ Custom properties (team ownership, SLA)")
print("  ✓ Extended log retention (45 days for compliance)")
print("  ✓ Data skipping on all 7 columns")

# Show the full description.
print("\n--- Full Table Description ---")
display(spark.sql("DESCRIBE EXTENDED production_events"))

# Show properties.
print("\n--- All Properties ---")
display(spark.sql("SHOW TBLPROPERTIES production_events"))

# COMMAND ----------

# DBTITLE 1,Section 5 - Advanced Example 2: Clone for Testing
# SECTION 5 — ADVANCED EXAMPLE 2: Clone-Based Testing Strategy
# Real-world: Use clones to test schema changes safely.

from pyspark.sql.functions import col, lit, expr, rand  # Imports.

print("=== Clone-Based Testing Strategy ===")  # Heading.
print("Pattern: Clone prod → test changes on clone → if good, apply to prod\n")

# Setup: Production table with data.
spark.sql("DROP TABLE IF EXISTS prod_customers")  # Clean.
spark.sql("""
    CREATE TABLE prod_customers (
        id INT, name STRING, email STRING, tier STRING, balance DOUBLE
    ) USING DELTA
""")
spark.sql("""
    INSERT INTO prod_customers VALUES
    (1, 'Alice', 'alice@co.com', 'gold', 5000.0),
    (2, 'Bob', 'bob@co.com', 'silver', 2000.0),
    (3, 'Carol', 'carol@co.com', 'bronze', 500.0)
""")
print("Production table: prod_customers")
display(spark.sql("SELECT * FROM prod_customers"))

# Step 1: Clone for testing.
print("\n--- Step 1: Create test clone ---")
spark.sql("DROP TABLE IF EXISTS test_customers")  # Clean.
spark.sql("CREATE TABLE test_customers SHALLOW CLONE prod_customers")
print("Created shallow clone: test_customers")

# Step 2: Test schema change on clone.
print("\n--- Step 2: Test schema change on clone ---")
spark.sql("ALTER TABLE test_customers ADD COLUMNS (loyalty_points INT COMMENT 'Calculated loyalty points')")
spark.sql("UPDATE test_customers SET loyalty_points = CAST(balance * 10 AS INT)")
print("Added loyalty_points column and populated it:")
display(spark.sql("SELECT * FROM test_customers"))

# Step 3: Validate.
print("\n--- Step 3: Validate ---")
assert spark.table("test_customers").filter("loyalty_points IS NULL").count() == 0, "Nulls found!"
assert spark.table("test_customers").filter("loyalty_points < 0").count() == 0, "Negatives found!"
print("✓ All validations passed!")

# Step 4: Apply to production.
print("\n--- Step 4: Apply to production (confirmed safe) ---")
spark.sql("ALTER TABLE prod_customers ADD COLUMNS (loyalty_points INT COMMENT 'Calculated loyalty points')")
spark.sql("UPDATE prod_customers SET loyalty_points = CAST(balance * 10 AS INT)")
print("Applied to production:")
display(spark.sql("SELECT * FROM prod_customers"))

# Cleanup test.
spark.sql("DROP TABLE IF EXISTS test_customers")
print("\n✓ Test clone dropped. Production safely updated!")

# COMMAND ----------

# DBTITLE 1,Section 6 and 7 - Takeaways and Exercises
# MAGIC %md
# MAGIC ## SECTION 6 — Key Takeaways
# MAGIC
# MAGIC ### Essential Properties for Production Tables
# MAGIC
# MAGIC | Property | Recommended Value | Purpose |
# MAGIC |----------|------------------|---------|
# MAGIC | `delta.autoOptimize.optimizeWrite` | `true` | Prevent small files |
# MAGIC | `delta.autoOptimize.autoCompact` | `true` | Auto-compact after writes |
# MAGIC | `delta.logRetentionDuration` | `30-90 days` | Keep history for compliance |
# MAGIC | `delta.deletedFileRetentionDuration` | `7-14 days` | Time travel window |
# MAGIC | `delta.enableChangeDataFeed` | `true` | Track row-level changes |
# MAGIC | `delta.columnMapping.mode` | `name` | Enable column rename/drop |
# MAGIC
# MAGIC ### Clone Comparison
# MAGIC
# MAGIC | Feature | Shallow Clone | Deep Clone |
# MAGIC |---------|--------------|------------|
# MAGIC | Speed | Instant | Proportional to data size |
# MAGIC | Storage | Near zero | Full copy |
# MAGIC | Independence | Depends on source | Fully independent |
# MAGIC | Use case | Testing, dev | Backups, migration |
# MAGIC | VACUUM safe | No (source VACUUM breaks it) | Yes |
# MAGIC
# MAGIC ### Best Practices
# MAGIC 1. Always add comments to columns AND the table
# MAGIC 2. Set properties at table creation time
# MAGIC 3. Use shallow clone for testing, deep clone for backups
# MAGIC 4. Enable column mapping mode for flexibility
# MAGIC 5. Use custom properties for governance metadata (team, SLA, owner)

# COMMAND ----------

# DBTITLE 1,Section 7 - Practice Exercises
# SECTION 7 — HOMEWORK & SOLUTIONS

print("="*60)
print("HOMEWORK — Delta Table Properties & Management")
print("="*60)

# Level 1: Create table and DESCRIBE it.
print("\n=== Level 1: DESCRIBE ===")
spark.sql("DROP TABLE IF EXISTS hw58_l1")
spark.sql("CREATE TABLE hw58_l1 (id INT, name STRING, score DOUBLE) USING DELTA")
spark.sql("INSERT INTO hw58_l1 VALUES (1,'A',90.5), (2,'B',85.0)")
display(spark.sql("DESCRIBE DETAIL hw58_l1"))

# Level 2: Set properties.
print("\n=== Level 2: Set Properties ===")
spark.sql("ALTER TABLE hw58_l1 SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true')")
display(spark.sql("SHOW TBLPROPERTIES hw58_l1"))

# Level 3: Add columns with comments.
print("\n=== Level 3: ADD COLUMNS ===")
spark.sql("ALTER TABLE hw58_l1 ADD COLUMNS (grade STRING COMMENT 'Letter grade')")
spark.sql("UPDATE hw58_l1 SET grade = CASE WHEN score >= 90 THEN 'A' ELSE 'B' END")
display(spark.sql("SELECT * FROM hw58_l1"))

# Level 4: Create a shallow clone.
print("\n=== Level 4: SHALLOW CLONE ===")
spark.sql("DROP TABLE IF EXISTS hw58_clone")
spark.sql("CREATE TABLE hw58_clone SHALLOW CLONE hw58_l1")
print(f"Source: {spark.table('hw58_l1').count()} rows")
print(f"Clone: {spark.table('hw58_clone').count()} rows")
spark.sql("INSERT INTO hw58_clone VALUES (3,'C',75.0,'C')")
print(f"After insert to clone: {spark.table('hw58_clone').count()} rows")
print(f"Source unchanged: {spark.table('hw58_l1').count()} rows")

# Level 5: Full production setup.
print("\n=== Level 5: Production Setup ===")
spark.sql("DROP TABLE IF EXISTS hw58_prod")
spark.sql("""
CREATE TABLE hw58_prod (
    txn_id BIGINT COMMENT 'Transaction ID',
    amount DOUBLE COMMENT 'Amount in USD',
    txn_date DATE COMMENT 'Transaction date'
) USING DELTA
COMMENT 'Homework production table'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'quality.owner' = 'student'
)
""")
print("Production table created with all best practices!")
display(spark.sql("SHOW TBLPROPERTIES hw58_prod"))

print("\n" + "="*60)
print("All exercises completed!")
print("="*60)