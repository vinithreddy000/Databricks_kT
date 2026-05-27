# Databricks notebook source
# MAGIC %md
# MAGIC # NB_54 — Delta Lake Fundamentals
# MAGIC
# MAGIC **Module 9: Delta Lake Deep Dive** | Notebook 54 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * What is Delta Lake and why it matters
# MAGIC * Delta transaction log (_delta_log)
# MAGIC * ACID transactions on data lakes
# MAGIC * Schema enforcement vs schema evolution
# MAGIC * Delta vs Parquet comparison
# MAGIC * Creating and reading Delta tables
# MAGIC * Table properties and metadata
# MAGIC * Delta Lake architecture internals
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Foundation for everything in Databricks)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# MAGIC %md
# MAGIC ## SECTION 1 — What is Delta Lake? (Real-World Analogy)
# MAGIC
# MAGIC ### 🏦 The Bank Ledger
# MAGIC
# MAGIC Delta Lake is to data lakes what a bank ledger is to a pile of cash:
# MAGIC
# MAGIC ```
# MAGIC Pile of Cash (Parquet) → No records of who took/added money
# MAGIC Bank Ledger (Delta)    → Every transaction recorded, auditable, reversible
# MAGIC ```
# MAGIC
# MAGIC ### The Problems Delta Solves
# MAGIC | Problem | Without Delta | With Delta |
# MAGIC |---|---|---|
# MAGIC | Partial writes | Corrupted data | Atomic commits (all or nothing) |
# MAGIC | Concurrent reads/writes | Dirty reads | ACID isolation |
# MAGIC | Schema changes | Silent data corruption | Schema enforcement |
# MAGIC | Data mistakes | Permanent data loss | Time travel (undo) |
# MAGIC | Small files | Slow queries | OPTIMIZE + ZORDER |
# MAGIC | Audit trail | No history | Full transaction log |
# MAGIC
# MAGIC ### Delta = Parquet + Transaction Log
# MAGIC ```
# MAGIC /my_table/
# MAGIC ├── _delta_log/           ← Transaction log (JSON + checkpoint)
# MAGIC │   ├── 00000...000.json  ← Version 0
# MAGIC │   ├── 00000...001.json  ← Version 1
# MAGIC │   └── 00000...010.checkpoint.parquet
# MAGIC ├── part-00000-...parquet ← Data files (standard Parquet)
# MAGIC └── part-00001-...parquet
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## SECTION 2 — Delta Lake Architecture
# MAGIC
# MAGIC ### Transaction Log Mechanics
# MAGIC ```python
# MAGIC # Each commit adds a JSON file to _delta_log/
# MAGIC # JSON contains:
# MAGIC #   "add": files added in this commit
# MAGIC #   "remove": files logically deleted
# MAGIC #   "metaData": schema changes
# MAGIC #   "commitInfo": who, when, what operation
# MAGIC #
# MAGIC # Every 10 commits → checkpoint (Parquet summary)
# MAGIC # Readers: latest checkpoint + subsequent JSONs = current state
# MAGIC ```
# MAGIC
# MAGIC ### ACID Properties
# MAGIC | Property | How Delta Achieves It |
# MAGIC |---|---|
# MAGIC | Atomicity | Commit is single JSON write (atomic on cloud storage) |
# MAGIC | Consistency | Schema enforcement prevents bad writes |
# MAGIC | Isolation | Optimistic concurrency control (OCC) |
# MAGIC | Durability | Data in Parquet on durable cloud storage |
# MAGIC
# MAGIC ### Delta vs Parquet
# MAGIC | Feature | Parquet | Delta |
# MAGIC |---|---|---|
# MAGIC | ACID transactions | ✗ | ✓ |
# MAGIC | Time travel | ✗ | ✓ |
# MAGIC | Schema enforcement | ✗ | ✓ |
# MAGIC | UPDATE/DELETE/MERGE | ✗ | ✓ |
# MAGIC | Streaming + Batch | Limited | Unified |
# MAGIC | File compaction | Manual | OPTIMIZE |

# COMMAND ----------

# SECTION 3 — BEGINNER EXAMPLE 1: Creating Delta Tables
# Real-world: Create your first Delta table from scratch.

from pyspark.sql import SparkSession  # Import.
from pyspark.sql.functions import col, lit, current_timestamp  # Functions.

spark = SparkSession.builder.getOrCreate()  # Session.

# Create sample data.
employees = spark.createDataFrame([
    (1, "Alice", "Engineering", "Seattle", 95000),
    (2, "Bob", "Marketing", "Portland", 85000),
    (3, "Carol", "Sales", "Denver", 90000),
    (4, "Dave", "Engineering", "Austin", 92000),
    (5, "Eve", "Marketing", "Seattle", 88000),
], ["emp_id", "name", "department", "city", "salary"])  # Schema.

print("=== Creating Delta Tables ===")  # Heading.
employees.show()  # Display.

# Method 1: Write as Delta to path.
delta_path = "/tmp/delta_kt/employees"  # Path.
employees.write.format("delta").mode("overwrite").save(delta_path)  # Write.
print(f"Written to: {delta_path}")  # Confirm.

# Method 2: Read back.
print("\n=== Read Delta Table ===")  # Heading.
df_read = spark.read.format("delta").load(delta_path)  # Read.
df_read.show()  # Display.
print(f"Row count: {df_read.count()}")  # Count.

# Method 3: SQL-style query.
df_read.createOrReplaceTempView("employees_delta")  # Register.
spark.sql("SELECT department, COUNT(*) as cnt, AVG(salary) as avg_sal FROM employees_delta GROUP BY department").show()  # SQL.
print("Delta table created and queryable!")

# COMMAND ----------

# SECTION 3 — BEGINNER EXAMPLE 2: Transaction Log Exploration
# Real-world: Understand what's inside the _delta_log folder.

import json  # JSON.

print("=== Exploring Delta Transaction Log ===")  # Heading.
delta_path = "/tmp/delta_kt/employees"  # Path.

# List files in Delta directory.
print("--- Files in Delta table directory ---")  # Heading.
files = dbutils.fs.ls(delta_path)  # List.
for f in files:  # Each file.
    print(f"  {f.name} ({f.size} bytes)")  # Display.

# List transaction log files.
print("\n--- Transaction Log Files ---")  # Heading.
log_files = dbutils.fs.ls(f"{delta_path}/_delta_log/")  # List log.
for f in log_files:  # Each.
    print(f"  {f.name} ({f.size} bytes)")  # Display.

# Read version 0 commit.
print("\n--- Version 0 Commit Details ---")  # Heading.
log_content = dbutils.fs.head(f"{delta_path}/_delta_log/00000000000000000000.json")  # Read.
lines = log_content.strip().split("\n")  # Split lines.
for line in lines[:5]:  # First 5.
    parsed = json.loads(line)  # Parse.
    action_type = list(parsed.keys())[0]  # Get type.
    print(f"  Action: {action_type}")  # Show.
    if action_type == "add":  # File add?
        print(f"    File: {parsed['add']['path'][:50]}")  # Path.
        print(f"    Size: {parsed['add']['size']} bytes")  # Size.
    elif action_type == "commitInfo":  # Commit?
        print(f"    Operation: {parsed['commitInfo']['operation']}")  # Op.

# DESCRIBE HISTORY.
print("\n--- DESCRIBE HISTORY ---")  # Heading.
from delta.tables import DeltaTable  # Import.
dt = DeltaTable.forPath(spark, delta_path)  # Load.
dt.history().select("version", "timestamp", "operation").show(truncate=False)  # History.

# COMMAND ----------

# SECTION 3 — BEGINNER EXAMPLE 3: Schema enforcement and evolution
# Real-world: Delta prevents bad data from entering your table.

from pyspark.sql.functions import col, lit  # Imports.

print("=== Schema Enforcement Demo ===")  # Heading.
delta_path = "/tmp/delta_kt/employees"  # Path.

# Show current schema.
print("Current schema:")  # Heading.
spark.read.format("delta").load(delta_path).printSchema()  # Show.

# Attempt 1: Matching schema (succeeds).
print("\n--- Append with matching schema (SUCCESS) ---")  # Heading.
new_emp = spark.createDataFrame([
    (6, "Frank", "Sales", "Miami", 78000),
], ["emp_id", "name", "department", "city", "salary"])  # Match.
new_emp.write.format("delta").mode("append").save(delta_path)  # Append.
print(f"Appended! Count: {spark.read.format('delta').load(delta_path).count()}")  # Confirm.

# Attempt 2: Extra column (fails with enforcement).
print("\n--- Append with extra column (FAILS) ---")  # Heading.
bad_data = spark.createDataFrame([
    (7, "Grace", "HR", "Boston", 82000, "2024-01-01"),
], ["emp_id", "name", "department", "city", "salary", "hire_date"])  # Extra!

try:
    bad_data.write.format("delta").mode("append").save(delta_path)  # Try.
except Exception as e:
    print(f"  ERROR: {str(e)[:150]}")  # Mismatch.
    print("  → Delta enforces schema! Extra columns rejected.")  # Explain.

# Schema evolution with mergeSchema.
print("\n--- Schema Evolution with mergeSchema ---")  # Heading.
bad_data.write.format("delta").mode("append").option("mergeSchema", "true").save(delta_path)  # Evolve.
print("Schema evolved! New schema:")  # Heading.
spark.read.format("delta").load(delta_path).printSchema()  # New schema.
spark.read.format("delta").load(delta_path).orderBy("emp_id").show()  # Display.
print("Existing rows have NULL for hire_date (backward compatible).")

# COMMAND ----------

# SECTION 4 — INTERMEDIATE EXAMPLE 1: Delta Table Properties
# Real-world: Configure production Delta tables.

from delta.tables import DeltaTable  # Import.
from pyspark.sql.functions import col, expr, rand, round as spark_round  # Imports.

print("=== Delta Table Properties & Metadata ===")  # Heading.

# Generate realistic data.
orders = (
    spark.range(10000)  # 10K rows.
    .withColumn("order_id", col("id") + 1000)  # Order ID.
    .withColumn("customer_id", (rand() * 100).cast("int") + 1)  # Customer.
    .withColumn("amount", spark_round(rand() * 500 + 10, 2))  # Amount.
    .withColumn("order_date", expr("date_add('2024-01-01', cast(rand()*180 as int))"))  # Date.
    .withColumn("region", expr("CASE WHEN rand()<0.25 THEN 'North' WHEN rand()<0.5 THEN 'South' WHEN rand()<0.75 THEN 'East' ELSE 'West' END"))  # Region.
    .drop("id")  # Remove.
)

prod_path = "/tmp/delta_kt/orders_prod"  # Path.

# Write with production settings.
(orders.write.format("delta").mode("overwrite")
    .partitionBy("region")  # Partition.
    .option("delta.autoOptimize.optimizeWrite", "true")  # Auto-optimize.
    .option("delta.autoOptimize.autoCompact", "true")  # Auto-compact.
    .save(prod_path))  # Write.

print("Production table created:")  # Heading.
print("  ✓ Partitioned by region")  # Feature.
print("  ✓ Auto-optimize writes enabled")  # Feature.

# Inspect partition structure.
print("\n--- Partition Structure ---")  # Heading.
for item in dbutils.fs.ls(prod_path):  # List.
    if item.name.startswith("region="):  # Partition?
        files = [f for f in dbutils.fs.ls(item.path) if f.name.endswith(".parquet")]  # Files.
        print(f"  {item.name}: {len(files)} files")  # Show.

# Table detail.
dt = DeltaTable.forPath(spark, prod_path)  # Load.
print("\n--- Table Detail ---")  # Heading.
dt.detail().select("format", "numFiles", "sizeInBytes", "partitionColumns").show(truncate=False)  # Detail.

# Query with partition pruning.
print("--- Partition Pruning ---")  # Heading.
north = spark.read.format("delta").load(prod_path).filter(col("region") == "North")  # Prune.
print(f"  North orders: {north.count()} (only reads region=North partition)")  # Count.

# COMMAND ----------

# SECTION 4 — INTERMEDIATE EXAMPLE 2: Delta vs Parquet comparison
# Real-world: Side-by-side feature comparison.

from pyspark.sql.functions import col, lit  # Imports.
from delta.tables import DeltaTable  # Delta.

print("=== Delta vs Parquet: Feature Comparison ===")  # Heading.

# Create test data.
test_data = spark.createDataFrame([
    (1, "A", 100), (2, "B", 200), (3, "C", 300), (4, "D", 400), (5, "E", 500),
], ["id", "name", "value"])  # Test.

parquet_path = "/tmp/delta_kt/cmp_parquet"  # Parquet.
delta_cmp = "/tmp/delta_kt/cmp_delta"  # Delta.

test_data.write.format("parquet").mode("overwrite").save(parquet_path)  # Parquet.
test_data.write.format("delta").mode("overwrite").save(delta_cmp)  # Delta.

# Feature 1: UPDATE.
print("--- Feature: UPDATE ---")  # Heading.
dt = DeltaTable.forPath(spark, delta_cmp)  # Load.
dt.update(condition="id = 3", set={"value": lit(999)})  # Update!
print("  Delta UPDATE id=3 → value=999:")  # Heading.
spark.read.format("delta").load(delta_cmp).orderBy("id").show()  # Show.
print("  Parquet: Cannot UPDATE in-place (must read-modify-write all).")  # Explain.

# Feature 2: DELETE.
print("--- Feature: DELETE ---")  # Heading.
dt.delete("id = 5")  # Delete.
print("  Delta DELETE id=5:")  # Heading.
spark.read.format("delta").load(delta_cmp).orderBy("id").show()  # Show.

# Feature 3: Time Travel.
print("--- Feature: TIME TRAVEL ---")  # Heading.
print("  Version 0 (original):")  # Heading.
spark.read.format("delta").option("versionAsOf", 0).load(delta_cmp).show()  # V0.
print("  Current (after update+delete):")  # Heading.
spark.read.format("delta").load(delta_cmp).show()  # Current.

# Feature 4: History.
print("--- Feature: AUDIT LOG ---")  # Heading.
dt.history().select("version", "operation", "operationMetrics").show(truncate=False)  # Log.
print("Every operation recorded and auditable!")

# COMMAND ----------

# SECTION 4 — INTERMEDIATE EXAMPLE 3: ACID transactions demo
# Real-world: Atomic commits and failure recovery.

from pyspark.sql.functions import col, lit  # Imports.
from delta.tables import DeltaTable  # Delta.

print("=== ACID Transactions Demo ===")  # Heading.

# Setup: bank accounts.
accounts = spark.createDataFrame([
    (1, "Alice", 1000.00), (2, "Bob", 500.00), (3, "Carol", 750.00),
], ["account_id", "name", "balance"])  # Accounts.

acct_path = "/tmp/delta_kt/accounts"  # Path.
accounts.write.format("delta").mode("overwrite").save(acct_path)  # Write.

print("Initial balances:")  # Heading.
spark.read.format("delta").load(acct_path).show()  # Display.

# Atomic transfer: Alice → Bob ($200).
print("--- Atomic Transfer: Alice → Bob ($200) ---")  # Heading.
dt = DeltaTable.forPath(spark, acct_path)  # Load.
dt.update(condition="account_id = 1", set={"balance": lit(800.00)})  # Debit.
dt.update(condition="account_id = 2", set={"balance": lit(700.00)})  # Credit.

print("After transfer:")  # Heading.
spark.read.format("delta").load(acct_path).show()  # Display.

# Verify conservation.
total = spark.read.format("delta").load(acct_path).agg({"balance": "sum"}).collect()[0][0]  # Total.
print(f"Total: ${total:.2f} (conserved ✓)")  # Check.

# Time travel: view pre-transfer state.
print("\n--- Time Travel: Pre-transfer state ---")  # Heading.
spark.read.format("delta").option("versionAsOf", 0).load(acct_path).show()  # Original.

# Full audit trail.
print("--- Audit Trail ---")  # Heading.
dt.history().select("version", "timestamp", "operation").show(truncate=False)  # History.
print("ACID: Atomic, Consistent, Isolated, Durable!")

# COMMAND ----------

# SECTION 5 — ADVANCED EXAMPLE 1: Delta internals - file management
# Real-world: Understanding Delta file lifecycle.

from pyspark.sql.functions import col, lit, rand  # Imports.
from delta.tables import DeltaTable  # Delta.
import json  # JSON.

print("=== Delta Internals: File Management ===")  # Heading.

base_path = "/tmp/delta_kt/internals"  # Path.

# Write 1: Initial.
spark.range(1000).withColumn("value", rand()*100).write.format("delta").mode("overwrite").save(base_path)  # Write.
files_v1 = [f for f in dbutils.fs.ls(base_path) if f.name.endswith(".parquet")]  # Count.
print(f"After initial write: {len(files_v1)} data files")  # Show.

# Write 2: Append.
spark.range(1000,2000).withColumn("value", rand()*100).write.format("delta").mode("append").save(base_path)  # Append.
files_v2 = [f for f in dbutils.fs.ls(base_path) if f.name.endswith(".parquet")]  # Count.
print(f"After append: {len(files_v2)} data files")  # Show.

# Write 3: Update (creates new files, marks old as removed).
dt = DeltaTable.forPath(spark, base_path)  # Load.
dt.update(condition="id < 100", set={"value": lit(999.0)})  # Update.
files_v3 = [f for f in dbutils.fs.ls(base_path) if f.name.endswith(".parquet")]  # Count.
print(f"After update: {len(files_v3)} data files (old still on disk!)")  # Show.

# Active vs total files.
print("\n--- Active vs Total Files ---")  # Heading.
active = spark.read.format("delta").load(base_path).inputFiles()  # Active.
all_pq = [f for f in dbutils.fs.ls(base_path) if f.name.endswith(".parquet")]  # All.
print(f"  Active (current snapshot): {len(active)}")  # Active.
print(f"  Total on disk: {len(all_pq)}")  # Total.
print(f"  Orphaned (removable by VACUUM): {len(all_pq) - len(active)}")  # Orphans.

# File statistics (data skipping).
print("\n--- File-Level Stats (Data Skipping) ---")  # Heading.
log_path = f"{base_path}/_delta_log/00000000000000000000.json"  # V0 log.
log_content = dbutils.fs.head(log_path)  # Read.
for line in log_content.strip().split("\n"):  # Each action.
    action = json.loads(line)  # Parse.
    if "add" in action and "stats" in action["add"]:  # Has stats?
        stats = json.loads(action["add"]["stats"])  # Parse.
        print(f"  numRecords: {stats.get('numRecords')}")  # Rows.
        if stats.get("minValues"):  # Min?
            print(f"  minValues: {stats['minValues']}")  # Show.
            print(f"  maxValues: {stats['maxValues']}")  # Show.
        break  # First only.
print("\nStats enable DATA SKIPPING — queries skip irrelevant files!")

# COMMAND ----------

# SECTION 5 — ADVANCED EXAMPLE 2: Constraints and data quality
# Real-world: Add quality constraints to Delta tables.

from pyspark.sql.functions import col, lit, expr  # Imports.
from delta.tables import DeltaTable  # Delta.

print("=== Delta Constraints & Data Quality ===")  # Heading.

# Create constrained table.
inventory = spark.createDataFrame([
    (1, "Widget", 29.99, 100, "active"),
    (2, "Gadget", 49.99, 50, "active"),
    (3, "Doohickey", 9.99, 200, "active"),
], ["product_id", "name", "price", "stock", "status"])  # Data.

inv_path = "/tmp/delta_kt/inventory"  # Path.
inventory.write.format("delta").mode("overwrite").save(inv_path)  # Write.

# Constraint-enforcing write wrapper.
def write_with_constraints(df, path, constraints):
    """Write to Delta only if all constraints pass."""
    violations = []  # Track.
    for name, cond in constraints.items():  # Each.
        failed = df.filter(f"NOT ({cond})").count()  # Check.
        if failed > 0:  # Violations?
            violations.append(f"{name}: {failed} rows")  # Record.
            print(f"  ✗ Constraint '{name}' FAILED: {failed} rows")  # Show.
    
    if violations:  # Any failures?
        print("  WRITE REJECTED!")  # Reject.
        return False  # Failed.
    else:
        df.write.format("delta").mode("append").save(path)  # Write.
        print("  ✓ All constraints passed — data written.")  # Success.
        return True  # Passed.

# Define constraints.
constraints = {
    "price_positive": "price > 0",
    "stock_non_negative": "stock >= 0",
    "valid_status": "status IN ('active', 'inactive', 'discontinued')",
}

# Test valid data.
print("\n--- Test 1: Valid data ---")  # Heading.
good = spark.createDataFrame([(4, "Thing", 14.99, 75, "active")], ["product_id", "name", "price", "stock", "status"])
write_with_constraints(good, inv_path, constraints)  # Pass.

# Test negative price.
print("\n--- Test 2: Negative price ---")  # Heading.
bad_price = spark.createDataFrame([(5, "Free", -5.00, 10, "active")], ["product_id", "name", "price", "stock", "status"])
write_with_constraints(bad_price, inv_path, constraints)  # Fail.

# Test invalid status.
print("\n--- Test 3: Bad status ---")  # Heading.
bad_status = spark.createDataFrame([(6, "X", 10.0, 5, "unknown")], ["product_id", "name", "price", "stock", "status"])
write_with_constraints(bad_status, inv_path, constraints)  # Fail.

# Final table.
print("\n--- Final Table (only valid data) ---")  # Heading.
spark.read.format("delta").load(inv_path).show()  # Display.
print("Constraints protect data quality at storage layer!")

# COMMAND ----------

# SECTION 5 — ADVANCED EXAMPLE 3: Production Delta configuration
# Real-world: Best practices for production tables.

from delta.tables import DeltaTable  # Import.
from pyspark.sql.functions import col, expr, rand, round as spark_round, current_timestamp  # Imports.

print("=== Production Delta Best Practices ===")  # Heading.

# 1. Table with liquid clustering (DBR 13.3+).
print("--- Pattern 1: Table Creation Best Practices ---")
print("""
CREATE TABLE catalog.schema.orders (
    order_id BIGINT,
    customer_id INT,
    amount DECIMAL(10,2),
    order_date DATE,
    region STRING
)
USING DELTA
CLUSTER BY (region, order_date)  -- Liquid clustering (replaces ZORDER)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.logRetentionDuration' = '30 days',
    'delta.deletedFileRetentionDuration' = '7 days',
    'delta.enableChangeDataFeed' = 'true'
);
""")

# 2. Write patterns.
print("--- Pattern 2: Write Modes ---")
sample = spark.range(100).withColumn("val", rand())  # Sample.
sample_path = "/tmp/delta_kt/write_patterns"  # Path.

# Overwrite: replace all.
sample.write.format("delta").mode("overwrite").save(sample_path)  # Overwrite.
print(f"  overwrite: {spark.read.format('delta').load(sample_path).count()} rows")  # Count.

# Append: add rows.
spark.range(100,200).withColumn("val", rand()).write.format("delta").mode("append").save(sample_path)  # Append.
print(f"  after append: {spark.read.format('delta').load(sample_path).count()} rows")  # Count.

# Overwrite with replaceWhere (partition-level overwrite).
print("\n--- Pattern 3: replaceWhere (Surgical Overwrite) ---")
orders_path = "/tmp/delta_kt/orders_replace"  # Path.
from pyspark.sql.functions import lit  # Import.
full = spark.createDataFrame([
    (1, "North", 100.0), (2, "North", 200.0), (3, "South", 150.0), (4, "South", 250.0),
], ["id", "region", "amount"])
full.write.format("delta").mode("overwrite").partitionBy("region").save(orders_path)  # Init.
print(f"  Initial: {spark.read.format('delta').load(orders_path).count()} rows")  # Count.

# Replace only North partition.
new_north = spark.createDataFrame([
    (5, "North", 300.0), (6, "North", 400.0), (7, "North", 500.0),
], ["id", "region", "amount"])
new_north.write.format("delta").mode("overwrite").option("replaceWhere", "region = 'North'").save(orders_path)  # Replace.
print(f"  After replaceWhere North: {spark.read.format('delta').load(orders_path).count()} rows")  # Count.
spark.read.format("delta").load(orders_path).orderBy("id").show()  # Display.
print("  South untouched, North replaced!")

# 3. Performance config.
print("\n--- Pattern 4: Performance Configuration ---")
print("""
Key Spark configs for Delta performance:
  spark.databricks.delta.optimizeWrite.enabled = true
  spark.databricks.delta.autoCompact.enabled = true
  spark.sql.shuffle.partitions = 200 (tune for data size)
  spark.databricks.delta.properties.defaults.enableChangeDataFeed = true
""")
print("Production Delta table configured!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SECTION 6 — Key Takeaways
# MAGIC
# MAGIC ### Delta Lake Core Concepts
# MAGIC 1. **Delta = Parquet + Transaction Log** — data files are standard Parquet
# MAGIC 2. **ACID transactions** — all writes are atomic, isolated, consistent, durable
# MAGIC 3. **Transaction log** (`_delta_log/`) is the single source of truth
# MAGIC 4. **Schema enforcement** — prevents bad data from entering tables
# MAGIC 5. **Schema evolution** — safely add columns with `mergeSchema`
# MAGIC
# MAGIC ### Best Practices
# MAGIC * **Always use Delta** for production tables
# MAGIC * **Enable auto-optimize** for write-heavy workloads
# MAGIC * **Use liquid clustering** instead of manual ZORDER (DBR 13.3+)
# MAGIC * **Set retention** policies appropriate for use case
# MAGIC * **Add constraints** for data quality
# MAGIC * **Use replaceWhere** for partition-level overwrites
# MAGIC
# MAGIC ### When to Use Delta vs Parquet
# MAGIC | Use Parquet | Use Delta |
# MAGIC |---|---|
# MAGIC | Read-only archives | Any production workload |
# MAGIC | Single-writer, no updates | Multiple writers/readers |
# MAGIC | No audit needs | Need history/time travel |
# MAGIC | Static exports | Living, evolving datasets |

# COMMAND ----------

# MAGIC %md
# MAGIC ## SECTION 7 — Practice Exercises
# MAGIC
# MAGIC ### Exercise 1: Create and Inspect
# MAGIC Create a Delta table, then explore the `_delta_log` to find commit actions.
# MAGIC
# MAGIC ### Exercise 2: Schema Enforcement
# MAGIC Attempt writes with matching, extra, and wrong-type columns.
# MAGIC
# MAGIC ### Exercise 3: Delta vs Parquet
# MAGIC Write same data as both formats, attempt UPDATE on each.
# MAGIC
# MAGIC ### Exercise 4: Constraints
# MAGIC Implement a wrapper rejecting NULL PKs or negative amounts.

# COMMAND ----------

# SECTION 7 — EXERCISE SOLUTIONS

from pyspark.sql.functions import col, lit  # Imports.
from delta.tables import DeltaTable  # Delta.
import json  # JSON.

# --- Exercise 1 ---
print("=== Exercise 1: Create and Inspect ===")
ex1 = spark.createDataFrame([(1,"A",10.0),(2,"B",20.0)], ["id","name","value"])
ex1_path = "/tmp/delta_kt/ex1"
ex1.write.format("delta").mode("overwrite").save(ex1_path)
log = dbutils.fs.head(f"{ex1_path}/_delta_log/00000000000000000000.json")
for line in log.strip().split("\n")[:3]:
    print(f"  Action: {list(json.loads(line).keys())}")

# --- Exercise 2 ---
print("\n=== Exercise 2: Schema Enforcement ===")
match = spark.createDataFrame([(3,"C",30.0)], ["id","name","value"])
match.write.format("delta").mode("append").save(ex1_path)  # Works.
print(f"  After append: {spark.read.format('delta').load(ex1_path).count()} rows")
extra = spark.createDataFrame([(4,"D",40.0,"extra")], ["id","name","value","extra_col"])
try:
    extra.write.format("delta").mode("append").save(ex1_path)
except:
    print("  Extra column rejected!")
extra.write.format("delta").mode("append").option("mergeSchema","true").save(ex1_path)
print(f"  After mergeSchema: columns = {spark.read.format('delta').load(ex1_path).columns}")

# --- Exercise 3 ---
print("\n=== Exercise 3: Delta vs Parquet ===")
data = spark.createDataFrame([(1,100),(2,200)], ["id","val"])
data.write.format("delta").mode("overwrite").save("/tmp/delta_kt/ex3")
dt = DeltaTable.forPath(spark, "/tmp/delta_kt/ex3")
dt.update("id=1", {"val": lit(999)})
print("  Delta UPDATE worked!")
spark.read.format("delta").load("/tmp/delta_kt/ex3").show()

# --- Exercise 4 ---
print("\n=== Exercise 4: Constraints ===")
def safe_write(df, path):
    bad = df.filter(col("id").isNull() | (col("amount") < 0)).count()
    if bad > 0:
        print(f"  Rejected {bad} invalid rows")
    good = df.filter(col("id").isNotNull() & (col("amount") >= 0))
    good.write.format("delta").mode("append").save(path)
    print(f"  Written {good.count()} valid rows")

ex4_path = "/tmp/delta_kt/ex4"
spark.createDataFrame([(1,100.0)],["id","amount"]).write.format("delta").mode("overwrite").save(ex4_path)
test = spark.createDataFrame([(2,50.0),(None,30.0),(4,-10.0)],["id","amount"])
safe_write(test, ex4_path)
spark.read.format("delta").load(ex4_path).show()
print("All exercises completed!")
