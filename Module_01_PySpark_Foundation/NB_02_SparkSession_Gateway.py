# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 02: SparkSession — Your Gateway to Everything
# MAGIC # Module: PySpark Foundation & SparkSession
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 40 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: The Front Door to a Building
# MAGIC
# MAGIC Imagine a huge office building with hundreds of departments:  
# MAGIC - **Legal** (Spark SQL)  
# MAGIC - **Logistics** (Streaming)  
# MAGIC - **Research** (MLlib)  
# MAGIC - **Maintenance** (Core)  
# MAGIC
# MAGIC To enter this building, you don't go through 4 different doors. You walk through **ONE main entrance** — the reception desk. From there, you can reach ANY department.
# MAGIC
# MAGIC **SparkSession is that front door.** It's the single entry point to everything in Spark.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What SparkSession Does
# MAGIC
# MAGIC | What you want to do | You use SparkSession for... |
# MAGIC |---------------------|----------------------------|
# MAGIC | Read a CSV file | `spark.read.csv(...)` |
# MAGIC | Run a SQL query | `spark.sql("SELECT ...")` |
# MAGIC | Create a DataFrame | `spark.createDataFrame(...)` |
# MAGIC | Access configuration | `spark.conf.get(...)` |
# MAGIC | Access the catalog | `spark.catalog.listTables()` |
# MAGIC | Start a stream | `spark.readStream...` |
# MAGIC | Stop everything | `spark.stop()` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Before SparkSession (The Old Way)
# MAGIC
# MAGIC In Spark 1.x, you needed SEPARATE objects for different tasks:
# MAGIC - `SparkContext` for RDDs
# MAGIC - `SQLContext` for SQL
# MAGIC - `HiveContext` for Hive tables
# MAGIC - `StreamingContext` for streaming
# MAGIC
# MAGIC **SparkSession (Spark 2.0+) unified ALL of these into one object.** Much simpler!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Fact for Databricks Users
# MAGIC
# MAGIC In Databricks, **you NEVER need to create a SparkSession yourself**. It's already created for you as the variable `spark`. Just use it!

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### SparkSession Internal Structure
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────────────────────────────┐
# MAGIC │              SparkSession ("spark")                │
# MAGIC │                                                   │
# MAGIC │   ┌─────────────┐  ┌───────────────┐  ┌─────────┐ │
# MAGIC │   │SparkContext│  │  SQL Engine  │  │ Catalog │ │
# MAGIC │   │   (sc)     │  │  (Catalyst)  │  │(metadata)│ │
# MAGIC │   └─────────────┘  └───────────────┘  └─────────┘ │
# MAGIC │                                                   │
# MAGIC │   ┌─────────────┐  ┌───────────────┐  ┌─────────┐ │
# MAGIC │   │   Config  │  │ StreamReader │  │  UDF    │ │
# MAGIC │   │ (settings)│  │ (readStream) │  │Registry│ │
# MAGIC │   └─────────────┘  └───────────────┘  └─────────┘ │
# MAGIC └───────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### The Builder Pattern
# MAGIC
# MAGIC SparkSession is created using the **Builder pattern** — like ordering a custom pizza:
# MAGIC
# MAGIC ```python
# MAGIC # You DON'T do this in Databricks (it's already done for you)
# MAGIC # But this is how it works under the hood:
# MAGIC spark = SparkSession.builder \     # Start building
# MAGIC     .appName("My App") \           # Name your application
# MAGIC     .config("spark.executor.memory", "4g") \  # Set memory
# MAGIC     .config("spark.sql.shuffle.partitions", "50") \  # Set partitions
# MAGIC     .enableHiveSupport() \          # Enable Hive tables
# MAGIC     .getOrCreate()                  # Create or reuse existing session
# MAGIC ```
# MAGIC
# MAGIC ### Key Concept: `getOrCreate()`
# MAGIC
# MAGIC - If a SparkSession already exists → returns the existing one
# MAGIC - If no SparkSession exists → creates a new one
# MAGIC - This prevents accidentally creating multiple sessions
# MAGIC
# MAGIC ### What Lives Inside SparkSession
# MAGIC
# MAGIC 1. **SparkContext (sc)** — Low-level access to the cluster (RDDs, accumulators, broadcasts)
# MAGIC 2. **SQL Engine** — The Catalyst optimizer that makes your queries fast
# MAGIC 3. **Catalog** — Knows about all tables, views, databases, and functions
# MAGIC 4. **Config** — All Spark settings (memory, parallelism, etc.)
# MAGIC 5. **StreamReader** — For reading streaming data sources
# MAGIC 6. **UDF Registry** — Where custom functions are registered

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Explore SparkSession
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 1: Explore the SparkSession Object
# ═══════════════════════════════════════════════════════

# In Databricks, 'spark' is pre-created and ready to use
# Let's explore what it contains

# Check the type of the spark variable
print("Type of 'spark':", type(spark))  # Should be SparkSession

# Get the Spark version
print("\nSpark Version:", spark.version)  # e.g., 3.5.0

# Access SparkContext through SparkSession
sc = spark.sparkContext  # The low-level engine
print("\nSparkContext details:")
print(f"  App Name: {sc.appName}")  # Name of the Spark application
print(f"  App ID: {sc.applicationId}")  # Unique ID for this application
print(f"  Master: {sc.master}")  # Where Spark is running
print(f"  Default Parallelism: {sc.defaultParallelism}")  # Number of parallel tasks
print(f"  Spark Home: {sc.sparkHome}")  # Where Spark is installed

# Check the Spark UI URL (useful for debugging)
print(f"\nSpark UI URL: Available in Databricks cluster UI")  # Access via cluster tab

# Check if we have Hive support
print(f"\nHive Support: Built into Databricks")  # Always available in Databricks

# Expected Output:
# Type of 'spark': <class 'pyspark.sql.session.SparkSession'>
# Spark Version: 3.5.0
# SparkContext details:
#   App Name: Databricks Shell
#   App ID: app-20240101...
#   Master: local[*]
#   Default Parallelism: 8
#   Spark Home: /databricks/spark

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Reading and Setting Configs
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 2: Reading and Setting Configuration
# ═══════════════════════════════════════════════════════

# SparkSession gives you access to ALL Spark configuration
# Think of configs as the "settings menu" of Spark

print("=== Reading Spark Configuration ===")
print()

# Read some important configuration values
shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")  # How many partitions after a shuffle
print(f"Shuffle Partitions: {shuffle_partitions}")  # Default is usually 200

# Read the auto broadcast join threshold
broadcast_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")  # Size limit for broadcast joins
print(f"Broadcast Join Threshold: {broadcast_threshold} bytes")  # Default is 10MB

# Read adaptive query execution setting
aqe_enabled = spark.conf.get("spark.sql.adaptive.enabled")  # Is AQE on?
print(f"Adaptive Query Execution: {aqe_enabled}")  # Should be 'true' in modern Spark

print("\n=== Setting Configuration ===")

# Change the shuffle partitions (this is the MOST common config change)
print(f"Before: shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")
spark.conf.set("spark.sql.shuffle.partitions", "50")  # Change from 200 to 50
print(f"After:  shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")

# Reset it back to a good default for Databricks
spark.conf.set("spark.sql.shuffle.partitions", "auto")  # Let Spark decide automatically
print(f"Reset:  shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")

print("\n--- Key Insight ---")
print("Configuration changes made with spark.conf.set() last only for this session.")
print("When the cluster restarts, configs go back to their defaults.")

# Expected Output:
# === Reading Spark Configuration ===
# Shuffle Partitions: 200 (or auto)
# Broadcast Join Threshold: 10485760 bytes
# Adaptive Query Execution: true
#
# === Setting Configuration ===
# Before: shuffle.partitions = 200
# After:  shuffle.partitions = 50
# Reset:  shuffle.partitions = auto

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Using SparkSession to Create Data
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 3: Using SparkSession to Create DataFrames
# ═══════════════════════════════════════════════════════

# SparkSession provides multiple ways to create DataFrames
# Let's explore the most common ones

print("=== 4 Ways to Create DataFrames with SparkSession ===")
print()

# Method 1: spark.createDataFrame() with a list of tuples
print("Method 1: From list of tuples")
df1 = spark.createDataFrame(  # Create from Python data
    [("Alice", 30), ("Bob", 25)],  # Data as list of tuples
    ["name", "age"]  # Column names
)
display(df1)  # Show the result

# Method 2: spark.range() — creates sequential numbers
print("\nMethod 2: spark.range()")
df2 = spark.range(1, 11)  # Creates numbers 1 through 10 (start inclusive, end exclusive)
display(df2)  # Shows a column called 'id' with values 1-10

# Method 3: spark.sql() — create from a SQL query
print("\nMethod 3: spark.sql()")
df3 = spark.sql("SELECT 'Hello' as greeting, 42 as answer, current_date() as today")  # SQL query
display(df3)  # Shows the SQL result as a DataFrame

# Method 4: spark.createDataFrame() with explicit schema
print("\nMethod 4: With explicit schema (StructType)")
from pyspark.sql.types import StructType, StructField, StringType, IntegerType  # Import types

my_schema = StructType([  # Define the exact structure
    StructField("city", StringType(), True),  # city column, string, allows nulls
    StructField("population", IntegerType(), True)  # population column, integer, allows nulls
])
df4 = spark.createDataFrame(  # Create with explicit schema
    [("London", 9000000), ("Tokyo", 14000000), ("Paris", 2200000)],  # Data
    schema=my_schema  # Use our defined schema
)
display(df4)  # Show the result
df4.printSchema()  # Verify the schema matches what we defined

# Expected Output:
# Method 1: Table with Alice/30, Bob/25
# Method 2: Table with id column 1-10
# Method 3: Table with greeting='Hello', answer=42, today=<current date>
# Method 4: Table with city/population and correct types

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Builder Pattern
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# Example 1: The Builder Pattern and getOrCreate()
# ═══════════════════════════════════════════════════════

from pyspark.sql import SparkSession  # Import SparkSession class

# In Databricks, 'spark' already exists. Let's prove getOrCreate() returns the SAME session
print("=== Demonstrating getOrCreate() ===")
print()

# Try to create a "new" session (it won't — it returns the existing one)
new_spark = SparkSession.builder.appName("Test App").getOrCreate()  # Attempts to create

# Check if it's the SAME object as the pre-existing 'spark'
print(f"Original spark id: {id(spark)}")  # Memory address of original
print(f"New spark id:      {id(new_spark)}")  # Memory address of "new" one
print(f"Are they the same? {spark is new_spark}")  # Should be True!

# This proves getOrCreate() returns the existing session, not a new one
print("\n--- Key Insight ---")
print("getOrCreate() is SAFE to call multiple times.")
print("It never creates duplicates — it always returns the existing session.")

# Show how configs work with the builder pattern
print("\n=== Builder Pattern Config Demo ===")
# You can also set configs through SparkSession.builder
SparkSession.builder.config("spark.sql.shuffle.partitions", "100").getOrCreate()  # Set config
print(f"Shuffle partitions now: {spark.conf.get('spark.sql.shuffle.partitions')}")  # Verify

# Reset for the rest of the notebook
spark.conf.set("spark.sql.shuffle.partitions", "auto")  # Reset to auto

# Expected Output:
# Original spark id: 140234567890
# New spark id:      140234567890
# Are they the same? True
# Shuffle partitions now: 100

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: SparkSession Catalog
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# Example 2: Exploring the Catalog (Metadata About Your Data)
# ═══════════════════════════════════════════════════════

# The Catalog is like a librarian — it knows where everything is
# It tracks all databases, tables, views, and functions

print("=== Exploring the Spark Catalog ===")
print()

# First, let's create a temp view so we have something to find
test_data = [("Alice", 30, "Engineering"), ("Bob", 25, "Marketing")]  # Sample data
test_df = spark.createDataFrame(test_data, ["name", "age", "dept"])  # Create DataFrame
test_df.createOrReplaceTempView("employees_temp")  # Register as a temporary view

# List all databases (schemas)
print("Databases (schemas) available:")
databases = spark.catalog.listDatabases()  # Get list of databases
for db in databases[:5]:  # Show first 5
    print(f"  - {db.name}: {db.description or 'No description'}")  # Name and description

# List tables in the current database
print("\nTables in current database:")
tables = spark.catalog.listTables()  # Get list of tables
for table in tables[:10]:  # Show first 10
    print(f"  - {table.name} (type: {table.tableType}, temp: {table.isTemporary})")  # Table info

# Check if a specific table exists
table_exists = spark.catalog.tableExists("employees_temp")  # Check if our view exists
print(f"\nDoes 'employees_temp' exist? {table_exists}")  # Should be True

# List columns of a table
print("\nColumns in 'employees_temp':")
columns = spark.catalog.listColumns("employees_temp")  # Get column details
for col_info in columns:  # Loop through each column
    print(f"  - {col_info.name}: {col_info.dataType} (nullable: {col_info.nullable})")  # Details

# Get current database
current_db = spark.catalog.currentDatabase()  # What database are we using?
print(f"\nCurrent database: {current_db}")  # Usually 'default'

# Expected Output:
# Databases (schemas) available:
#   - default: No description
# Tables in current database:
#   - employees_temp (type: TEMPORARY, temp: True)
# Does 'employees_temp' exist? True
# Columns in 'employees_temp':
#   - name: string (nullable: True)
#   - age: bigint (nullable: True)
#   - dept: string (nullable: True)
# Current database: default

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Multiple Operations
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# Example 3: Using SparkSession for Multiple Operations
# ═══════════════════════════════════════════════════════

# SparkSession is your gateway to EVERYTHING. Let's use it for multiple tasks in one workflow.

print("=== Complete Workflow Using SparkSession ===")
print()

# Step 1: Create data using spark.createDataFrame()
print("Step 1: Create sales data")
sales = spark.createDataFrame([  # Create a sales DataFrame
    ("2024-01-15", "Widget A", 10, 5.99),  # (date, product, quantity, price)
    ("2024-01-15", "Widget B", 3, 12.99),
    ("2024-01-16", "Widget A", 7, 5.99),
    ("2024-01-16", "Widget C", 15, 3.49),
    ("2024-01-17", "Widget B", 5, 12.99),
    ("2024-01-17", "Widget A", 20, 5.99),
], ["date", "product", "quantity", "price"])  # Column names
print(f"  Created {sales.count()} rows of sales data")  # Count rows

# Step 2: Register as SQL view using createOrReplaceTempView
print("\nStep 2: Register as SQL-queryable view")
sales.createOrReplaceTempView("daily_sales")  # Now accessible via SQL
print("  Registered as 'daily_sales'")

# Step 3: Query with spark.sql()
print("\nStep 3: Run SQL analytics")
result = spark.sql("""  -- SQL query for total revenue by product
    SELECT 
        product,
        SUM(quantity) as total_qty,
        ROUND(SUM(quantity * price), 2) as total_revenue
    FROM daily_sales
    GROUP BY product
    ORDER BY total_revenue DESC
""")  # Execute the SQL
display(result)  # Show results

# Step 4: Use spark.conf to optimize
print("\nStep 4: Check optimization settings")
print(f"  AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")  # Should be true
print(f"  Broadcast threshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")  # 10MB

# Step 5: Verify with catalog
print("\nStep 5: Verify table exists in catalog")
print(f"  'daily_sales' exists: {spark.catalog.tableExists('daily_sales')}")  # True

print("\n--- Summary ---")
print("All operations (create, SQL, config, catalog) go through 'spark'!")

# Expected Output:
# Step 1: Create sales data - Created 6 rows
# Step 2: Register as SQL-queryable view
# Step 3: SQL results showing revenue by product
# Step 4: Config values
# Step 5: Table exists confirmation

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: All Config Categories
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Examples
# Example 1: The Most Important Spark Configs
# ═══════════════════════════════════════════════════════

# Every Spark engineer should know these configs
# They control performance, memory, and behavior

print("=== The 10 Most Important Spark Configurations ===")
print()

# Define the configs we want to check
important_configs = [  # List of (config_name, what_it_does)
    ("spark.sql.shuffle.partitions", "Number of partitions after a shuffle operation"),
    ("spark.sql.adaptive.enabled", "Adaptive Query Execution - auto-optimizes at runtime"),
    ("spark.sql.adaptive.coalescePartitions.enabled", "Auto-reduce empty partitions"),
    ("spark.sql.autoBroadcastJoinThreshold", "Max size of table to broadcast in joins"),
    ("spark.default.parallelism", "Default parallelism for RDD operations"),
    ("spark.sql.files.maxPartitionBytes", "Max bytes per partition when reading files"),
    ("spark.sql.execution.arrow.pyspark.enabled", "Use Apache Arrow for Pandas conversion"),
    ("spark.sql.sources.partitionOverwriteMode", "How partition overwrite works"),
    ("spark.databricks.delta.optimizeWrite.enabled", "Auto-optimize Delta writes"),
    ("spark.sql.adaptive.skewJoin.enabled", "Handle data skew in joins automatically"),
]

# Print each config with its current value and explanation
for config_name, description in important_configs:  # Loop through each config
    try:
        value = spark.conf.get(config_name)  # Try to read the config value
        print(f"  {config_name}")
        print(f"    Value: {value}")
        print(f"    Purpose: {description}")
        print()  # Blank line for readability
    except Exception as e:  # Some configs might not be set
        print(f"  {config_name}")
        print(f"    Value: <not set or not available>")
        print(f"    Purpose: {description}")
        print()

print("--- Pro Tip ---")
print("Most of these are already optimally set in Databricks.")
print("The #1 config you'll change most often: spark.sql.shuffle.partitions")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Session vs Context
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Examples
# Example 2: SparkSession vs SparkContext — When to Use Each
# ═══════════════════════════════════════════════════════

print("=== SparkSession vs SparkContext ===")
print()

# Get SparkContext from SparkSession
sc = spark.sparkContext  # Access the underlying SparkContext

# Things you can ONLY do with SparkSession (not SparkContext)
print("--- SparkSession ONLY ---")
print("1. spark.read.csv(...)        → Read files into DataFrames")
print("2. spark.sql('SELECT ...')    → Run SQL queries")
print("3. spark.createDataFrame(...) → Create DataFrames")
print("4. spark.catalog.*            → Explore metadata")
print("5. spark.readStream.*         → Start streaming")
print("6. spark.udf.register(...)    → Register UDFs for SQL")

# Things you can ONLY do with SparkContext
print("\n--- SparkContext ONLY ---")
print("1. sc.parallelize(list)       → Create RDDs from Python lists")
print("2. sc.textFile(path)          → Read text files as RDDs")
print("3. sc.broadcast(value)        → Create broadcast variables")
print("4. sc.accumulator(0)          → Create accumulators")
print("5. sc.setCheckpointDir(path)  → Set checkpoint directory")
print("6. sc.addFile(path)           → Add files to all workers")

# Demonstrate both in action
print("\n--- Demo: Using Both Together ---")

# SparkContext: Create an RDD and broadcast a lookup
lookup = {"A": "Alpha", "B": "Bravo", "C": "Charlie"}  # Lookup dictionary
broadcast_lookup = sc.broadcast(lookup)  # Broadcast to all workers (SparkContext)
print(f"Broadcast value on driver: {broadcast_lookup.value}")  # Access the broadcast value

# SparkSession: Create a DataFrame and use the broadcast
df = spark.createDataFrame([("A", 1), ("B", 2), ("C", 3)], ["code", "value"])  # DataFrame
print("\nDataFrame created via SparkSession:")
display(df)  # Show it

# Cleanup
broadcast_lookup.unpersist()  # Release the broadcast variable memory
print("\nBroadcast unpersisted (memory freed)")

# Expected Output:
# Shows which operations belong to SparkSession vs SparkContext
# Demonstrates creating a broadcast with sc and a DataFrame with spark

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: spark.stop() and Session Lifecycle
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Examples
# Example 3: SparkSession Lifecycle and spark.stop()
# ═══════════════════════════════════════════════════════

# WARNING: Do NOT run spark.stop() in Databricks! It will kill your session.
# This cell is for UNDERSTANDING ONLY. We'll demonstrate the concept safely.

print("=== SparkSession Lifecycle ===")
print()
print("Phase 1: CREATION")
print("  - In Databricks: automatic (already done for you)")
print("  - Outside Databricks: SparkSession.builder.getOrCreate()")
print()
print("Phase 2: USAGE")
print("  - Read data, transform, write, run SQL")
print("  - This is where you spend 99% of your time")
print()
print("Phase 3: STOPPING")
print("  - spark.stop() — shuts down all executors, frees all resources")
print("  - In Databricks: NEVER call this manually!")
print("  - Databricks handles session lifecycle for you")
print()

# Show what happens conceptually
print("=== What spark.stop() Does ===")
print("1. Cancels all running jobs")
print("2. Shuts down all executors")
print("3. Releases all memory and CPU resources")
print("4. Unregisters from the cluster manager")
print("5. Closes all connections")
print()
print("=== When You WOULD Call spark.stop() ===")
print("- At the end of a standalone Python script (not a notebook)")
print("- When running unit tests (each test gets a fresh session)")
print("- When you explicitly want to release cluster resources")
print()
print("=== NEVER Do This in Databricks ===")
print("- spark.stop() will BREAK your notebook")
print("- You'd need to detach and reattach the cluster")
print("- Let Databricks manage the session lifecycle")

# Prove the session is alive and well
print(f"\n\u2705 Session is active: {spark.version}")  # Verify session is working
print(f"\u2705 SparkContext status: {spark.sparkContext._jsc.sc().isStopped()}")  # Should be False

# Expected Output:
# Explains the lifecycle without actually stopping the session
# Confirms session is active at the end

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Creating SparkSession Manually in Databricks
# MAGIC
# MAGIC **What happens:** You write `SparkSession.builder.appName("X").getOrCreate()` at the top of your notebook.  
# MAGIC **Why it's wrong:** The session already exists. At best it's useless code; at worst it can override cluster-level settings.  
# MAGIC **The fix:** Just use the pre-existing `spark` variable directly.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #2: Calling `spark.stop()` in a Notebook
# MAGIC
# MAGIC **What happens:** Your notebook stops working. All subsequent cells fail.  
# MAGIC **Why it's bad:** `spark.stop()` kills the entire SparkSession. In Databricks, you can't easily restart it without reattaching the cluster.  
# MAGIC **The fix:** Never call `spark.stop()` in Databricks notebooks. It's only for standalone scripts.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #3: Confusing Session-Level vs Cluster-Level Config
# MAGIC
# MAGIC **What happens:** You set a config with `spark.conf.set(...)`, restart the cluster, and it's gone.  
# MAGIC **Why it's confusing:** Session-level configs only last for the current session. Cluster-level configs persist.  
# MAGIC **The fix:**
# MAGIC - For temporary changes: `spark.conf.set("key", "value")` (dies with session)
# MAGIC - For permanent changes: Set in Cluster → Configuration → Spark Config
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #4: Not Checking if a Table Exists Before Querying
# MAGIC
# MAGIC **What happens:** `spark.sql("SELECT * FROM non_existent_table")` throws an AnalysisException.  
# MAGIC **Why it's bad:** Crashes your pipeline without a helpful error message.  
# MAGIC **The fix:** Check first with `spark.catalog.tableExists("table_name")`.
# MAGIC
# MAGIC ```python
# MAGIC # GOOD:
# MAGIC if spark.catalog.tableExists("my_table"):
# MAGIC     df = spark.sql("SELECT * FROM my_table")
# MAGIC else:
# MAGIC     print("Table not found!")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #5: Using SparkContext When SparkSession Would Do
# MAGIC
# MAGIC **What happens:** You write `sc.parallelize()` to create data, then convert to DataFrame.  
# MAGIC **Why it's roundabout:** SparkSession can create DataFrames directly — no need to go through RDDs.  
# MAGIC **The fix:** Use `spark.createDataFrame()` or `spark.range()` directly.
# MAGIC
# MAGIC ```python
# MAGIC # ROUNDABOUT (via RDD):
# MAGIC rdd = sc.parallelize([(1, "a"), (2, "b")])
# MAGIC df = rdd.toDF(["id", "value"])
# MAGIC
# MAGIC # DIRECT (better):
# MAGIC df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC ### Level 1 (Just Read and Run)
# MAGIC Run the Beginner Example 1 cell. Write down your Spark version and App Name.
# MAGIC
# MAGIC ### Level 2 (Tiny Change)
# MAGIC Use `spark.conf.get()` to read the value of `spark.sql.adaptive.enabled`. Print it.
# MAGIC
# MAGIC ### Level 3 (Combine Two Things)
# MAGIC Create a DataFrame, register it as a temp view, then use `spark.catalog.listColumns()` to list its columns.
# MAGIC
# MAGIC ### Level 4 (New Scenario)
# MAGIC Use `spark.range(1, 1001)` to create 1000 numbers. Then add a column called `squared` that contains `id * id`. Display the first 10 rows.
# MAGIC
# MAGIC ### Level 5 (Intermediate Project)
# MAGIC Create a temp view called "movies", use `spark.sql()` to query it, and verify with `spark.catalog.tableExists()`.
# MAGIC
# MAGIC ### Level 6 (Design First)
# MAGIC Describe in comments: How would you build a function that lists ALL temp views in your session and their column counts? Then implement it.
# MAGIC
# MAGIC ### Level 7 (Optimize It)
# MAGIC Write code that reads 5 different Spark configs, changes 2 of them, verifies the changes, then resets them back.
# MAGIC
# MAGIC ### Level 8 (Edge Cases)
# MAGIC What happens if you call `spark.conf.get()` on a config that doesn't exist? How do you handle it gracefully? (Hint: there's a default parameter)
# MAGIC
# MAGIC ### Level 9 (Production-Grade)
# MAGIC Create a function `validate_session()` that checks: (a) session is active, (b) key configs are set correctly, (c) prints a health report.
# MAGIC
# MAGIC ### Level 10 (Teach It)
# MAGIC Write a markdown explanation of SparkSession for a colleague. Cover: what it is, what it replaces, and 3 things you can do with it.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions (All Levels)
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS — All 10 Levels
# ═══════════════════════════════════════════════════════

from pyspark.sql.functions import col  # Import col for column operations
from pyspark.sql.types import StructType, StructField, StringType, IntegerType  # For schemas

# ---- LEVEL 1: Just Run ----
print("=== Level 1 ===")
print(f"Spark Version: {spark.version}")  # Print version
print(f"App Name: {spark.sparkContext.appName}")  # Print app name
# WHY: Confirms your environment works

# ---- LEVEL 2: Tiny Change ----
print("\n=== Level 2 ===")
aqe = spark.conf.get("spark.sql.adaptive.enabled")  # Read the AQE config
print(f"AQE enabled: {aqe}")  # Should print 'true'
# WHY: Practice reading configs with spark.conf.get()

# ---- LEVEL 3: Combine Two Things ----
print("\n=== Level 3 ===")
book_data = [("Spark Guide", 400), ("Python Basics", 250)]  # Sample data
book_df = spark.createDataFrame(book_data, ["title", "pages"])  # Create DataFrame
book_df.createOrReplaceTempView("books")  # Register as temp view
columns = spark.catalog.listColumns("books")  # List columns from catalog
for c in columns:  # Loop through columns
    print(f"  Column: {c.name}, Type: {c.dataType}")  # Print column info
# WHY: Combines createDataFrame + createOrReplaceTempView + catalog

# ---- LEVEL 4: New Scenario ----
print("\n=== Level 4 ===")
nums_df = spark.range(1, 1001)  # Create 1000 numbers (1 to 1000)
nums_df = nums_df.withColumn("squared", col("id") * col("id"))  # Add squared column
display(nums_df.limit(10))  # Show first 10 rows
# WHY: Practice spark.range() and withColumn()

# ---- LEVEL 5: Intermediate Project ----
print("\n=== Level 5 ===")
movie_data = [  # Movie data
    ("Inception", 2010, 8.8), ("Interstellar", 2014, 8.6),
    ("The Matrix", 1999, 8.7), ("Parasite", 2019, 8.5)
]
movie_df = spark.createDataFrame(movie_data, ["title", "year", "rating"])  # Create DF
movie_df.createOrReplaceTempView("movies")  # Register view
top_movies = spark.sql("SELECT * FROM movies WHERE rating > 8.6 ORDER BY rating DESC")  # Query
display(top_movies)  # Show results
print(f"Table exists: {spark.catalog.tableExists('movies')}")  # Verify
# WHY: Full workflow: create → register → query → verify

# ---- LEVEL 6: Design First ----
print("\n=== Level 6 ===")
# DESIGN:
# 1. Get all tables from catalog
# 2. Filter for temporary tables only
# 3. For each temp view, get column count
# 4. Print a summary

def list_temp_views_with_columns():
    """Lists all temporary views and their column counts."""
    tables = spark.catalog.listTables()  # Get all tables
    temp_views = [t for t in tables if t.isTemporary]  # Filter for temp views
    print(f"Found {len(temp_views)} temporary views:")  # Count
    for view in temp_views:  # Loop through views
        cols = spark.catalog.listColumns(view.name)  # Get columns for this view
        print(f"  - {view.name}: {len(cols)} columns")  # Print name and count

list_temp_views_with_columns()  # Run the function
# WHY: Teaches planning before coding and working with the catalog API

# ---- LEVEL 7: Optimize It ----
print("\n=== Level 7 ===")
configs_to_check = [  # 5 configs to read
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled"
]
print("Current values:")
for cfg in configs_to_check:  # Read all 5
    print(f"  {cfg} = {spark.conf.get(cfg)}")

# Change 2 configs
spark.conf.set("spark.sql.shuffle.partitions", "100")  # Change shuffle partitions
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20971520")  # Change to 20MB
print("\nAfter changes:")
print(f"  shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")  # Verify
print(f"  broadcastThreshold = {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")  # Verify

# Reset them back
spark.conf.set("spark.sql.shuffle.partitions", "auto")  # Reset
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # Reset to 10MB
print("\nReset to defaults \u2705")
# WHY: Practice reading, changing, verifying, and resetting configs

# ---- LEVEL 8: Edge Cases ----
print("\n=== Level 8 ===")
# What happens with non-existent config?
try:
    val = spark.conf.get("spark.non.existent.config")  # This will throw an exception
    print(f"Value: {val}")
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")  # Show the error type

# Graceful handling with default value
val_safe = spark.conf.get("spark.non.existent.config", "my_default_value")  # With default!
print(f"With default parameter: {val_safe}")  # Returns 'my_default_value'
print("\nKey insight: Always use the 2nd parameter of spark.conf.get() for safety!")
# WHY: Production code should never crash on a missing config

# ---- LEVEL 9: Production-Grade ----
print("\n=== Level 9 ===")

def validate_session():
    """Validates SparkSession health and prints a report."""
    print("\u2500" * 50)  # Separator
    print("SPARK SESSION HEALTH REPORT")
    print("\u2500" * 50)
    
    # Check 1: Session is active
    try:
        version = spark.version  # If this works, session is active
        print(f"\u2705 Session Active: Yes (v{version})")  # Green check
    except Exception:
        print("\u274c Session Active: No - SESSION IS DOWN!")  # Red X
        return  # Exit early
    
    # Check 2: Key configs
    expected_configs = {  # Configs we expect to be set
        "spark.sql.adaptive.enabled": "true",
    }
    for key, expected in expected_configs.items():
        actual = spark.conf.get(key, "NOT SET")  # Get current value
        status = "\u2705" if actual == expected else "\u26a0\ufe0f"  # Check vs expectation
        print(f"{status} {key} = {actual} (expected: {expected})")
    
    # Check 3: Context status
    is_stopped = spark.sparkContext._jsc.sc().isStopped()  # Check if context is stopped
    status = "\u2705" if not is_stopped else "\u274c"  # Good if NOT stopped
    print(f"{status} SparkContext running: {not is_stopped}")
    
    print("\u2500" * 50)
    print("Report complete.")

validate_session()  # Run the health check
# WHY: Production pipelines need health checks before processing data

# ---- LEVEL 10: Teach It ----
print("\n=== Level 10 ===")
print("""
--- SparkSession Explained for a Colleague ---

What is SparkSession?
  SparkSession is the SINGLE ENTRY POINT to Apache Spark.
  Think of it as the front door to a building with many departments.
  Through this one door (the 'spark' variable), you can:
  1. Read files (CSV, JSON, Parquet, Delta)
  2. Run SQL queries
  3. Create and transform data
  4. Configure performance settings
  5. Access the data catalog

What did it replace?
  Before Spark 2.0, you needed 4 separate objects:
  - SparkContext (for RDDs)
  - SQLContext (for SQL)
  - HiveContext (for Hive)
  - StreamingContext (for streams)
  SparkSession unified all of them into ONE object.

Quick example:
  # Read a CSV, query it with SQL, save as Delta:
  df = spark.read.csv('data.csv', header=True)
  df.createOrReplaceTempView('my_table')
  result = spark.sql('SELECT * FROM my_table WHERE value > 100')
  result.write.format('delta').save('/output')

That's it! SparkSession handles everything.
""")
print("\u2705 All 10 homework levels complete!")
# WHY: Teaching solidifies understanding and tests if you truly get it