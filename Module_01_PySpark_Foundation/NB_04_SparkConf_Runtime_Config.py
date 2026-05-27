# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 04: SparkConf and Runtime Configuration
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
# MAGIC ### Real-World Analogy: Car Settings
# MAGIC
# MAGIC Think of Spark as a race car. Before the race, you adjust settings:
# MAGIC - **Seat position** (memory) — how much room each component gets
# MAGIC - **Tire pressure** (parallelism) — how many tasks run at once
# MAGIC - **Fuel mixture** (shuffle partitions) — how data is redistributed
# MAGIC - **Suspension** (broadcast threshold) — when to use special join strategies
# MAGIC
# MAGIC You can adjust some settings **before starting** (cluster-level) and some **during the race** (runtime/session-level).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What Is SparkConf?
# MAGIC
# MAGIC `SparkConf` is a class that holds all Spark settings as key-value pairs. Think of it as the "settings file" for Spark.
# MAGIC
# MAGIC | Level | How to Set | When Applied | Persistence |
# MAGIC |-------|-----------|--------------|-------------|
# MAGIC | Cluster-level | Databricks UI → Cluster → Spark Config | At cluster start | Until cluster edit |
# MAGIC | Session-level | `spark.conf.set("key", "value")` | Immediately | Until session ends |
# MAGIC | Per-query | SQL: `SET key=value` | For that query | Until session ends |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Single Most Important Config
# MAGIC
# MAGIC **`spark.sql.shuffle.partitions`** — Controls how many partitions are created after a shuffle (groupBy, join, etc.)
# MAGIC
# MAGIC - Default: 200 (almost always wrong!)
# MAGIC - Too high (200 for small data) = thousands of tiny empty partitions = slow
# MAGIC - Too low (4 for big data) = few overloaded partitions = slow or OOM
# MAGIC - Sweet spot: depends on data size. Rule of thumb: aim for 128MB per partition
# MAGIC - Modern Spark (3.2+): Set to `auto` and let AQE handle it
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Must-Know Configs (Every Engineer)
# MAGIC
# MAGIC 1. `spark.sql.shuffle.partitions` — Post-shuffle partition count
# MAGIC 2. `spark.sql.adaptive.enabled` — Auto-optimization at runtime
# MAGIC 3. `spark.sql.autoBroadcastJoinThreshold` — Max table size for broadcast joins
# MAGIC 4. `spark.executor.memory` — RAM per executor
# MAGIC 5. `spark.sql.files.maxPartitionBytes` — Max file chunk size when reading

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Configuration Priority (Highest Wins)
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────┐
# MAGIC │  PRIORITY 1 (Highest): Code-level override           │
# MAGIC │  spark.conf.set("key", "value")                      │
# MAGIC │  Lasts only for current session                      │
# MAGIC ├─────────────────────────────────────────────────┤
# MAGIC │  PRIORITY 2: Cluster Spark Config                     │
# MAGIC │  Set in Databricks UI when creating/editing cluster  │
# MAGIC │  Lasts until cluster is edited                       │
# MAGIC ├─────────────────────────────────────────────────┤
# MAGIC │  PRIORITY 3: Spark Environment Config                 │
# MAGIC │  Set in spark-defaults.conf (rare in Databricks)     │
# MAGIC ├─────────────────────────────────────────────────┤
# MAGIC │  PRIORITY 4 (Lowest): Spark built-in defaults         │
# MAGIC │  Hardcoded in Spark source code                      │
# MAGIC └─────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Configuration Categories
# MAGIC
# MAGIC ```
# MAGIC Spark Configs ─┬─ Application  (spark.app.name, spark.driver.memory)
# MAGIC               ├─ Execution    (spark.executor.memory, spark.executor.cores)
# MAGIC               ├─ SQL          (spark.sql.shuffle.partitions, spark.sql.adaptive.*)
# MAGIC               ├─ Shuffle      (spark.shuffle.compress, spark.reducer.maxSizeInFlight)
# MAGIC               ├─ Network      (spark.network.timeout, spark.rpc.askTimeout)
# MAGIC               ├─ Serialization(spark.serializer, spark.kryo.registrator)
# MAGIC               ├─ Dynamic Alloc(spark.dynamicAllocation.enabled, min/maxExecutors)
# MAGIC               └─ Delta/Databricks (spark.databricks.*, delta.*)
# MAGIC ```
# MAGIC
# MAGIC ### Important Note
# MAGIC
# MAGIC Some configs can be changed at runtime, others CANNOT:
# MAGIC - **Changeable at runtime:** `spark.sql.shuffle.partitions`, `spark.sql.autoBroadcastJoinThreshold`, most `spark.sql.*` configs
# MAGIC - **NOT changeable at runtime:** `spark.executor.memory`, `spark.executor.cores`, `spark.driver.memory` (require cluster restart)

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Reading Current Configuration
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 1: Reading Current Configuration Values
# ═══════════════════════════════════════════════════════

print("=== Reading Spark Configuration ===")
print()

# Method 1: spark.conf.get(key) — get a single config value
print("--- Method 1: spark.conf.get() ---")
shuffle_parts = spark.conf.get("spark.sql.shuffle.partitions")  # Get shuffle partitions
print(f"spark.sql.shuffle.partitions = {shuffle_parts}")  # Print the value

broadcast_thresh = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")  # Get broadcast threshold
print(f"spark.sql.autoBroadcastJoinThreshold = {broadcast_thresh}")  # In bytes (10485760 = 10MB)

aqe = spark.conf.get("spark.sql.adaptive.enabled")  # Is AQE enabled?
print(f"spark.sql.adaptive.enabled = {aqe}")  # Should be 'true'

# Method 2: spark.conf.get(key, default) — with a fallback value
print("\n--- Method 2: With default value ---")
# If the config doesn't exist, return the default instead of crashing
custom = spark.conf.get("my.custom.setting", "not_set")  # Won't exist, returns default
print(f"my.custom.setting = {custom}")  # Prints 'not_set'

# Method 3: SQL SET command to see configs
print("\n--- Method 3: Using SQL SET ---")
all_sql_configs = spark.sql("SET")  # Gets ALL current settings as a DataFrame
print(f"Total SQL configs available: {all_sql_configs.count()}")  # Count all configs

# Show a few specific ones
print("\n--- Key Configs for Data Engineers ---")
key_configs = [  # The configs you'll use most often
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.files.maxPartitionBytes",
    "spark.sql.adaptive.coalescePartitions.enabled",
]
for cfg in key_configs:  # Print each one
    val = spark.conf.get(cfg, "N/A")  # Get with default
    print(f"  {cfg} = {val}")  # Display

# Expected Output:
# spark.sql.shuffle.partitions = 200 (or auto)
# spark.sql.autoBroadcastJoinThreshold = 10485760
# spark.sql.adaptive.enabled = true
# my.custom.setting = not_set
# Total SQL configs available: ~300+

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Setting Configuration
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 2: Setting and Overriding Configuration
# ═══════════════════════════════════════════════════════

print("=== Setting Configuration at Runtime ===")
print()

# Save original values so we can reset later
original_shuffle = spark.conf.get("spark.sql.shuffle.partitions")  # Save original
original_broadcast = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")  # Save original
print(f"Original shuffle partitions: {original_shuffle}")
print(f"Original broadcast threshold: {original_broadcast}")

# --- Change configs ---
print("\n--- Changing configs ---")
spark.conf.set("spark.sql.shuffle.partitions", "50")  # Reduce to 50 partitions
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20971520")  # Increase to 20MB

# Verify changes
print(f"New shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")  # Should be 50
print(f"New broadcast threshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")  # 20MB

# --- Show the effect ---
print("\n--- Effect of shuffle.partitions ---")
df = spark.range(0, 1000)  # Create 1000 numbers
result = df.groupBy((col("id") % 5).alias("group")).count()  # Group by modulo 5
print(f"Result partitions: {result.rdd.getNumPartitions()}")  # Should be 50 (not 200!)

# --- Reset configs back ---
print("\n--- Resetting to original values ---")
spark.conf.set("spark.sql.shuffle.partitions", original_shuffle)  # Reset
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", original_broadcast)  # Reset
print(f"Shuffle partitions reset to: {spark.conf.get('spark.sql.shuffle.partitions')}")  # Verify
print(f"Broadcast threshold reset to: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")  # Verify

print("\n--- Best Practice ---")
print("Always save original values before changing configs!")
print("This ensures you can reset them and avoid surprising behavior in later cells.")

# Expected Output:
# Shows original, changed, and reset values
# Demonstrates that shuffle.partitions affects actual partition count

# Need to import col for this cell
from pyspark.sql.functions import col  # Import here since each cell should be self-contained

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: SQL SET Command
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Examples
# Example 3: Using SQL SET Command for Configuration
# ═══════════════════════════════════════════════════════

# You can also read and set configs using SQL!
# This is useful when working in SQL cells or mixed SQL/Python workflows

print("=== SQL-Based Configuration ===")
print()

# Read a specific config using SQL
print("--- Reading configs with SQL ---")
result = spark.sql("SET spark.sql.shuffle.partitions")  # Get one config
display(result)  # Shows key-value pair

# Set a config using SQL
print("--- Setting configs with SQL ---")
spark.sql("SET spark.sql.shuffle.partitions=100")  # Set to 100 via SQL
verify = spark.sql("SET spark.sql.shuffle.partitions")  # Verify
display(verify)  # Should show 100

# Reset back
spark.sql("SET spark.sql.shuffle.partitions=auto")  # Reset to auto
print("Reset to auto")

# List ALL configs (useful for exploration)
print("\n--- All SQL-accessible configs (first 10) ---")
all_configs = spark.sql("SET")  # Get ALL configs
display(all_configs.limit(10))  # Show first 10

# Search for specific configs
print("\n--- Searching for 'adaptive' configs ---")
adaptive_configs = spark.sql("SET").filter("key LIKE '%adaptive%'")  # Filter for adaptive
display(adaptive_configs)  # Show all adaptive-related configs

print("\nTip: In SQL cells, you can just write: SET spark.sql.shuffle.partitions = 100")

# Expected Output:
# Shows config values via SQL SET commands
# Lists adaptive-related configs

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Important Configs
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# Example 1: The Most Important Configs Every Engineer Must Know
# ═══════════════════════════════════════════════════════

print("=== Top 12 Spark Configs You Must Know ===")
print()

important_configs = [  # List of critical configs
    ("spark.sql.shuffle.partitions", "Post-shuffle partitions (groupBy, join, orderBy)"),
    ("spark.sql.adaptive.enabled", "Adaptive Query Execution (auto-optimizes runtime plan)"),
    ("spark.sql.autoBroadcastJoinThreshold", "Max table size to broadcast in joins (bytes)"),
    ("spark.default.parallelism", "Default partitions for RDDs"),
    ("spark.sql.files.maxPartitionBytes", "Max bytes per input partition when reading files"),
    ("spark.sql.adaptive.coalescePartitions.enabled", "Auto-merge small partitions"),
    ("spark.sql.adaptive.skewJoin.enabled", "Auto-handle data skew in joins"),
    ("spark.sql.execution.arrow.pyspark.enabled", "Speed up Spark ↔ Pandas conversion"),
    ("spark.sql.sources.partitionOverwriteMode", "How partition overwrite behaves"),
    ("spark.serializer", "Java vs Kryo serialization"),
    ("spark.network.timeout", "How long Spark waits before timing out"),
    ("spark.task.maxFailures", "How many times a task is retried before job fails"),
]

# Loop through and print each config with meaning
for config_name, meaning in important_configs:  # Each config pair
    current_value = spark.conf.get(config_name, "<not set>")  # Get current value safely
    print(f"{config_name}")  # Print config name
    print(f"  Current Value: {current_value}")  # Current value
    print(f"  Why it matters: {meaning}")  # Explanation
    print()  # Blank line

print("--- The #1 Most Impactful Setting ---")
print("spark.sql.shuffle.partitions")
print("Why? Because shuffles happen in joins, groupBy, distinct, sort, and more.")
print("If this is badly set, EVERYTHING becomes slower.")
print("Modern best practice: use 'auto' when available so AQE can optimize it.")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Runtime vs Non-Runtime Configs
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Examples
# Example 2: What You CAN and CANNOT Change at Runtime
# ═══════════════════════════════════════════════════════

print("=== Runtime-Changeable vs Restart-Required Configs ===")
print()

# These CAN be changed at runtime (most spark.sql.* configs)
runtime_changeable = [  # Safe to change now
    "spark.sql.shuffle.partitions",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.adaptive.enabled",
    "spark.sql.execution.arrow.pyspark.enabled",
]
print("--- Can Change at Runtime ---")
for cfg in runtime_changeable:  # Loop through runtime-safe configs
    print(f"  ✅ {cfg} = {spark.conf.get(cfg, 'N/A')}")  # Show current value

# These CANNOT be changed at runtime (require cluster restart)
restart_required = [  # Need cluster restart
    "spark.executor.memory",
    "spark.executor.cores",
    "spark.driver.memory",
    "spark.driver.cores",
]
print("\n--- Require Cluster Restart ---")
for cfg in restart_required:  # Loop through restart-required configs
    print(f"  🔒 {cfg} = {spark.conf.get(cfg, '<cluster-level setting>')}")  # Might not show here

print("\n--- Why some need restart ---")
print("Memory and CPU configs determine how executors are CREATED.")
print("Once executors are running, you can't resize them without restarting the cluster.")
print("SQL configs affect query planning, so Spark can apply them immediately.")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Shuffle Partition Impact
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Examples
# Example 1: Why spark.sql.shuffle.partitions Matters So Much
# ═══════════════════════════════════════════════════════

import time  # Import time for timing
from pyspark.sql.functions import col  # Import col

print("=== Impact of spark.sql.shuffle.partitions ===")
print("Task: groupBy on 1 million rows")
print()

# Create test data
big_df = spark.range(0, 1000000)  # 1 million rows
big_df = big_df.withColumn("group_id", col("id") % 100)  # 100 groups

# Test different partition settings
for partitions in [10, 50, 200, "auto"]:  # Try different values
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))  # Set the config
    start = time.time()  # Start timer
    result = big_df.groupBy("group_id").count()  # This causes a shuffle!
    result.count()  # Trigger execution
    elapsed = time.time() - start  # Measure time
    actual_partitions = result.rdd.getNumPartitions()  # How many partitions did we get?
    print(f"  Setting: {partitions:>4} | Time: {elapsed:.3f}s | Result partitions: {actual_partitions}")

# Reset to auto at the end
spark.conf.set("spark.sql.shuffle.partitions", "auto")  # Reset to modern default

print("\n--- What to Learn ---")
print("Too few partitions = each partition has too much work")
print("Too many partitions = lots of tiny tasks, scheduling overhead")
print("'auto' lets AQE pick the best number dynamically")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Thinking `spark.conf.set()` Changes the Cluster Forever
# MAGIC
# MAGIC **What happens:** You set a config, restart the cluster, and it's gone.  
# MAGIC **Why:** `spark.conf.set()` only changes the CURRENT SESSION.  
# MAGIC **Fix:** Use cluster Spark Config for permanent changes.
# MAGIC
# MAGIC ### Mistake #2: Changing Executor Memory at Runtime
# MAGIC
# MAGIC **What happens:** `spark.conf.set("spark.executor.memory", "16g")` appears to work, but has no real effect.  
# MAGIC **Why:** Executors are already running. You can't resize them without restarting.  
# MAGIC **Fix:** Set executor memory in cluster config before starting the cluster.
# MAGIC
# MAGIC ### Mistake #3: Forgetting to Reset Experimental Configs
# MAGIC
# MAGIC **What happens:** You set `spark.sql.shuffle.partitions=5` for a test, then later queries are mysteriously slow.  
# MAGIC **Fix:** Always save originals and reset after experiments.
# MAGIC
# MAGIC ### Mistake #4: Blindly Setting `spark.sql.shuffle.partitions=200`
# MAGIC
# MAGIC **What happens:** People copy old tutorials without understanding the workload.  
# MAGIC **Why it's bad:** 200 may be WAY too high for small data and WAY too low for huge data.  
# MAGIC **Fix:** Use `auto` or measure performance with different values.
# MAGIC
# MAGIC ### Mistake #5: Not Using Default Values in `spark.conf.get()`
# MAGIC
# MAGIC **What happens:** Your code crashes when a custom config isn't defined.  
# MAGIC **Fix:** Use the second parameter: `spark.conf.get("my.key", "default_value")`

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC ### Level 1 (Just Read and Run)
# MAGIC Read and print `spark.sql.shuffle.partitions`.
# MAGIC
# MAGIC ### Level 2 (Tiny Change)
# MAGIC Set `spark.sql.shuffle.partitions` to 25, print it, then reset it.
# MAGIC
# MAGIC ### Level 3 (Combine Two Things)
# MAGIC Use both `spark.conf.get()` and `spark.sql("SET ...")` to read the same config and compare outputs.
# MAGIC
# MAGIC ### Level 4 (New Scenario)
# MAGIC Change the broadcast threshold to 50MB. Verify the change. Reset it.
# MAGIC
# MAGIC ### Level 5 (Intermediate Project)
# MAGIC Create a DataFrame, run a groupBy, and measure runtime under 3 different shuffle partition settings.
# MAGIC
# MAGIC ### Level 6 (Design First)
# MAGIC Design a function that saves current config values, applies temporary changes, and restores the originals after a block of work.
# MAGIC
# MAGIC ### Level 7 (Optimize It)
# MAGIC Find 5 adaptive query execution configs. Explain in comments what each does.
# MAGIC
# MAGIC ### Level 8 (Edge Cases)
# MAGIC Try reading a config that doesn't exist both with and without a default value. Handle the error.
# MAGIC
# MAGIC ### Level 9 (Production-Grade)
# MAGIC Write a `ConfigManager` helper with methods: `get`, `set`, `reset`, `snapshot`.
# MAGIC
# MAGIC ### Level 10 (Teach It)
# MAGIC Explain Spark configuration to a new engineer: cluster-level vs session-level vs query-level.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS — All 10 Levels
# ═══════════════════════════════════════════════════════

import time  # Import time
from pyspark.sql.functions import col  # Import col

# ---- LEVEL 1 ----
print("=== Level 1 ===")
print(f"shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")  # Read config

# ---- LEVEL 2 ----
print("\n=== Level 2 ===")
orig = spark.conf.get("spark.sql.shuffle.partitions")  # Save original
spark.conf.set("spark.sql.shuffle.partitions", "25")  # Set to 25
print(f"After set: {spark.conf.get('spark.sql.shuffle.partitions')}")  # Verify
spark.conf.set("spark.sql.shuffle.partitions", orig)  # Reset
print(f"After reset: {spark.conf.get('spark.sql.shuffle.partitions')}")  # Verify reset

# ---- LEVEL 3 ----
print("\n=== Level 3 ===")
via_api = spark.conf.get("spark.sql.shuffle.partitions")  # Read via API
via_sql = spark.sql("SET spark.sql.shuffle.partitions").collect()[0][1]  # Read via SQL
print(f"API says: {via_api}")  # Print
print(f"SQL says: {via_sql}")  # Print
print(f"Match: {via_api == via_sql}")  # Compare

# ---- LEVEL 4 ----
print("\n=== Level 4 ===")
orig_b = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")  # Save original
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))  # Set 50MB
print(f"Broadcast threshold now: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')} bytes")  # Verify
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", orig_b)  # Reset

# ---- LEVEL 5 ----
print("\n=== Level 5 ===")
test_df = spark.range(0, 500000).withColumn("grp", col("id") % 50)  # Test data
for parts in [10, 50, 100]:  # Test 3 values
    spark.conf.set("spark.sql.shuffle.partitions", str(parts))  # Set
    start = time.time()  # Start timer
    test_df.groupBy("grp").count().count()  # Shuffle + action
    elapsed = time.time() - start  # Measure
    print(f"  {parts} partitions -> {elapsed:.3f}s")  # Print
spark.conf.set("spark.sql.shuffle.partitions", "auto")  # Reset

# ---- LEVEL 6 ----
print("\n=== Level 6 ===")
# DESIGN: save originals in a dict, apply changes, do work, restore originals

def with_temp_configs(config_changes, work_function):
    """Applies temporary configs, runs work, then restores originals."""
    originals = {}  # Store original values
    for key, new_val in config_changes.items():  # Save and set
        originals[key] = spark.conf.get(key, None)  # Save original
        spark.conf.set(key, new_val)  # Apply temporary value
    try:
        return work_function()  # Run the caller's work
    finally:
        for key, old_val in originals.items():  # Restore originals no matter what
            if old_val is not None:
                spark.conf.set(key, old_val)  # Reset

# Example usage
result = with_temp_configs(
    {"spark.sql.shuffle.partitions": "30"},
    lambda: spark.range(10).count()  # Simple work
)
print(f"Result: {result}, restored to {spark.conf.get('spark.sql.shuffle.partitions')}")

# ---- LEVEL 7 ----
print("\n=== Level 7 ===")
aqe_configs = spark.sql("SET").filter("key LIKE 'spark.sql.adaptive.%'")  # Find AQE configs
display(aqe_configs)  # Show all adaptive configs
# WHY in comments:
# adaptive.enabled -> turns AQE on/off
# adaptive.coalescePartitions.enabled -> merges tiny partitions
# adaptive.skewJoin.enabled -> handles skewed joins
# adaptive.localShuffleReader.enabled -> uses local shuffle reads when possible
# adaptive.advisoryPartitionSizeInBytes -> target partition size after AQE

# ---- LEVEL 8 ----
print("\n=== Level 8 ===")
try:
    val = spark.conf.get("does.not.exist")  # This throws
    print(val)
except Exception as e:
    print(f"Without default -> Error: {type(e).__name__}")  # Show error type
safe_val = spark.conf.get("does.not.exist", "fallback")  # Safe version
print(f"With default -> {safe_val}")  # Prints fallback

# ---- LEVEL 9 ----
print("\n=== Level 9 ===")
class ConfigManager:
    """Simple Spark config manager for safe config changes."""
    def __init__(self, spark_session):
        self.spark = spark_session  # Store SparkSession
        self._snapshot = {}  # Internal storage for originals
    def get(self, key, default=None):
        return self.spark.conf.get(key, default)  # Get config safely
    def set(self, key, value):
        if key not in self._snapshot:
            self._snapshot[key] = self.spark.conf.get(key, None)  # Snapshot original once
        self.spark.conf.set(key, value)  # Apply new value
    def reset(self, key):
        if key in self._snapshot and self._snapshot[key] is not None:
            self.spark.conf.set(key, self._snapshot[key])  # Restore original
    def snapshot(self, keys):
        return {k: self.spark.conf.get(k, None) for k in keys}  # Return current values

cm = ConfigManager(spark)  # Create manager
print(cm.snapshot(["spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled"]))  # Test snapshot

# ---- LEVEL 10 ----
print("\n=== Level 10 ===")
print("""
Spark Configuration Explained:

1. Cluster-level configs:
   - Set in Databricks cluster settings before startup
   - Good for memory, cores, libraries, and persistent behavior
   - Example: spark.executor.memory = 8g

2. Session-level configs:
   - Set in notebook code with spark.conf.set(...)
   - Last only for the current notebook session
   - Good for temporary experiments and SQL tuning
   - Example: spark.conf.set('spark.sql.shuffle.partitions', '50')

3. Query-level configs:
   - Set with SQL SET command
   - Good when working directly in SQL
   - Example: SET spark.sql.shuffle.partitions = 50

Rule of thumb:
- Hardware/resources -> cluster-level
- Query tuning/testing -> session-level
- SQL notebooks -> query-level
""")
print("\u2705 Module 1 homework complete!")