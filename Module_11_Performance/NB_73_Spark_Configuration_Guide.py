# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Notebook 73: Spark Configuration — The Complete Tuning Guide
# MAGIC ## Module 11: Performance Optimization
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 40 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Spark has **hundreds** of configuration parameters that control everything from memory allocation to join strategies. Knowing the **20 most impactful configs** lets you tune any workload.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Think of Spark like a **race car**: the engine (cluster) is powerful but the car's settings (tire pressure, suspension, gearing) determine whether you win. A Formula 1 engineer doesn't change all 500 settings — they know the 10 that matter for each track.
# MAGIC
# MAGIC Similarly, for most Spark workloads, tuning **5-10 key configs** gives you 90% of possible performance gains.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Config Precedence (highest to lowest):
# MAGIC   1. Per-query:   SET key = value (in SQL cell or spark.conf.set in Python)
# MAGIC   2. Session:     spark.conf.set("key", "value") at notebook start
# MAGIC   3. Cluster:     Set in cluster Spark config tab (restart required)
# MAGIC   4. Default:     Built into Spark/Databricks runtime
# MAGIC
# MAGIC The 5 Most Impactful Configs:
# MAGIC   ┌───┬───────────────────────────────────┬──────────┬────────────────────┐
# MAGIC   │ # │ Config                            │ Default  │ Impact             │
# MAGIC   ├───┼───────────────────────────────────┼──────────┼────────────────────┤
# MAGIC   │ 1 │ spark.sql.shuffle.partitions      │ 200      │ Parallelism        │
# MAGIC   │ 2 │ spark.sql.adaptive.enabled        │ true     │ Auto-optimization  │
# MAGIC   │ 3 │ spark.sql.autoBroadcastJoinThreshold│ 10MB    │ Join strategy      │
# MAGIC   │ 4 │ spark.executor.memory             │ varies   │ Available RAM      │
# MAGIC   │ 5 │ spark.sql.files.maxPartitionBytes  │ 128MB    │ Input parallelism  │
# MAGIC   └───┴───────────────────────────────────┴──────────┴────────────────────┘
# MAGIC
# MAGIC Configs you CAN change at session level:
# MAGIC   spark.sql.shuffle.partitions, spark.sql.autoBroadcastJoinThreshold,
# MAGIC   spark.sql.adaptive.*, spark.sql.files.maxPartitionBytes
# MAGIC
# MAGIC Configs that require CLUSTER RESTART:
# MAGIC   spark.executor.memory, spark.executor.cores, spark.driver.memory,
# MAGIC   spark.dynamicAllocation.*
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Config Guide
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7 — COMPLETE CONFIG GUIDE WITH HOMEWORK
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTIONS 3-7: Spark Configuration Complete Reference")
print("="*70)

# ─── SECTION 3: Reading current configs ───
print("\n" + "-"*60)
print("EXAMPLE 1: Reading current key configurations")
print("-"*60)

key_configs = [
    ("spark.sql.shuffle.partitions", "Post-shuffle parallelism"),
    ("spark.sql.adaptive.enabled", "AQE auto-optimization"),
    ("spark.sql.autoBroadcastJoinThreshold", "Auto-broadcast threshold"),
    ("spark.sql.files.maxPartitionBytes", "Max bytes per input partition"),
    ("spark.executor.memory", "Memory per executor"),
    ("spark.driver.memory", "Memory for driver"),
    ("spark.executor.cores", "Cores per executor"),
]
print("\nCurrent cluster configuration:")
for cfg, desc in key_configs:
    val = spark.conf.get(cfg, "(not accessible)")  # Read config value.
    print(f"  {desc:35s} = {val}")

# ─── SECTION 4: Setting configs ───
print("\n" + "-"*60)
print("EXAMPLE 2: Setting configs at session level")
print("-"*60)

# Save original.
orig = spark.conf.get("spark.sql.shuffle.partitions")
print(f"\nOriginal shuffle.partitions: {orig}")

# Change it.
spark.conf.set("spark.sql.shuffle.partitions", "50")  # Set to 50.
print(f"After set: {spark.conf.get('spark.sql.shuffle.partitions')}")

# Reset.
spark.conf.set("spark.sql.shuffle.partitions", orig)  # Restore.
print(f"Reset to: {spark.conf.get('spark.sql.shuffle.partitions')}")
print("")
print("Note: Session-level changes apply to THIS notebook only.")
print("They reset when the cluster restarts.")

# ─── SECTION 5: Complete config reference ───
print("\n" + "-"*60)
print("EXAMPLE 3: Complete Configuration Reference")
print("-"*60)
print("""
═══ PARALLELISM & PARTITIONS ═══
spark.sql.shuffle.partitions = 200
  → Post-shuffle task count. Small data: 8-50. Large: 500-4000.
  → With AQE: leave at 200, AQE coalesces empty ones.

spark.sql.files.maxPartitionBytes = 128MB
  → Max size per partition when reading files.
  → Decrease for memory-constrained workloads.

═══ JOINS ═══
spark.sql.autoBroadcastJoinThreshold = 10MB
  → Tables < this auto-broadcast. Safe to set 50-200MB.
  → Set to -1 to disable all auto-broadcasts.

═══ ADAPTIVE QUERY EXECUTION ═══
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.skewJoin.enabled = true

═══ MEMORY ═══
spark.executor.memory = 4g (CLUSTER-LEVEL ONLY)
spark.memory.fraction = 0.6 (execution+storage out of total)
spark.memory.storageFraction = 0.5 (cache's share of fraction)

═══ SERIALIZATION ═══
spark.sql.execution.arrow.pyspark.enabled = true
  → 10x faster toPandas() and createDataFrame(pandas_df).

═══ DELTA-SPECIFIC ═══
spark.databricks.delta.optimizeWrite.enabled = true
spark.databricks.delta.autoCompact.enabled = true
spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true
""")

# ─── SECTION 6: Common Mistakes ───
print("-"*60)
print("SECTION 6: Common Mistakes")
print("-"*60)
print("""
1. Setting executor.memory at session level (requires cluster restart).
2. Setting shuffle.partitions = 1 for 'simplicity' (kills parallelism).
3. Disabling AQE because you 'don't understand it' (loses free optimization).
4. Setting autoBroadcastJoinThreshold to 2GB (OOM when table > executor memory).
5. Not checking configs with spark.conf.get() before/after changes.
""")

# ─── SECTION 7: Homework ───
print("-"*60)
print("SECTION 7: HOMEWORK")
print("-"*60)

# Level 1: Read a config.
print(f"\nLevel 1: shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")

# Level 2: Change and verify.
spark.conf.set("spark.sql.shuffle.partitions", "20")
print(f"Level 2: Changed to {spark.conf.get('spark.sql.shuffle.partitions')}")
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Reset.

# Level 3: Change broadcast threshold.
orig_bcast = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")  # 100MB.
print(f"Level 3: Broadcast threshold = {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')} (100MB)")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", orig_bcast)  # Reset.

# Level 4-10: Config decisions.
print("\nLevel 5: Config for 500GB table join with 5MB lookup:")
print("  Set autoBroadcastJoinThreshold = 10MB (lookup auto-broadcasts).")
print("\nLevel 7: Config for streaming with small batches:")
print("  Enable: delta.autoOptimize.optimizeWrite + autoCompact.")
print("\nLevel 10: Teach configs to a colleague:")
print("""
"Key Spark configs:
  shuffle.partitions: controls parallelism after groupBy/join (default 200).
  autoBroadcastJoinThreshold: tables smaller auto-broadcast (default 10MB).
  AQE: auto-optimizes at runtime (leave ON).
  executor.memory: set at cluster level only.
  Rule: tune 5 configs, leave the rest at defaults."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 73")
print("="*70)