# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 10: RDD Persistence & Partitioning
# MAGIC # Module: RDDs (Resilient Distributed Datasets)
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 45 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: Meal Prep vs Cooking from Scratch
# MAGIC
# MAGIC **Persistence** = Meal prepping on Sunday:
# MAGIC - You cook rice once and store it in containers (cache)
# MAGIC - Every day you reheat in 30 seconds instead of cooking 20 minutes from scratch
# MAGIC - If the fridge is full, some containers go to the freezer (disk) — slower but still faster than raw ingredients
# MAGIC
# MAGIC **Partitioning** = Organizing your kitchen team:
# MAGIC - 4 chefs = 4 partitions (parallel cooking stations)
# MAGIC - Too few chefs = idle stoves, slow
# MAGIC - Too many chefs = bumping into each other, overhead
# MAGIC - `repartition()` = hiring more chefs (shuffles everyone)
# MAGIC - `coalesce()` = sending extra chefs home (no shuffle, just merge)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Why Persistence Matters
# MAGIC
# MAGIC Without caching:
# MAGIC ```
# MAGIC rdd.map(...).filter(...)  →  Job 1: recomputes from raw data
# MAGIC rdd.map(...).filter(...)  →  Job 2: recomputes AGAIN from scratch!
# MAGIC ```
# MAGIC
# MAGIC With caching:
# MAGIC ```
# MAGIC cached = rdd.map(...).filter(...).cache()
# MAGIC cached  →  Job 1: computes and STORES result
# MAGIC cached  →  Job 2: reads from memory (instant!)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Storage Levels
# MAGIC
# MAGIC | Level | Where | Serialized? | Copies | Best For |
# MAGIC |-------|-------|------------|--------|----------|
# MAGIC | MEMORY_ONLY | RAM only | No | 1 | Default, fastest |
# MAGIC | MEMORY_AND_DISK | RAM + overflow to disk | No | 1 | Large datasets |
# MAGIC | DISK_ONLY | Disk only | Yes | 1 | Very large, rarely re-used |
# MAGIC | MEMORY_ONLY_SER | RAM, serialized | Yes | 1 | Save memory (slower reads) |
# MAGIC | MEMORY_AND_DISK_2 | RAM+disk, 2 copies | No | 2 | Fault-tolerant clusters |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Partitioning Rules
# MAGIC
# MAGIC - Default partitions = number of cores (or `spark.default.parallelism`)
# MAGIC - Ideal: 2-4 partitions per CPU core
# MAGIC - Each partition = one task
# MAGIC - Too few partitions = not enough parallelism
# MAGIC - Too many partitions = scheduling overhead
# MAGIC - `repartition(n)` = increase or decrease (full shuffle)
# MAGIC - `coalesce(n)` = decrease only (no shuffle, faster)

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Persistence Internals
# MAGIC
# MAGIC ```
# MAGIC   cache() is just persist(MEMORY_ONLY)
# MAGIC   
# MAGIC   What happens when you call .cache():
# MAGIC   1. Spark marks the RDD for caching (nothing happens yet — lazy!)
# MAGIC   2. First action triggers computation
# MAGIC   3. As each partition is computed, it's stored in executor memory
# MAGIC   4. Next action reads from memory instead of recomputing
# MAGIC   
# MAGIC   Memory pressure:
# MAGIC   ┌────────────────────────────────────────┐
# MAGIC   │ Executor Memory                        │
# MAGIC   │  [Cached RDD A] [Cached RDD B] [Free]  │
# MAGIC   │  If full: LRU eviction drops oldest     │
# MAGIC   └────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Partitioning Internals
# MAGIC
# MAGIC ```
# MAGIC   repartition(n):           coalesce(n) [reduce only]:
# MAGIC   ───────────────           ──────────────────────
# MAGIC   
# MAGIC   [P1][P2][P3][P4]         [P1][P2][P3][P4]
# MAGIC        │                       \  |  /  |
# MAGIC     SHUFFLE                  MERGE (no shuffle!)
# MAGIC        │                       \  |/    |
# MAGIC   [P1][P2][P3][P4]...[Pn]  [P1]      [P2]
# MAGIC ```
# MAGIC
# MAGIC ### When to Use Each
# MAGIC
# MAGIC | Scenario | Use |
# MAGIC |----------|-----|
# MAGIC | RDD used in 2+ actions | `cache()` or `persist()` |
# MAGIC | Memory tight, data reused | `persist(MEMORY_AND_DISK)` |
# MAGIC | Very long lineage (100+ steps) | `checkpoint()` |
# MAGIC | Reduce partitions before write | `coalesce(n)` |
# MAGIC | Increase partitions for parallelism | `repartition(n)` |
# MAGIC | Data is skewed to few partitions | `repartition(n)` |
# MAGIC
# MAGIC ### Checkpoint vs Cache
# MAGIC
# MAGIC | Feature | cache/persist | checkpoint |
# MAGIC |---------|--------------|------------|
# MAGIC | Storage | Memory/Disk (executor) | HDFS/DBFS (reliable) |
# MAGIC | Lineage | Preserved | **Truncated** |
# MAGIC | Survives failure | No (recompute) | Yes |
# MAGIC | Speed | Very fast | Slower (disk write) |
# MAGIC | Use case | Performance | Fault tolerance |

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner: cache and persist
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner: cache(), persist(), unpersist()
# ═══════════════════════════════════════════════════════

import time  # For timing
from pyspark import StorageLevel  # Storage level options

sc = spark.sparkContext  # Get SparkContext

print("=== cache() vs persist() ===")
print()

# Create a computationally expensive RDD
expensive_rdd = (
    sc.parallelize(range(2000000), 8)  # 2M numbers in 8 partitions
    .map(lambda x: x * x)  # Square each number
    .filter(lambda x: x % 7 == 0)  # Keep multiples of 7
)

# WITHOUT caching: each action recomputes from scratch
print("--- Without caching (recomputes each time) ---")
start = time.time()
count1 = expensive_rdd.count()  # First computation
t1 = time.time() - start
start = time.time()
sum1 = expensive_rdd.sum()  # Recomputes EVERYTHING from scratch!
t2 = time.time() - start
print(f"count: {count1:,} in {t1:.3f}s")
print(f"sum: recomputed in {t2:.3f}s")

# WITH caching: compute once, reuse from memory
print("\n--- With cache() (compute once, reuse) ---")
cached_rdd = expensive_rdd.cache()  # Mark for caching (lazy!)
start = time.time()
count2 = cached_rdd.count()  # First call: computes AND caches
t1 = time.time() - start
start = time.time()
sum2 = cached_rdd.sum()  # Second call: reads from cache!
t2 = time.time() - start
print(f"count (computes+caches): {count2:,} in {t1:.3f}s")
print(f"sum (from cache): in {t2:.3f}s  ← much faster!")

# persist() with specific storage level
print("\n--- persist() with storage levels ---")
mem_disk_rdd = expensive_rdd.persist(StorageLevel.MEMORY_AND_DISK)  # Overflow to disk
print(f"Storage level: {mem_disk_rdd.getStorageLevel()}")  # Shows the level

# Check if cached
print(f"\nis_cached: {cached_rdd.is_cached}")  # True

# unpersist() to free memory
cached_rdd.unpersist()  # Remove from cache
print(f"After unpersist: {cached_rdd.is_cached}")  # False
mem_disk_rdd.unpersist()  # Clean up

print("\n--- Key Rules ---")
print("cache() = persist(MEMORY_ONLY) (shortcut)")
print("Cache ONLY if you use an RDD multiple times.")
print("Always unpersist() when done to free memory.")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Storage Levels Explained
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: Storage Levels
# ═══════════════════════════════════════════════════════

from pyspark import StorageLevel  # Import storage levels

print("=== Storage Levels: Where and How to Store ===")
print()

# StorageLevel controls WHERE data is cached and HOW
# Format: StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication)

levels = {
    "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,             # RAM only, deserialized (fastest)
    "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,     # RAM, spill to disk if full
    "DISK_ONLY": StorageLevel.DISK_ONLY,                 # Disk only (slowest)
    "MEMORY_ONLY_2": StorageLevel.MEMORY_ONLY_2,         # RAM only, 2 replicas
    "MEMORY_AND_DISK_2": StorageLevel.MEMORY_AND_DISK_2, # RAM+Disk, 2 replicas
}

print("Available Storage Levels:")
print(f"{'Level':<20} {'Disk':>5} {'Memory':>7} {'Deser':>6} {'Repl':>5}")
print("-" * 50)
for name, level in levels.items():
    print(f"{name:<20} {str(level.useDisk):>5} {str(level.useMemory):>7} "
          f"{str(level.deserialized):>6} {level.replication:>5}")

# Demonstrate persist with different levels
print("\n--- Using persist() with Storage Levels ---")
rdd = sc.parallelize(range(100000), 4)  # Sample data

# MEMORY_ONLY (same as .cache())
mem_rdd = rdd.persist(StorageLevel.MEMORY_ONLY)  # Store in RAM only
mem_rdd.count()  # Trigger caching
print(f"\n1. MEMORY_ONLY: is_cached = {mem_rdd.is_cached}")  # True
print("   Best for: Small-medium RDDs that fit in memory")
mem_rdd.unpersist()  # Free it

# MEMORY_AND_DISK (safest default for large data)
disk_rdd = rdd.persist(StorageLevel.MEMORY_AND_DISK)  # RAM + disk fallback
disk_rdd.count()  # Trigger
print(f"\n2. MEMORY_AND_DISK: is_cached = {disk_rdd.is_cached}")  # True
print("   Best for: Large RDDs where losing partitions to eviction is costly")
disk_rdd.unpersist()  # Free it

# DISK_ONLY (when memory is precious)
disk_only_rdd = rdd.persist(StorageLevel.DISK_ONLY)  # Disk only
disk_only_rdd.count()  # Trigger
print(f"\n3. DISK_ONLY: is_cached = {disk_only_rdd.is_cached}")  # True
print("   Best for: Very large RDDs + limited memory + expensive recomputation")
disk_only_rdd.unpersist()  # Free it

print("\n--- Decision Guide ---")
print("  1. RDD fits in memory entirely? → MEMORY_ONLY")
print("  2. Might not fit, eviction is costly? → MEMORY_AND_DISK")
print("  3. Very large, recomputation is expensive? → DISK_ONLY")
print("  4. Critical data, need fault tolerance? → *_2 (replicated)")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Partition Basics with glom()
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: Understanding Partitions
# ═══════════════════════════════════════════════════════

print("=== Understanding Partitions ===")
print()

# Partitions = how Spark splits data for parallel processing
# More partitions = more parallelism (up to # of cores)

# Create RDD with specific partition count
data = sc.parallelize(range(20), 4)  # 20 items, 4 partitions

print(f"1. Number of partitions: {data.getNumPartitions()}")  # 4

# glom() — shows what's in EACH partition
print(f"\n2. glom() — data per partition:")
partitions = data.glom().collect()  # Returns list of lists
for i, partition in enumerate(partitions):
    print(f"   Partition {i}: {partition}")
# Expected: [0-4], [5-9], [10-14], [15-19] (roughly equal distribution)

# Default partitions depend on cluster config
default_rdd = sc.parallelize(range(100))  # No partition count specified
print(f"\n3. Default partition count: {default_rdd.getNumPartitions()}")
print(f"   (Typically = number of cores in cluster)")

# Partition count affects parallelism
print("\n4. How partition count affects parallelism:")
print("   1 partition  = 1 task = NO parallelism (sequential!)")
print("   4 partitions = 4 tasks = 4x parallelism")
print("   100 partitions = 100 tasks = high parallelism")
print("   10000 partitions = 10000 tasks = TOO MANY (overhead!)")

# mapPartitionsWithIndex — see partition assignment
print("\n5. Partition index for each element:")
def show_partition(idx, iterator):
    for item in iterator:
        yield (idx, item)  # (partition_index, value)

sample = data.mapPartitionsWithIndex(show_partition).take(8)  # First 8
for part_idx, value in sample:
    print(f"   Partition {part_idx} contains: {value}")

print("\n--- Rule of thumb: 2-4 partitions per CPU core ---")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate: Partitioning
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: repartition() vs coalesce()
# ═══════════════════════════════════════════════════════

import time  # For timing

print("=== repartition() vs coalesce() ===")
print()

# Create an RDD with 8 partitions
data = sc.parallelize(range(100), 8)  # 100 items, 8 partitions
print(f"Original partitions: {data.getNumPartitions()}")  # 8
print(f"Data distribution: {[len(p) for p in data.glom().collect()]}")  # Items per partition

# repartition(n) — change to any number (full shuffle)
print("\n--- repartition() (shuffles data) ---")
repartitioned = data.repartition(4)  # Reduce from 8 to 4
print(f"After repartition(4): {repartitioned.getNumPartitions()} partitions")
print(f"Distribution: {[len(p) for p in repartitioned.glom().collect()]}")

uppartitioned = data.repartition(12)  # Increase from 8 to 12
print(f"After repartition(12): {uppartitioned.getNumPartitions()} partitions")

# coalesce(n) — reduce partitions WITHOUT shuffling (merge adjacent)
print("\n--- coalesce() (no shuffle — faster) ---")
coalesced = data.coalesce(2)  # Merge 8 → 2 (no shuffle!)
print(f"After coalesce(2): {coalesced.getNumPartitions()} partitions")
print(f"Distribution: {[len(p) for p in coalesced.glom().collect()]}")
# Note: coalesce CANNOT increase partitions (use repartition for that)

# Performance comparison
print("\n--- Performance: repartition vs coalesce ---")
big = sc.parallelize(range(1000000), 16)  # 1M items, 16 partitions

start = time.time()
_ = big.repartition(4).count()  # Full shuffle!
repart_time = time.time() - start

start = time.time()
_ = big.coalesce(4).count()  # No shuffle!
coal_time = time.time() - start

print(f"repartition(4): {repart_time:.3f}s (shuffles all data)")
print(f"coalesce(4):    {coal_time:.3f}s (merges without shuffle)")
print(f"coalesce is {repart_time/coal_time:.1f}x faster for reducing partitions!")

print("\n--- When to Use Which ---")
print("repartition: Need MORE partitions, or need balanced partitions")
print("coalesce: Need FEWER partitions (e.g., before writing to fewer files)")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Smart Partition Sizing
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 2: Partition Sizing Strategy
# ═══════════════════════════════════════════════════════

import time  # For timing

print("=== How Partition Count Affects Performance ===")
print()

# Generate data for benchmarking
data_size = 2000000  # 2 million elements
base_data = list(range(data_size))  # Reusable list

# Test different partition counts
partition_tests = [1, 2, 4, 8, 16, 32, 64, 200]

print(f"Data size: {data_size:,} elements")
print(f"Operation: map(x*x).filter(x%3==0).count()")
print()
print(f"{'Partitions':<12} {'Time (s)':<10} {'Items/Part':<12} {'Notes'}")
print("-" * 60)

for num_parts in partition_tests:
    rdd = sc.parallelize(base_data, num_parts)  # Create with N partitions
    start = time.time()
    rdd.map(lambda x: x * x).filter(lambda x: x % 3 == 0).count()  # Process
    elapsed = time.time() - start
    items_per_part = data_size // num_parts  # Items per partition
    
    # Determine notes
    if num_parts == 1:
        note = "⚠️  No parallelism!"
    elif num_parts > 100:
        note = "⚠️  Too many (scheduling overhead)"
    elif 4 <= num_parts <= 32:
        note = "✅ Good range"
    else:
        note = ""
    
    print(f"{num_parts:<12} {elapsed:<10.3f} {items_per_part:<12,} {note}")

print("\n--- Recommendations ---")
print("  • Target: 100MB-200MB per partition (for large data)")
print("  • Minimum: 2-4 partitions per CPU core")
print("  • Maximum: avoid > 100K partitions (scheduling overhead)")
print("  • Sweet spot: data_size_bytes / 128MB = good partition count")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Detecting and Fixing Data Skew
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 3: Detecting Data Skew
# ═══════════════════════════════════════════════════════

print("=== Detecting and Fixing Partition Skew ===")
print()

# Skew = uneven data distribution across partitions
# One partition has MUCH more data than others
# Causes: one task takes 10x longer (stragglers)

# Create a SKEWED dataset (simulating real-world skew)
skewed = sc.parallelize(
    [("US", i) for i in range(80000)] +    # 80K records for US (hot key!)
    [("UK", i) for i in range(10000)] +    # 10K for UK
    [("DE", i) for i in range(5000)] +     # 5K for DE
    [("FR", i) for i in range(3000)] +     # 3K for FR
    [("JP", i) for i in range(2000)],      # 2K for JP
    numSlices=4  # 4 partitions
)

# Diagnose: Check partition sizes
print("--- Diagnosis: Check partition sizes ---")
partition_sizes = skewed.glom().map(len).collect()  # Size of each partition
print(f"  Partition sizes: {partition_sizes}")
print(f"  Min: {min(partition_sizes):,}, Max: {max(partition_sizes):,}")
print(f"  Skew ratio: {max(partition_sizes) / min(partition_sizes):.1f}x")

# Check key distribution
print("\n--- Key distribution ---")
key_counts = skewed.map(lambda x: x[0]).countByValue()  # Count per key
for key, count in sorted(key_counts.items(), key=lambda x: -x[1]):
    print(f"  {key}: {count:,} ({count/sum(key_counts.values())*100:.1f}%)")

# Fix 1: repartition() to redistribute
print("\n--- Fix 1: repartition() ---")
balanced = skewed.repartition(8)  # Redistribute randomly across 8 parts
balanced_sizes = balanced.glom().map(len).collect()  # Check new sizes
print(f"  After repartition(8): {balanced_sizes}")
print(f"  Skew ratio: {max(balanced_sizes) / max(1, min(balanced_sizes)):.1f}x")

# Fix 2: Custom partitioner for PairRDDs
print("\n--- Fix 2: Better partitioning with mapPartitions ---")
# For key-value RDDs, use partitionBy with more partitions
repartitioned_by_key = skewed.partitionBy(10)  # 10 partitions by hash
key_part_sizes = repartitioned_by_key.glom().map(len).collect()
print(f"  After partitionBy(10): {key_part_sizes}")

print("\n--- Skew Detection Checklist ---")
print("  1. Check partition sizes: glom().map(len).collect()")
print("  2. If max/min > 5x, you have skew")
print("  3. Fix options:")
print("     a) repartition() for random redistribution")
print("     b) Key salting for aggregation skew")
print("     c) Increase partition count to spread load")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced: Checkpointing
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Checkpointing (Break the Lineage)
# ═══════════════════════════════════════════════════════

print("=== Checkpointing — Saving Progress ===")
print()

# Why checkpoint?
# After 100 transformations, the lineage is HUGE.
# If a partition fails, recomputing from step 1 takes forever.
# Checkpoint writes to disk and CUTS the lineage.

# Set checkpoint directory
sc.setCheckpointDir("/tmp/spark_checkpoints")  # Where to save

# Build a long chain of transformations
rdd = sc.parallelize(range(100000), 4)  # Start
for i in range(20):  # 20 transformations
    rdd = rdd.map(lambda x: x + 1)  # Add 1, twenty times

# Check lineage BEFORE checkpoint
print("Lineage BEFORE checkpoint:")
lineage_before = rdd.toDebugString().decode('utf-8')
print(f"  Lines in lineage: {len(lineage_before.splitlines())}")
print(f"  (Very deep — would recompute all 20 steps on failure)")

# Checkpoint it!
rdd.checkpoint()  # Mark for checkpointing
rdd.count()  # Triggers checkpoint (must call action)

# Check lineage AFTER checkpoint
print("\nLineage AFTER checkpoint:")
lineage_after = rdd.toDebugString().decode('utf-8')
print(f"  Lines in lineage: {len(lineage_after.splitlines())}")
print(f"  (Short! — starts from checkpoint file, not original data)")

# Verify data is correct
print(f"\nFirst 5 elements: {rdd.take(5)}")  # Should be 20, 21, 22, 23, 24
print(f"Expected: {[i+20 for i in range(5)]}")  # Each element had +1 applied 20 times

print("\n--- cache vs checkpoint ---")
print("cache: stores in memory, keeps lineage, lost on restart")
print("checkpoint: stores on disk, CUTS lineage, survives restart")
print("Best practice: cache + checkpoint for long iterative algorithms")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Cache Management Strategy
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 2: Strategic Cache Management
# ═══════════════════════════════════════════════════════

import time
from pyspark import StorageLevel

print("=== Advanced: Cache Management in Complex Pipelines ===")
print()

# Real-World Scenario: ETL pipeline with branching
# One intermediate RDD feeds into 3 different outputs
# Without caching: recomputed 3 times!

# Step 1: Expensive transformation (simulated)
print("--- Pipeline with Branching ---")
raw_data = sc.parallelize(range(1000000), 8)  # 1M elements

# Expensive intermediate result (used by 3 branches)
intermediate = (
    raw_data
    .map(lambda x: x * x)          # Square (expensive math)
    .filter(lambda x: x % 2 == 0)  # Keep evens
    .map(lambda x: x + 1)          # Add 1
)

# Strategy 1: NO cache (recomputes 3 times)
print("Strategy 1: No cache (3 recomputations):")
start = time.time()
branch_a = intermediate.filter(lambda x: x < 1000000).count()    # Branch A
branch_b = intermediate.filter(lambda x: x > 1000000).count()    # Branch B
branch_c = intermediate.map(lambda x: x * 0.5).sum()             # Branch C
no_cache_time = time.time() - start
print(f"  Branch A: {branch_a:,} items")
print(f"  Branch B: {branch_b:,} items")
print(f"  Branch C sum: {branch_c:,.0f}")
print(f"  Total time: {no_cache_time:.3f}s")

# Strategy 2: Cache before branching
print("\nStrategy 2: Cache before branching:")
cached_intermediate = intermediate.cache()  # Mark for caching
start = time.time()
branch_a = cached_intermediate.filter(lambda x: x < 1000000).count()  # Computes + caches
branch_b = cached_intermediate.filter(lambda x: x > 1000000).count()  # From cache!
branch_c = cached_intermediate.map(lambda x: x * 0.5).sum()           # From cache!
cache_time = time.time() - start
print(f"  Branch A: {branch_a:,} items")
print(f"  Branch B: {branch_b:,} items")
print(f"  Branch C sum: {branch_c:,.0f}")
print(f"  Total time: {cache_time:.3f}s")
print(f"  Speedup: {no_cache_time/cache_time:.1f}x")

# IMPORTANT: Unpersist when done with all branches!
cached_intermediate.unpersist()  # Free memory immediately
print("\n  ✅ unpersist() called — memory freed")

# Cache lifecycle management
print("\n--- Cache Lifecycle Best Practice ---")
print("  1. Identify RDDs used in 2+ actions")
print("  2. Cache at the branching point")
print("  3. Trigger caching with first action")
print("  4. Use cached RDD for remaining actions")
print("  5. unpersist() IMMEDIATELY when all branches done")
print("  6. Never cache in loops without unpersist!")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: localCheckpoint vs checkpoint
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 3: Checkpoint Strategies
# ═══════════════════════════════════════════════════════

import time

print("=== Checkpoint Strategies: Regular vs Local ===")
print()

# Two types of checkpointing:
# 1. checkpoint() — writes to reliable storage (HDFS/DBFS)
# 2. localCheckpoint() — writes to executor local disk (faster, less reliable)

sc.setCheckpointDir("/tmp/spark_checkpoints")  # For regular checkpoint

# Build an iterative computation (like ML training)
print("--- Iterative Computation with Lineage Growth ---")
rdd = sc.parallelize(range(50000), 4)  # Start

# Simulate 10 iterations (each adds to lineage)
for i in range(10):
    rdd = rdd.map(lambda x: x + 1)  # Each iteration adds a map step

print(f"After 10 iterations:")
lineage = rdd.toDebugString().decode('utf-8') if isinstance(rdd.toDebugString(), bytes) else rdd.toDebugString()
print(f"  Lineage depth: {len(lineage.splitlines())} steps")
print(f"  Problem: If partition fails, ALL 10 iterations recomputed!")

# Strategy 1: Regular checkpoint (reliable, slower)
print("\n--- Strategy 1: Regular checkpoint() ---")
rdd_ckpt = sc.parallelize(range(50000), 4)
for i in range(10):
    rdd_ckpt = rdd_ckpt.map(lambda x: x + 1)

start = time.time()
rdd_ckpt.checkpoint()     # Mark for checkpointing
rdd_ckpt.count()          # Triggers checkpoint (writes to DBFS)
ckpt_time = time.time() - start
lineage_after = rdd_ckpt.toDebugString().decode('utf-8') if isinstance(rdd_ckpt.toDebugString(), bytes) else rdd_ckpt.toDebugString()
print(f"  Time: {ckpt_time:.3f}s")
print(f"  Lineage after: {len(lineage_after.splitlines())} steps (truncated!)")
print(f"  Storage: DBFS (survives executor failure)")

# Strategy 2: Local checkpoint (faster, executor-local)
print("\n--- Strategy 2: localCheckpoint() ---")
rdd_local = sc.parallelize(range(50000), 4)
for i in range(10):
    rdd_local = rdd_local.map(lambda x: x + 1)

start = time.time()
rdd_local = rdd_local.localCheckpoint()  # Local disk (faster!)
rdd_local.count()  # Triggers local checkpoint
local_time = time.time() - start
print(f"  Time: {local_time:.3f}s")
print(f"  Storage: Executor local disk (lost if executor dies)")

# Comparison table
print(f"\n{'=' * 55}")
print(f"{'Feature':<25} {'checkpoint()':<15} {'localCheckpoint()'}")
print(f"{'-' * 55}")
print(f"{'Storage':<25} {'DBFS/HDFS':<15} {'Local disk'}")
print(f"{'Speed':<25} {'Slower':<15} {'Faster'}")
print(f"{'Survives failure':<25} {'Yes':<15} {'No'}")
print(f"{'Truncates lineage':<25} {'Yes':<15} {'Yes'}")
print(f"{'Time in this test':<25} {ckpt_time:<15.3f} {local_time:.3f}")
print(f"{'=' * 55}")

print("\n--- When to use which ---")
print("  checkpoint(): Long-running jobs, expensive recomputation")
print("  localCheckpoint(): Fast iterations, can afford recompute on failure")
print("  cache(): Performance only, no lineage truncation")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Caching an RDD Used Only Once
# MAGIC **Why bad:** Caching uses memory. If you only use the RDD once, caching wastes memory.  
# MAGIC **Fix:** Only cache if you call multiple actions on the same RDD.
# MAGIC
# MAGIC ### Mistake #2: Forgetting to unpersist()
# MAGIC **Issue:** Cached RDDs stay in memory forever until evicted by LRU.  
# MAGIC **Fix:** Always `unpersist()` when done. Especially in loops.
# MAGIC
# MAGIC ### Mistake #3: coalesce to Increase Partitions
# MAGIC **Issue:** `coalesce(100)` on a 4-partition RDD stays at 4 partitions!  
# MAGIC **Fix:** Use `repartition()` to INCREASE partitions.
# MAGIC
# MAGIC ### Mistake #4: Repartitioning Before Every Operation
# MAGIC **Issue:** repartition triggers a costly shuffle.  
# MAGIC **Fix:** Only repartition when needed (before joins, writes, or skewed operations).
# MAGIC
# MAGIC ### Mistake #5: Not Setting Checkpoint Directory
# MAGIC **Issue:** Calling `rdd.checkpoint()` without `sc.setCheckpointDir()` fails silently.  
# MAGIC **Fix:** Always set checkpoint dir BEFORE calling checkpoint.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC ### Level 1: Create an RDD, cache it, call count() twice, and time both.
# MAGIC ### Level 2: Try all StorageLevel options and print their properties.
# MAGIC ### Level 3: Create a 4-partition RDD. Use glom() to see data per partition.
# MAGIC ### Level 4: repartition from 4 to 8, then coalesce from 8 to 2.
# MAGIC ### Level 5: Time: repartition vs coalesce on 5M elements.
# MAGIC ### Level 6: Cache an RDD, verify is_cached, unpersist, verify again.
# MAGIC ### Level 7: Build a 50-step lineage, checkpoint it, verify shorter lineage.
# MAGIC ### Level 8: Demonstrate data skew: one partition with 90% of data.
# MAGIC ### Level 9: Write a function that recommends partition count based on data size.
# MAGIC ### Level 10: Explain to a colleague when to use cache vs checkpoint vs neither.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════
import time
from pyspark import StorageLevel

# Level 1: Cache timing
print("=== Level 1 ===")
rdd = sc.parallelize(range(2000000)).map(lambda x: x*x).filter(lambda x: x%3==0)
cached = rdd.cache()
start = time.time(); cached.count(); t1 = time.time() - start
start = time.time(); cached.count(); t2 = time.time() - start
print(f"First count (compute+cache): {t1:.3f}s")
print(f"Second count (from cache): {t2:.3f}s")
cached.unpersist()

# Level 2: Storage levels
print("\n=== Level 2 ===")
levels = [
    ("MEMORY_ONLY", StorageLevel.MEMORY_ONLY),
    ("MEMORY_AND_DISK", StorageLevel.MEMORY_AND_DISK),
    ("DISK_ONLY", StorageLevel.DISK_ONLY),
    ("MEMORY_ONLY_SER", StorageLevel(False, True, False, False, 1)),
    ("MEMORY_AND_DISK_2", StorageLevel.MEMORY_AND_DISK_2)
]
for name, level in levels:
    print(f"  {name}: disk={level.useDisk}, memory={level.useMemory}, repl={level.replication}")

# Level 3: glom
print("\n=== Level 3 ===")
data = sc.parallelize(list(range(20)), 4)
for i, part in enumerate(data.glom().collect()):
    print(f"  Partition {i}: {part}")

# Level 8: Data skew demonstration
print("\n=== Level 8 ===")
skewed = sc.parallelize([("hot_key", i) for i in range(900)] + [(f"key_{i}", i) for i in range(100)], 4)
by_key = skewed.groupByKey().mapValues(len)  # Count per key
hot = by_key.filter(lambda x: x[1] > 100).collect()
print(f"Skewed key: {hot}")  # hot_key has 900 values!
print("  Solution: salting or custom partitioner")

# Level 9: Partition recommender
print("\n=== Level 9 ===")
def recommend_partitions(data_size_mb, cores=8):
    """Recommend partition count based on data size."""
    target_partition_mb = 128  # 128MB per partition is ideal
    by_size = max(1, data_size_mb // target_partition_mb)  # At least 1
    by_cores = cores * 3  # 2-4x cores is good
    recommended = max(by_size, by_cores)  # Take the larger
    return recommended
print(f"  1GB data, 8 cores: {recommend_partitions(1024, 8)} partitions")
print(f"  10GB data, 16 cores: {recommend_partitions(10240, 16)} partitions")
print(f"  100MB data, 4 cores: {recommend_partitions(100, 4)} partitions")

print("\n\u2705 All homework complete!")