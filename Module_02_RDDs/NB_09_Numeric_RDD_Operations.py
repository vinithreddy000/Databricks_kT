# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 09: Numeric RDD Operations
# MAGIC # Module: RDDs (Resilient Distributed Datasets)
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 30 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: A Calculator for Lists
# MAGIC
# MAGIC When your RDD contains numbers, Spark provides **built-in statistics** — like having a calculator that works on millions of numbers instantly:
# MAGIC - Sum, mean, min, max
# MAGIC - Standard deviation and variance
# MAGIC - Histograms (distribution)
# MAGIC - All statistics at once via `stats()`
# MAGIC
# MAGIC Think of it as: **Excel's descriptive statistics panel, but for billions of numbers.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What Are Numeric RDD Operations?
# MAGIC
# MAGIC When you have an RDD of numbers (int, float, long), Spark provides extra methods:
# MAGIC
# MAGIC | Method | What It Returns |
# MAGIC |--------|----------------|
# MAGIC | `sum()` | Total of all elements |
# MAGIC | `mean()` | Average value |
# MAGIC | `max()` / `min()` | Largest / smallest |
# MAGIC | `stdev()` | Standard deviation (sample) |
# MAGIC | `variance()` | Variance (sample) |
# MAGIC | `stats()` | ALL of the above in one call |
# MAGIC | `histogram(buckets)` | Distribution in buckets |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The `stats()` Shortcut
# MAGIC
# MAGIC Instead of calling `sum()`, `mean()`, `count()`, `stdev()`, `min()`, `max()` separately (6 passes through the data), call `stats()` ONCE and get everything in a single pass. Much more efficient!

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### How Numeric Operations Execute
# MAGIC
# MAGIC ```
# MAGIC   Driver calls: rdd.stats()
# MAGIC       │
# MAGIC       ├─ Partition 1: compute local (count, sum, min, max, sumOfSquares)
# MAGIC       ├─ Partition 2: compute local (count, sum, min, max, sumOfSquares)
# MAGIC       ├─ Partition 3: compute local (count, sum, min, max, sumOfSquares)
# MAGIC       │
# MAGIC       └─ Driver merges all partial stats into ONE StatCounter
# MAGIC          → mean = totalSum / totalCount
# MAGIC          → stdev = sqrt(sumOfSquares/count - mean²)
# MAGIC          → All computed WITHOUT collecting raw data!
# MAGIC ```
# MAGIC
# MAGIC ### StatCounter Object
# MAGIC
# MAGIC The `stats()` method returns a `StatCounter` that holds:
# MAGIC - `count()` — number of elements
# MAGIC - `mean()` — average (sum/count)
# MAGIC - `sum()` — total
# MAGIC - `min()` / `max()` — extremes
# MAGIC - `stdev()` — population standard deviation
# MAGIC - `sampleStdev()` — sample standard deviation (n-1)
# MAGIC - `variance()` / `sampleVariance()`
# MAGIC
# MAGIC ### Histogram Internals
# MAGIC
# MAGIC ```python
# MAGIC # Auto-buckets: Spark computes min/max, divides into N equal-width ranges
# MAGIC buckets, counts = rdd.histogram(5)  # Returns (boundaries, counts)
# MAGIC # boundaries = [min, ..., max]  (6 boundaries for 5 buckets)
# MAGIC # counts = [count_in_bucket_1, ..., count_in_bucket_5]
# MAGIC
# MAGIC # Custom buckets: You specify the boundaries
# MAGIC counts = rdd.histogram([0, 25, 50, 75, 100])  # 4 buckets
# MAGIC ```
# MAGIC
# MAGIC ### Key Insight: One Pass vs Multiple Passes
# MAGIC
# MAGIC | Approach | Passes Through Data | Network Cost |
# MAGIC |----------|--------------------|--------------|
# MAGIC | `sum()` + `mean()` + `stdev()` | 3 separate passes | 3x shuffle |
# MAGIC | `stats()` | 1 single pass | 1x shuffle |
# MAGIC
# MAGIC Always prefer `stats()` when you need multiple statistics!

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Histogram and Distribution
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: Histograms
# ═══════════════════════════════════════════════════════

print("=== Histograms: Understanding Data Distribution ===")
print()

# Scenario: Student exam scores (want to see grade distribution)
exam_scores = sc.parallelize([
    95, 87, 73, 62, 88, 91, 45, 78, 82, 69,
    94, 55, 71, 83, 90, 67, 76, 84, 59, 92,
    88, 75, 81, 66, 93, 70, 85, 77, 64, 96
])  # 30 students

# Method 1: Auto-histogram (specify NUMBER of buckets)
print("1. Auto-histogram (5 equal-width buckets):")
buckets, counts = exam_scores.histogram(5)  # 5 buckets
print(f"   Boundaries: {[round(b, 1) for b in buckets]}")
print(f"   Counts: {counts}")
print(f"   Interpretation: {counts[0]} scores in [{buckets[0]:.0f}-{buckets[1]:.0f}), etc.")

# Method 2: Custom boundaries (grade ranges)
print("\n2. Custom histogram (grade boundaries):")
grade_counts = exam_scores.histogram([0, 60, 70, 80, 90, 101])  # F, D, C, B, A
print(f"   F (0-59):  {grade_counts[0]} students")
print(f"   D (60-69): {grade_counts[1]} students")
print(f"   C (70-79): {grade_counts[2]} students")
print(f"   B (80-89): {grade_counts[3]} students")
print(f"   A (90-100):{grade_counts[4]} students")

# Visualize with ASCII art
print("\n3. ASCII histogram:")
grades = ['F', 'D', 'C', 'B', 'A']  # Grade labels
for i, (grade, count) in enumerate(zip(grades, grade_counts)):
    bar = '█' * count  # One block per student
    print(f"   {grade}: {bar} ({count})")

# Expected Output:
# A nice distribution showing most students in B-C range
print("\n--- Key: histogram() reveals distribution patterns in one pass ---")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Comparing Datasets with Stats
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: Comparing Datasets
# ═══════════════════════════════════════════════════════

print("=== Comparing Multiple Datasets ===")
print()

# Scenario: Compare response times of two servers
server_a = sc.parallelize([  # Response times in ms
    120, 135, 115, 140, 125, 130, 118, 142, 128, 133,
    122, 137, 119, 145, 126, 131, 124, 138, 121, 136
])

server_b = sc.parallelize([  # Response times in ms
    95, 180, 88, 200, 92, 175, 90, 210, 85, 190,
    87, 195, 93, 185, 91, 220, 86, 178, 89, 205
])

# Get stats for both in one pass each
stats_a = server_a.stats()  # All stats for server A
stats_b = server_b.stats()  # All stats for server B

# Comparison table
print("┌───────────────┬───────────────┬───────────────┐")
print("│ Metric        │ Server A      │ Server B      │")
print("├───────────────┼───────────────┼───────────────┤")
print(f"│ Count         │ {stats_a.count():>13} │ {stats_b.count():>13} │")
print(f"│ Mean (ms)     │ {stats_a.mean():>13.1f} │ {stats_b.mean():>13.1f} │")
print(f"│ Min (ms)      │ {stats_a.min():>13.1f} │ {stats_b.min():>13.1f} │")
print(f"│ Max (ms)      │ {stats_a.max():>13.1f} │ {stats_b.max():>13.1f} │")
print(f"│ Stdev (ms)    │ {stats_a.stdev():>13.2f} │ {stats_b.stdev():>13.2f} │")
print(f"│ Range (ms)    │ {stats_a.max()-stats_a.min():>13.1f} │ {stats_b.max()-stats_b.min():>13.1f} │")
print("└───────────────┴───────────────┴───────────────┘")

# Analysis
print("\nAnalysis:")
if stats_a.mean() < stats_b.mean():
    print(f"  • Server A is faster on average ({stats_a.mean():.0f}ms vs {stats_b.mean():.0f}ms)")
else:
    print(f"  • Server B is faster on average ({stats_b.mean():.0f}ms vs {stats_a.mean():.0f}ms)")

if stats_a.stdev() < stats_b.stdev():
    print(f"  • Server A is more consistent (stdev {stats_a.stdev():.1f} vs {stats_b.stdev():.1f})")
else:
    print(f"  • Server B is more consistent (stdev {stats_b.stdev():.1f} vs {stats_a.stdev():.1f})")

print(f"  • Server A range: {stats_a.max()-stats_a.min():.0f}ms (tight)")
print(f"  • Server B range: {stats_b.max()-stats_b.min():.0f}ms (highly variable!)")
print("\n  Verdict: Server A is better — faster AND more predictable")
print("\n--- Key: stats() lets you compare datasets quickly and efficiently ---")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — All Numeric RDD Operations
# ═══════════════════════════════════════════════════════
# SECTIONS 3-5 — All Numeric RDD Operations
# ═══════════════════════════════════════════════════════

sc = spark.sparkContext  # Get SparkContext

print("=== Numeric RDD Operations ===")
print()

# Create a numeric RDD (simulating sensor readings)
readings = sc.parallelize([23.5, 24.1, 22.8, 25.0, 23.9, 24.5, 22.1, 25.5, 24.8, 23.2,
                           26.0, 21.5, 24.3, 25.2, 23.7, 22.9, 24.6, 25.8, 23.0, 24.0])

# --- Individual Statistics ---
print("--- Individual Statistics ---")
print(f"count(): {readings.count()}")  # 20 readings
print(f"sum():   {readings.sum()}")  # Total
print(f"mean():  {readings.mean():.2f}")  # Average
print(f"max():   {readings.max()}")  # Highest reading
print(f"min():   {readings.min()}")  # Lowest reading
print(f"stdev(): {readings.stdev():.4f}")  # Standard deviation
print(f"variance(): {readings.variance():.4f}")  # Variance

# --- stats() — ALL statistics in one call ---
print("\n--- stats() — Everything at Once ---")
stat_result = readings.stats()  # Returns a StatCounter object
print(f"Stats object: {stat_result}")
print(f"  count: {stat_result.count()}")
print(f"  mean:  {stat_result.mean():.2f}")
print(f"  stdev: {stat_result.stdev():.4f}")
print(f"  max:   {stat_result.max()}")
print(f"  min:   {stat_result.min()}")
print(f"  sum:   {stat_result.sum()}")

# --- histogram() — Distribution analysis ---
print("\n--- histogram() — Distribution ---")
# histogram with number of buckets (auto-ranges)
buckets, counts = readings.histogram(5)  # Split into 5 equal-width buckets
print(f"Bucket boundaries: {[round(b, 1) for b in buckets]}")
print(f"Counts per bucket: {counts}")
print("Interpretation: How many readings fall in each range")

# histogram with explicit boundaries
print("\nCustom buckets [20, 22, 24, 26, 28]:")
custom_counts = readings.histogram([20, 22, 24, 26, 28])  # Custom ranges
print(f"Counts: {custom_counts}")
print("  20-22: readings in the 'cold' range")
print("  22-24: readings in the 'normal' range")
print("  24-26: readings in the 'warm' range")
print("  26-28: readings in the 'hot' range")

print("\n--- Pro Tip ---")
print("stats() does ONE pass through the data for ALL statistics.")
print("Calling sum(), mean(), stdev() separately = 3 passes = 3x slower!")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Single-Pass Multi-Stat Efficiency
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 1: Efficiency Demo
# ═══════════════════════════════════════════════════════

import time  # For timing comparisons

print("=== Efficiency: stats() vs Separate Calls ===")
print()

# Create a large numeric RDD (1 million elements)
large_nums = sc.parallelize(range(1000000), 8)  # 1M numbers, 8 partitions
large_nums.cache()  # Cache to make comparison fair
large_nums.count()  # Trigger cache

# --- Approach 1: Call each stat separately (SLOW: multiple passes) ---
print("--- Approach 1: Separate calls (multiple passes) ---")
start = time.time()
my_count = large_nums.count()      # Pass 1 through data
my_sum = large_nums.sum()          # Pass 2 through data
my_mean = large_nums.mean()        # Pass 3 through data
my_min = large_nums.min()          # Pass 4 through data
my_max = large_nums.max()          # Pass 5 through data
my_stdev = large_nums.stdev()      # Pass 6 through data
separate_time = time.time() - start
print(f"  count={my_count}, sum={my_sum}, mean={my_mean:.1f}")
print(f"  min={my_min}, max={my_max}, stdev={my_stdev:.2f}")
print(f"  Time: {separate_time:.3f}s (6 passes through data!)")

# --- Approach 2: Single stats() call (FAST: one pass) ---
print("\n--- Approach 2: stats() (single pass) ---")
start = time.time()
all_stats = large_nums.stats()  # ONE pass gets everything!
single_time = time.time() - start
print(f"  count={all_stats.count()}, sum={all_stats.sum()}, mean={all_stats.mean():.1f}")
print(f"  min={all_stats.min()}, max={all_stats.max()}, stdev={all_stats.stdev():.2f}")
print(f"  Time: {single_time:.3f}s (1 pass through data!)")

# Comparison
print(f"\n{'=' * 40}")
speedup = separate_time / single_time if single_time > 0 else float('inf')
print(f"  Separate: {separate_time:.3f}s")
print(f"  stats(): {single_time:.3f}s")
print(f"  Speedup: ~{speedup:.1f}x faster with stats()")
print(f"\n  Rule: ALWAYS use stats() when you need > 1 statistic")

large_nums.unpersist()  # Cleanup

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Percentile Estimation
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 2: Percentiles and Quartiles
# ═══════════════════════════════════════════════════════

print("=== Percentile Computation with RDDs ===")
print()

# PySpark RDDs don't have built-in percentile, but we can compute them!

# Scenario: API response times (want P50, P90, P95, P99)
import random
random.seed(42)  # Reproducible results

# Generate realistic response times (mostly fast, some slow)
response_times = sc.parallelize(
    [random.gauss(100, 20) for _ in range(5000)] +   # Normal: ~100ms
    [random.gauss(500, 100) for _ in range(500)] +   # Slow: ~500ms
    [random.gauss(2000, 300) for _ in range(50)]     # Very slow: ~2000ms
)  # 5550 requests

# Method 1: Sort and index (exact percentiles for small-medium data)
def compute_percentiles(rdd, percentiles):
    """Compute exact percentiles by sorting."""
    sorted_data = rdd.sortBy(lambda x: x).collect()  # Sort all data
    n = len(sorted_data)  # Total count
    results = {}  # Store results
    for p in percentiles:
        idx = int(n * p / 100)  # Index for this percentile
        idx = min(idx, n - 1)  # Don't exceed bounds
        results[f"P{p}"] = sorted_data[idx]  # Get value at index
    return results

percentiles = compute_percentiles(response_times, [50, 75, 90, 95, 99])

print("API Response Time Percentiles:")
print("-" * 40)
for label, value in percentiles.items():
    print(f"  {label}: {value:.1f}ms")

# Also show basic stats for context
stats = response_times.stats()  # Single pass stats
print(f"\n  Mean: {stats.mean():.1f}ms")
print(f"  Stdev: {stats.stdev():.1f}ms")
print(f"  Min: {stats.min():.1f}ms")
print(f"  Max: {stats.max():.1f}ms")

# SLA analysis
print("\n  SLA Analysis:")
sla_200ms = response_times.filter(lambda x: x <= 200).count()  # Under 200ms
total = response_times.count()  # Total requests
print(f"  Requests under 200ms: {sla_200ms}/{total} ({sla_200ms/total*100:.1f}%)")
print(f"  Requests over 1000ms: {response_times.filter(lambda x: x > 1000).count()}")

print("\n--- Key: Percentiles need sorting; use approx methods for very large data ---")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Rolling Statistics with Partitions
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 3: Per-Partition Statistics
# ═══════════════════════════════════════════════════════

print("=== Per-Partition Statistics ===")
print()

# Scenario: Data from multiple sensors stored in different partitions
# We want stats PER partition to detect sensor anomalies

import random
random.seed(42)

# Simulate 4 sensors (each gets its own partition)
sensor_data = (
    [("S1", random.gauss(25, 2)) for _ in range(100)] +   # Sensor 1: ~25°C
    [("S2", random.gauss(30, 5)) for _ in range(100)] +   # Sensor 2: ~30°C (noisy)
    [("S3", random.gauss(22, 1)) for _ in range(100)] +   # Sensor 3: ~22°C (stable)
    [("S4", random.gauss(28, 8)) for _ in range(100)]     # Sensor 4: ~28°C (very noisy)
)

sensors = sc.parallelize(sensor_data, 4)  # 4 partitions

# Compute stats per sensor using mapPartitions
def partition_stats(index, iterator):
    """Compute stats for one partition (sensor)."""
    values = []  # Collect values for this partition
    sensor_id = None  # Track sensor ID
    for sensor, reading in iterator:
        sensor_id = sensor  # Get sensor name
        values.append(reading)  # Collect readings
    
    if values:  # If partition has data
        n = len(values)  # Count
        mean_val = sum(values) / n  # Mean
        variance = sum((x - mean_val) ** 2 for x in values) / n  # Variance
        stdev_val = variance ** 0.5  # Stdev
        yield (sensor_id, {
            "count": n,
            "mean": round(mean_val, 2),
            "stdev": round(stdev_val, 2),
            "min": round(min(values), 2),
            "max": round(max(values), 2)
        })

results = sensors.mapPartitionsWithIndex(partition_stats).collect()

print("Per-Sensor Statistics:")
print("=" * 65)
print(f"{'Sensor':<8} {'Count':>6} {'Mean':>8} {'Stdev':>8} {'Min':>8} {'Max':>8}")
print("-" * 65)
for sensor_id, stats in sorted(results):
    print(f"{sensor_id:<8} {stats['count']:>6} {stats['mean']:>8.2f} {stats['stdev']:>8.2f} {stats['min']:>8.2f} {stats['max']:>8.2f}")

# Anomaly detection: flag sensors with high stdev
print("\nAnomaly Detection (stdev > 4.0):")
for sensor_id, stats in sorted(results):
    status = "⚠️  HIGH VARIANCE" if stats["stdev"] > 4.0 else "✅ Normal"
    print(f"  {sensor_id}: stdev={stats['stdev']:.2f} → {status}")

print("\n--- Key: mapPartitionsWithIndex gives per-partition control ---")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Outlier Detection Pipeline
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 1: Outlier Detection Pipeline
# ═══════════════════════════════════════════════════════

import random
random.seed(42)

print("=== Advanced: Statistical Outlier Detection ===")
print()

# Scenario: Detect anomalous transactions in financial data
# Use multiple statistical methods to identify outliers

# Generate transaction amounts: mostly normal, some fraudulent
normal_txns = [random.gauss(100, 30) for _ in range(980)]   # Normal: ~$100
fraud_txns = [random.gauss(5000, 1000) for _ in range(15)]  # Fraud: ~$5000
micro_txns = [random.uniform(0.01, 0.50) for _ in range(5)]  # Suspiciously tiny

transactions = sc.parallelize(normal_txns + fraud_txns + micro_txns, 4)
print(f"Total transactions: {transactions.count()}")

# Method 1: Z-Score (how many standard deviations from mean)
print("\n--- Method 1: Z-Score Outliers (|z| > 3) ---")
stats = transactions.stats()  # Get stats in one pass
mean = stats.mean()  # Mean amount
stdev = stats.stdev()  # Standard deviation
print(f"  Mean: ${mean:.2f}, Stdev: ${stdev:.2f}")

# Filter outliers where |value - mean| > 3 * stdev
z_threshold = 3  # Standard threshold
z_outliers = transactions.filter(
    lambda x: abs(x - mean) > z_threshold * stdev  # Z-score > 3
).collect()
print(f"  Z-score outliers: {len(z_outliers)} transactions")
print(f"  Examples: {[f'${x:.2f}' for x in sorted(z_outliers)[:5]]}")

# Method 2: IQR (Interquartile Range) — robust to extreme outliers
print("\n--- Method 2: IQR Outliers ---")
sorted_data = transactions.sortBy(lambda x: x).collect()  # Sort for percentiles
n = len(sorted_data)
q1 = sorted_data[n // 4]       # 25th percentile
q3 = sorted_data[3 * n // 4]   # 75th percentile
iqr = q3 - q1                   # Interquartile range
lower_fence = q1 - 1.5 * iqr   # Lower bound
upper_fence = q3 + 1.5 * iqr   # Upper bound
print(f"  Q1: ${q1:.2f}, Q3: ${q3:.2f}, IQR: ${iqr:.2f}")
print(f"  Fences: [${lower_fence:.2f}, ${upper_fence:.2f}]")

iqr_outliers = transactions.filter(
    lambda x: x < lower_fence or x > upper_fence  # Outside fences
).collect()
print(f"  IQR outliers: {len(iqr_outliers)} transactions")

# Method 3: Percentile-based (top/bottom 1%)
print("\n--- Method 3: Percentile Outliers (top/bottom 1%) ---")
p1 = sorted_data[int(n * 0.01)]    # 1st percentile
p99 = sorted_data[int(n * 0.99)]   # 99th percentile
print(f"  P1: ${p1:.2f}, P99: ${p99:.2f}")
pct_outliers = transactions.filter(lambda x: x < p1 or x > p99).collect()
print(f"  Percentile outliers: {len(pct_outliers)} transactions")

# Summary
print(f"\n{'=' * 50}")
print("OUTLIER DETECTION SUMMARY:")
print(f"{'=' * 50}")
print(f"  Z-Score (|z|>3): {len(z_outliers)} flagged")
print(f"  IQR (1.5×IQR):  {len(iqr_outliers)} flagged")
print(f"  Percentile (1%): {len(pct_outliers)} flagged")
print("\n  Recommendation: Use IQR for robust detection (less sensitive to extreme values)")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: StatCounter Merge for Streaming
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 2: StatCounter Merge
# ═══════════════════════════════════════════════════════

from pyspark import StatCounter  # Import StatCounter class

print("=== StatCounter: Mergeable Statistics ===")
print()

# Real-World Scenario: Incrementally updating statistics
# as new data arrives (like a streaming pipeline)

# Batch 1: First hour of data
batch1 = sc.parallelize([10.5, 12.3, 11.8, 13.2, 10.9, 12.6])
stats1 = batch1.stats()  # Compute stats for batch 1
print("Batch 1 stats:")
print(f"  count={stats1.count()}, mean={stats1.mean():.2f}, stdev={stats1.stdev():.2f}")

# Batch 2: Second hour of data
batch2 = sc.parallelize([14.1, 15.5, 13.8, 16.2, 14.7, 15.0, 13.3])
stats2 = batch2.stats()  # Compute stats for batch 2
print("\nBatch 2 stats:")
print(f"  count={stats2.count()}, mean={stats2.mean():.2f}, stdev={stats2.stdev():.2f}")

# Merge stats WITHOUT reprocessing raw data!
print("\n--- Merging StatCounters (no raw data needed!) ---")
merged = stats1.mergeStats(stats2)  # Combine both StatCounters
print(f"Merged stats:")
print(f"  count={merged.count()}, mean={merged.mean():.2f}, stdev={merged.stdev():.2f}")
print(f"  min={merged.min()}, max={merged.max()}, sum={merged.sum():.1f}")

# Verify: compute stats on combined data directly
all_data = sc.parallelize([10.5, 12.3, 11.8, 13.2, 10.9, 12.6, 14.1, 15.5, 13.8, 16.2, 14.7, 15.0, 13.3])
verify = all_data.stats()
print(f"\nVerification (recomputed from scratch):")
print(f"  count={verify.count()}, mean={verify.mean():.2f}, stdev={verify.stdev():.2f}")
print(f"  Match: {abs(merged.mean() - verify.mean()) < 0.001}")  # True!

# Simulate streaming: merge 5 batches incrementally
print("\n--- Simulating 5-Batch Streaming Pipeline ---")
import random
random.seed(42)

running_stats = None  # Will hold merged stats

for batch_num in range(1, 6):
    # Generate a new batch of data
    batch_data = sc.parallelize([random.gauss(100, 15) for _ in range(100)])
    batch_stats = batch_data.stats()  # Stats for this batch
    
    if running_stats is None:
        running_stats = batch_stats  # First batch
    else:
        running_stats = running_stats.mergeStats(batch_stats)  # Merge with history
    
    print(f"  After batch {batch_num}: count={running_stats.count()}, "
          f"mean={running_stats.mean():.2f}, stdev={running_stats.stdev():.2f}")

print("\n  Key: StatCounter.mergeStats() gives O(1) merge regardless of data size!")
print("  Perfect for streaming: keep running stats without storing all raw data.")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Multi-Dimensional Stats with aggregate()
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 3: Custom Aggregate Statistics
# ═══════════════════════════════════════════════════════

import random
import math
random.seed(42)

print("=== Advanced: Custom Multi-Metric Aggregation ===")
print()

# Scenario: Compute MULTIPLE custom statistics in a SINGLE pass
# Can't use stats() alone (need median estimate, percentiles, skewness)

# Generate realistic dataset: website page load times
page_loads = sc.parallelize(
    [random.lognormvariate(1.5, 0.8) for _ in range(10000)],  # Log-normal distribution
    8  # 8 partitions
)

# Custom aggregate: compute (count, sum, sumSq, min, max, histogram_buckets)
# in ONE pass using aggregate()

# Define histogram boundaries for page load categorization
bucket_boundaries = [0, 1, 2, 3, 5, 10, float('inf')]  # seconds
num_buckets = len(bucket_boundaries) - 1

# Zero value: initial accumulator state
zero_value = {
    "count": 0,
    "sum": 0.0,
    "sum_sq": 0.0,  # For variance/stdev
    "min": float('inf'),
    "max": float('-inf'),
    "histogram": [0] * num_buckets  # Counts per bucket
}

# seqOp: merge one element into accumulator (within partition)
def seq_op(acc, value):
    acc["count"] += 1                            # Increment count
    acc["sum"] += value                          # Add to sum
    acc["sum_sq"] += value * value               # Add to sum of squares
    acc["min"] = min(acc["min"], value)           # Track minimum
    acc["max"] = max(acc["max"], value)           # Track maximum
    # Assign to histogram bucket
    for i in range(num_buckets):
        if bucket_boundaries[i] <= value < bucket_boundaries[i + 1]:
            acc["histogram"][i] += 1
            break
    return acc

# combOp: merge two accumulators (across partitions)
def comb_op(acc1, acc2):
    return {
        "count": acc1["count"] + acc2["count"],
        "sum": acc1["sum"] + acc2["sum"],
        "sum_sq": acc1["sum_sq"] + acc2["sum_sq"],
        "min": min(acc1["min"], acc2["min"]),
        "max": max(acc1["max"], acc2["max"]),
        "histogram": [a + b for a, b in zip(acc1["histogram"], acc2["histogram"])]
    }

# Run the single-pass aggregation!
result = page_loads.aggregate(zero_value, seq_op, comb_op)  # ONE pass!

# Compute derived statistics
mean = result["sum"] / result["count"]  # Mean
variance = (result["sum_sq"] / result["count"]) - (mean ** 2)  # Variance
stdev = math.sqrt(max(0, variance))  # Stdev (max 0 for floating point safety)

# Print comprehensive report
print("╔═════════════════════════════════════════════╗")
print("║     PAGE LOAD TIME ANALYSIS              ║")
print("╠═════════════════════════════════════════════╣")
print(f"║ Total Requests:    {result['count']:>20,} ║")
print(f"║ Mean:              {mean:>20.3f}s ║")
print(f"║ Stdev:             {stdev:>20.3f}s ║")
print(f"║ Min:               {result['min']:>20.3f}s ║")
print(f"║ Max:               {result['max']:>20.3f}s ║")
print("╠═════════════════════════════════════════════╣")
print("║ DISTRIBUTION (Page Load Buckets)         ║")
print("╠═════════════════════════════════════════════╣")
bucket_labels = ["< 1s", "1-2s", "2-3s", "3-5s", "5-10s", "> 10s"]
for label, count in zip(bucket_labels, result["histogram"]):
    pct = count / result["count"] * 100  # Percentage
    bar = '█' * int(pct / 2)  # Scale bar
    print(f"║ {label:<6} {bar:<20} {count:>5} ({pct:>5.1f}%) ║")
print("╚═════════════════════════════════════════════╝")

print("\n--- Key: aggregate() computes EVERYTHING in a single pass ---")
print("--- Even custom histograms, no second scan needed! ---")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Calling Multiple Stats Separately
# MAGIC **Issue:** `rdd.sum()` + `rdd.mean()` + `rdd.stdev()` = 3 passes through the data.  
# MAGIC **Fix:** Use `rdd.stats()` for ONE pass that gives you everything.
# MAGIC
# MAGIC ### Mistake #2: Using stats() on Non-Numeric RDD
# MAGIC **Issue:** Calling `.mean()` on an RDD of strings crashes.  
# MAGIC **Fix:** Ensure your RDD contains only numbers before using numeric operations.
# MAGIC
# MAGIC ### Mistake #3: Confusing stdev() with sampleStdev()
# MAGIC **Note:** In PySpark, `stdev()` computes the POPULATION standard deviation.  
# MAGIC For sample stdev, use `sampleStdev()`.
# MAGIC
# MAGIC ### Mistake #4: histogram() Bucket Boundaries
# MAGIC **Issue:** Custom boundaries must be sorted and have at least 2 elements.  
# MAGIC **Fix:** Always provide boundaries in ascending order.
# MAGIC
# MAGIC ### Mistake #5: Empty RDD Statistics
# MAGIC **Issue:** Calling stats on empty RDD gives count=0, but mean/stdev are undefined.  
# MAGIC **Fix:** Check `count() > 0` before computing statistics.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC ### Level 1: Create a numeric RDD and call sum(), mean(), max(), min().
# MAGIC ### Level 2: Use stats() and print each field from the StatCounter.
# MAGIC ### Level 3: Create a histogram with 10 buckets from random data.
# MAGIC ### Level 4: Compare calling 5 individual stats vs one stats() call (timing).
# MAGIC ### Level 5: Generate 1M random numbers, compute stats, and create a histogram.
# MAGIC ### Level 6: Design a function that detects outliers using mean ± 2*stdev.
# MAGIC ### Level 7: Use histogram with custom boundaries for grade distribution.
# MAGIC ### Level 8: Handle edge case: what does stats() return for a single-element RDD?
# MAGIC ### Level 9: Build a streaming-style stats updater using StatCounter.merge().
# MAGIC ### Level 10: Explain when to use RDD numeric ops vs DataFrame describe().

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════
import time, random

# Level 1
print("=== Level 1 ===")
nums = sc.parallelize([15, 28, 42, 7, 93, 56, 31])
print(f"sum={nums.sum()}, mean={nums.mean():.1f}, max={nums.max()}, min={nums.min()}")

# Level 2
print("\n=== Level 2 ===")
stats = nums.stats()
print(f"count={stats.count()}, mean={stats.mean():.2f}, stdev={stats.stdev():.2f}")
print(f"min={stats.min()}, max={stats.max()}, sum={stats.sum()}")

# Level 4: Timing comparison
print("\n=== Level 4 ===")
big = sc.parallelize(range(1000000))
start = time.time()
_ = big.sum(); _ = big.mean(); _ = big.stdev(); _ = big.min(); _ = big.max()
indiv_time = time.time() - start
start = time.time()
_ = big.stats()
stats_time = time.time() - start
print(f"5 separate calls: {indiv_time:.3f}s")
print(f"1 stats() call: {stats_time:.3f}s")
print(f"stats() is {indiv_time/stats_time:.1f}x faster!")

# Level 6: Outlier detection
print("\n=== Level 6 ===")
def find_outliers(rdd, num_stdevs=2):
    """Find values more than num_stdevs standard deviations from mean."""
    s = rdd.stats()  # Get all stats in one pass
    lower = s.mean() - num_stdevs * s.stdev()  # Lower bound
    upper = s.mean() + num_stdevs * s.stdev()  # Upper bound
    outliers = rdd.filter(lambda x: x < lower or x > upper).collect()  # Find outliers
    return outliers, lower, upper

data = sc.parallelize([10, 12, 11, 13, 12, 100, 11, 12, 13, -50, 12, 11])  # Has outliers
outliers, lo, hi = find_outliers(data)
print(f"Bounds: [{lo:.1f}, {hi:.1f}]")
print(f"Outliers: {outliers}")  # [100, -50]

# Level 7: Grade histogram
print("\n=== Level 7 ===")
grades = sc.parallelize([95,87,73,62,88,91,45,78,82,69,94,55,71,83,90,67,76,84,59,92])
counts = grades.histogram([0, 60, 70, 80, 90, 100])  # F, D, C, B, A
print(f"Grade distribution [F,D,C,B,A]: {counts}")

print("\n\u2705 All homework complete!")