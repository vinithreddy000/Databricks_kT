# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 110: Cost Optimization in Databricks
# MAGIC ## Module 20: Advanced Topics
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Cost optimization** means getting the same (or better) results while spending less on compute. In Databricks, the biggest cost drivers are clusters running too long, too big, or doing unnecessary work. This notebook covers strategies to cut costs 30-60% without sacrificing performance.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Running a **taxi fleet**: You don't keep all taxis running 24/7. You scale up during rush hour, scale down at night, use smaller cars for short trips, and plan routes efficiently. Databricks cost optimization works the same way.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Cost Optimization Levers:
# MAGIC
# MAGIC   ┌─────────────────┬─────────────────┬───────────────┬───────────────┐
# MAGIC   │  Compute        │  Storage         │  Code           │  Governance    │
# MAGIC   ├─────────────────┼─────────────────┼───────────────┼───────────────┤
# MAGIC   │ Auto-terminate  │ Delta OPTIMIZE   │ Predicate      │ Cluster       │
# MAGIC   │ Right-size      │ VACUUM old files │ pushdown       │ policies      │
# MAGIC   │ Spot instances  │ Partition        │ Broadcast joins│ Budgets       │
# MAGIC   │ Autoscale       │ pruning          │ Cache reuse    │ Chargebacks   │
# MAGIC   │ Job clusters    │ Z-order/Liquid   │ Fewer shuffles │ Tags          │
# MAGIC   │ Serverless      │ clustering       │ AQE            │ Monitoring    │
# MAGIC   └─────────────────┴─────────────────┴───────────────┴───────────────┘
# MAGIC
# MAGIC Quick Wins (impact vs effort):
# MAGIC   1. Auto-termination (60 min) → Saves 40% idle cost. [HIGH impact, LOW effort]
# MAGIC   2. Job clusters (not interactive) → Saves 30-50%. [HIGH impact, LOW effort]
# MAGIC   3. Spot instances for workers → Saves 60-80% on workers. [HIGH, MEDIUM]
# MAGIC   4. Right-size clusters → Don't use 10 nodes for 1GB of data. [MEDIUM, LOW]
# MAGIC   5. Photon engine → 2-8x faster = less runtime = less cost. [HIGH, LOW]
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: Cost Optimization Examples
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7 — COST OPTIMIZATION
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTIONS 3-7: Cost Optimization Strategies")
print("="*70)

# ─── STRATEGY 1: Cluster sizing ───
print("\n" + "-"*60)
print("STRATEGY 1: Right-size your clusters")
print("-"*60)

print("""
Cluster Sizing Guide:

  Data Size    | Workers | Node Type           | Estimated Cost/hr
  < 10 GB      | 1-2     | Standard_E4ds_v5    | ~$2-4/hr
  10-100 GB    | 2-4     | Standard_E8ds_v5    | ~$5-15/hr
  100 GB-1 TB  | 4-8     | Standard_E16ds_v5   | ~$15-40/hr
  > 1 TB       | 8-16+   | Standard_E32ds_v5   | ~$40-100/hr

Rules of thumb:
  - Each worker handles ~50-100GB of data efficiently.
  - Autoscale: set min=2, max=8 (scales down when idle).
  - Single-node: fine for < 5GB and development.
  - Spot instances for workers: 60-80% cheaper (risk: preemption).
""")

# ─── STRATEGY 2: Job clusters vs interactive ───
print("-"*60)
print("STRATEGY 2: Job clusters (auto-terminate after task)")
print("-"*60)

print("""
Interactive cluster: Stays running until you stop it. Expensive!
  Cost: $10/hr × 24hr = $240/day (even if idle 90% of the time!).

Job cluster: Starts for the job, terminates immediately after.
  Cost: $10/hr × 2hr actual work = $20/day (90% savings!).

Migration:
  Before: Interactive cluster running 24/7 for scheduled notebooks.
  After:  Job cluster per task. Only pays for actual compute time.

Serverless Jobs:
  - No cluster management. Sub-second startup.
  - Pay only for compute seconds used.
  - Best for: short-running jobs, variable workloads.
""")

# ─── STRATEGY 3: Code-level optimizations ───
print("-"*60)
print("STRATEGY 3: Code optimizations that reduce cost")
print("-"*60)

print("""
1. FILTER EARLY (predicate pushdown):
   # BAD: Read all data, then filter.
   df = spark.table("big_table")  # Reads 1TB.
   result = df.filter(col("date") == "2024-05-27")  # Uses only 10GB.
   
   # GOOD: Partition pruning — only reads relevant partitions.
   # If table is partitioned by date, Spark only reads the 10GB partition.

2. BROADCAST SMALL TABLES:
   # BAD: Shuffle-join (moves 1TB across network).
   big.join(small, "key")  # SortMergeJoin = expensive shuffle.
   
   # GOOD: Broadcast the small table (no shuffle!).
   big.join(broadcast(small), "key")  # BroadcastHashJoin = fast.

3. CACHE WISELY:
   # BAD: Caching everything (wastes memory, spills to disk).
   df.cache()  # Only cache if df is reused 2+ times!
   
   # GOOD: Cache only reused DataFrames. Unpersist when done.
   df.cache()  # Used in 3 downstream operations.
   # ... use df ...
   df.unpersist()  # Free memory after use.

4. USE DELTA OPTIMIZE:
   OPTIMIZE catalog.schema.table;  # Compacts small files.
   # Reduces file I/O by 10-50x for tables with many small files.
""")

# ─── STRATEGY 4: Monitoring costs ───
print("-"*60)
print("STRATEGY 4: Monitor and control costs")
print("-"*60)

print("""
System tables for cost monitoring:

  -- Daily cost by workspace.
  SELECT
    date(usage_date) as day,
    SUM(usage_quantity * list_price) as estimated_cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices p ON u.sku_name = p.sku_name
  WHERE usage_date >= current_date() - INTERVAL 30 DAYS
  GROUP BY 1 ORDER BY 1;

  -- Cost by cluster (find expensive clusters).
  SELECT
    cluster_id,
    SUM(usage_quantity) as total_dbus
  FROM system.billing.usage
  WHERE usage_date >= current_date() - INTERVAL 7 DAYS
  GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

Governance:
  - Cluster policies: limit max nodes, enforce auto-terminate.
  - Budgets: set alerts when spending exceeds threshold.
  - Tags: tag clusters by team/project for chargeback.
  - Reviews: weekly cost review dashboard.
""")

# ─── HOMEWORK ───
print("\n" + "="*70)
print("HOMEWORK — Cost Optimization")
print("="*70)
print("  Level 1-3: Auto-terminate, right-size, job clusters.")
print("  Level 4-6: Spot instances, Photon, broadcast joins.")
print("  Level 7-10: Billing queries, cluster policies, budgets, serverless.")
print("  Rule: 'If it's not running a job, it shouldn't be running.'")
print("\n" + "="*70)
print("✓ HOMEWORK COMPLETED — Notebook 110")
print("="*70)