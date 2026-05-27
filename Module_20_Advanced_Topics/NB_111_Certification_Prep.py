# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 111: Databricks Certification Prep
# MAGIC ## Module 20: Advanced Topics
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 60 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC This notebook is a **study guide** for Databricks certifications: the **Data Engineer Associate**, **Data Engineer Professional**, and **Spark Developer Associate**. It covers the key topics, question patterns, and must-know concepts from across all 20 modules.
# MAGIC
# MAGIC ### Certifications Overview:
# MAGIC | Certification | Level | Focus | Duration |
# MAGIC |--------------|-------|-------|----------|
# MAGIC | Data Engineer Associate | Beginner | ETL, Delta Lake, Spark basics, UC | 90 min, 45 questions |
# MAGIC | Data Engineer Professional | Advanced | Production pipelines, optimization, CI/CD | 120 min, 60 questions |
# MAGIC | Spark Developer Associate | Intermediate | Spark API, DataFrames, SQL, performance | 120 min, 60 questions |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Exam Preparation Strategy:
# MAGIC
# MAGIC   1. REVIEW all 20 modules (notebooks 55-111).
# MAGIC   2. PRACTICE with the key concepts below.
# MAGIC   3. HANDS-ON: Run code, break things, fix them.
# MAGIC   4. TIME yourself: 2 min per question average.
# MAGIC   5. FOCUS on: Delta Lake, Spark SQL, UC, Jobs, Streaming.
# MAGIC
# MAGIC Topic Weight Distribution (Data Engineer Associate):
# MAGIC
# MAGIC   Delta Lake & ETL:        25%  (Modules 9-10)
# MAGIC   Spark SQL & DataFrames:  20%  (Modules 1-8)
# MAGIC   Unity Catalog:           15%  (Module 18)
# MAGIC   Incremental Processing:  15%  (Module 12)
# MAGIC   Jobs & Orchestration:    15%  (Module 17)
# MAGIC   Production Practices:    10%  (Modules 19-20)
# MAGIC
# MAGIC Question Types:
# MAGIC   - Multiple choice (single answer).
# MAGIC   - Multiple select (2-3 correct answers).
# MAGIC   - Code completion (fill in the blank).
# MAGIC   - Scenario-based ("Given this situation, what do you do?")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: Certification Key Concepts
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7 — CERTIFICATION KEY CONCEPTS
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("CERTIFICATION PREP — Key Concepts Quick Reference")
print("="*70)

# ─── TOPIC 1: Delta Lake (25% of exam) ───
print("\n" + "="*60)
print("TOPIC 1: DELTA LAKE (25%)")
print("="*60)

print("""
Must-know:
  ✓ ACID transactions (atomicity, consistency, isolation, durability).
  ✓ Time travel: SELECT * FROM t VERSION AS OF 5; RESTORE TABLE t TO VERSION AS OF 5;
  ✓ MERGE (upsert): MERGE INTO target USING source ON condition WHEN MATCHED/NOT MATCHED.
  ✓ Schema evolution: .option("mergeSchema", "true").
  ✓ OPTIMIZE + ZORDER: OPTIMIZE table ZORDER BY (col); Liquid Clustering.
  ✓ VACUUM: Remove old files. VACUUM table RETAIN 168 HOURS;
  ✓ CDF (Change Data Feed): track row-level changes. table_changes() function.
  ✓ Delta vs Parquet: Delta = Parquet + transaction log + time travel + ACID.

Common exam questions:
  Q: "What happens if you VACUUM with 0 hours retention?"
  A: Time travel breaks (old versions deleted). Default 168h protects against this.

  Q: "How to handle schema changes when appending?"
  A: .option("mergeSchema", "true") on write.
""")

# ─── TOPIC 2: Spark SQL & DataFrames (20%) ───
print("="*60)
print("TOPIC 2: SPARK SQL & DATAFRAMES (20%)")
print("="*60)

print("""
Must-know:
  ✓ Transformations vs Actions:
    Transformations (lazy): filter, select, join, groupBy. Return new DF.
    Actions (eager): count, show, collect, write. Trigger execution.
  ✓ Joins: inner, left, right, full, cross, semi, anti.
  ✓ Window functions: ROW_NUMBER, RANK, LAG, LEAD, SUM OVER.
  ✓ GroupBy + agg: .groupBy("col").agg(sum("x"), avg("y")).
  ✓ Null handling: .isNull(), .isNotNull(), coalesce(), na.fill().
  ✓ UDFs: @udf (slow), @pandas_udf (fast, vectorized).
  ✓ Spark SQL: spark.sql("SELECT ..."), createOrReplaceTempView.

Common exam questions:
  Q: "What is the difference between a transformation and an action?"
  A: Transformations are lazy (build plan), actions are eager (execute plan).

  Q: "How to handle NULLs in a join key?"
  A: NULL != NULL in Spark. Use coalesce() or filter NULLs before joining.
""")

# ─── TOPIC 3: Unity Catalog (15%) ───
print("="*60)
print("TOPIC 3: UNITY CATALOG (15%)")
print("="*60)

print("""
Must-know:
  ✓ Three-level namespace: catalog.schema.table.
  ✓ Permissions: GRANT USE CATALOG, GRANT USE SCHEMA, GRANT SELECT.
  ✓ Data security modes: USER_ISOLATION (shared), SINGLE_USER (jobs).
  ✓ Row filters + Column masks: dynamic security based on user identity.
  ✓ External locations: govern access to cloud storage paths.
  ✓ Volumes: managed file storage within UC.
  ✓ Lineage: automatic tracking via system.access.table_lineage.

Common exam questions:
  Q: "What permissions are needed to SELECT from a table?"
  A: USE CATALOG on catalog + USE SCHEMA on schema + SELECT on table.

  Q: "Where is lineage tracked?"
  A: Automatically by UC. Query system.access.table_lineage.
""")

# ─── TOPIC 4: Streaming & Incremental (15%) ───
print("="*60)
print("TOPIC 4: STRUCTURED STREAMING (15%)")
print("="*60)

print("""
Must-know:
  ✓ Auto Loader: spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").
  ✓ Triggers: availableNow (batch), processingTime("10 seconds").
  ✓ Output modes: append (new rows), complete (all rows), update (changed rows).
  ✓ Watermarks: .withWatermark("timestamp", "10 minutes") for late data.
  ✓ Checkpointing: .option("checkpointLocation", "/path") for fault tolerance.
  ✓ Stream-static joins: streaming DF joined with static (batch) DF.

Common exam questions:
  Q: "What does Auto Loader's cloudFiles.schemaLocation do?"
  A: Stores inferred/evolved schema. Enables schema evolution tracking.

  Q: "What happens if you restart a stream without a checkpoint?"
  A: Reprocesses ALL data from scratch (no exactly-once guarantee).
""")

# ─── TOPIC 5: Jobs & Production (15%) ───
print("="*60)
print("TOPIC 5: JOBS & PRODUCTION (15%)")
print("="*60)

print("""
Must-know:
  ✓ Job clusters vs interactive (auto-terminate, cost savings).
  ✓ Task dependencies: depends_on for DAG ordering.
  ✓ Parameters: Job passes to notebook widgets.
  ✓ Task values: dbutils.jobs.taskValues.set/get for inter-task communication.
  ✓ Retries + timeouts: max_retries, timeout_seconds.
  ✓ Alerts: email_notifications on_failure.
  ✓ DABs: Infrastructure-as-Code for Databricks (databricks.yml + bundle deploy).
  ✓ Idempotency: MERGE or replaceWhere (safe to re-run).

Common exam questions:
  Q: "How to make an ETL notebook idempotent?"
  A: Use MERGE (upsert) or overwrite with replaceWhere on partition.

  Q: "Job cluster vs all-purpose cluster for scheduled jobs?"
  A: Job cluster (auto-terminates = cost-efficient, dedicated resources).
""")

# ─── TOPIC 6: Performance (10%) ───
print("="*60)
print("TOPIC 6: PERFORMANCE & OPTIMIZATION (10%)")
print("="*60)

print("""
Must-know:
  ✓ AQE (Adaptive Query Execution): auto-coalesce, auto-skew-fix.
  ✓ Broadcast joins: small table (<10MB default) broadcast to all executors.
  ✓ Partition pruning: filter on partition column = read less data.
  ✓ Predicate pushdown: filters pushed to storage layer.
  ✓ Caching: .cache() for reused DFs. .unpersist() when done.
  ✓ Shuffle: Exchange operator in plan. Minimize with broadcast/repartition.
  ✓ Photon: native vectorized engine (2-8x faster, auto-enabled on Photon clusters).
  ✓ EXPLAIN: df.explain(mode="formatted") to read execution plans.

Common exam questions:
  Q: "What does AQE do?"
  A: Dynamically optimizes query at runtime: coalesces partitions,
     handles skew joins, switches join strategy based on actual data size.

  Q: "How to force a broadcast join?"
  A: from pyspark.sql.functions import broadcast; big.join(broadcast(small), key)
""")

# ─── FINAL TIPS ───
print("\n" + "="*60)
print("EXAM TIPS")
print("="*60)

print("""
  1. Read EVERY answer choice (tricky distractors!).
  2. Eliminate obviously wrong answers first.
  3. For code questions: trace the logic step by step.
  4. Time management: 2 min/question. Flag hard ones, come back.
  5. Databricks-specific: know UC, Delta Lake, Auto Loader, Jobs.
     (Not just generic Spark knowledge!)
  6. Practice: Databricks Academy courses + hands-on labs.
  7. Review these notebooks (55-111) — they cover 90% of exam topics.
""")

print("\n" + "="*70)
print("✓ CERTIFICATION PREP COMPLETED — Notebook 111")
print("✓ MODULE 20 (Advanced Topics) COMPLETE! All 8 notebooks done.")
print("✓ ENTIRE DATABRICKS KT SERIES COMPLETE! Notebooks 55-111.")
print("="*70)