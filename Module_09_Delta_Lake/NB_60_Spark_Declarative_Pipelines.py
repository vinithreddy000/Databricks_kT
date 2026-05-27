# Databricks notebook source
# DBTITLE 1,Sections 1-2 Overview
# MAGIC %md
# MAGIC # Notebook 60: Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables)
# MAGIC ## Module 09: Delta Lake Deep Dive
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Lakeflow Spark Declarative Pipelines (SDP) is a **managed ETL framework** built on Delta Lake. Instead of writing complex pipeline orchestration code, you **declare** what your tables should look like, and Databricks handles the execution, monitoring, error recovery, and data quality.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Think of **ordering food at a restaurant**:
# MAGIC - **Without SDP (imperative)**: You go into the kitchen, find ingredients, cook it yourself, serve it, clean up
# MAGIC - **With SDP (declarative)**: You tell the waiter "I want a Caesar salad" and it arrives perfectly
# MAGIC
# MAGIC You describe WHAT you want (the end result), not HOW to build it.
# MAGIC
# MAGIC ### Key Features:
# MAGIC 1. **Declarative** — Define tables, not execution steps
# MAGIC 2. **Managed** — Automatic retries, checkpointing, scaling
# MAGIC 3. **Data Quality** — Built-in expectations (constraints) with actions
# MAGIC 4. **Medallion Architecture** — Built for Bronze → Silver → Gold patterns
# MAGIC 5. **Streaming + Batch** — Same code works for both
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Spark Declarative Pipeline Architecture:
# MAGIC
# MAGIC   ┌───────────────┐     ┌───────────────┐     ┌───────────────┐
# MAGIC   │   BRONZE        │     │   SILVER        │     │   GOLD          │
# MAGIC   │  (Raw Ingest)   │ →   │  (Cleaned)      │ →   │  (Aggregated)   │
# MAGIC   │                 │     │                 │     │                 │
# MAGIC   │ @dlt.table      │     │ @dlt.table      │     │ @dlt.table      │
# MAGIC   │ Streaming Table │     │ + Expectations  │     │ Materialized    │
# MAGIC   └───────────────┘     └───────────────┘     └───────────────┘
# MAGIC
# MAGIC Key Decorators:
# MAGIC   @dlt.table          → Define a materialized table
# MAGIC   @dlt.view           → Define a temporary view (not persisted)
# MAGIC   @dlt.expect()       → Log quality violations (continue processing)
# MAGIC   @dlt.expect_or_drop() → Drop rows that fail quality check
# MAGIC   @dlt.expect_or_fail() → Fail the pipeline if ANY row violates
# MAGIC
# MAGIC Reading from other SDP tables:
# MAGIC   dlt.read("table_name")        → Batch read
# MAGIC   dlt.read_stream("table_name") → Streaming read
# MAGIC ```
# MAGIC
# MAGIC **IMPORTANT**: SDP code runs ONLY inside a Databricks Pipeline. The cells below show the code patterns — you cannot run them directly in a regular notebook. They must be deployed to a pipeline.

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 1: Basic SDP Table
# SECTION 3 — BEGINNER EXAMPLE 1: Basic SDP Table Definitions
# Real-world: Define a simple Bronze → Silver pipeline.
# NOTE: This code is for a PIPELINE notebook, not regular execution.

print("=== Spark Declarative Pipeline Code Patterns ===")
print("NOTE: This code must run inside a Databricks Pipeline, not a regular notebook.\n")

# Pattern 1: Basic table definition.
print("--- Pattern 1: Basic Table Definition ---")
print("""
import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(
    comment="Raw orders ingested from source system"
)
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/landing/orders/")
        .withColumn("ingestion_time", current_timestamp())
    )
""")

# Pattern 2: Cleaned silver table.
print("\n--- Pattern 2: Silver Table (reads from Bronze) ---")
print("""
@dlt.table(
    comment="Cleaned and validated orders"
)
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .select(
            col("order_id").cast("int"),
            col("customer_id").cast("int"),
            col("amount").cast("double"),
            col("order_date").cast("date"),
            col("status")
        )
        .filter(col("order_id").isNotNull())
    )
""")

# Pattern 3: Gold aggregation.
print("\n--- Pattern 3: Gold Table (business aggregation) ---")
print("""
@dlt.table(
    comment="Daily order summary by customer"
)
def gold_daily_summary():
    return (
        dlt.read("silver_orders")
        .groupBy("customer_id", "order_date")
        .agg(
            count("*").alias("num_orders"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        )
    )
""")

# COMMAND ----------

# DBTITLE 1,Section 3 - Beginner Example 2: Views and Expectations
# SECTION 3 — BEGINNER EXAMPLE 2: Views and Data Quality Expectations
# Real-world: Temporary transformations and quality gates.

print("=== SDP Views and Expectations ===")  # Heading.

# Views: temporary, not persisted.
print("--- Views (Temporary Transformations) ---")
print("""
import dlt

# A view is NOT persisted as a table.
# Use it for intermediate logic that doesn't need to be stored.
@dlt.view(
    comment="Intermediate: orders with enriched region mapping"
)
def enriched_orders():
    region_map = spark.createDataFrame([
        ("NY", "Northeast"), ("CA", "West"), ("TX", "South")
    ], ["state", "region"])
    return (
        dlt.read_stream("bronze_orders")
        .join(region_map, "state", "left")
    )
""")

# Expectations: Three levels of enforcement.
print("\n--- Expectations: Data Quality Rules ---")
print("""
# Level 1: EXPECT (log warning, continue processing)
@dlt.table()
@dlt.expect("valid_email", "email LIKE '%@%'")
def users_warn():
    return dlt.read("raw_users")
    # Bad rows pass through, but violation is logged in metrics

# Level 2: EXPECT_OR_DROP (remove bad rows silently)
@dlt.table()
@dlt.expect_or_drop("positive_amount", "amount > 0")
@dlt.expect_or_drop("valid_date", "order_date <= current_date()")
def orders_clean():
    return dlt.read("raw_orders")
    # Bad rows are dropped. Only clean data lands in table.

# Level 3: EXPECT_OR_FAIL (stop the pipeline)
@dlt.table()
@dlt.expect_or_fail("no_nulls_in_pk", "id IS NOT NULL")
def critical_table():
    return dlt.read("source_data")
    # If ANY row has null id, the entire pipeline FAILS.
    # Use for critical data quality that must never be violated.
""")

# Multiple expectations.
print("\n--- Multiple Expectations ---")
print("""
@dlt.table()
@dlt.expect("valid_age", "age BETWEEN 0 AND 150")
@dlt.expect_or_drop("has_name", "name IS NOT NULL")
@dlt.expect_or_drop("valid_status", "status IN ('active', 'inactive')")
@dlt.expect_or_fail("has_id", "customer_id IS NOT NULL")
def validated_customers():
    return dlt.read_stream("raw_customers")
""")

# COMMAND ----------

# DBTITLE 1,Section 4-5 Full Pipeline and Exercises
# SECTION 4 — INTERMEDIATE: Full Medallion Pipeline
# Real-world: Complete Bronze → Silver → Gold with all best practices.

print("=== Full Medallion Pipeline Pattern ===")  # Heading.
print("""
# ============================================================
# COMPLETE SDP PIPELINE: E-Commerce Orders
# ============================================================

import dlt
from pyspark.sql.functions import *

# ---- BRONZE: Raw Ingestion ----
@dlt.table(
    comment="Raw orders from landing zone. Append-only.",
    table_properties={"quality": "bronze"}
)
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/mnt/landing/orders/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

# ---- SILVER: Cleaned + Validated ----
@dlt.table(
    comment="Validated orders. Bad rows dropped.",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0 AND amount < 100000")
@dlt.expect_or_drop("valid_date", "order_date IS NOT NULL")
@dlt.expect("reasonable_quantity", "quantity BETWEEN 1 AND 1000")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .select(
            col("order_id").cast("int"),
            col("customer_id").cast("int"),
            col("amount").cast("double"),
            col("quantity").cast("int"),
            col("order_date").cast("date"),
            trim(upper(col("category"))).alias("category"),
            trim(col("status")).alias("status"),
            col("_ingested_at")
        )
        .dropDuplicates(["order_id"])
    )

# ---- GOLD: Business Aggregations ----
@dlt.table(
    comment="Daily revenue by category. Updated incrementally.",
    table_properties={"quality": "gold"}
)
def gold_daily_revenue():
    return (
        dlt.read("silver_orders")
        .groupBy("order_date", "category")
        .agg(
            count("*").alias("num_orders"),
            sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_order_value"),
            sum("quantity").alias("total_quantity")
        )
    )

@dlt.table(
    comment="Customer lifetime value summary.",
    table_properties={"quality": "gold"}
)
def gold_customer_ltv():
    return (
        dlt.read("silver_orders")
        .groupBy("customer_id")
        .agg(
            count("*").alias("total_orders"),
            sum("amount").alias("lifetime_value"),
            min("order_date").alias("first_order"),
            max("order_date").alias("last_order")
        )
    )
""")

print("\n" + "="*60)
print("SECTION 6 — Key Takeaways")
print("="*60)
print("""
1. SDP is DECLARATIVE: define WHAT, not HOW
2. Three expectation levels: expect (log), expect_or_drop (filter), expect_or_fail (stop)
3. Use dlt.read_stream() for incremental, dlt.read() for full refresh
4. Views are temporary; tables are persisted
5. Pipeline manages checkpoints, retries, ordering automatically
6. Perfect for Medallion Architecture (Bronze/Silver/Gold)
7. Monitor quality metrics in the pipeline UI
""")

print("\n" + "="*60)
print("SECTION 7 — HOMEWORK")
print("="*60)
print("""
Level 1: Write a @dlt.table that reads a CSV file from a landing zone
Level 2: Add a @dlt.expect_or_drop for a NOT NULL constraint
Level 3: Create a Silver table that reads from your Bronze table
Level 4: Add multiple expectations with different enforcement levels
Level 5: Build a complete Bronze → Silver → Gold pipeline

To test: Create a Pipeline in Databricks, point it at your notebook,
and click "Start" to run the pipeline.
""")
print("Module 9 complete! All 7 notebooks (54-60) cover Delta Lake comprehensively.")