# Databricks notebook source
# DBTITLE 1,NB_53 Header
# MAGIC %md
# MAGIC # NB_53 — Medallion Architecture (Bronze / Silver / Gold)
# MAGIC
# MAGIC **Module 8: Transformations & Reshaping** | Notebook 53 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Bronze layer: raw ingestion, append-only, schema-on-read
# MAGIC * Silver layer: cleaned, conformed, deduplicated, typed
# MAGIC * Gold layer: aggregated, business-ready, feature-engineered
# MAGIC * Layer transition patterns and contracts
# MAGIC * Data quality gates between layers
# MAGIC * Metadata and lineage tracking
# MAGIC * Multi-source medallion pipelines
# MAGIC * Production patterns and anti-patterns
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐⭐ (Architecture-level pattern for Lakehouse)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What is Medallion Architecture
# MAGIC %md
# MAGIC ## SECTION 1 — What is Medallion Architecture? (Real-World Analogy)
# MAGIC
# MAGIC ### 🏅 The Water Purification Plant
# MAGIC
# MAGIC The Medallion Architecture is like a water treatment system:
# MAGIC
# MAGIC ```
# MAGIC River Water (Raw)   →  Bronze: Capture everything, no filtering
# MAGIC        ↓
# MAGIC Settling Tanks      →  Silver: Remove debris, standardize, deduplicate
# MAGIC        ↓
# MAGIC Drinking Water      →  Gold: Ready for consumption, meets quality standards
# MAGIC ```
# MAGIC
# MAGIC ### The Three Layers
# MAGIC
# MAGIC | Layer | Purpose | Format | Quality |
# MAGIC |---|---|---|---|
# MAGIC | 🥉 Bronze | Raw ingestion | As-is from source | No guarantees |
# MAGIC | 🥈 Silver | Cleaned & conformed | Typed, deduped, validated | Business rules applied |
# MAGIC | 🥇 Gold | Business-ready | Aggregated, enriched | SLA-quality |
# MAGIC
# MAGIC ### Key Principles
# MAGIC 1. **Immutable Bronze** — Never modify raw data (reprocess from Bronze)
# MAGIC 2. **Incremental Processing** — Each layer processes only new/changed data
# MAGIC 3. **Schema Evolution** — Bronze is flexible, Gold is strict
# MAGIC 4. **Replayability** — Can rebuild Silver/Gold from Bronze at any time
# MAGIC 5. **Quality Gates** — Data must pass checks to advance layers
# MAGIC
# MAGIC ### Anti-Patterns to Avoid
# MAGIC ```
# MAGIC ✘ Skipping Bronze (loading directly to Silver)
# MAGIC ✘ Business logic in Bronze layer
# MAGIC ✘ No deduplication in Silver
# MAGIC ✘ Untyped columns in Gold
# MAGIC ✘ No data quality checks between layers
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 2 — Layer Design Patterns
# MAGIC %md
# MAGIC ## SECTION 2 — Layer Design Patterns
# MAGIC
# MAGIC ### Bronze Layer Contract
# MAGIC ```python
# MAGIC # What goes IN:
# MAGIC # - Raw files (JSON, CSV, Parquet, XML)
# MAGIC # - API responses
# MAGIC # - CDC events
# MAGIC # - Streaming data
# MAGIC
# MAGIC # What it ADDS:
# MAGIC # - _ingest_timestamp: when data was loaded
# MAGIC # - _source_file: which file/API it came from
# MAGIC # - _batch_id: processing batch identifier
# MAGIC
# MAGIC # Rules:
# MAGIC # - Append-only (never update/delete)
# MAGIC # - Schema-on-read (store as-is)
# MAGIC # - No transformations
# MAGIC # - No data type casting
# MAGIC ```
# MAGIC
# MAGIC ### Silver Layer Contract
# MAGIC ```python
# MAGIC # Transformations:
# MAGIC # - Deduplicate (exact + fuzzy)
# MAGIC # - Cast to proper types
# MAGIC # - Standardize formats (dates, booleans, codes)
# MAGIC # - Apply business rules (NULL handling, defaults)
# MAGIC # - Conform across sources (same field = same name/type)
# MAGIC # - Add surrogate keys
# MAGIC
# MAGIC # Quality Checks:
# MAGIC # - No duplicate primary keys
# MAGIC # - Required fields are NOT NULL
# MAGIC # - Referential integrity (FK exists in parent)
# MAGIC # - Value ranges within bounds
# MAGIC ```
# MAGIC
# MAGIC ### Gold Layer Contract
# MAGIC ```python
# MAGIC # Transformations:
# MAGIC # - Aggregate (daily, weekly, monthly)
# MAGIC # - Join dimensions to facts
# MAGIC # - Calculate KPIs and metrics
# MAGIC # - Feature engineering for ML
# MAGIC # - Denormalize for query performance
# MAGIC
# MAGIC # Quality:
# MAGIC # - Matches business definitions exactly
# MAGIC # - Tested against known results
# MAGIC # - SLA on freshness and completeness
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Bronze layer ingestion
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Bronze Layer Ingestion
# ============================================================
# Real-world: Ingest raw e-commerce events into Bronze.

from pyspark.sql import SparkSession  # Import.
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name, monotonically_increasing_id
)  # Functions.

spark = SparkSession.builder.getOrCreate()  # Session.

# Simulate raw event data (as it arrives from source).
print("=== BRONZE LAYER: Raw Ingestion ===")  # Heading.

# Raw data: messy, untyped, potentially duplicated.
raw_events = spark.createDataFrame([
    ("evt_001", "2024-01-15 10:30:00", "click", "user_1", "/products/widget", "mobile", "US"),
    ("evt_002", "2024-01-15 10:31:00", "view", "user_2", "/home", "desktop", "UK"),
    ("evt_003", "2024-01-15 10:32:00", "purchase", "user_1", "/checkout", "mobile", "US"),
    ("evt_001", "2024-01-15 10:30:00", "click", "user_1", "/products/widget", "mobile", "US"),  # DUPLICATE!
    ("evt_004", "2024-01-15 10:33:00", "click", None, "/products/gadget", "tablet", None),  # NULLs.
    ("evt_005", "bad-timestamp", "unknown_event", "user_3", "", "mobile", "XX"),  # Bad data.
    ("evt_006", "2024-01-15 10:35:00", "view", "user_4", "/products/doohickey", "desktop", "DE"),
], ["event_id", "timestamp", "event_type", "user_id", "page_url", "device", "country"])  # Raw.

print("Raw source data (as-is from source system):")
raw_events.show(truncate=False)  # Display.

# Bronze ingestion: add metadata, keep everything as-is.
bronze_events = raw_events.withColumn(
    "_ingest_timestamp", current_timestamp()  # When we ingested.
).withColumn(
    "_source_system", lit("web_analytics_v2")  # Source identifier.
).withColumn(
    "_batch_id", lit("batch_2024011501")  # Processing batch.
).withColumn(
    "_bronze_id", monotonically_increasing_id()  # Unique row ID in Bronze.
)

print("=== Bronze Table (raw + metadata) ===")  # Heading.
bronze_events.printSchema()  # Schema.
bronze_events.show(truncate=False)  # Display.

print(f"""
Bronze Layer Rules Applied:
✓ All original data preserved (including duplicates and bad data)
✓ Metadata columns added (_ingest_timestamp, _source_system, _batch_id)
✓ No type casting (timestamp is still a STRING)
✓ No filtering or deduplication
✓ Append-only (each batch adds new rows)

Row count: {bronze_events.count()} (includes duplicate evt_001)
""")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Silver layer cleaning
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Silver Layer Cleaning
# ============================================================
# Real-world: Clean and conform Bronze events into Silver.

from pyspark.sql.functions import (
    col, when, trim, upper, to_timestamp, lit, row_number,
    count, sum as spark_sum
)  # Imports.
from pyspark.sql.window import Window  # Window.

print("=== SILVER LAYER: Clean & Conform ===")  # Heading.

# Start from Bronze (remove metadata columns for clarity).
bronze = bronze_events.drop("_ingest_timestamp", "_source_system", "_batch_id", "_bronze_id")  # Source.

print(f"Bronze input rows: {bronze.count()}")  # Count.

# Step 1: Deduplicate.
print("\n--- Step 1: Deduplication ---")  # Heading.
w_dedup = Window.partitionBy("event_id").orderBy(col("timestamp").desc())  # Window.
deduped = bronze.withColumn(
    "_row_num", row_number().over(w_dedup)  # Rank duplicates.
).filter(col("_row_num") == 1).drop("_row_num")  # Keep first.
print(f"After dedup: {deduped.count()} rows (removed {bronze.count() - deduped.count()} duplicates)")  # Count.

# Step 2: Type casting and parsing.
print("\n--- Step 2: Type Casting ---")  # Heading.
typed = deduped.withColumn(
    "event_timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")  # Parse.
).drop("timestamp")  # Remove raw.

# Step 3: Standardize values.
print("\n--- Step 3: Standardization ---")  # Heading.
standardized = typed.withColumn(
    "device", upper(trim(col("device")))  # Uppercase.
).withColumn(
    "country",
    when(col("country").isin("XX", "", "UNKNOWN"), None)  # Invalid -> NULL.
    .otherwise(upper(trim(col("country"))))  # Clean.
).withColumn(
    "event_type",
    when(col("event_type") == "unknown_event", None)  # Invalid type -> NULL.
    .otherwise(col("event_type"))  # Keep.
).withColumn(
    "page_url",
    when(trim(col("page_url")) == "", None)  # Empty -> NULL.
    .otherwise(col("page_url"))  # Keep.
)

# Step 4: Quality filter (remove rows that fail critical checks).
print("\n--- Step 4: Quality Gate ---")  # Heading.
quality_passed = standardized.filter(
    col("event_timestamp").isNotNull() &  # Must have valid timestamp.
    col("event_type").isNotNull()  # Must have valid event type.
)  # Filter.

quarantined = standardized.filter(
    col("event_timestamp").isNull() | col("event_type").isNull()  # Failed.
)

print(f"Passed quality gate: {quality_passed.count()} rows")  # Passed.
print(f"Quarantined (bad data): {quarantined.count()} rows")  # Failed.

if quarantined.count() > 0:  # Show failures.
    print("\nQuarantined records:")  # Heading.
    quarantined.show(truncate=False)  # Display.

# Final Silver table.
print("\n=== Silver Table (cleaned & conformed) ===")  # Heading.
silver_events = quality_passed.select(
    col("event_id"),  # PK.
    col("event_timestamp"),  # Proper timestamp.
    col("event_type"),  # Validated.
    col("user_id"),  # May be NULL (anonymous).
    col("page_url"),  # Cleaned.
    col("device"),  # Standardized.
    col("country"),  # Standardized.
)
silver_events.printSchema()  # Schema.
silver_events.show(truncate=False)  # Display.
print(f"Silver: {silver_events.count()} rows (from {bronze.count()} Bronze rows)")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Gold layer aggregation
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Gold Layer Aggregation
# ============================================================
# Real-world: Business-ready metrics from Silver data.

from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg,
    date_trunc, hour, dayofweek, when, round as spark_round
)  # Imports.

print("=== GOLD LAYER: Business Metrics ===")  # Heading.

# Gold Table 1: Daily event summary.
print("\n--- Gold Table: Daily Event Summary ---")  # Heading.
gold_daily = silver_events.withColumn(
    "event_date", date_trunc("day", col("event_timestamp"))  # Truncate to day.
).groupBy("event_date").agg(
    count("*").alias("total_events"),  # Total events.
    countDistinct("user_id").alias("unique_users"),  # DAU.
    count(when(col("event_type") == "click", 1)).alias("clicks"),  # Clicks.
    count(when(col("event_type") == "view", 1)).alias("views"),  # Views.
    count(when(col("event_type") == "purchase", 1)).alias("purchases"),  # Purchases.
)

# Add derived KPIs.
gold_daily_kpi = gold_daily.withColumn(
    "conversion_rate",
    spark_round(col("purchases") / col("unique_users") * 100, 2)  # Conv rate.
).withColumn(
    "click_through_rate",
    spark_round(col("clicks") / col("views") * 100, 2)  # CTR.
)

gold_daily_kpi.show()  # Display.

# Gold Table 2: User engagement scores.
print("--- Gold Table: User Engagement ---")  # Heading.
gold_users = silver_events.filter(
    col("user_id").isNotNull()  # Only identified users.
).groupBy("user_id").agg(
    count("*").alias("total_actions"),  # Total.
    countDistinct("event_type").alias("action_types"),  # Variety.
    count(when(col("event_type") == "purchase", 1)).alias("purchases"),  # Purchases.
    countDistinct("page_url").alias("pages_visited"),  # Breadth.
)

# Engagement score: weighted composite.
gold_engagement = gold_users.withColumn(
    "engagement_score",
    spark_round(
        col("total_actions") * 1.0 +  # Activity weight.
        col("action_types") * 5.0 +  # Variety bonus.
        col("purchases") * 20.0 +  # High-value action.
        col("pages_visited") * 2.0,  # Exploration.
    2)  # Score.
).withColumn(
    "segment",
    when(col("engagement_score") >= 50, "HIGH")
    .when(col("engagement_score") >= 20, "MEDIUM")
    .otherwise("LOW")  # Segment.
)

gold_engagement.show()  # Display.

# Gold Table 3: Device & country breakdown.
print("--- Gold Table: Traffic by Device & Country ---")  # Heading.
gold_traffic = silver_events.groupBy("device", "country").agg(
    count("*").alias("events"),  # Total.
    countDistinct("user_id").alias("users"),  # Unique.
).orderBy(col("events").desc())  # Sort.
gold_traffic.show()  # Display.

print("""
Gold Layer Summary:
✓ Daily KPIs: conversion rate, CTR, DAU
✓ User segments: engagement scoring
✓ Traffic analysis: device × country
✓ All derived from clean Silver data
✓ Ready for dashboards and ML
""")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Multi-source medallion
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Multi-Source Medallion
# ============================================================
# Real-world: Merge data from multiple sources through medallion layers.

from pyspark.sql.functions import (
    col, lit, when, to_timestamp, to_date, upper, trim,
    current_timestamp, md5, concat_ws, coalesce
)  # Imports.

print("=== Multi-Source Medallion Pipeline ===")  # Heading.

# Source A: CRM system (structured).
crm_raw = spark.createDataFrame([
    ("C001", "Alice Johnson", "alice@email.com", "2024-01-10", "Gold", "Seattle"),
    ("C002", "Bob Smith", "bob@work.com", "2024-01-11", "Silver", "Portland"),
    ("C003", "Carol Lee", "carol@home.com", "2024-01-12", "Bronze", "Denver"),
], ["customer_id", "full_name", "email", "signup_date", "tier", "city"])  # CRM.

# Source B: E-commerce platform (different format).
ecom_raw = spark.createDataFrame([
    ("alice@email.com", "Alice J.", "2024-01-15", "widget", 2, 10.00, "completed"),
    ("alice@email.com", "Alice J.", "2024-01-16", "gadget", 1, 25.00, "completed"),
    ("bob@work.com", "Bob S.", "2024-01-15", "widget", 5, 10.00, "pending"),
    ("unknown@test.com", "Unknown", "2024-01-15", "doohickey", 3, 5.00, "completed"),
], ["email", "name", "order_date", "product", "quantity", "unit_price", "status"])  # E-com.

print("--- Source A: CRM ---")  # Heading.
crm_raw.show()  # Display.
print("--- Source B: E-commerce ---")  # Heading.
ecom_raw.show()  # Display.

# BRONZE: Ingest both sources with metadata.
print("\n=== BRONZE: Raw Ingestion ===")  # Heading.
bronze_crm = crm_raw.withColumn("_source", lit("crm")).withColumn("_ingested_at", current_timestamp())  # Meta.
bronze_ecom = ecom_raw.withColumn("_source", lit("ecommerce")).withColumn("_ingested_at", current_timestamp())  # Meta.
print(f"Bronze CRM: {bronze_crm.count()} rows, Bronze Ecom: {bronze_ecom.count()} rows")  # Count.

# SILVER: Clean and conform both to unified schema.
print("\n=== SILVER: Clean & Conform ===")  # Heading.

# Silver customers (from CRM).
silver_customers = bronze_crm.select(
    col("customer_id"),  # PK.
    col("full_name").alias("name"),  # Standardize name.
    col("email"),  # Key for joining.
    to_date(col("signup_date")).alias("signup_date"),  # Cast.
    upper(col("tier")).alias("tier"),  # Standardize.
    col("city"),  # Keep.
)
print("Silver Customers:")  # Heading.
silver_customers.show()  # Display.

# Silver orders (from E-commerce).
silver_orders = bronze_ecom.select(
    md5(concat_ws("|", "email", "order_date", "product")).alias("order_id"),  # Generate PK.
    col("email").alias("customer_email"),  # FK.
    to_date(col("order_date")).alias("order_date"),  # Cast.
    col("product"),  # Keep.
    col("quantity").cast("int"),  # Cast.
    col("unit_price").cast("double"),  # Cast.
    (col("quantity") * col("unit_price")).alias("total_amount"),  # Compute.
    upper(col("status")).alias("status"),  # Standardize.
)
print("Silver Orders:")  # Heading.
silver_orders.show(truncate=False)  # Display.

# GOLD: Joined and aggregated.
print("\n=== GOLD: Business-Ready ===")  # Heading.

# Gold: Customer spending summary.
gold_customer_spending = silver_orders.filter(
    col("status") == "COMPLETED"  # Only completed orders.
).groupBy("customer_email").agg(
    count("*").alias("total_orders"),  # Order count.
    spark_sum("total_amount").alias("lifetime_value"),  # LTV.
    avg("total_amount").alias("avg_order_value"),  # AOV.
)

# Join with customer dimension.
from pyspark.sql.functions import count, avg  # Re-import.
gold_enriched = gold_customer_spending.join(
    silver_customers, gold_customer_spending["customer_email"] == silver_customers["email"], "left"  # Join.
).select(
    coalesce(col("customer_id"), lit("UNKNOWN")).alias("customer_id"),  # Handle unknown.
    col("name"),
    col("tier"),
    col("city"),
    col("total_orders"),
    spark_round(col("lifetime_value"), 2).alias("lifetime_value"),
    spark_round(col("avg_order_value"), 2).alias("avg_order_value"),
)

from pyspark.sql.functions import round as spark_round  # Import.
print("Gold: Customer Spending Summary")  # Heading.
gold_enriched.show()  # Display.
print("Multi-source data unified through Bronze -> Silver -> Gold!")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Quality gates between layers
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Quality Gates Between Layers
# ============================================================
# Real-world: Data quality checks that gate promotion between layers.

from pyspark.sql.functions import (
    col, count, when, isnan, isnull, sum as spark_sum, lit,
    round as spark_round, length, regexp_extract
)  # Imports.
from typing import Dict, List, Tuple  # Typing.

class QualityGate:
    """Data quality checkpoint between medallion layers."""
    
    def __init__(self, layer_name: str, threshold: float = 0.95):
        """Initialize quality gate."""
        self.layer_name = layer_name  # Name.
        self.threshold = threshold  # Min pass rate.
        self.checks = []  # Registered checks.
        self.results = []  # Check results.
    
    def add_not_null_check(self, columns: List[str]):
        """Add NOT NULL check for columns."""
        self.checks.append(("not_null", columns))  # Register.
        return self  # Chain.
    
    def add_unique_check(self, columns: List[str]):
        """Add uniqueness check."""
        self.checks.append(("unique", columns))  # Register.
        return self  # Chain.
    
    def add_range_check(self, column: str, min_val, max_val):
        """Add range validation."""
        self.checks.append(("range", (column, min_val, max_val)))  # Register.
        return self  # Chain.
    
    def add_regex_check(self, column: str, pattern: str, description: str):
        """Add regex pattern check."""
        self.checks.append(("regex", (column, pattern, description)))  # Register.
        return self  # Chain.
    
    def validate(self, df) -> Tuple["DataFrame", "DataFrame", Dict]:
        """Run all checks, return (passed, failed, report)."""
        total_rows = df.count()  # Total.
        failed_mask = lit(False)  # Start with no failures.
        report = {"layer": self.layer_name, "total_rows": total_rows, "checks": []}  # Report.
        
        for check_type, check_params in self.checks:  # Each check.
            if check_type == "not_null":  # NULL check.
                for col_name in check_params:  # Each column.
                    null_count = df.filter(col(col_name).isNull()).count()  # Count.
                    pass_rate = (total_rows - null_count) / total_rows  # Rate.
                    report["checks"].append({
                        "type": "not_null", "column": col_name,
                        "failures": null_count, "pass_rate": round(pass_rate, 4)
                    })  # Log.
                    failed_mask = failed_mask | col(col_name).isNull()  # Accumulate.
            
            elif check_type == "unique":  # Uniqueness.
                key_cols = check_params  # Columns.
                dup_count = df.groupBy(*key_cols).count().filter(col("count") > 1).count()  # Dups.
                pass_rate = (total_rows - dup_count) / total_rows  # Rate.
                report["checks"].append({
                    "type": "unique", "columns": key_cols,
                    "duplicates": dup_count, "pass_rate": round(pass_rate, 4)
                })  # Log.
            
            elif check_type == "range":  # Range.
                col_name, min_val, max_val = check_params  # Unpack.
                out_of_range = df.filter(
                    (col(col_name) < min_val) | (col(col_name) > max_val)  # Out of range.
                ).count()  # Count.
                pass_rate = (total_rows - out_of_range) / total_rows  # Rate.
                report["checks"].append({
                    "type": "range", "column": col_name,
                    "failures": out_of_range, "pass_rate": round(pass_rate, 4)
                })  # Log.
                failed_mask = failed_mask | (col(col_name) < min_val) | (col(col_name) > max_val)  # Accumulate.
        
        # Split into passed and failed.
        passed = df.filter(~failed_mask)  # Good records.
        failed = df.filter(failed_mask)  # Bad records.
        
        # Overall assessment.
        overall_rate = passed.count() / total_rows  # Overall.
        report["overall_pass_rate"] = round(overall_rate, 4)  # Record.
        report["gate_passed"] = overall_rate >= self.threshold  # Pass/fail.
        report["passed_rows"] = passed.count()  # Passed.
        report["failed_rows"] = failed.count()  # Failed.
        
        return passed, failed, report  # Return.

# Demo: Apply quality gates.
print("=== Quality Gate Demo ===")  # Heading.

# Sample data with quality issues.
test_data = spark.createDataFrame([
    ("O1", "C1", "2024-01-15", 150.0, "widget"),
    ("O2", "C2", "2024-01-16", 25.0, "gadget"),
    ("O3", None, "2024-01-17", 500.0, "widget"),     # NULL customer.
    ("O4", "C1", "2024-01-18", -10.0, "doohickey"),  # Negative amount.
    ("O5", "C3", "2024-01-19", 75.0, None),          # NULL product.
    ("O6", "C4", "2024-01-20", 200.0, "gadget"),
    ("O7", "C5", "2024-01-21", 10000.0, "widget"),   # Suspiciously high.
], ["order_id", "customer_id", "date", "amount", "product"])  # Test data.

# Define Bronze -> Silver quality gate.
gate = QualityGate("bronze_to_silver", threshold=0.80)  # 80% threshold.
gate.add_not_null_check(["customer_id", "product"])  # Required fields.
gate.add_range_check("amount", 0.01, 5000.0)  # Valid range.

# Run gate.
passed, failed, report = gate.validate(test_data)  # Validate.

print("\n=== Quality Report ===")  # Heading.
for key, val in report.items():  # Print report.
    if key != "checks":  # Skip details.
        print(f"  {key}: {val}")  # Print.
print("\n  Individual checks:")  # Heading.
for check in report["checks"]:  # Each check.
    print(f"    {check}")  # Print.

print(f"\n  Gate {'PASSED ✓' if report['gate_passed'] else 'FAILED ✘'}")  # Result.

print("\n--- Passed Records (promote to Silver) ---")  # Heading.
passed.show()  # Display.
print("--- Failed Records (quarantine) ---")  # Heading.
failed.show()  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Incremental medallion processing
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Incremental Medallion Processing
# ============================================================
# Real-world: Process only new data through layers (not full reload).

from pyspark.sql.functions import (
    col, max as spark_max, lit, current_timestamp, to_timestamp
)  # Imports.

print("=== Incremental Medallion Processing ===")  # Heading.

# Simulate existing Silver table (already processed up to batch 2).
existing_silver = spark.createDataFrame([
    ("E1", "2024-01-01", 100.0, "batch_001"),
    ("E2", "2024-01-02", 200.0, "batch_001"),
    ("E3", "2024-01-03", 150.0, "batch_002"),
    ("E4", "2024-01-04", 300.0, "batch_002"),
], ["event_id", "event_date", "amount", "_batch_id"])  # Existing.

print("Existing Silver table:")  # Heading.
existing_silver.show()  # Display.

# New Bronze data (batches 3 and 4).
new_bronze = spark.createDataFrame([
    ("E5", "2024-01-05", "250.0", "batch_003"),  # New.
    ("E6", "2024-01-06", "175.0", "batch_003"),  # New.
    ("E3", "2024-01-03", "150.0", "batch_003"),  # Already processed (dup).
    ("E7", "2024-01-07", "400.0", "batch_004"),  # New.
    ("E8", "2024-01-08", "bad", "batch_004"),    # Invalid amount.
], ["event_id", "event_date", "amount_str", "_batch_id"])  # New bronze.

print("New Bronze data (batches 3-4):")  # Heading.
new_bronze.show()  # Display.

# Step 1: Determine watermark (last processed batch).
last_batch = existing_silver.agg(spark_max("_batch_id").alias("max_batch")).collect()[0][0]  # Max.
print(f"Last processed batch: {last_batch}")  # Show.

# Step 2: Filter new data only (batches after watermark).
# In real systems, use Delta's CDF or file modification timestamps.
incremental = new_bronze.filter(col("_batch_id") > last_batch)  # New only.
print(f"\nIncremental records to process: {incremental.count()}")  # Count.
incremental.show()  # Display.

# Step 3: Deduplicate against existing Silver (anti-join).
net_new = incremental.join(
    existing_silver.select("event_id"), "event_id", "left_anti"  # Not already in Silver.
)
print(f"Net new records (after dedup vs existing): {net_new.count()}")  # Count.

# Step 4: Clean and transform (Silver logic).
silver_increment = net_new.withColumn(
    "amount", col("amount_str").cast("double")  # Cast.
).filter(
    col("amount").isNotNull()  # Quality gate: valid amounts only.
).drop("amount_str")  # Remove raw.

print(f"Records passing quality gate: {silver_increment.count()}")  # Count.
silver_increment.show()  # Display.

# Step 5: Append to Silver (in production: Delta append/merge).
updated_silver = existing_silver.unionByName(silver_increment)  # Append.

print("\n=== Updated Silver Table ===")  # Heading.
updated_silver.orderBy("event_date").show()  # Display.
print(f"Silver grew from {existing_silver.count()} to {updated_silver.count()} rows.")
print("Incremental processing: only new data flows through the pipeline!")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Full medallion framework
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Full Medallion Framework
# ============================================================
# Real-world: Reusable, configurable medallion pipeline class.

from pyspark.sql.functions import (
    col, lit, current_timestamp, md5, concat_ws, when,
    count, sum as spark_sum, avg, to_date, to_timestamp,
    upper, trim, monotonically_increasing_id
)  # Imports.
from pyspark.sql import DataFrame  # Type.
from typing import List, Dict, Callable, Optional  # Typing.
from datetime import datetime  # Date.

class MedallionPipeline:
    """Production medallion architecture framework."""
    
    def __init__(self, pipeline_name: str):
        """Initialize pipeline."""
        self.name = pipeline_name  # Name.
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")  # Run ID.
        self.metrics = {"bronze": {}, "silver": {}, "gold": {}}  # Metrics.
    
    # --- BRONZE ---
    def ingest_to_bronze(
        self,
        source_df: DataFrame,
        source_name: str,
    ) -> DataFrame:
        """Ingest raw data to Bronze with metadata."""
        bronze = source_df.withColumn(
            "_bronze_ingest_ts", current_timestamp()  # Ingest time.
        ).withColumn(
            "_source_system", lit(source_name)  # Source.
        ).withColumn(
            "_pipeline_run_id", lit(self.run_id)  # Run.
        ).withColumn(
            "_bronze_row_id", monotonically_increasing_id()  # Unique ID.
        )
        
        self.metrics["bronze"][source_name] = bronze.count()  # Track.
        return bronze  # Return.
    
    # --- SILVER ---
    def promote_to_silver(
        self,
        bronze_df: DataFrame,
        dedup_keys: List[str],
        type_mapping: Dict[str, str],
        required_cols: List[str],
        standardizations: Optional[Dict[str, Callable]] = None,
    ) -> tuple:
        """Promote Bronze to Silver with cleaning."""
        working = bronze_df  # Start.
        
        # Step 1: Deduplicate.
        from pyspark.sql.window import Window  # Import.
        from pyspark.sql.functions import row_number  # Import.
        
        w = Window.partitionBy(*dedup_keys).orderBy(col("_bronze_ingest_ts").desc())  # Window.
        working = working.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")  # Dedup.
        
        # Step 2: Type casting.
        for col_name, target_type in type_mapping.items():  # Each.
            if col_name in working.columns:  # Exists?
                working = working.withColumn(col_name, col(col_name).cast(target_type))  # Cast.
        
        # Step 3: Apply standardizations.
        if standardizations:  # If provided.
            for col_name, func in standardizations.items():  # Each.
                if col_name in working.columns:  # Exists?
                    working = working.withColumn(col_name, func(col(col_name)))  # Apply.
        
        # Step 4: Quality gate (split pass/fail).
        quality_filter = lit(True)  # Start.
        for req_col in required_cols:  # Each required.
            quality_filter = quality_filter & col(req_col).isNotNull()  # Must not be NULL.
        
        passed = working.filter(quality_filter)  # Good.
        quarantined = working.filter(~quality_filter)  # Bad.
        
        # Add silver metadata.
        silver = passed.withColumn(
            "_silver_processed_ts", current_timestamp()  # Processing time.
        ).drop("_bronze_ingest_ts", "_source_system", "_pipeline_run_id", "_bronze_row_id")  # Remove bronze meta.
        
        self.metrics["silver"] = {
            "input": bronze_df.count(),
            "output": silver.count(),
            "quarantined": quarantined.count()
        }  # Track.
        
        return silver, quarantined  # Return.
    
    # --- GOLD ---
    def build_gold(
        self,
        silver_df: DataFrame,
        aggregations: Dict[str, List],
        group_by: List[str],
    ) -> DataFrame:
        """Build Gold aggregation table from Silver."""
        from pyspark.sql.functions import sum as spark_sum, avg, count, min as spark_min, max as spark_max  # Imports.
        
        agg_funcs = {"sum": spark_sum, "avg": avg, "count": count, "min": spark_min, "max": spark_max}  # Map.
        
        agg_exprs = []  # Build.
        for col_name, agg_list in aggregations.items():  # Each.
            for agg_name in agg_list:  # Each agg.
                func = agg_funcs[agg_name]  # Get func.
                agg_exprs.append(func(col_name).alias(f"{agg_name}_{col_name}"))  # Add.
        
        gold = silver_df.groupBy(*group_by).agg(*agg_exprs)  # Aggregate.
        
        # Add gold metadata.
        gold = gold.withColumn("_gold_computed_ts", current_timestamp())  # Timestamp.
        
        self.metrics["gold"] = {"rows": gold.count()}  # Track.
        return gold  # Return.
    
    def print_report(self):
        """Print pipeline execution report."""
        print(f"\n{'='*50}")  # Separator.
        print(f"Pipeline: {self.name} | Run: {self.run_id}")  # Header.
        print(f"{'='*50}")  # Separator.
        for layer, metrics in self.metrics.items():  # Each layer.
            if metrics:  # Has data?
                print(f"\n  {layer.upper()}:")  # Layer.
                for k, v in metrics.items():  # Each metric.
                    print(f"    {k}: {v}")  # Print.
        print(f"{'='*50}\n")  # End.

# Demo: Full pipeline execution.
print("=== Full Medallion Pipeline Execution ===")  # Heading.

# Raw source data.
raw_orders = spark.createDataFrame([
    ("O001", "C1", "2024-01-15", "150.00", "widget", "2"),
    ("O002", "C2", "2024-01-16", "25.50", "gadget", "1"),
    ("O003", None, "2024-01-17", "500.00", "widget", "5"),  # NULL customer.
    ("O004", "C1", "2024-01-18", "bad", "doohickey", "3"),  # Bad amount.
    ("O001", "C1", "2024-01-15", "150.00", "widget", "2"),  # Duplicate.
    ("O005", "C3", "2024-01-19", "75.00", "gadget", "1"),
    ("O006", "C1", "2024-01-20", "200.00", "widget", "4"),
], ["order_id", "customer_id", "order_date", "amount", "product", "quantity"])  # Raw.

# Execute pipeline.
pipeline = MedallionPipeline("ecommerce_orders")  # Create.

# Bronze.
bronze = pipeline.ingest_to_bronze(raw_orders, "order_system")  # Ingest.
print("Bronze:")  # Heading.
bronze.show(truncate=False)  # Display.

# Silver.
silver, quarantine = pipeline.promote_to_silver(
    bronze,
    dedup_keys=["order_id"],  # Dedup on order_id.
    type_mapping={"amount": "double", "quantity": "int", "order_date": "date"},  # Cast.
    required_cols=["customer_id", "amount"],  # Required.
    standardizations={"product": lambda c: upper(trim(c))},  # Standardize.
)
print("Silver (passed):")  # Heading.
silver.show()  # Display.
print("Quarantined:")  # Heading.
quarantine.select("order_id", "customer_id", "amount").show()  # Display.

# Gold.
gold = pipeline.build_gold(
    silver.drop("_silver_processed_ts"),  # Remove meta.
    aggregations={"amount": ["sum", "avg", "count"]},  # Aggs.
    group_by=["customer_id"],  # Group.
)
print("Gold:")  # Heading.
gold.show()  # Display.

# Report.
pipeline.print_report()  # Final report.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Streaming medallion
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Streaming Medallion Patterns
# ============================================================
# Real-world: Design patterns for streaming through medallion layers.
# NOTE: This demonstrates the PATTERN (not actual streaming execution).

from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, upper, lit,
    window, count, sum as spark_sum, avg, current_timestamp
)  # Imports.
from pyspark.sql.types import *  # Types.

print("=== Streaming Medallion Architecture Patterns ===")  # Heading.

print("""
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Kafka/EventHub │ →→ │  Bronze (raw)   │ →→ │  Silver (clean) │ →→ │  Gold (agg)     │
│                 │    │  Auto Loader    │    │  Streaming      │    │  Streaming +    │
│  Source System  │    │  Append-only    │    │  Dedup + Clean  │    │  Windowed Agg   │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
""")

# Streaming Bronze Pattern (conceptual code).
print("--- Pattern: Streaming Bronze Ingestion ---")  # Heading.
bronze_streaming_code = """
# Bronze: Auto Loader ingests raw files as they arrive.
bronze_stream = (
    spark.readStream
    .format("cloudFiles")              # Auto Loader.
    .option("cloudFiles.format", "json")  # Source format.
    .option("cloudFiles.schemaLocation", "/mnt/schema/bronze")  # Schema tracking.
    .load("/mnt/landing/events/")  # Source path.
    .withColumn("_ingest_ts", current_timestamp())  # Metadata.
    .withColumn("_source_file", input_file_name())  # Track source.
)

bronze_stream.writeStream
    .format("delta")
    .outputMode("append")  # Always append to Bronze.
    .option("checkpointLocation", "/mnt/checkpoints/bronze")
    .trigger(availableNow=True)  # Process all available, then stop.
    .toTable("catalog.bronze.events")
"""
print(bronze_streaming_code)  # Show.

# Streaming Silver Pattern.
print("--- Pattern: Streaming Silver (with dedup) ---")  # Heading.
silver_streaming_code = """
# Silver: Read Bronze stream, deduplicate, clean, write.
silver_stream = (
    spark.readStream
    .format("delta")
    .table("catalog.bronze.events")  # Read from Bronze.
    .dropDuplicatesWithinWatermark(["event_id"], "10 minutes")  # Dedup.
    .withColumn("event_ts", to_timestamp(col("raw_timestamp")))  # Cast.
    .withColumn("event_type", upper(trim(col("event_type"))))  # Standardize.
    .filter(col("event_ts").isNotNull())  # Quality gate.
)

silver_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/silver")
    .trigger(availableNow=True)
    .toTable("catalog.silver.events")
"""
print(silver_streaming_code)  # Show.

# Streaming Gold Pattern.
print("--- Pattern: Streaming Gold (windowed aggregation) ---")  # Heading.
gold_streaming_code = """
# Gold: Read Silver stream, aggregate in time windows.
gold_stream = (
    spark.readStream
    .format("delta")
    .table("catalog.silver.events")  # Read from Silver.
    .withWatermark("event_ts", "1 hour")  # Watermark for late data.
    .groupBy(
        window(col("event_ts"), "1 hour"),  # Hourly windows.
        col("event_type")
    )
    .agg(
        count("*").alias("event_count"),
        countDistinct("user_id").alias("unique_users"),
    )
)

gold_stream.writeStream
    .format("delta")
    .outputMode("update")  # Update mode for aggregations.
    .option("checkpointLocation", "/mnt/checkpoints/gold")
    .trigger(processingTime="5 minutes")  # Every 5 min.
    .toTable("catalog.gold.hourly_metrics")
"""
print(gold_streaming_code)  # Show.

# Demonstrate batch equivalent of streaming patterns.
print("\n=== Batch Demo of Streaming Patterns ===")  # Heading.

# Simulate hourly event stream.
events = spark.createDataFrame([
    ("e1", "2024-01-15 10:05:00", "click", "u1"), ("e2", "2024-01-15 10:15:00", "view", "u2"),
    ("e3", "2024-01-15 10:45:00", "click", "u1"), ("e4", "2024-01-15 11:05:00", "purchase", "u3"),
    ("e5", "2024-01-15 11:30:00", "click", "u2"), ("e6", "2024-01-15 11:55:00", "view", "u4"),
    ("e7", "2024-01-15 12:10:00", "purchase", "u1"), ("e8", "2024-01-15 12:30:00", "click", "u5"),
], ["event_id", "event_ts", "event_type", "user_id"])  # Events.

events = events.withColumn("event_ts", to_timestamp("event_ts"))  # Cast.

# Windowed aggregation (Gold pattern).
from pyspark.sql.functions import window, countDistinct  # Import.
gold_hourly = events.groupBy(
    window(col("event_ts"), "1 hour"),  # Hourly.
    col("event_type"),  # By type.
).agg(
    count("*").alias("events"),  # Count.
    countDistinct("user_id").alias("users"),  # Unique.
).select(
    col("window.start").alias("window_start"),  # Window.
    col("window.end").alias("window_end"),  # Window.
    col("event_type"), col("events"), col("users"),  # Metrics.
)

gold_hourly.orderBy("window_start", "event_type").show(truncate=False)  # Display.
print("Streaming Gold: hourly aggregations would update continuously!")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Medallion with lineage tracking
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Medallion with Lineage Tracking
# ============================================================
# Real-world: Full audit trail through all medallion layers.

from pyspark.sql.functions import (
    col, lit, current_timestamp, monotonically_increasing_id,
    count, sum as spark_sum, avg, when, md5, concat_ws,
    collect_set, array_join
)  # Imports.
from datetime import datetime  # Date.

print("=== Medallion with Full Lineage ===")  # Heading.

class LineageTracker:
    """Track data lineage through medallion layers."""
    
    def __init__(self):
        """Initialize tracker."""
        self.lineage_records = []  # Store.
    
    def record(self, layer, operation, source_count, target_count, details=""):
        """Record a lineage event."""
        self.lineage_records.append({
            "timestamp": datetime.now().isoformat(),
            "layer": layer,
            "operation": operation,
            "source_rows": source_count,
            "target_rows": target_count,
            "delta": target_count - source_count,
            "details": details,
        })  # Add.
    
    def print_lineage(self):
        """Print full lineage trail."""
        print("\n=== Data Lineage Trail ===")  # Heading.
        for i, rec in enumerate(self.lineage_records, 1):  # Each.
            emoji = {"bronze": "🥉", "silver": "🥈", "gold": "🥇"}.get(rec["layer"], "•")  # Icon.
            print(f"  {emoji} Step {i}: [{rec['layer'].upper()}] {rec['operation']}")  # Step.
            print(f"     Rows: {rec['source_rows']} -> {rec['target_rows']} (delta: {rec['delta']})")  # Counts.
            if rec["details"]:  # Details?
                print(f"     Note: {rec['details']}")  # Show.
        print()  # Spacing.

# Pipeline with lineage.
lineage = LineageTracker()  # Init.

# Raw source.
raw = spark.createDataFrame([
    ("S1", "2024-01-15", "A", 100.0, "active"),
    ("S2", "2024-01-15", "B", 200.0, "active"),
    ("S3", "2024-01-16", "A", 150.0, "pending"),
    ("S1", "2024-01-15", "A", 100.0, "active"),  # Dup.
    ("S4", "2024-01-16", "C", None, "active"),   # NULL amount.
    ("S5", "2024-01-17", "B", 300.0, "closed"),
    ("S6", "2024-01-17", "A", 250.0, "active"),
    ("S7", "bad_date", "D", 50.0, "active"),     # Bad date.
], ["id", "date", "category", "amount", "status"])  # Raw.

source_count = raw.count()  # Count.
print(f"Source: {source_count} rows")  # Info.

# BRONZE.
bronze = raw.withColumn("_lineage_id", md5(concat_ws("|", *raw.columns)))  # Fingerprint.
bronze = bronze.withColumn("_layer", lit("bronze"))  # Mark layer.
bronze_count = bronze.count()  # Count.
lineage.record("bronze", "ingest (append-only)", source_count, bronze_count, "All data preserved")  # Log.
print(f"\nBronze: {bronze_count} rows (all preserved)")  # Info.

# SILVER.
from pyspark.sql.window import Window  # Import.
from pyspark.sql.functions import row_number, to_date  # Import.

# Dedup.
w = Window.partitionBy("id").orderBy("date")  # Window.
silver = bronze.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")  # Dedup.
after_dedup = silver.count()  # Count.
lineage.record("silver", "deduplicate", bronze_count, after_dedup, f"Removed {bronze_count - after_dedup} duplicates")  # Log.

# Type cast.
silver = silver.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))  # Cast.
silver = silver.withColumn("amount", col("amount").cast("double"))  # Already double.

# Quality gate.
before_gate = silver.count()  # Before.
silver_clean = silver.filter(
    col("date").isNotNull() & col("amount").isNotNull()  # Required.
)
silver_count = silver_clean.count()  # After.
lineage.record("silver", "quality_gate", before_gate, silver_count, f"Quarantined {before_gate - silver_count} rows")  # Log.

# Update layer marker.
silver_clean = silver_clean.withColumn("_layer", lit("silver"))  # Mark.
print(f"Silver: {silver_count} rows (cleaned)")  # Info.
silver_clean.drop("_lineage_id", "_layer").show()  # Display.

# GOLD.
gold = silver_clean.drop("_lineage_id", "_layer").groupBy("category").agg(
    count("*").alias("total_transactions"),  # Count.
    spark_sum("amount").alias("total_revenue"),  # Sum.
    avg("amount").alias("avg_transaction"),  # Average.
    collect_set("status").alias("statuses"),  # All statuses seen.
)
gold_count = gold.count()  # Count.
lineage.record("gold", "aggregate (by category)", silver_count, gold_count, "Business metrics computed")  # Log.

print(f"Gold: {gold_count} rows (aggregated)")  # Info.
gold.show(truncate=False)  # Display.

# Print full lineage.
lineage.print_lineage()  # Show trail.

print(f"""
Data Flow Summary:
  Source: {source_count} raw rows
  Bronze: {bronze_count} rows (all preserved)
  Silver: {silver_count} rows (deduped + validated)
  Gold:   {gold_count} rows (aggregated metrics)
  
  Data retained: {silver_count}/{source_count} = {silver_count/source_count*100:.0f}%
  Compression (Gold/Source): {gold_count}/{source_count} = {gold_count/source_count*100:.0f}%
""")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Key Takeaways
# MAGIC %md
# MAGIC ## SECTION 6 — Key Takeaways
# MAGIC
# MAGIC ### Layer Responsibilities
# MAGIC
# MAGIC | Layer | Owns | Does NOT Do |
# MAGIC |---|---|---|
# MAGIC | 🥉 Bronze | Ingestion, metadata, append | Transforms, filters, dedup |
# MAGIC | 🥈 Silver | Cleaning, typing, dedup, conforming | Aggregation, business logic |
# MAGIC | 🥇 Gold | Aggregation, KPIs, denormalization | Raw data access, re-cleaning |
# MAGIC
# MAGIC ### Best Practices
# MAGIC 1. **Bronze is sacred** — never modify, always append
# MAGIC 2. **Silver is the single source of truth** — all Gold tables derive from Silver
# MAGIC 3. **Gold is consumption-optimized** — one Gold table per use case
# MAGIC 4. **Quality gates between layers** — quarantine bad data, don't drop silently
# MAGIC 5. **Incremental processing** — don't reprocess everything every time
# MAGIC 6. **Lineage tracking** — know where every row came from
# MAGIC
# MAGIC ### Common Table Naming
# MAGIC ```
# MAGIC catalog.bronze.raw_events
# MAGIC catalog.silver.events_cleaned
# MAGIC catalog.gold.daily_event_metrics
# MAGIC catalog.gold.user_engagement_scores
# MAGIC catalog.quarantine.events_failed_quality
# MAGIC ```
# MAGIC
# MAGIC ### Anti-Patterns
# MAGIC * Putting business logic in Bronze (violates raw preservation)
# MAGIC * Having Gold read directly from Bronze (skipping Silver)
# MAGIC * Using same table for multiple Gold use cases (monolithic Gold)
# MAGIC * Not handling late-arriving data in streaming pipelines
# MAGIC * Forgetting to track lineage and data freshness

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Practice Exercises
# MAGIC %md
# MAGIC ## SECTION 7 — Practice Exercises
# MAGIC
# MAGIC ### Exercise 1: Bronze Ingestion
# MAGIC Design a Bronze ingestion for IoT sensor data. Add metadata columns: ingest timestamp, source device type, and batch ID.
# MAGIC
# MAGIC ### Exercise 2: Silver Cleaning
# MAGIC Given messy Bronze data with duplicates, bad dates, and NULL required fields, implement Silver cleaning with a quality report.
# MAGIC
# MAGIC ### Exercise 3: Gold KPIs
# MAGIC From Silver event data, build a Gold table with hourly KPIs: event count, unique users, conversion rate, and average session duration.
# MAGIC
# MAGIC ### Exercise 4: Full Pipeline
# MAGIC Design a 3-layer pipeline for customer order data that tracks lineage and produces a Gold table showing customer lifetime value.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Solutions
# ============================================================
# SECTION 7 — EXERCISE SOLUTIONS
# ============================================================

from pyspark.sql.functions import (
    col, lit, current_timestamp, count, sum as spark_sum,
    avg, when, countDistinct, to_date, to_timestamp, upper, trim,
    monotonically_increasing_id, row_number
)  # Imports.
from pyspark.sql.window import Window  # Window.

# --- Exercise 1: Bronze IoT Ingestion ---
print("=== Exercise 1: Bronze IoT ===")  # Heading.
iot_raw = spark.createDataFrame([
    ("S001", "temp", 22.5, "2024-01-15 10:00:00"),
    ("S002", "humidity", 45.0, "2024-01-15 10:01:00"),
    ("S001", "temp", 22.7, "2024-01-15 10:05:00"),
], ["sensor_id", "metric", "value", "reading_time"])  # Raw.

bronze_iot = iot_raw.withColumn(
    "_ingest_ts", current_timestamp()  # When ingested.
).withColumn(
    "_device_type", lit("temperature_sensor")  # Source type.
).withColumn(
    "_batch_id", lit("iot_batch_001")  # Batch.
)
bronze_iot.show()  # Display.

# --- Exercise 2: Silver Cleaning ---
print("=== Exercise 2: Silver Cleaning ===")  # Heading.
dirty_bronze = spark.createDataFrame([
    ("1", "2024-01-15", 100.0, "A"), ("2", "bad-date", 200.0, "B"),
    ("1", "2024-01-15", 100.0, "A"), ("3", "2024-01-17", None, "C"),
], ["id", "date", "amount", "category"])  # Dirty.

# Dedup.
w = Window.partitionBy("id").orderBy("date")  # Window.
silver = dirty_bronze.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")  # Dedup.
# Type + Quality.
silver = silver.withColumn("date", to_date("date", "yyyy-MM-dd"))  # Cast.
silver_pass = silver.filter(col("date").isNotNull() & col("amount").isNotNull())  # Gate.
silver_fail = silver.filter(col("date").isNull() | col("amount").isNull())  # Quarantine.
print(f"Passed: {silver_pass.count()}, Failed: {silver_fail.count()}")  # Report.
silver_pass.show()  # Display.

# --- Exercise 3: Gold KPIs ---
print("=== Exercise 3: Gold Hourly KPIs ===")  # Heading.
from pyspark.sql.functions import window  # Import.
events = spark.createDataFrame([
    ("e1", "2024-01-15 10:00:00", "click", "u1"), ("e2", "2024-01-15 10:30:00", "view", "u2"),
    ("e3", "2024-01-15 10:45:00", "purchase", "u1"), ("e4", "2024-01-15 11:10:00", "click", "u3"),
], ["event_id", "ts", "type", "user_id"])  # Events.
events = events.withColumn("ts", to_timestamp("ts"))  # Cast.

gold_kpi = events.groupBy(window("ts", "1 hour")).agg(
    count("*").alias("events"),
    countDistinct("user_id").alias("unique_users"),
    count(when(col("type") == "purchase", 1)).alias("purchases"),
).withColumn("conversion_pct", col("purchases") / col("unique_users") * 100)
gold_kpi.select("window.start", "events", "unique_users", "purchases", "conversion_pct").show()  # Display.

# --- Exercise 4: Full Pipeline (LTV) ---
print("=== Exercise 4: Customer LTV Pipeline ===")  # Heading.
orders_raw = spark.createDataFrame([
    ("O1", "C1", "2024-01-15", 150.0), ("O2", "C1", "2024-02-10", 200.0),
    ("O3", "C2", "2024-01-20", 300.0), ("O1", "C1", "2024-01-15", 150.0),  # Dup.
], ["order_id", "customer_id", "date", "amount"])  # Raw.

# Bronze.
b = orders_raw.withColumn("_ts", current_timestamp())  # Ingest.
# Silver.
w = Window.partitionBy("order_id").orderBy("date")  # Dedup window.
s = b.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn", "_ts")  # Clean.
# Gold.
g = s.groupBy("customer_id").agg(
    count("*").alias("total_orders"), spark_sum("amount").alias("lifetime_value"),
    avg("amount").alias("avg_order"),
)
g.show()  # Display.

print("All exercises completed! Build medallion pipelines in production.")