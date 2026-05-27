# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 108: Feature Store (Unity Catalog)
# MAGIC ## Module 20: Advanced Topics
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 40 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC The **Feature Store** is a centralized repository for ML features — precomputed, versioned, discoverable, and reusable. Instead of each ML model recomputing "customer average order value" from scratch, the feature store computes it ONCE and serves it to ALL models consistently.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC A **shared pantry** in a restaurant: instead of every chef preparing their own garlic paste, the pantry has it pre-made. Any chef can grab it, knowing it's always fresh, consistent, and properly stored.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Feature Store Flow:
# MAGIC
# MAGIC   1. COMPUTE features (PySpark transformations).
# MAGIC   2. PUBLISH to feature table (Delta table with primary key).
# MAGIC   3. DISCOVER features (UC search, descriptions, lineage).
# MAGIC   4. TRAIN models using FeatureEngineeringClient.
# MAGIC   5. SERVE features for real-time inference (auto-lookup).
# MAGIC
# MAGIC Key API:
# MAGIC   from databricks.feature_engineering import FeatureEngineeringClient
# MAGIC   fe = FeatureEngineeringClient()
# MAGIC
# MAGIC   # Create feature table.
# MAGIC   fe.create_table(name="catalog.schema.customer_features",
# MAGIC                   primary_keys=["customer_id"],
# MAGIC                   df=features_df,
# MAGIC                   description="Customer-level aggregated features")
# MAGIC
# MAGIC   # Train with feature lookup (automatic join at training time).
# MAGIC   training_set = fe.create_training_set(
# MAGIC       df=labels_df,
# MAGIC       feature_lookups=[FeatureLookup(table_name="...", lookup_key="customer_id")]
# MAGIC   )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: Feature Store Examples
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7 — FEATURE STORE
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, avg, count, max as spark_max, datediff, current_date  # Imports.

print("="*70)
print("SECTIONS 3-7: Feature Store")
print("="*70)

# ─── EXAMPLE 1: Compute features ───
print("\n" + "-"*60)
print("EXAMPLE 1: Computing features from raw data")
print("-"*60)

# Simulate order data.
orders = spark.createDataFrame([
    (1, 100.0, "2024-01-15"), (1, 200.0, "2024-02-20"), (1, 150.0, "2024-03-10"),
    (2, 50.0, "2024-01-10"), (2, 75.0, "2024-03-01"),
    (3, 500.0, "2024-02-15"), (3, 300.0, "2024-03-20"), (3, 400.0, "2024-03-25")
], ["customer_id", "amount", "order_date"])

# Compute customer features.
customer_features = orders.groupBy("customer_id").agg(
    count("*").alias("total_orders"),           # Feature 1: order count.
    avg("amount").alias("avg_order_value"),     # Feature 2: average spend.
    spark_max("amount").alias("max_order_value"), # Feature 3: max spend.
    spark_max("order_date").alias("last_order_date")  # Feature 4: recency.
)

print("\nCustomer features (ready for Feature Store):")
display(customer_features)  # display() for output.

# ─── EXAMPLE 2: Feature Store API ───
print("\n" + "-"*60)
print("EXAMPLE 2: Feature Engineering Client API")
print("-"*60)

print("""
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
fe = FeatureEngineeringClient()

# Step 1: Create/update feature table (Delta table with primary key).
fe.create_table(
    name="catalog.ml.customer_features",      # UC table name.
    primary_keys=["customer_id"],              # Primary key for lookups.
    df=customer_features,                      # Feature DataFrame.
    description="Customer aggregated features from orders"
)

# Step 2: Update features (run periodically).
fe.write_table(
    name="catalog.ml.customer_features",
    df=new_features,                           # Updated feature values.
    mode="merge"                               # Upsert (update existing, insert new).
)

# Step 3: Create training set (auto-joins features to labels).
training_set = fe.create_training_set(
    df=labels_df,                              # DataFrame with customer_id + label.
    feature_lookups=[
        FeatureLookup(
            table_name="catalog.ml.customer_features",
            lookup_key="customer_id"            # Join key.
        )
    ],
    label="churn_label"                        # Label column name.
)
training_df = training_set.load_df()           # Get joined DataFrame for training.

# Step 4: Log model with feature metadata (for serving).
import mlflow
fe.log_model(
    model=trained_model,
    artifact_path="model",
    flavor=mlflow.sklearn,
    training_set=training_set                  # Links features to model.
)
# At inference time: model automatically looks up latest features!
""")

print("✓ Feature Store = compute once, use everywhere.")
print("  Features are versioned, discoverable, and auto-joined at training.")

# ─── HOMEWORK ───
print("\n" + "="*70)
print("HOMEWORK — Feature Store")
print("="*70)
print("  Level 1-3: Compute features, create feature table with primary key.")
print("  Level 4-6: create_training_set with FeatureLookup, log model.")
print("  Level 7-10: Online serving, time-series features, point-in-time joins.")
print("\n" + "="*70)
print("✓ HOMEWORK COMPLETED — Notebook 108")
print("="*70)