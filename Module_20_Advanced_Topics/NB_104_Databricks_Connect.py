# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 104: Databricks Connect
# MAGIC ## Module 20: Advanced Topics
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 40 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Databricks Connect** lets you run Spark code from your LOCAL IDE (VS Code, PyCharm, IntelliJ) while executing on a remote Databricks cluster. You write and debug code locally but the heavy computation happens on the cluster. Think of it as a "remote control" for Spark.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine controlling a **powerful robot arm in a factory** from your laptop at home. You write the instructions (code), the robot arm (cluster) does the heavy lifting, and you see the results on your screen — without being physically in the factory.
# MAGIC
# MAGIC ### Key Benefits:
# MAGIC | Feature | Benefit |
# MAGIC |---------|--------|
# MAGIC | Local IDE | Full debugging (breakpoints, step-through) |
# MAGIC | Remote compute | Cluster's power without local setup |
# MAGIC | Unit testing | Run pytest with real Spark session |
# MAGIC | CI/CD integration | Run Spark tests in pipelines |
# MAGIC | Interactive dev | Same experience as notebooks, in IDE |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Databricks Connect Architecture:
# MAGIC
# MAGIC   Local Machine (IDE)              Databricks Cluster
# MAGIC   ─────────────────────              ───────────────────
# MAGIC   Your Python code        ──gRPC──▶  Spark Driver
# MAGIC   (DataFrame operations)             Executors
# MAGIC   Thin client (no Spark)             Data processing
# MAGIC   Results returned                   UC permissions
# MAGIC
# MAGIC Setup Steps:
# MAGIC   1. pip install databricks-connect==17.3.*  # Match your DBR version!
# MAGIC   2. Configure auth (profiles or env vars).
# MAGIC   3. Get SparkSession via DatabricksSession.
# MAGIC   4. Write normal PySpark code.
# MAGIC   5. Execution happens on cluster (transparent).
# MAGIC
# MAGIC Code Pattern:
# MAGIC
# MAGIC   from databricks.connect import DatabricksSession
# MAGIC
# MAGIC   spark = DatabricksSession.builder \
# MAGIC       .remote(host="https://adb-123.azuredatabricks.net",
# MAGIC               token="dapi...",
# MAGIC               cluster_id="0123-456789-abc") \
# MAGIC       .getOrCreate()
# MAGIC
# MAGIC   # Now use spark normally — runs on the remote cluster!
# MAGIC   df = spark.table("catalog.schema.table")
# MAGIC   df.filter(col("amount") > 100).show()
# MAGIC
# MAGIC Databricks Connect v2 vs v1:
# MAGIC   v1 (legacy): Full Spark JAR locally. Heavy, version conflicts.
# MAGIC   v2 (current): Thin gRPC client. Lightweight, no local Spark needed.
# MAGIC   Always use v2 (databricks-connect >= 13.0).
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-5: Examples
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-5 — DATABRICKS CONNECT EXAMPLES
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTIONS 3-5: Databricks Connect")
print("="*70)

# ─── EXAMPLE 1: Setup and authentication ───
print("\n" + "-"*60)
print("EXAMPLE 1: Setup (run this on your LOCAL machine)")
print("-"*60)

print("""
Step 1: Install (in local terminal):
  pip install databricks-connect==17.3.*  # Match cluster DBR version!
  pip install databricks-sdk               # For auth helpers.

Step 2: Configure authentication (~/.databrickscfg):
  [DEFAULT]
  host = https://adb-1234567890.azuredatabricks.net
  token = dapi1234567890abcdef
  cluster_id = 0123-456789-abcdefgh

  OR use environment variables:
    export DATABRICKS_HOST="https://adb-123.azuredatabricks.net"
    export DATABRICKS_TOKEN="dapi..."
    export DATABRICKS_CLUSTER_ID="0123-456789-abc"

Step 3: Connect in your Python script:
  from databricks.connect import DatabricksSession

  spark = DatabricksSession.builder.getOrCreate()  # Uses DEFAULT profile.
  # OR explicit:
  spark = DatabricksSession.builder.remote(
      host="https://adb-123.azuredatabricks.net",
      token="dapi...",
      cluster_id="0123-456789-abc"
  ).getOrCreate()
""")

# ─── EXAMPLE 2: Using Databricks Connect (code pattern) ───
print("\n" + "-"*60)
print("EXAMPLE 2: Code that works both in notebook AND locally")
print("-"*60)

print("""
Pattern: Write code that works in both environments:

  # my_etl.py (works locally via DB Connect AND in Databricks notebook)
  try:
      from databricks.connect import DatabricksSession
      spark = DatabricksSession.builder.getOrCreate()  # Local IDE.
  except ImportError:
      pass  # In notebook, 'spark' is already available.

  # Now use spark normally:
  df = spark.table("prod_catalog.sales.orders")
  result = df.groupBy("region").sum("revenue")
  result.show()

  # This code runs IDENTICALLY:
  #   - In your VS Code (execution on remote cluster).
  #   - In a Databricks notebook (execution on attached cluster).
""")

# ─── EXAMPLE 3: Debugging with breakpoints ───
print("\n" + "-"*60)
print("EXAMPLE 3: Local debugging with breakpoints")
print("-"*60)

print("""
In VS Code / PyCharm:

  1. Set a breakpoint on any line.
  2. Run in debug mode (F5 in VS Code).
  3. When breakpoint hits, inspect:
     - df.schema      (see column types)
     - df.count()     (check row count)
     - df.show(5)     (preview data)
     - df.explain()   (see execution plan)
  4. Step through transformations line by line.

  This is IMPOSSIBLE in a notebook but easy with DB Connect!
  Perfect for: debugging complex joins, understanding data flow.
""")

# ─── EXAMPLE 4: pytest integration ───
print("\n" + "-"*60)
print("EXAMPLE 4: Unit testing with pytest + Databricks Connect")
print("-"*60)

print("""
# tests/test_etl.py
import pytest
from databricks.connect import DatabricksSession

@pytest.fixture(scope="session")
def spark():
    return DatabricksSession.builder.getOrCreate()

def test_transform_filters_nulls(spark):
    # Arrange: create test data on the cluster.
    input_df = spark.createDataFrame([(1, "a"), (2, None)], ["id", "name"])
    # Act: apply your transform function.
    from src.transforms import clean_nulls
    result = clean_nulls(input_df)
    # Assert: verify result.
    assert result.count() == 1
    assert result.collect()[0]["name"] == "a"

def test_aggregation_correct(spark):
    input_df = spark.createDataFrame([
        ("A", 100), ("A", 200), ("B", 50)
    ], ["group", "value"])
    from src.transforms import aggregate_groups
    result = aggregate_groups(input_df)
    result_pd = result.toPandas().set_index("group")
    assert result_pd.loc["A", "total"] == 300
    assert result_pd.loc["B", "total"] == 50

# Run: pytest tests/ -v
# Tests execute on your Databricks cluster via gRPC.
""")

print("✓ Databricks Connect = local IDE + remote cluster power.")
print("  Best for: debugging, testing, CI/CD pipelines.")
print("  Limitation: No dbutils, no %run, no widgets.")

# COMMAND ----------

# DBTITLE 1,Section 6: Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Version mismatch (client vs cluster)
# MAGIC ```bash
# MAGIC # BAD: Client version doesn't match cluster DBR.
# MAGIC pip install databricks-connect==14.3.*  # But cluster is DBR 17.3!
# MAGIC # Error: "Version mismatch" or silent failures.
# MAGIC
# MAGIC # GOOD: Match EXACTLY.
# MAGIC pip install databricks-connect==17.3.*  # Cluster is DBR 17.3.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Using dbutils in DB Connect code
# MAGIC ```python
# MAGIC # BAD: dbutils not available in DB Connect.
# MAGIC path = dbutils.widgets.get("path")  # NameError: dbutils not defined!
# MAGIC
# MAGIC # GOOD: Use parameters, env vars, or config files instead.
# MAGIC import os
# MAGIC path = os.environ.get("DATA_PATH", "/default/path")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Cluster not running when connecting
# MAGIC ```python
# MAGIC # BAD: Connecting to a terminated cluster.
# MAGIC spark = DatabricksSession.builder.getOrCreate()  # Timeout!
# MAGIC
# MAGIC # GOOD: Ensure cluster is running, or enable auto-start.
# MAGIC # Set cluster to auto-start on connect (Admin Console > Cluster policy).
# MAGIC # Or: Check cluster status before connecting.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Large .collect() over network
# MAGIC ```python
# MAGIC # BAD: Collecting millions of rows over gRPC (very slow over network!).
# MAGIC all_data = df.collect()  # Transfers all data from cluster to your laptop!
# MAGIC
# MAGIC # GOOD: Aggregate on cluster, collect only summary.
# MAGIC summary = df.groupBy("key").count().collect()  # Small result.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not using profiles for multi-workspace
# MAGIC ```python
# MAGIC # BAD: Hard-coding credentials.
# MAGIC spark = DatabricksSession.builder.remote(host="...", token="dapi...").getOrCreate()
# MAGIC
# MAGIC # GOOD: Use profiles in ~/.databrickscfg
# MAGIC # [dev]
# MAGIC # host = https://dev-workspace.azuredatabricks.net
# MAGIC # token = dapi...
# MAGIC # cluster_id = ...
# MAGIC #
# MAGIC # [prod]
# MAGIC # host = https://prod-workspace.azuredatabricks.net
# MAGIC # ...
# MAGIC
# MAGIC spark = DatabricksSession.builder.profile("dev").getOrCreate()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7: Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK
# ═══════════════════════════════════════════════════════════════════
print("="*70)
print("HOMEWORK — Databricks Connect")
print("="*70)
print("\n--- Level 1-3: Setup ---")
print("  pip install databricks-connect==17.3.*")
print("  Configure ~/.databrickscfg with host, token, cluster_id.")
print("  from databricks.connect import DatabricksSession")
print("\n--- Level 4-6: Usage ---")
print("  Write code that works in both notebook and local IDE.")
print("  Debug with breakpoints in VS Code.")
print("  Run pytest with real Spark session.")
print("\n--- Level 7-10: Advanced ---")
print("  Profiles for multi-workspace. CI/CD integration.")
print("  Limitations: no dbutils, no widgets, no %run.")
print("  Use serverless compute for faster startup.")
print("\n" + "="*70)
print("✓ HOMEWORK COMPLETED — Notebook 104")
print("="*70)