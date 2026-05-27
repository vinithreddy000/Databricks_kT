# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 84: Enterprise Connectors — Snowflake, MongoDB, SAP, CosmosDB
# MAGIC ## Module 13: Data Sources & Connectors
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 40 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Beyond standard SQL databases, enterprises use specialized data systems. Databricks connects to all of them through **native connectors** or **Lakehouse Federation**. This notebook covers the most common enterprise integrations.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Think of Databricks as a **universal translator** at the UN. Each data system speaks a different language (Snowflake SQL, MongoDB queries, SAP BAPI calls), but Databricks translates them all into the same DataFrame language you already know.
# MAGIC
# MAGIC ### Enterprise Systems Covered:
# MAGIC | System | Connector | Use Case |
# MAGIC |--------|-----------|----------|
# MAGIC | Snowflake | spark-snowflake | Cloud data warehouse |
# MAGIC | MongoDB | mongodb-spark-connector | NoSQL document store |
# MAGIC | CosmosDB | azure-cosmosdb-spark | Global NoSQL (Azure) |
# MAGIC | SAP | Databricks SAP connector | ERP system data |
# MAGIC | Redshift | spark-redshift | AWS data warehouse |
# MAGIC | Elasticsearch | elasticsearch-hadoop | Search/log analytics |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Connector Architecture:
# MAGIC
# MAGIC   [Databricks Spark]  ─── native connector ───▶  [External System]
# MAGIC         │                                              │
# MAGIC   .format("snowflake")                         Pushes query down
# MAGIC   .format("mongodb")                           to external system
# MAGIC   .format("cosmos.oltp")                       (less data transferred)
# MAGIC         │                                              │
# MAGIC   DataFrame API                                Returns results
# MAGIC   (same as everything else!)                   as DataFrame
# MAGIC
# MAGIC Lakehouse Federation (Modern Approach):
# MAGIC   Instead of installing connectors, create a CONNECTION:
# MAGIC
# MAGIC   CREATE CONNECTION snowflake_conn TYPE SNOWFLAKE
# MAGIC     OPTIONS (host='account.snowflakecomputing.com', ...);
# MAGIC   
# MAGIC   CREATE FOREIGN CATALOG snow_cat USING CONNECTION snowflake_conn;
# MAGIC   
# MAGIC   -- Now query Snowflake like a local table:
# MAGIC   SELECT * FROM snow_cat.schema.my_table;
# MAGIC
# MAGIC   Benefits:
# MAGIC     - No JAR installations needed.
# MAGIC     - Unity Catalog governs access.
# MAGIC     - Query pushdown to source system.
# MAGIC     - Works from SQL, Python, notebooks, dashboards.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: Enterprise Connector Patterns
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7 — ENTERPRISE CONNECTORS
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTIONS 3-7: Enterprise Connector Patterns")
print("="*70)

# ─── EXAMPLE 1: Snowflake ───
print("\n" + "-"*60)
print("EXAMPLE 1: Snowflake Connector")
print("-"*60)

print("""
# Install: Already available on Databricks Runtime.
# Read from Snowflake.
options = {
    "sfUrl": "account.snowflakecomputing.com",
    "sfUser": dbutils.secrets.get("snowflake", "user"),
    "sfPassword": dbutils.secrets.get("snowflake", "password"),
    "sfDatabase": "ANALYTICS",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH"
}

# Read entire table.
df = spark.read.format("snowflake") \\
    .options(**options) \\
    .option("dbtable", "CUSTOMERS") \\
    .load()

# Read with query (pushes aggregation to Snowflake!).
df = spark.read.format("snowflake") \\
    .options(**options) \\
    .option("query", "SELECT region, COUNT(*) as cnt FROM ORDERS GROUP BY region") \\
    .load()

# Write to Snowflake.
df.write.format("snowflake") \\
    .options(**options) \\
    .option("dbtable", "OUTPUT_TABLE") \\
    .mode("overwrite") \\
    .save()
""")

# ─── EXAMPLE 2: MongoDB ───
print("\n" + "-"*60)
print("EXAMPLE 2: MongoDB Connector")
print("-"*60)

print("""
# Read from MongoDB.
df = spark.read.format("mongodb") \\
    .option("connection.uri", dbutils.secrets.get("mongo", "uri")) \\
    .option("database", "analytics") \\
    .option("collection", "events") \\
    .load()

# MongoDB returns nested documents → use explode/select to flatten.
from pyspark.sql.functions import explode, col
df_flat = df.select(
    col("_id"),
    col("user.name").alias("user_name"),
    col("user.email").alias("email"),
    explode(col("items")).alias("item")
)

# Write to MongoDB.
df.write.format("mongodb") \\
    .option("connection.uri", uri) \\
    .option("database", "output") \\
    .option("collection", "processed") \\
    .mode("append") \\
    .save()

# Streaming from MongoDB (Change Streams).
spark.readStream.format("mongodb") \\
    .option("connection.uri", uri) \\
    .option("database", "analytics") \\
    .option("collection", "events") \\
    .load()
""")

# ─── EXAMPLE 3: Azure CosmosDB ───
print("\n" + "-"*60)
print("EXAMPLE 3: Azure CosmosDB")
print("-"*60)

print("""
# CosmosDB OLTP connector (for real-time reads).
cosmos_config = {
    "spark.cosmos.accountEndpoint": dbutils.secrets.get("cosmos", "endpoint"),
    "spark.cosmos.accountKey": dbutils.secrets.get("cosmos", "key"),
    "spark.cosmos.database": "IoTData",
    "spark.cosmos.container": "telemetry"
}

df = spark.read.format("cosmos.oltp") \\
    .options(**cosmos_config) \\
    .option("spark.cosmos.read.inferSchema.enabled", "true") \\
    .load()

# CosmosDB Analytical Store (for analytics, no RU cost!).
df_analytical = spark.read.format("cosmos.olap") \\
    .options(**cosmos_config) \\
    .load()
""")

# ─── EXAMPLE 4: Lakehouse Federation ───
print("\n" + "-"*60)
print("EXAMPLE 4: Lakehouse Federation (modern, no JARs)")
print("-"*60)

print("""
Lakehouse Federation queries external systems through Unity Catalog:

  -- Step 1: Create a connection.
  CREATE CONNECTION my_sqlserver TYPE SQLSERVER
  OPTIONS (
    host = 'server.database.windows.net',
    port = '1433',
    user = secret('scope', 'user'),
    password = secret('scope', 'pass')
  );

  -- Step 2: Create a foreign catalog.
  CREATE FOREIGN CATALOG sqlserver_cat USING CONNECTION my_sqlserver
  OPTIONS (database = 'production_db');

  -- Step 3: Query it like any local table!
  SELECT * FROM sqlserver_cat.dbo.customers WHERE region = 'EMEA';

Supported: SQL Server, PostgreSQL, MySQL, Snowflake, Redshift, BigQuery, Salesforce.
Benefits: No JAR management, UC governance, query pushdown, SQL + Python.
""")

# ─── SECTION 6 & 7 ───
print("\n" + "="*70)
print("SECTION 6 — COMMON MISTAKES")
print("="*70)
print("""
1. Installing old connector JARs when Lakehouse Federation is available.
2. Not using query pushdown (reading full table then filtering in Spark).
3. Hardcoding connection strings (use Secret Scopes!).
4. Not handling nested documents from MongoDB (need explode/flatten).
5. Using OLTP connector for analytics (use Analytical Store for CosmosDB).
""")

print("="*70)
print("SECTION 7 — HOMEWORK")
print("="*70)
print("""
Level 1: Snowflake format name? Answer: "snowflake"
Level 2: MongoDB format? Answer: "mongodb"
Level 3: CosmosDB format? Answer: "cosmos.oltp" (transactional) or "cosmos.olap" (analytical)
Level 4: How to query Snowflake without connector JAR? Lakehouse Federation.
Level 5: How to flatten nested MongoDB documents? Use col("nested.field") or explode().
Level 6: Where to store connection credentials? Databricks Secret Scopes.
Level 7: What is query pushdown? Spark sends filter/agg to external system (less data transferred).
Level 10: Teach enterprise connectors:
  "Databricks connects to Snowflake, MongoDB, CosmosDB, SAP via native connectors.
   Same DataFrame API. Reads/writes just work.
   Modern approach: Lakehouse Federation (SQL-based, no JARs, UC governed).
   Always use Secret Scopes for credentials."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 84")
print("="*70)