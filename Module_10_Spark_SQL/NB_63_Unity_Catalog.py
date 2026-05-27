# Databricks notebook source
# DBTITLE 1,Sections 1-2 Overview
# MAGIC %md
# MAGIC # Notebook 63: Unity Catalog — The Modern Data Governance Layer
# MAGIC ## Module 10: Spark SQL Complete Reference
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Unity Catalog is Databricks' **centralized governance solution** that provides a unified way to manage all your data assets — tables, views, functions, models, files — with fine-grained access control, automatic lineage tracking, and a consistent three-level namespace.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Think of a **modern city library system**:
# MAGIC - **Catalog** = A branch library (Downtown Library, University Library)
# MAGIC - **Schema** = A section within a library (Science Fiction, History, Reference)
# MAGIC - **Table** = A specific book on the shelf
# MAGIC - **Unity Catalog** = The central system connecting ALL libraries, with one library card that works everywhere, and a record of who borrowed what
# MAGIC
# MAGIC ### Three-Level Namespace:
# MAGIC ```
# MAGIC catalog.schema.table
# MAGIC │       │      └── The actual data (table, view, function)
# MAGIC │       └──────── Logical grouping (like a database)
# MAGIC └─────────────── Top-level container (organization/env)
# MAGIC
# MAGIC Examples:
# MAGIC   prod_catalog.sales.orders
# MAGIC   dev_catalog.raw.events
# MAGIC   analytics.finance.monthly_revenue
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Unity Catalog Architecture:
# MAGIC
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │                    METASTORE                              │
# MAGIC │  (One per Databricks account region)                      │
# MAGIC │                                                           │
# MAGIC │  ┌─────────────────┐  ┌─────────────────┐              │
# MAGIC │  │   Catalog: prod  │  │  Catalog: dev   │              │
# MAGIC │  │  ┌─────────────┐  │  │  ┌─────────────┐  │              │
# MAGIC │  │  │Schema: sales│  │  │  │Schema: raw  │  │              │
# MAGIC │  │  │  - orders   │  │  │  │  - events  │  │              │
# MAGIC │  │  │  - products │  │  │  │  - logs    │  │              │
# MAGIC │  │  └─────────────┘  │  │  └─────────────┘  │              │
# MAGIC │  └─────────────────┘  └─────────────────┘              │
# MAGIC │                                                           │
# MAGIC │  Access Control:   GRANT SELECT ON TABLE TO group          │
# MAGIC │  Data Lineage:     Automatic tracking of data flow         │
# MAGIC │  Audit Logs:       Every access recorded                   │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC Managed vs External Tables:
# MAGIC   Managed:  UC controls both metadata AND data files
# MAGIC             DROP TABLE = data deleted
# MAGIC   External: UC controls metadata, data is at external location
# MAGIC             DROP TABLE = only metadata removed, data stays
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3 - UC Basics
# SECTION 3 — BEGINNER: Unity Catalog Basics
# Real-world: Navigate and understand the three-level namespace.

print("=== Unity Catalog: Three-Level Namespace ===")  # Heading.

# Show available catalogs.
print("--- Available Catalogs ---")
display(spark.sql("SHOW CATALOGS"))

# Show current catalog.
print("\n--- Current Catalog ---")
display(spark.sql("SELECT current_catalog()"))

# Show schemas in current catalog.
print("\n--- Schemas in Current Catalog ---")
display(spark.sql("SHOW SCHEMAS"))

# Fully qualified reference.
print("\n--- Fully Qualified Table Reference ---")
print("""
Best Practice: ALWAYS use fully qualified names in production code:

  SELECT * FROM catalog_name.schema_name.table_name

This makes code portable and unambiguous.

Shortcuts (avoid in production):
  USE CATALOG my_catalog;     -- Set default catalog
  USE SCHEMA my_schema;       -- Set default schema
  SELECT * FROM table_name;   -- Now resolves to my_catalog.my_schema.table_name
""")

# Show tables in a schema.
print("\n--- Show Tables ---")
display(spark.sql("SHOW TABLES IN sql_kt_demo"))

# DESCRIBE EXTENDED shows UC metadata.
print("\n--- DESCRIBE EXTENDED (shows governance info) ---")
try:
    display(spark.sql("DESCRIBE EXTENDED sql_kt_demo.employees").filter(
        "col_name IN ('Catalog', 'Database', 'Table', 'Type', 'Provider', 'Owner', 'Comment')"
    ))
except:
    print("  (Table may not exist — run NB 61 first)")

# COMMAND ----------

# DBTITLE 1,Section 4 - Access Control
# SECTION 4 — INTERMEDIATE: Access Control and Governance
# Real-world: Controlling who can see and modify data.

print("=== Unity Catalog Access Control ===")  # Heading.

# GRANT/REVOKE syntax (demonstration — requires admin privileges).
print("--- GRANT/REVOKE Syntax ---")
print("""
-- Grant SELECT (read) on a table to a group:
GRANT SELECT ON TABLE catalog.schema.table TO `data-analysts@company.com`;

-- Grant all privileges on a schema:
GRANT ALL PRIVILEGES ON SCHEMA catalog.schema TO `data-engineers@company.com`;

-- Grant CREATE TABLE in a schema:
GRANT CREATE TABLE ON SCHEMA catalog.schema TO `etl-team@company.com`;

-- Grant USE SCHEMA (browse/see the schema):
GRANT USE SCHEMA ON SCHEMA catalog.schema TO `all-users@company.com`;

-- Grant USE CATALOG (browse the catalog):
GRANT USE CATALOG ON CATALOG my_catalog TO `all-users@company.com`;

-- Revoke access:
REVOKE SELECT ON TABLE catalog.schema.table FROM `intern-group@company.com`;

-- Show grants:
SHOW GRANTS ON TABLE catalog.schema.table;
SHOW GRANTS `user@company.com`;
""")

# Table types in UC.
print("\n--- Table Types ---")
print("""
Managed Table (recommended):
  CREATE TABLE catalog.schema.my_table (id INT, name STRING) USING DELTA;
  → Data stored in UC-managed location
  → DROP TABLE deletes both metadata AND data
  → UC handles storage lifecycle

External Table:
  CREATE TABLE catalog.schema.my_table (id INT, name STRING)
  USING DELTA
  LOCATION 'abfss://container@storage.dfs.core.windows.net/path';
  → Data stays at external location you control
  → DROP TABLE only removes metadata (data survives)
  → Need EXTERNAL LOCATION or STORAGE CREDENTIAL
""")

# Row-level security.
print("\n--- Row-Level Security (Row Filters) ---")
print("""
-- Create a function that returns TRUE for rows the user can see:
CREATE FUNCTION catalog.schema.region_filter(region STRING)
RETURNS BOOLEAN
RETURN IF(IS_MEMBER('global-admins'), true, region = current_user_region());

-- Apply to table:
ALTER TABLE catalog.schema.sales
SET ROW FILTER catalog.schema.region_filter ON (region);

-- Now: users only see rows for their region!
""")

# Column masking.
print("\n--- Column Masking ---")
print("""
-- Create a masking function:
CREATE FUNCTION catalog.schema.mask_ssn(ssn STRING)
RETURNS STRING
RETURN IF(IS_MEMBER('hr-team'), ssn, CONCAT('***-**-', RIGHT(ssn, 4)));

-- Apply to column:
ALTER TABLE catalog.schema.employees
ALTER COLUMN ssn SET MASK catalog.schema.mask_ssn;

-- Now: non-HR users see '***-**-1234' instead of full SSN!
""")

# COMMAND ----------

# DBTITLE 1,Section 5 and Exercises
# SECTION 5 — ADVANCED: Lineage, Volumes, and Best Practices

print("=== Unity Catalog Advanced Features ===")  # Heading.

# Data Lineage.
print("--- Data Lineage ---")
print("""
Unity Catalog AUTOMATICALLY tracks data lineage:
  - Which tables were read to create another table
  - Which notebooks/jobs write to which tables
  - Column-level lineage (which source columns feed which target columns)

View lineage in the Databricks UI:
  Catalog Explorer → Select table → "Lineage" tab

No code needed — it's automatic!
""")

# Volumes (for unstructured files).
print("\n--- Volumes (File Storage) ---")
print("""
Volumes are Unity Catalog's way to manage FILES (not tables):
  - PDFs, images, CSVs, model artifacts, config files
  - Governed by UC access control
  - Accessible via /Volumes/catalog/schema/volume_name/

Create a managed volume:
  CREATE VOLUME catalog.schema.my_volume;

Create an external volume:
  CREATE EXTERNAL VOLUME catalog.schema.my_volume
  LOCATION 'abfss://container@storage.dfs.core.windows.net/files';

Use it:
  -- Copy files
  PUT '/local/file.csv' INTO '/Volumes/catalog/schema/my_volume/file.csv'
  
  -- Read files
  SELECT * FROM read_files('/Volumes/catalog/schema/my_volume/*.csv')
""")

# Best practices.
print("\n--- Unity Catalog Best Practices ---")
print("""
1. NAMING CONVENTION:
   Catalogs: prod, dev, staging (or by business unit)
   Schemas: raw, bronze, silver, gold (by data layer)
   Tables: plural, snake_case (orders, customer_events)

2. ACCESS CONTROL:
   - Use GROUPS, not individual users
   - Principle of least privilege
   - Analysts: SELECT only
   - Engineers: SELECT + MODIFY + CREATE
   - Admins: ALL PRIVILEGES

3. ALWAYS use fully qualified names in production code

4. Add COMMENTS to all tables and columns

5. Use TAGS for governance metadata:
   ALTER TABLE t SET TAGS ('pii' = 'true', 'retention' = '7years')
""")

# HOMEWORK.
print("\n" + "="*60)
print("HOMEWORK — Unity Catalog")
print("="*60)
print("""
Level 1: Run SHOW CATALOGS and SHOW SCHEMAS
Level 2: Create a table with COMMENT on columns
Level 3: Use DESCRIBE EXTENDED to see all metadata
Level 4: Write a query using fully qualified 3-level name
Level 5: Explain the difference between managed and external tables
""")

# Level 1 solution.
print("Level 1 Solution:")
display(spark.sql("SHOW CATALOGS"))
display(spark.sql("SHOW SCHEMAS"))

print("\nModule 10 complete! Notebooks 61-63 cover Spark SQL comprehensively.")