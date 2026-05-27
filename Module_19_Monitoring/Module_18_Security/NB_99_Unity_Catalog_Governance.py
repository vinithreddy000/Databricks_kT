# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 99: Unity Catalog & Data Governance
# MAGIC ## Module 18: Security
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 55 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Unity Catalog (UC)** is Databricks' unified governance layer that provides centralized access control, auditing, lineage, and data discovery across ALL your data assets (tables, files, ML models, functions). It enforces WHO can access WHAT and tracks HOW data flows through your organization.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Think of Unity Catalog as a **bank vault system**:
# MAGIC - **Metastore** = The bank itself (one per region)
# MAGIC - **Catalog** = A vault room (dev_catalog, prod_catalog)
# MAGIC - **Schema** = A shelf inside the vault (sales, marketing)
# MAGIC - **Table** = A lockbox on the shelf (orders, customers)
# MAGIC - **Permissions** = Keys (GRANT SELECT to analyst_group)
# MAGIC - **Audit log** = Security cameras recording every access
# MAGIC - **Lineage** = Chain of custody (where data came from, where it went)
# MAGIC
# MAGIC ### Three-Level Namespace:
# MAGIC ```
# MAGIC catalog.schema.table
# MAGIC   │       │      │
# MAGIC   │       │      └─ Table/View/Volume/Function/Model
# MAGIC   │       └──────── Schema (logical grouping)
# MAGIC   └──────────────── Catalog (environment/org boundary)
# MAGIC ```
# MAGIC
# MAGIC ### Key UC Features:
# MAGIC | Feature | Description |
# MAGIC |---------|------------|
# MAGIC | Centralized ACLs | GRANT/REVOKE on catalog, schema, table |
# MAGIC | Column-level security | Mask or restrict specific columns |
# MAGIC | Row-level security | Filter rows based on user identity |
# MAGIC | Data lineage | Track data flow across tables and notebooks |
# MAGIC | Audit logs | Record every data access event |
# MAGIC | Tags & classification | Label PII, sensitive, public data |
# MAGIC | External locations | Govern access to cloud storage paths |
# MAGIC | Volumes | Managed file storage with governance |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Unity Catalog Hierarchy:
# MAGIC
# MAGIC   ┌─────────────────────────────────────────────────────────────┐
# MAGIC   │                     METASTORE                                │
# MAGIC   │  (One per region. Assigned to workspace.)                    │
# MAGIC   ├─────────────────────────────────────────────────────────────┤
# MAGIC   │                                                             │
# MAGIC   │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
# MAGIC   │  │ dev_catalog  │  │ prod_catalog │  │ shared_catalog│      │
# MAGIC   │  ├──────────────┤  ├──────────────┤  ├──────────────┤      │
# MAGIC   │  │ sales        │  │ sales        │  │ reference    │      │
# MAGIC   │  │  └─ orders   │  │  └─ orders   │  │  └─ countries│      │
# MAGIC   │  │  └─ customers│  │  └─ customers│  │  └─ currencies│     │
# MAGIC   │  │ marketing    │  │ marketing    │  │              │      │
# MAGIC   │  │  └─ campaigns│  │  └─ campaigns│  │              │      │
# MAGIC   │  └──────────────┘  └──────────────┘  └──────────────┘      │
# MAGIC   │                                                             │
# MAGIC   └─────────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC Permission Model (inheritance):
# MAGIC
# MAGIC   GRANT USE CATALOG ON CATALOG dev_catalog TO `analysts`;
# MAGIC   GRANT USE SCHEMA ON SCHEMA dev_catalog.sales TO `analysts`;
# MAGIC   GRANT SELECT ON TABLE dev_catalog.sales.orders TO `analysts`;
# MAGIC
# MAGIC   Inheritance: Catalog permission → Schema → Table.
# MAGIC   Explicit DENY overrides inherited GRANT.
# MAGIC
# MAGIC Data Lineage:
# MAGIC
# MAGIC   source_table ──[notebook_1]──> bronze_table
# MAGIC   bronze_table ──[notebook_2]──> silver_table
# MAGIC   silver_table ──[notebook_3]──> gold_table
# MAGIC   gold_table   ──[dashboard]───> CEO Report
# MAGIC
# MAGIC   UC tracks this automatically. No extra code needed!
# MAGIC
# MAGIC Row/Column Level Security:
# MAGIC
# MAGIC   Row filter:  Users only see their own department's data.
# MAGIC   Column mask: PII columns show '***' to unauthorized users.
# MAGIC   Dynamic:     Based on IS_MEMBER('group') function.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: Beginner Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTION 3 — BEGINNER: Unity Catalog Governance")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Exploring the UC namespace
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Navigating the three-level namespace")
print("-"*60)

# List available catalogs.
print("\nAvailable catalogs:")
display(spark.sql("SHOW CATALOGS"))  # display() for output.

# List schemas in a catalog (using current catalog).
print("\nSchemas in current catalog:")
current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]  # Get current.
print(f"  Current catalog: {current_catalog}")
display(spark.sql(f"SHOW SCHEMAS IN {current_catalog}"))  # display() for output.

# List tables in a schema.
print("\nTables in 'default' schema:")
display(spark.sql(f"SHOW TABLES IN {current_catalog}.default"))  # display() for output.

print("\n✓ Three-level namespace: catalog.schema.table")
print("  Always use fully qualified names in production code.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: GRANT/REVOKE permissions (SQL syntax)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Permission management (GRANT/REVOKE)")
print("-"*60)

print("""
GRANT syntax (requires ADMIN or OWNER privileges):

  -- Catalog-level access.
  GRANT USE CATALOG ON CATALOG my_catalog TO `data_analysts`;
  GRANT CREATE SCHEMA ON CATALOG my_catalog TO `data_engineers`;

  -- Schema-level access.
  GRANT USE SCHEMA ON SCHEMA my_catalog.sales TO `data_analysts`;
  GRANT CREATE TABLE ON SCHEMA my_catalog.sales TO `data_engineers`;

  -- Table-level access.
  GRANT SELECT ON TABLE my_catalog.sales.orders TO `data_analysts`;
  GRANT MODIFY ON TABLE my_catalog.sales.orders TO `etl_service_principal`;

  -- View-level (restrict what analysts see).
  GRANT SELECT ON VIEW my_catalog.sales.orders_sanitized TO `analysts`;

  -- Revoke access.
  REVOKE SELECT ON TABLE my_catalog.sales.customers FROM `temp_contractor`;

Permission types:
  USE CATALOG/SCHEMA:  Browse metadata (required to see contents).
  CREATE TABLE/SCHEMA: Create new objects.
  SELECT:              Read data.
  MODIFY:              Write/update/delete data.
  ALL PRIVILEGES:      Everything (use sparingly!).
""")

# Show grants on current catalog (if permitted).
try:
    print("Current grants (if accessible):")
    display(spark.sql(f"SHOW GRANTS ON CATALOG {current_catalog}"))  # display().
except Exception as e:
    print(f"  (Permission to view grants not available: {type(e).__name__})")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Information Schema (metadata queries)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Information Schema (query metadata)")
print("-"*60)

# Every catalog has an information_schema with metadata.
print("\nQuery table metadata via information_schema:")
info_query = f"""
    SELECT table_catalog, table_schema, table_name, table_type
    FROM {current_catalog}.information_schema.tables
    WHERE table_schema != 'information_schema'
    LIMIT 10
"""
try:
    display(spark.sql(info_query))  # display() for output.
except Exception as e:
    print(f"  (Query result: {type(e).__name__} - may need permissions)")

print("\n✓ information_schema: standard SQL way to discover metadata.")
print("  Tables: tables, columns, table_privileges, schemata.")
print("  Use for: auditing, discovery, documentation automation.")

# COMMAND ----------

# DBTITLE 1,Sections 4-5: Intermediate and Advanced
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4-5 — INTERMEDIATE & ADVANCED
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTIONS 4-5: Advanced UC Governance")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Row-Level Security (row filters)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Row-Level Security (filter rows by user)")
print("-"*60)

print("""
Row Filter Function (restricts which rows users can see):

  -- Step 1: Create a filter function.
  CREATE OR REPLACE FUNCTION sales.row_filter(department STRING)
  RETURNS BOOLEAN
  RETURN
    IS_ACCOUNT_GROUP_MEMBER('admin_group')  -- Admins see all.
    OR department = CURRENT_USER_ATTRIBUTE('department');  -- Others see own dept.

  -- Step 2: Apply to a table.
  ALTER TABLE sales.employees
  SET ROW FILTER sales.row_filter ON (department);

  -- Result:
  -- Admin users: see ALL rows.
  -- Marketing users: only see rows WHERE department = 'Marketing'.
  -- Engineering users: only see rows WHERE department = 'Engineering'.

  -- Remove filter.
  ALTER TABLE sales.employees DROP ROW FILTER;
""")

print("✓ Row filters are TRANSPARENT: queries look normal, but results are filtered.")
print("  No code changes needed in notebooks/dashboards.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: Column Masking (hide sensitive data)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: Column Masking (hide PII from unauthorized users)")
print("-"*60)

print("""
Column Mask Function (masks sensitive column values):

  -- Step 1: Create a mask function.
  CREATE OR REPLACE FUNCTION sales.mask_email(email STRING)
  RETURNS STRING
  RETURN
    CASE
      WHEN IS_ACCOUNT_GROUP_MEMBER('pii_authorized')
        THEN email                    -- Authorized: see real email.
      ELSE CONCAT('***@', SPLIT(email, '@')[1])  -- Others: masked.
    END;

  -- Step 2: Apply to a column.
  ALTER TABLE sales.customers
  ALTER COLUMN email
  SET MASK sales.mask_email;

  -- Result:
  -- PII-authorized users: see 'alice@company.com'
  -- Other users: see '***@company.com'

  -- More masking patterns:
  CREATE FUNCTION mask_ssn(ssn STRING) RETURNS STRING
  RETURN CASE WHEN IS_ACCOUNT_GROUP_MEMBER('hr_team')
    THEN ssn ELSE CONCAT('XXX-XX-', RIGHT(ssn, 4)) END;

  CREATE FUNCTION mask_salary(salary DOUBLE) RETURNS DOUBLE
  RETURN CASE WHEN IS_ACCOUNT_GROUP_MEMBER('finance')
    THEN salary ELSE NULL END;
""")

print("✓ Column masks run dynamically at query time.")
print("  Same table, different views based on user group membership.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Tags and Data Classification
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Tags for data classification")
print("-"*60)

print("""
Tags (metadata labels for governance):

  -- Tag a table.
  ALTER TABLE sales.customers
  SET TAGS ('pii' = 'true', 'data_owner' = 'privacy_team', 'retention' = '7_years');

  -- Tag a column.
  ALTER TABLE sales.customers
  ALTER COLUMN email
  SET TAGS ('pii_type' = 'email', 'sensitivity' = 'high');

  ALTER TABLE sales.customers
  ALTER COLUMN phone
  SET TAGS ('pii_type' = 'phone', 'sensitivity' = 'high');

  -- Query tagged objects.
  SELECT * FROM system.information_schema.table_tags
  WHERE tag_name = 'pii' AND tag_value = 'true';

  -- Find all PII columns across the catalog.
  SELECT * FROM system.information_schema.column_tags
  WHERE tag_name = 'sensitivity' AND tag_value = 'high';

Use cases:
  - Compliance: Find all PII tables for GDPR deletion requests.
  - Data catalog: Auto-classify tables by domain, owner, freshness.
  - Access control: Combine tags with row/column security policies.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 7: Data Lineage
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 7: Data Lineage (automatic tracking)")
print("-"*60)

print("""
Lineage is tracked AUTOMATICALLY by Unity Catalog:

  When you run:
    df = spark.table("bronze.raw_orders")
    df_clean = df.filter(col("amount") > 0)
    df_clean.write.saveAsTable("silver.clean_orders")

  UC records:
    bronze.raw_orders ──[notebook_123]──> silver.clean_orders
    (upstream)           (transform)       (downstream)

  Query lineage via system tables:
    SELECT * FROM system.access.table_lineage
    WHERE target_table_full_name = 'prod.silver.clean_orders'
    ORDER BY event_time DESC;

  Lineage shows:
    - Which tables feed into which other tables.
    - Which notebooks/jobs perform the transformations.
    - Column-level lineage (which source columns map to target columns).
    - Impact analysis: "If I change bronze.raw_orders, what breaks?"

  Also visible in:
    - Catalog Explorer UI (visual lineage graph).
    - Data tab of any table in the workspace.
""")
print("✓ Lineage requires NO extra code. UC tracks it from Spark operations.")

# COMMAND ----------

# DBTITLE 1,Section 6: Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Using ALL PRIVILEGES instead of least privilege
# MAGIC ```sql
# MAGIC -- BAD: Giving everyone full access.
# MAGIC GRANT ALL PRIVILEGES ON CATALOG prod_catalog TO `all_users`;
# MAGIC
# MAGIC -- GOOD: Grant minimum required access.
# MAGIC GRANT USE CATALOG ON CATALOG prod_catalog TO `analysts`;
# MAGIC GRANT USE SCHEMA ON SCHEMA prod_catalog.gold TO `analysts`;
# MAGIC GRANT SELECT ON SCHEMA prod_catalog.gold TO `analysts`;  -- Read-only on gold.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Not using three-level namespace (breaks in UC)
# MAGIC ```python
# MAGIC # BAD: Implicit catalog/schema (ambiguous, breaks across environments).
# MAGIC df = spark.table("orders")  # Which catalog? Which schema?
# MAGIC
# MAGIC # GOOD: Always fully qualified.
# MAGIC df = spark.table("prod_catalog.sales.orders")  # Explicit and clear.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Forgetting USE CATALOG/USE SCHEMA permissions
# MAGIC ```sql
# MAGIC -- BAD: Granting SELECT but user can't even see the catalog.
# MAGIC GRANT SELECT ON TABLE prod.sales.orders TO `analyst`;  -- Still fails!
# MAGIC
# MAGIC -- GOOD: Grant the full path of access.
# MAGIC GRANT USE CATALOG ON CATALOG prod TO `analyst`;
# MAGIC GRANT USE SCHEMA ON SCHEMA prod.sales TO `analyst`;
# MAGIC GRANT SELECT ON TABLE prod.sales.orders TO `analyst`;  -- Now works!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Storing sensitive data without column masking
# MAGIC ```sql
# MAGIC -- BAD: PII visible to everyone with SELECT access.
# MAGIC SELECT email, phone, ssn FROM customers;  -- All users see everything!
# MAGIC
# MAGIC -- GOOD: Apply column masks to sensitive fields.
# MAGIC ALTER TABLE customers ALTER COLUMN ssn SET MASK mask_ssn;
# MAGIC ALTER TABLE customers ALTER COLUMN email SET MASK mask_email;
# MAGIC -- Now unauthorized users see masked values automatically.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not tagging PII data (compliance nightmare)
# MAGIC ```sql
# MAGIC -- BAD: No way to find all PII tables for GDPR deletion request.
# MAGIC -- How do you know which tables have personal data?
# MAGIC
# MAGIC -- GOOD: Tag everything systematically.
# MAGIC ALTER TABLE customers SET TAGS ('pii' = 'true', 'gdpr_relevant' = 'true');
# MAGIC ALTER TABLE customers ALTER COLUMN email SET TAGS ('pii_type' = 'email');
# MAGIC -- Now: SELECT * FROM information_schema.column_tags WHERE tag_name = 'pii_type';
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7: Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("HOMEWORK — Unity Catalog & Data Governance")
print("="*70)

# Level 1: Navigate the namespace.
print("\n--- Level 1: Three-level namespace ---")
print(f"  Current catalog: {spark.sql('SELECT current_catalog()').collect()[0][0]}")
print(f"  Current schema:  {spark.sql('SELECT current_schema()').collect()[0][0]}")
print("  Fully qualified: catalog.schema.table")
# WHY: Always use fully qualified names for clarity and portability.

# Level 2: Show grants.
print("\n--- Level 2: View permissions ---")
print("  SHOW GRANTS ON CATALOG my_catalog;")
print("  SHOW GRANTS ON TABLE catalog.schema.table;")
print("  SHOW GRANTS TO `user@company.com`;")
# WHY: Audit who has access to what.

# Level 3: GRANT SELECT.
print("\n--- Level 3: Grant read access ---")
print("  GRANT USE CATALOG ON CATALOG cat TO `group`;")
print("  GRANT USE SCHEMA ON SCHEMA cat.schema TO `group`;")
print("  GRANT SELECT ON TABLE cat.schema.table TO `group`;")
# WHY: Three grants needed for a user to actually read a table.

# Level 4: Information schema.
print("\n--- Level 4: Query metadata ---")
print("  SELECT * FROM catalog.information_schema.tables;")
print("  SELECT * FROM catalog.information_schema.columns;")
print("  SELECT * FROM catalog.information_schema.table_privileges;")
# WHY: Programmatic discovery of all objects and permissions.

# Level 5: Tags.
print("\n--- Level 5: Data classification ---")
print("  ALTER TABLE t SET TAGS ('pii'='true', 'owner'='team');")
print("  ALTER TABLE t ALTER COLUMN c SET TAGS ('sensitivity'='high');")
# WHY: Enables automated compliance scanning.

# Levels 6-10: Conceptual.
print("\n--- Level 6: Row-level security ---")
print("  CREATE FUNCTION filter_fn(col) RETURNS BOOLEAN ...")
print("  ALTER TABLE t SET ROW FILTER filter_fn ON (col);")

print("\n--- Level 7: Column masking ---")
print("  CREATE FUNCTION mask_fn(col) RETURNS STRING ...")
print("  ALTER TABLE t ALTER COLUMN c SET MASK mask_fn;")

print("\n--- Level 8: Lineage ---")
print("  Automatic! Query: system.access.table_lineage.")
print("  Also visible in Catalog Explorer UI.")

print("\n--- Level 9: External locations ---")
print("  CREATE EXTERNAL LOCATION loc URL 'abfss://...' WITH (CREDENTIAL cred);")
print("  GRANT READ FILES ON EXTERNAL LOCATION loc TO `group`;")

print("\n--- Level 10: Teach UC governance ---")
print("""
"Unity Catalog = centralized governance for all data assets.
  Three-level namespace: catalog.schema.table.
  Permissions: GRANT/REVOKE (USE CATALOG, USE SCHEMA, SELECT, MODIFY).
  Row filters: Restrict rows by user group membership.
  Column masks: Hide sensitive values from unauthorized users.
  Tags: Classify data (PII, sensitivity, owner).
  Lineage: Automatic tracking of data flow.
  Best practices: least privilege, fully qualified names, tag PII,
  use groups (not individuals), audit regularly."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 99")
print("="*70)