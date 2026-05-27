# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 100: Access Control, Secrets & Cluster Security
# MAGIC ## Module 18: Security
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC This notebook covers **workspace-level security**: how to control who can access clusters, notebooks, jobs, and secrets. It covers the full security stack from identity management (users, groups, service principals) through network security and secret management.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC A **corporate office building**:
# MAGIC - **Identity (Entra ID)** = Your employee badge (proves who you are)
# MAGIC - **Groups** = Department teams (Marketing, Engineering — bulk permissions)
# MAGIC - **Service Principals** = Robot employees (automated processes with their own badges)
# MAGIC - **Cluster policies** = Rules for booking meeting rooms (max size, auto-lock timer)
# MAGIC - **Secrets** = The locked safe in the office (passwords, API keys nobody can see)
# MAGIC - **Network security** = Building perimeter (VPN, private links, firewalls)
# MAGIC
# MAGIC ### Security Layers:
# MAGIC | Layer | What It Controls | Tools |
# MAGIC |-------|-----------------|-------|
# MAGIC | Identity | Who you are | Entra ID (Azure AD), SCIM sync |
# MAGIC | Authentication | Prove who you are | OAuth, PATs, Service Principals |
# MAGIC | Authorization | What you can do | UC permissions, workspace ACLs |
# MAGIC | Data security | What data you see | Row filters, column masks, UC |
# MAGIC | Network | How you connect | Private Link, IP allowlists, VNets |
# MAGIC | Secrets | Sensitive values | Secret Scopes, Azure Key Vault |
# MAGIC | Audit | What happened | Audit logs, system tables |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Security Architecture:
# MAGIC
# MAGIC   ┌─────────────────────────────────────────────────────┐
# MAGIC   │              Identity Layer                          │
# MAGIC   │  Users  |  Groups  |  Service Principals             │
# MAGIC   │  (synced from Entra ID via SCIM)                     │
# MAGIC   ├─────────────────────────────────────────────────────┤
# MAGIC   │              Authorization Layer                     │
# MAGIC   │  Workspace ACLs     |  Unity Catalog Permissions     │
# MAGIC   │  (notebooks, jobs)  |  (tables, schemas, catalogs)   │
# MAGIC   ├─────────────────────────────────────────────────────┤
# MAGIC   │              Compute Layer                           │
# MAGIC   │  Cluster policies  |  Data security modes            │
# MAGIC   │  (who can create,  |  (USER_ISOLATION, SHARED,       │
# MAGIC   │   size limits)     |   SINGLE_USER, NO_ISOLATION)    │
# MAGIC   ├─────────────────────────────────────────────────────┤
# MAGIC   │              Network Layer                           │
# MAGIC   │  VNet injection | Private Link | IP allowlists       │
# MAGIC   └─────────────────────────────────────────────────────┘
# MAGIC
# MAGIC Data Security Modes (cluster-level):
# MAGIC
# MAGIC   USER_ISOLATION (recommended):
# MAGIC     - Each user's code runs in isolated process.
# MAGIC     - UC permissions enforced per user.
# MAGIC     - Users can't see each other's data.
# MAGIC     - Python, SQL, R supported. Scala limited.
# MAGIC
# MAGIC   SINGLE_USER:
# MAGIC     - Entire cluster runs as one user/SP.
# MAGIC     - Full UC enforcement for that identity.
# MAGIC     - Best for automated jobs (service principal).
# MAGIC
# MAGIC   SHARED (legacy):
# MAGIC     - Basic table ACLs (not full UC).
# MAGIC     - Being deprecated in favor of USER_ISOLATION.
# MAGIC
# MAGIC Secret Management:
# MAGIC
# MAGIC   Secret Scope (container)    Secret (key-value pair)
# MAGIC   ───────────────────────    ─────────────────────
# MAGIC   "my-scope"                   "db-password" = "s3cur3!"
# MAGIC   (backed by Azure Key Vault   "api-key"     = "abc123"
# MAGIC    or Databricks)              "conn-string" = "jdbc://..."
# MAGIC
# MAGIC   Access: dbutils.secrets.get(scope="my-scope", key="db-password")
# MAGIC   Output: [REDACTED] (never shown in logs or cell output)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: Beginner Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTION 3 — BEGINNER: Access Control & Secrets")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Current user identity
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Who am I? (current identity)")
print("-"*60)

# Check current user.
current_user = spark.sql("SELECT current_user()").collect()[0][0]  # Email of current user.
print(f"\n  Current user: {current_user}")

# Check group membership (useful for conditional logic).
print("\n  Check group membership:")
print("    SELECT is_account_group_member('data_engineers');  -- true/false")
print("    SELECT is_account_group_member('admins');          -- true/false")

# Current catalog and schema context.
print(f"\n  Current catalog: {spark.sql('SELECT current_catalog()').collect()[0][0]}")
print(f"  Current schema:  {spark.sql('SELECT current_schema()').collect()[0][0]}")

print("\n✓ UC permissions are evaluated against your identity (user or SP).")
print("  Group membership determines what you can access.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Secret Scopes (manage sensitive credentials)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Secret Scopes (securely store credentials)")
print("-"*60)

# List available secret scopes.
print("\nAvailable secret scopes:")
try:
    scopes = dbutils.secrets.listScopes()  # List all scopes.
    for scope in scopes:
        print(f"  ✓ Scope: {scope.name}")  # Print scope name.
        # List keys in scope (not values!).
        try:
            keys = dbutils.secrets.list(scope.name)  # List keys.
            for key in keys:
                print(f"      Key: {key.key}")  # Key name only.
        except Exception:
            print("      (Cannot list keys — permission denied)")
except Exception as e:
    print(f"  (No scopes available: {e})")

# How to use secrets.
print("\nUsage patterns:")
print("  # Get a secret value.")
print('  password = dbutils.secrets.get(scope="jdbc-scope", key="db-password")')
print('  api_key = dbutils.secrets.get(scope="api-scope", key="openai-key")')
print("")
print("  # Use in JDBC connection.")
print('  df = spark.read.jdbc(url, table, properties={"password": password})')
print("")
print("✓ Secret values are NEVER displayed (always shows [REDACTED]).")
print("  Even print(password) shows [REDACTED] in cell output.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Cluster security modes
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Cluster data security modes")
print("-"*60)

print("""
Data Security Modes (set when creating cluster):

  1. USER_ISOLATION (Shared, UC-enabled) ← RECOMMENDED for interactive.
     - Multiple users share one cluster.
     - Each user's queries run with THEIR permissions.
     - User A can't see User B's temp tables.
     - UC permissions enforced per query.
     - Supports: Python, SQL, R. Scala limited.

  2. SINGLE_USER ← RECOMMENDED for jobs.
     - Cluster dedicated to one user or service principal.
     - All code runs as that identity.
     - Full language support (Python, SQL, R, Scala).
     - Best for: automated jobs, ML training.

  3. NO_ISOLATION (legacy) ← AVOID in production.
     - No UC enforcement.
     - All users share same permissions.
     - Only for: dev/test with trusted users.

Current cluster info:
""")

# Show current cluster security mode.
print(f"  This cluster's security mode: USER_ISOLATION")
print(f"  (Shown in cluster config UI under 'Access Mode')")
print("\n✓ Always use USER_ISOLATION for shared clusters.")
print("  Use SINGLE_USER + Service Principal for production jobs.")

# COMMAND ----------

# DBTITLE 1,Sections 4-5: Intermediate and Advanced
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4-5 — INTERMEDIATE & ADVANCED
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTIONS 4-5: Advanced Security Patterns")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Service Principals (machine identities)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Service Principals for automation")
print("-"*60)

print("""
Service Principals (SPs) = machine identities for automation:

  Why use SPs instead of user accounts?
    - User accounts have passwords that expire.
    - When a user leaves, their jobs break.
    - SP credentials are managed centrally (no password sharing).
    - SPs can be audited separately from humans.

  Setup in Azure:
    1. Create SP in Entra ID (Azure AD).
    2. Add SP to Databricks workspace (Admin Console → Service Principals).
    3. Add SP to account-level groups.
    4. Grant UC permissions to SP.
    5. Use SP for Jobs (run_as: service_principal_name).

  Authentication methods for SPs:
    - OAuth M2M (machine-to-machine) ← RECOMMENDED.
    - Personal Access Token (PAT) ← legacy, avoid.

  Example: SP-authenticated Databricks SDK:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(
        host="https://adb-123.azuredatabricks.net",
        client_id="sp-app-id",         # SP application ID.
        client_secret="sp-secret"      # SP secret (from Key Vault!).
    )

  Best practices:
    - One SP per application/pipeline (not shared).
    - Rotate secrets regularly.
    - Store SP secrets in Azure Key Vault.
    - Grant least-privilege permissions.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: Cluster Policies (restrict compute resources)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: Cluster Policies (governance over compute)")
print("-"*60)

print("""
Cluster Policies: Admin-defined rules for cluster creation.

  Example policy (JSON):
  {
    "spark_version": {
      "type": "fixed",
      "value": "17.3.x-scala2.13"       // Force latest LTS runtime.
    },
    "num_workers": {
      "type": "range",
      "minValue": 1,
      "maxValue": 10                     // Max 10 workers (cost control).
    },
    "node_type_id": {
      "type": "allowlist",
      "values": ["Standard_E4ds_v5", "Standard_E8ds_v5"]  // Approved sizes.
    },
    "autotermination_minutes": {
      "type": "fixed",
      "value": 60                        // Auto-stop after 1 hour idle.
    },
    "data_security_mode": {
      "type": "fixed",
      "value": "USER_ISOLATION"          // Enforce UC security.
    },
    "custom_tags.team": {
      "type": "fixed",
      "value": "data-engineering"        // For cost tracking.
    }
  }

  Assign to groups:
    Admin Console → Cluster Policies → Permissions.
    Grant 'data_engineers' group: Can Use policy.

  Users can ONLY create clusters within policy constraints.
  Prevents: runaway costs, insecure configs, unapproved runtimes.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Audit Logs (track all access)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Audit Logs (system tables)")
print("-"*60)

print("""
Databricks System Tables for auditing:

  -- Who accessed which tables? (UC audit log)
  SELECT
    event_time,
    user_identity.email AS user,
    action_name,
    request_params.full_name_arg AS table_name
  FROM system.access.audit
  WHERE action_name IN ('getTable', 'commandSubmit')
    AND event_date >= current_date() - INTERVAL 7 DAYS
  ORDER BY event_time DESC;

  -- Failed access attempts (security investigations).
  SELECT *
  FROM system.access.audit
  WHERE response.status_code != 200
    AND event_date >= current_date() - INTERVAL 1 DAY;

  -- Cluster creation events (cost governance).
  SELECT
    event_time,
    user_identity.email,
    request_params.cluster_name,
    request_params.num_workers
  FROM system.access.audit
  WHERE action_name = 'create'
    AND service_name = 'clusters';

Available system tables:
  system.access.audit           ← All workspace events.
  system.access.table_lineage   ← Data flow between tables.
  system.billing.usage          ← Cost tracking.
  system.compute.clusters       ← Cluster inventory.
""")
print("✓ System tables = built-in observability. No extra setup needed.")

# COMMAND ----------

# DBTITLE 1,Section 6: Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Sharing Personal Access Tokens (PATs)
# MAGIC ```python
# MAGIC # BAD: Sharing your personal token with teammates or committing to Git.
# MAGIC token = "dapi1234567890abcdef"  # Your PAT in code!
# MAGIC
# MAGIC # GOOD: Use service principals for automation.
# MAGIC # Store secrets in Azure Key Vault or Databricks Secret Scopes.
# MAGIC token = dbutils.secrets.get(scope="automation", key="sp-token")
# MAGIC ```
# MAGIC **Rule**: Never share PATs. Use SPs for automation, secrets for storage.
# MAGIC
# MAGIC ### Mistake 2: NO_ISOLATION clusters in production
# MAGIC ```
# MAGIC # BAD: Cluster with no security mode.
# MAGIC # All users share the same identity. No UC enforcement.
# MAGIC # Anyone can access any table without permission checks!
# MAGIC
# MAGIC # GOOD: USER_ISOLATION for shared clusters.
# MAGIC # Each user's queries are checked against their UC permissions.
# MAGIC # SINGLE_USER + Service Principal for automated jobs.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Granting permissions to individual users
# MAGIC ```sql
# MAGIC -- BAD: Granting to individuals (doesn't scale, breaks on departure).
# MAGIC GRANT SELECT ON TABLE orders TO `alice@company.com`;
# MAGIC GRANT SELECT ON TABLE orders TO `bob@company.com`;
# MAGIC GRANT SELECT ON TABLE orders TO `charlie@company.com`;
# MAGIC
# MAGIC -- GOOD: Grant to groups (managed in Entra ID).
# MAGIC GRANT SELECT ON TABLE orders TO `data_analysts`;  -- One grant, many users.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Not rotating secrets
# MAGIC ```python
# MAGIC # BAD: Same API key for 3 years. If leaked, attacker has permanent access.
# MAGIC
# MAGIC # GOOD: Rotate secrets regularly.
# MAGIC # Azure Key Vault: Enable auto-rotation (90-day policy).
# MAGIC # Databricks: Use OAuth tokens (short-lived, auto-refresh).
# MAGIC # PATs: Set expiry date (max 90 days).
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: No cluster policy (users create huge clusters)
# MAGIC ```
# MAGIC # BAD: Any user can create a 100-node cluster. $10,000/day surprise!
# MAGIC
# MAGIC # GOOD: Cluster policies limit:
# MAGIC #   - Max workers (e.g., 10).
# MAGIC #   - Allowed node types (cost-effective only).
# MAGIC #   - Auto-termination (60 min max idle).
# MAGIC #   - Required tags (team, project for cost allocation).
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7: Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("HOMEWORK — Access Control & Secrets")
print("="*70)

# Level 1: Check your identity.
print("\n--- Level 1: Current user ---")
print(f"  User: {spark.sql('SELECT current_user()').collect()[0][0]}")
# WHY: Know who you are = know what you can access.

# Level 2: List secret scopes.
print("\n--- Level 2: Secret scopes ---")
try:
    scopes = [s.name for s in dbutils.secrets.listScopes()]
    print(f"  Available scopes: {scopes}")
except Exception:
    print("  No scopes available.")
# WHY: Secrets keep credentials out of code and logs.

# Level 3: Use a secret.
print("\n--- Level 3: Access a secret ---")
print('  value = dbutils.secrets.get(scope="name", key="key")')
print('  # Output: [REDACTED] (always hidden)').
# WHY: Secrets are never exposed in cell output.

# Level 4: Data security modes.
print("\n--- Level 4: Cluster security ---")
print("  USER_ISOLATION: shared, per-user UC enforcement.")
print("  SINGLE_USER: dedicated, one identity, full language support.")
print("  NO_ISOLATION: legacy, no UC. Avoid in production.")
# WHY: Security mode determines if UC permissions are enforced.

# Level 5: Service principals.
print("\n--- Level 5: Service principals ---")
print("  Machine identity for automated jobs.")
print("  OAuth M2M authentication (no password sharing).")
print("  One SP per pipeline. Rotate secrets.")
# WHY: SPs don't expire when employees leave.

# Levels 6-10: Conceptual.
print("\n--- Level 6: Cluster policies ---")
print("  Admin-defined rules: max workers, node types, auto-terminate.")
print("  Prevent cost overruns and insecure configurations.")

print("\n--- Level 7: Audit logs ---")
print("  system.access.audit: who accessed what, when, from where.")
print("  Use for: compliance, incident investigation, cost tracking.")

print("\n--- Level 8: Network security ---")
print("  Private Link: workspace accessible only via private network.")
print("  IP allowlists: restrict access to known IPs.")
print("  VNet injection: clusters deploy into your own VNet.")

print("\n--- Level 9: Token management ---")
print("  PATs: personal, set short expiry (90 days max).")
print("  OAuth: preferred for automation (short-lived, auto-refresh).")
print("  Admin can disable PATs and force OAuth.")

print("\n--- Level 10: Teach Databricks security ---")
print("""
"Databricks security layers:
  Identity: Users, Groups, Service Principals (via Entra ID/SCIM).
  Authorization: UC permissions (GRANT/REVOKE on catalog/schema/table).
  Compute: Data security modes (USER_ISOLATION, SINGLE_USER).
  Secrets: dbutils.secrets.get() (backed by Key Vault or Databricks).
  Network: Private Link, VNet injection, IP allowlists.
  Audit: system.access.audit (all events logged automatically).
  Best practices: Least privilege, groups not individuals,
  service principals for jobs, cluster policies for cost control,
  secrets for credentials, rotate regularly."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 100")
print("✓ MODULE 18 (Security) COMPLETE! Both notebooks (99-100) done.")
print("="*70)