# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 98: CI/CD & Declarative Automation Bundles (DABs)
# MAGIC ## Module 17: Orchestration & CI/CD
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **CI/CD** (Continuous Integration / Continuous Deployment) automates the process of testing, validating, and deploying code changes to Databricks. **Declarative Automation Bundles (DABs)** are Databricks' native Infrastructure-as-Code (IaC) tool for defining jobs, pipelines, and resources in YAML files that can be version-controlled and deployed across environments.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC - **CI/CD** = An **automated factory quality inspector**: Every time a worker (developer) finishes a part (code), the inspector automatically checks it (tests), approves it (review), and installs it on the assembly line (deployment).
# MAGIC - **DABs** = The **factory blueprints**: Written once in YAML, they define exactly how every machine (job/pipeline) should be configured. Deploy the same blueprint to the test factory (dev) and production factory (prod) with different parameters.
# MAGIC
# MAGIC ### Key Concepts:
# MAGIC | Concept | Tool | Purpose |
# MAGIC |---------|------|---------|
# MAGIC | Version control | Git (Azure DevOps, GitHub) | Track code changes |
# MAGIC | CI pipeline | Azure DevOps / GitHub Actions | Automated testing |
# MAGIC | CD pipeline | DABs + CLI | Automated deployment |
# MAGIC | DABs | `databricks bundle` CLI | IaC for Databricks resources |
# MAGIC | Environments | dev / staging / prod | Isolated deployment targets |
# MAGIC | Repos | Databricks Repos | Git integration in workspace |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC CI/CD Pipeline Flow:
# MAGIC
# MAGIC   Developer         Git Repo          CI Pipeline        Databricks
# MAGIC   ─────────         ────────          ───────────        ──────────
# MAGIC   1. Write code → 2. Push to branch → 3. Run tests   → 4. Deploy
# MAGIC      (notebook)     (PR created)       (lint, unit,      (DABs to
# MAGIC                                         integration)      prod)
# MAGIC
# MAGIC DABs Project Structure:
# MAGIC
# MAGIC   my-project/
# MAGIC   ├── databricks.yml         # Bundle configuration (root).
# MAGIC   ├── resources/
# MAGIC   │   ├── jobs.yml           # Job definitions.
# MAGIC   │   └── pipelines.yml      # Pipeline definitions.
# MAGIC   ├── src/
# MAGIC   │   ├── notebooks/         # Notebook source files.
# MAGIC   │   │   ├── ingest.py
# MAGIC   │   │   ├── transform.py
# MAGIC   │   │   └── load.py
# MAGIC   │   └── libraries/         # Shared Python modules.
# MAGIC   └── tests/                 # Unit + integration tests.
# MAGIC       ├── test_transform.py
# MAGIC       └── test_quality.py
# MAGIC
# MAGIC DABs CLI Commands:
# MAGIC   databricks bundle init       # Create new project from template.
# MAGIC   databricks bundle validate   # Check YAML syntax.
# MAGIC   databricks bundle deploy     # Deploy to target workspace.
# MAGIC   databricks bundle run        # Run a job/pipeline.
# MAGIC   databricks bundle destroy    # Remove deployed resources.
# MAGIC
# MAGIC Environment Promotion:
# MAGIC   databricks bundle deploy --target dev      # Deploy to dev.
# MAGIC   databricks bundle deploy --target staging  # Deploy to staging.
# MAGIC   databricks bundle deploy --target prod     # Deploy to production.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: Beginner Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTION 3 — BEGINNER: CI/CD & DABs Fundamentals")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: databricks.yml (bundle root config)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: databricks.yml (DABs root configuration)")
print("-"*60)

databricks_yml = """
# databricks.yml - Root configuration for Declarative Automation Bundle.
bundle:
  name: sales-etl-pipeline      # Bundle name (unique identifier).

include:
  - resources/*.yml             # Include all resource definitions.

targets:
  dev:
    mode: development           # Development mode (permissive).
    default: true               # Default target when no --target specified.
    workspace:
      host: https://adb-1234567890.azuredatabricks.net

  staging:
    workspace:
      host: https://adb-1234567890.azuredatabricks.net
    variables:
      catalog: staging_catalog
      warehouse_id: abc123def456

  prod:
    mode: production            # Production mode (strict validation).
    workspace:
      host: https://adb-9876543210.azuredatabricks.net
    variables:
      catalog: prod_catalog
      warehouse_id: xyz789abc012
    run_as:
      service_principal_name: etl-service-principal  # Run as SP in prod.

variables:
  catalog:
    default: dev_catalog        # Default catalog (overridden per target).
"""

print(databricks_yml)
print("✓ databricks.yml defines: bundle name, targets (envs), variables.")
print("  Each target overrides variables for its environment.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Job resource definition (resources/jobs.yml)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Job definition in YAML (resources/jobs.yml)")
print("-"*60)

jobs_yml = """
# resources/jobs.yml - Job definitions.
resources:
  jobs:
    daily_sales_etl:
      name: "Daily Sales ETL - ${var.catalog}"   # Uses variable.
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"    # Daily at 6 AM.
        timezone_id: "Europe/Berlin"
      email_notifications:
        on_failure:
          - oncall@company.com
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ../src/notebooks/ingest.py
            base_parameters:
              catalog: ${var.catalog}             # Dynamic catalog.
          new_cluster:
            spark_version: "17.3.x-scala2.13"
            node_type_id: "Standard_E4ds_v5"
            num_workers: 2

        - task_key: transform
          depends_on:
            - task_key: ingest
          notebook_task:
            notebook_path: ../src/notebooks/transform.py
            base_parameters:
              catalog: ${var.catalog}

        - task_key: load_gold
          depends_on:
            - task_key: transform
          notebook_task:
            notebook_path: ../src/notebooks/load.py
            base_parameters:
              catalog: ${var.catalog}
"""

print(jobs_yml)
print("✓ ${var.catalog} resolves to 'dev_catalog', 'staging_catalog', or 'prod_catalog'.")
print("  Same YAML → different environments via targets.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: DABs CLI commands
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: DABs CLI workflow")
print("-"*60)

print("""
Development workflow with DABs:

  # 1. Initialize a new project.
  $ databricks bundle init default-python
  # Creates project structure from template.

  # 2. Validate configuration.
  $ databricks bundle validate
  # Checks YAML syntax and references.

  # 3. Deploy to dev (default target).
  $ databricks bundle deploy
  # Uploads notebooks, creates/updates jobs in dev workspace.

  # 4. Run the job.
  $ databricks bundle run daily_sales_etl
  # Triggers the job. Shows real-time output.

  # 5. Deploy to production.
  $ databricks bundle deploy --target prod
  # Same code, production config (different catalog, SP identity).

  # 6. Tear down (remove all resources).
  $ databricks bundle destroy --target dev
  # Removes jobs, pipelines, uploaded files from dev.
""")

# COMMAND ----------

# DBTITLE 1,Sections 4-5: Intermediate and Advanced
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4-5 — INTERMEDIATE & ADVANCED
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTIONS 4-5: Advanced CI/CD Patterns")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Azure DevOps CI/CD pipeline (azure-pipelines.yml)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Azure DevOps CI/CD pipeline")
print("-"*60)

azure_pipeline_yml = """
# azure-pipelines.yml (Azure DevOps CI/CD)
trigger:
  branches:
    include:
      - main          # Trigger on push to main.
      - release/*     # Trigger on release branches.

stages:
  - stage: CI
    displayName: 'Continuous Integration'
    jobs:
      - job: Test
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '3.11'

          - script: pip install databricks-cli pytest
            displayName: 'Install dependencies'

          - script: pytest tests/ -v
            displayName: 'Run unit tests'

          - script: databricks bundle validate
            displayName: 'Validate DABs config'
            env:
              DATABRICKS_HOST: $(DATABRICKS_HOST)
              DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)

  - stage: DeployStaging
    displayName: 'Deploy to Staging'
    dependsOn: CI
    condition: succeeded()
    jobs:
      - job: Deploy
        steps:
          - script: databricks bundle deploy --target staging
            env:
              DATABRICKS_HOST: $(DATABRICKS_HOST)
              DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)

  - stage: DeployProd
    displayName: 'Deploy to Production'
    dependsOn: DeployStaging
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - deployment: ProdDeploy
        environment: 'production'  # Requires manual approval.
        strategy:
          runOnce:
            deploy:
              steps:
                - script: databricks bundle deploy --target prod
"""

print(azure_pipeline_yml)
print("✓ CI: test → validate. CD: deploy staging → approval → deploy prod.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: Testing patterns for Databricks code
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: Testing strategies for Databricks code")
print("-"*60)

print("""
Test pyramid for Databricks:

  ┌────────────────┐
  │  Integration  │  ← Run on real cluster (expensive, few tests).
  │    Tests      │    Test full pipeline end-to-end.
  ├────────────────┤
  │   Unit Tests  │  ← Run locally with PySpark (fast, many tests).
  │  (transform   │    Test individual functions.
  │   functions)  │
  └────────────────┘

Unit test example (tests/test_transform.py):

  import pytest
  from pyspark.sql import SparkSession
  from src.transform import clean_data, add_metrics

  @pytest.fixture(scope="session")
  def spark():
      return SparkSession.builder.master("local[*]").getOrCreate()

  def test_clean_data_removes_nulls(spark):
      # Arrange.
      input_df = spark.createDataFrame([(1, "a"), (2, None)], ["id", "name"])
      # Act.
      result = clean_data(input_df)
      # Assert.
      assert result.count() == 1
      assert result.collect()[0]["name"] == "a"

  def test_add_metrics_computes_correctly(spark):
      input_df = spark.createDataFrame([(100.0,), (200.0,)], ["amount"])
      result = add_metrics(input_df)
      assert "amount_with_tax" in result.columns

Integration test (run on Databricks cluster):
  - Deploy to staging via DABs.
  - Run job with test data.
  - Validate output table contents.
  - Cleanup test tables.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Databricks Repos (Git integration)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Databricks Repos (direct Git integration)")
print("-"*60)

print("""
Databricks Repos:
  1. Connect workspace to Git provider (Azure DevOps, GitHub, GitLab).
  2. Clone repo into /Repos/user@company.com/my-project.
  3. Develop notebooks directly in the repo.
  4. Commit, push, pull — all from the Databricks UI.
  5. PR review happens in Git provider.

Branch workflow:
  main      ───○─────────○─────○────  (production)
               \\         /
  feature    ──○──○──○─○    (development)

  Developer workflow:
    1. Create feature branch in Repos UI.
    2. Develop + test interactively on cluster.
    3. Commit changes.
    4. Create PR in Azure DevOps/GitHub.
    5. CI pipeline runs tests automatically.
    6. Reviewer approves → merge to main.
    7. CD pipeline deploys to production.

Repos vs DABs:
  Repos: Interactive development, notebook-first, Git UI in Databricks.
  DABs:  Automated deployment, IaC, CI/CD-first, production deployments.
  Best practice: Use BOTH! Develop in Repos, deploy with DABs.
""")

# COMMAND ----------

# DBTITLE 1,Section 6: Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Manual deployments to production (no audit trail)
# MAGIC ```bash
# MAGIC # BAD: Manually copying notebooks to production.
# MAGIC # No version history, no review, no rollback capability.
# MAGIC
# MAGIC # GOOD: All changes go through Git + CI/CD.
# MAGIC # Every deployment is traceable, reviewable, and reversible.
# MAGIC git push origin feature/my-change  # Triggers CI.
# MAGIC # PR review → merge → CD deploys automatically.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Secrets/tokens in code
# MAGIC ```python
# MAGIC # BAD: Credentials committed to Git.
# MAGIC token = "dapi12345abcdef"  # NEVER commit secrets!
# MAGIC
# MAGIC # GOOD: Use Databricks secrets or environment variables.
# MAGIC token = dbutils.secrets.get(scope="my-scope", key="token")
# MAGIC # In CI/CD: use pipeline variables or Azure Key Vault.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: No environment separation (dev writes to prod tables)
# MAGIC ```yaml
# MAGIC # BAD: Same catalog for all environments.
# MAGIC catalog: prod_catalog  # Dev testing hits production!
# MAGIC
# MAGIC # GOOD: Variables per target in DABs.
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     default: dev_catalog  # Safe default.
# MAGIC # targets.prod overrides with prod_catalog.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: No validation step in CI
# MAGIC ```yaml
# MAGIC # BAD: Deploy directly without testing.
# MAGIC steps:
# MAGIC   - script: databricks bundle deploy --target prod  # What if code is broken?
# MAGIC
# MAGIC # GOOD: Test first, then deploy.
# MAGIC steps:
# MAGIC   - script: pytest tests/ -v              # Unit tests.
# MAGIC   - script: databricks bundle validate     # Config validation.
# MAGIC   - script: databricks bundle deploy --target staging  # Test in staging.
# MAGIC   - script: databricks bundle deploy --target prod     # Then production.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not using service principals in production
# MAGIC ```yaml
# MAGIC # BAD: Jobs run as a user account (breaks when user leaves).
# MAGIC run_as:
# MAGIC   user_name: john@company.com  # What if John leaves?
# MAGIC
# MAGIC # GOOD: Jobs run as a service principal (eternal, auditable).
# MAGIC run_as:
# MAGIC   service_principal_name: etl-service-principal
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7: Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("HOMEWORK — CI/CD & DABs")
print("="*70)

print("\n--- Level 1: DABs project structure ---")
print("  databricks.yml, resources/, src/notebooks/, tests/")

print("\n--- Level 2: Key CLI commands ---")
print("  databricks bundle init       # Create from template.")
print("  databricks bundle validate   # Check YAML.")
print("  databricks bundle deploy     # Deploy to target.")
print("  databricks bundle run <job>  # Run job.")

print("\n--- Level 3: Targets (environments) ---")
print("  targets: {dev, staging, prod} with different variables.")
print("  deploy --target prod  # Uses prod variables.")

print("\n--- Level 4: Variables ---")
print("  variables: {catalog: {default: dev_catalog}}")
print("  Use in YAML: ${var.catalog}")
print("  Override per target: targets.prod.variables.catalog: prod_catalog")

print("\n--- Level 5: CI pipeline stages ---")
print("  1. Lint (ruff, black). 2. Unit tests (pytest).")
print("  3. Validate (bundle validate). 4. Deploy staging.")
print("  5. Integration test. 6. Deploy prod (with approval).")

print("\n--- Level 6: Testing strategy ---")
print("  Unit: Test transform functions locally with PySpark.")
print("  Integration: Deploy to staging, run job, check outputs.")

print("\n--- Level 7: Repos workflow ---")
print("  Clone repo → branch → develop → commit → PR → CI → merge → CD.")

print("\n--- Level 8: Service principals ---")
print("  run_as: service_principal_name: my-sp")
print("  Production jobs always run as SP (not user accounts).")

print("\n--- Level 9: Rollback strategy ---")
print("  Git revert + redeploy: git revert HEAD; databricks bundle deploy")
print("  Or: deploy a previous tag: git checkout v1.2.0; bundle deploy")

print("\n--- Level 10: Teach CI/CD + DABs ---")
print("""
"CI/CD for Databricks:
  DABs (Declarative Automation Bundles) = IaC for Databricks.
  Define jobs/pipelines in YAML, deploy with CLI.
  Targets: dev/staging/prod with different variables.
  CI: lint → test → validate. CD: deploy staging → prod.
  Git workflow: branch → PR → review → merge → auto-deploy.
  Best practices: service principals, no secrets in code,
  environment separation, automated tests before deploy."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 98")
print("✓ MODULE 17 (Orchestration & CI/CD) COMPLETE! All 3 notebooks done.")
print("="*70)