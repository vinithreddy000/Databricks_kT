# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 96: Lakeflow Jobs (Scheduling & Orchestration)
# MAGIC ## Module 17: Orchestration & CI/CD
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Lakeflow Jobs** (formerly Databricks Workflows) is the native orchestration service for scheduling and running notebooks, Python scripts, JAR files, and pipelines on a recurring basis. It supports multi-task DAGs, retries, alerts, conditional logic, and parameterization.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Think of a **factory assembly line supervisor**:
# MAGIC - **Job** = The production plan (what gets built, in what order)
# MAGIC - **Task** = Each station on the line (extract, transform, load)
# MAGIC - **Schedule** = The shift (run daily at 6 AM)
# MAGIC - **Trigger** = Start when the previous batch arrives
# MAGIC - **Alert** = Fire alarm if something breaks (email/Slack notification)
# MAGIC
# MAGIC ### Key Concepts:
# MAGIC | Concept | Description |
# MAGIC |---------|------------|
# MAGIC | **Job** | Container for one or more tasks with a schedule |
# MAGIC | **Task** | A unit of work (notebook, Python script, pipeline, SQL) |
# MAGIC | **DAG** | Directed Acyclic Graph — task dependency chain |
# MAGIC | **Trigger** | When to run: cron schedule, file arrival, or manual |
# MAGIC | **Cluster** | Compute for each task (shared or per-task) |
# MAGIC | **Parameters** | Key-value pairs passed to notebooks (widgets) |
# MAGIC | **Retry** | Auto-retry failed tasks N times |
# MAGIC | **Alert** | Email/webhook on success/failure/timeout |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Lakeflow Jobs Architecture:
# MAGIC
# MAGIC   ┌────────────────────────────────────────────────┐
# MAGIC   │              JOB: Daily ETL Pipeline                │
# MAGIC   │  Schedule: 0 0 6 * * ? * (daily at 6 AM UTC)       │
# MAGIC   ├────────────────────────────────────────────────┤
# MAGIC   │                                                    │
# MAGIC   │  [Task 1: Ingest]                                  │
# MAGIC   │       │                                            │
# MAGIC   │       ├─────────────────┐                            │
# MAGIC   │       │                 │                            │
# MAGIC   │  [Task 2: Transform]  [Task 3: Validate]           │
# MAGIC   │       │                 │                            │
# MAGIC   │       └─────────────────┘                            │
# MAGIC   │              │                                      │
# MAGIC   │       [Task 4: Load]                                │
# MAGIC   │              │                                      │
# MAGIC   │       [Task 5: Notify]                              │
# MAGIC   │                                                    │
# MAGIC   └────────────────────────────────────────────────┘
# MAGIC
# MAGIC Task Types:
# MAGIC   - Notebook task: Run a .ipynb notebook.
# MAGIC   - Python script: Run a .py file.
# MAGIC   - SQL task: Run a SQL query or file.
# MAGIC   - Pipeline task: Trigger an SDP (Spark Declarative Pipeline).
# MAGIC   - JAR task: Run a compiled Java/Scala JAR.
# MAGIC   - If/Else task: Conditional branching.
# MAGIC   - For-each task: Loop over a list.
# MAGIC
# MAGIC Job Creation Methods:
# MAGIC   1. UI: Jobs page → Create Job (visual DAG builder).
# MAGIC   2. REST API: POST /api/2.1/jobs/create (JSON payload).
# MAGIC   3. Databricks CLI: databricks jobs create --json-file job.json
# MAGIC   4. Declarative Automation Bundles (DABs): YAML-based IaC.
# MAGIC   5. Terraform: databricks_job resource.
# MAGIC
# MAGIC Parameter Passing:
# MAGIC   Job → Task → Notebook widget.
# MAGIC   {"start_date": "2024-01-01", "env": "prod"}
# MAGIC   Notebook receives via: dbutils.widgets.get("start_date")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: Beginner Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

import json  # For JSON formatting.

print("="*70)
print("SECTION 3 — BEGINNER: Lakeflow Jobs Fundamentals")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Job JSON structure (single-task job)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Simple single-task job (JSON definition)")
print("-"*60)

# This is what a job looks like when defined via API/CLI.
single_task_job = {
    "name": "Daily Sales Report",  # Job name.
    "schedule": {
        "quartz_cron_expression": "0 0 8 * * ?",  # Every day at 8 AM.
        "timezone_id": "Europe/Berlin"  # Timezone.
    },
    "tasks": [
        {
            "task_key": "generate_report",  # Unique task identifier.
            "notebook_task": {
                "notebook_path": "/Repos/team/etl/sales_report",  # Notebook to run.
                "base_parameters": {  # Parameters passed to widgets.
                    "report_date": "{{job.start_time.iso_date}}",  # Dynamic date.
                    "environment": "production"
                }
            },
            "new_cluster": {  # Dedicated cluster for this task.
                "spark_version": "17.3.x-scala2.13",
                "node_type_id": "Standard_E4ds_v5",
                "num_workers": 2
            },
            "timeout_seconds": 3600,  # 1-hour timeout.
            "max_retries": 2,  # Retry up to 2 times on failure.
            "retry_on_timeout": True  # Also retry on timeout.
        }
    ],
    "email_notifications": {
        "on_failure": ["team@company.com"]  # Alert on failure.
    }
}

print("\nSingle-task job definition:")
print(json.dumps(single_task_job, indent=2))  # Pretty print.

print("\n✓ Key parts: name, schedule (cron), tasks (what to run), alerts.")
print("  Create via: UI (easiest), CLI, REST API, or DABs (IaC).")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Multi-task DAG (dependencies)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Multi-task DAG with dependencies")
print("-"*60)

multi_task_job = {
    "name": "ETL Pipeline - Bronze to Gold",
    "tasks": [
        {
            "task_key": "ingest_bronze",  # Task 1: No dependencies (runs first).
            "notebook_task": {"notebook_path": "/etl/01_ingest"},
            "existing_cluster_id": "0123-456789-abcdef"  # Use existing cluster.
        },
        {
            "task_key": "transform_silver",  # Task 2: Depends on Task 1.
            "depends_on": [{"task_key": "ingest_bronze"}],  # Run AFTER ingest.
            "notebook_task": {"notebook_path": "/etl/02_transform"}
        },
        {
            "task_key": "validate_quality",  # Task 3: Also depends on Task 1.
            "depends_on": [{"task_key": "ingest_bronze"}],  # Parallel with Task 2!
            "notebook_task": {"notebook_path": "/etl/03_validate"}
        },
        {
            "task_key": "load_gold",  # Task 4: Depends on BOTH 2 and 3.
            "depends_on": [
                {"task_key": "transform_silver"},
                {"task_key": "validate_quality"}
            ],
            "notebook_task": {"notebook_path": "/etl/04_load_gold"}
        }
    ]
}

print("\nDAG structure:")
print("  [ingest_bronze]")
print("       |")
print("       ├──────────────────┐")
print("       |                  |")
print("  [transform_silver]  [validate_quality]  ← run in PARALLEL")
print("       |                  |")
print("       └──────────────────┘")
print("              |")
print("         [load_gold]  ← waits for BOTH to complete")

print("\n✓ depends_on: defines task execution order.")
print("  Tasks without shared dependencies run in PARALLEL automatically.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: Writing a job-ready notebook (with widgets)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: Job-ready notebook pattern")
print("-"*60)

print("""
Template for notebooks that will be run by Jobs:

  # Cell 1: Define parameters with defaults.
  dbutils.widgets.text("start_date", "2024-01-01", "Start Date")
  dbutils.widgets.text("end_date", "2024-01-31", "End Date")
  dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "Environment")

  # Cell 2: Get parameter values.
  start_date = dbutils.widgets.get("start_date")
  end_date = dbutils.widgets.get("end_date")
  env = dbutils.widgets.get("env")
  catalog = f"{env}_catalog"  # Dynamic catalog based on environment.

  # Cell 3: Do the work.
  df = spark.table(f"{catalog}.sales.orders") \\
      .filter(f"order_date BETWEEN '{start_date}' AND '{end_date}'")
  df.write.mode("overwrite").saveAsTable(f"{catalog}.gold.daily_summary")

  # Cell 4: Exit with status (optional, for notebook.run callers).
  dbutils.notebook.exit(json.dumps({"status": "success", "rows": df.count()}))

When the Job runs, it passes parameters:
  {"start_date": "2024-05-27", "end_date": "2024-05-27", "env": "prod"}
  These override the widget defaults automatically.
""")

# COMMAND ----------

# DBTITLE 1,Sections 4-5: Intermediate and Advanced
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4-5 — INTERMEDIATE & ADVANCED
# ═══════════════════════════════════════════════════════════════════

import json  # JSON formatting.

print("="*70)
print("SECTIONS 4-5: Advanced Job Patterns")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Dynamic job parameters and task values
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Dynamic parameters and task values")
print("-"*60)

print("""
Built-in dynamic parameters (auto-resolved at runtime):

  {{job.id}}                  → 12345
  {{job.name}}                → "Daily ETL Pipeline"
  {{job.start_time.iso_date}} → "2024-05-27"
  {{job.start_time.epoch_ms}} → 1716825600000
  {{task.name}}               → "ingest_bronze"
  {{run_id}}                  → 67890

Task value passing (output from one task → input to another):

  # Task 1 notebook (sets a task value):
  row_count = df.count()
  dbutils.jobs.taskValues.set(key="rows_processed", value=row_count)

  # Task 2 notebook (reads the task value):
  rows = dbutils.jobs.taskValues.get(
      taskKey="ingest_bronze",    # Which task set the value.
      key="rows_processed",       # Which key to read.
      default=0                   # Fallback if not found.
  )
  print(f"Previous task processed {rows} rows.")

This enables data-driven DAGs (if task 1 found 0 rows, task 2 can skip).
""")

# Demonstrate task value API.
print("Task value API demo (won't work outside a Job run):")
print("  dbutils.jobs.taskValues.set(key='status', value='complete')")
print("  dbutils.jobs.taskValues.get(taskKey='task_1', key='status')")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: Programmatic job management (REST API)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: Managing jobs via REST API / SDK")
print("-"*60)

print("""
Databricks SDK (Python) for job management:

  from databricks.sdk import WorkspaceClient
  w = WorkspaceClient()  # Auto-authenticates in notebook.

  # List all jobs.
  for job in w.jobs.list():
      print(f"{job.job_id}: {job.settings.name}")

  # Create a job.
  from databricks.sdk.service.jobs import *
  created = w.jobs.create(
      name="My New Job",
      tasks=[Task(
          task_key="main",
          notebook_task=NotebookTask(notebook_path="/my/notebook"),
          new_cluster=ClusterSpec(spark_version="17.3.x-scala2.13", ...)
      )]
  )
  print(f"Job created: {created.job_id}")

  # Trigger a run.
  run = w.jobs.run_now(job_id=created.job_id)

  # Get run status.
  status = w.jobs.get_run(run_id=run.run_id)
  print(f"Status: {status.state.life_cycle_state}")

CLI equivalents:
  databricks jobs list
  databricks jobs create --json-file job_definition.json
  databricks jobs run-now --job-id 12345
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Conditional and for-each tasks
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Conditional (If/Else) and For-Each tasks")
print("-"*60)

print("""
If/Else Task (conditional branching):

  {
    "task_key": "check_data_quality",
    "condition_task": {
      "op": "GREATER_THAN",
      "left": "{{tasks.validate.values.error_count}}",
      "right": "0"
    }
  }
  If TRUE:  run "send_alert" task.
  If FALSE: run "load_to_gold" task.

For-Each Task (loop over a list):

  {
    "task_key": "process_regions",
    "for_each_task": {
      "inputs": ["us-east", "us-west", "eu-west", "ap-south"],
      "task": {
        "task_key": "process_one_region",
        "notebook_task": {
          "notebook_path": "/etl/process_region",
          "base_parameters": {"region": "{{input}}"}
        }
      },
      "concurrency": 4  # Process all 4 regions in parallel!
    }
  }

  This runs the notebook 4 times (once per region), in parallel.
""")

# COMMAND ----------

# DBTITLE 1,Section 6: Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: No retries configured (jobs fail silently)
# MAGIC ```json
# MAGIC // BAD: No retry on transient failures.
# MAGIC {"task_key": "ingest", "max_retries": 0}
# MAGIC
# MAGIC // GOOD: Retry transient failures (network, cluster startup).
# MAGIC {"task_key": "ingest", "max_retries": 2, "min_retry_interval_millis": 60000}
# MAGIC ```
# MAGIC **Rule**: Always set `max_retries: 2` for production jobs.
# MAGIC
# MAGIC ### Mistake 2: Using interactive clusters for jobs (expensive!)
# MAGIC ```json
# MAGIC // BAD: Running jobs on your always-on interactive cluster.
# MAGIC {"existing_cluster_id": "my-dev-cluster"}  // Cluster stays up 24/7!
# MAGIC
# MAGIC // GOOD: Use job clusters (auto-terminate after task completes).
# MAGIC {"new_cluster": {"num_workers": 2, "spark_version": "17.3.x-scala2.13"}}
# MAGIC ```
# MAGIC **Rule**: Job clusters auto-terminate = you only pay for actual compute time.
# MAGIC
# MAGIC ### Mistake 3: No timeout (job runs forever on stuck queries)
# MAGIC ```json
# MAGIC // BAD: No timeout. If a query hangs, the job runs (and costs) indefinitely.
# MAGIC {"task_key": "etl", "timeout_seconds": 0}
# MAGIC
# MAGIC // GOOD: Set reasonable timeout.
# MAGIC {"task_key": "etl", "timeout_seconds": 7200}  // 2-hour max.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Not parameterizing notebooks (hard-coded environments)
# MAGIC ```python
# MAGIC # BAD: Hard-coded production catalog.
# MAGIC catalog = "prod_catalog"  # Can't reuse for dev/staging!
# MAGIC
# MAGIC # GOOD: Parameterized with widgets.
# MAGIC dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"])
# MAGIC catalog = f"{dbutils.widgets.get('env')}_catalog"
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not setting up failure alerts
# MAGIC ```json
# MAGIC // BAD: Job fails silently. No one knows until data is stale.
# MAGIC {"email_notifications": {}}
# MAGIC
# MAGIC // GOOD: Alert team on failure.
# MAGIC {"email_notifications": {"on_failure": ["oncall@company.com"]},
# MAGIC  "webhook_notifications": {"on_failure": [{"id": "slack-webhook-id"}]}}
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7: Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("HOMEWORK — Lakeflow Jobs")
print("="*70)

# Level 1: Cron expression.
print("\n--- Level 1: Cron expressions ---")
print("  '0 0 8 * * ?'   = Daily at 8 AM")
print("  '0 30 6 ? * MON-FRI' = Weekdays at 6:30 AM")
print("  '0 0 */4 * * ?' = Every 4 hours")
print("  '0 0 0 1 * ?'   = 1st of every month at midnight")
# WHY: Quartz cron has 6 fields: sec min hr day month dow.

# Level 2: Task dependencies.
print("\n--- Level 2: depends_on ---")
print("  'depends_on': [{'task_key': 'task_a'}]  # Run after task_a.")
print("  No depends_on = runs immediately (root task).")
print("  Multiple depends_on = waits for ALL to complete.")
# WHY: DAG structure controls parallel vs sequential execution.

# Level 3: Pass parameters to notebook.
print("\n--- Level 3: Parameters ---")
print("  Job config: base_parameters: {'date': '2024-01-01', 'env': 'prod'}")
print("  Notebook:   dbutils.widgets.get('date')  # Returns '2024-01-01'")
# WHY: Parameters make notebooks reusable across environments.

# Level 4: Task values (pass data between tasks).
print("\n--- Level 4: Task values ---")
print("  Task 1: dbutils.jobs.taskValues.set(key='count', value=1000)")
print("  Task 2: dbutils.jobs.taskValues.get(taskKey='task1', key='count')")
# WHY: Enables data-driven workflows (skip if 0 rows, alert if high).

# Level 5: Job clusters vs existing clusters.
print("\n--- Level 5: Cluster choice ---")
print("  Job cluster (new_cluster): auto-creates, auto-terminates. Cost-efficient.")
print("  Existing cluster: shares, always-on. Good for dev/testing.")
print("  Serverless: fastest startup, no config. Premium tier.")
# WHY: Job clusters save 40-60% vs interactive clusters.

# Levels 6-10: Conceptual.
print("\n--- Level 6: Retry strategy ---")
print("  max_retries: 2-3 for transient failures.")
print("  min_retry_interval_millis: 60000 (wait 1 min between retries).")
print("  retry_on_timeout: true (also retry if task times out).")

print("\n--- Level 7: Alerts ---")
print("  email_notifications: on_failure, on_success, on_start.")
print("  webhook_notifications: Slack, Teams, PagerDuty.")

print("\n--- Level 8: File arrival trigger ---")
print("  Instead of cron, trigger when new files land in storage.")
print("  trigger: {file_arrival: {url: '/Volumes/cat/schema/vol/landing/'}}")

print("\n--- Level 9: Job access control ---")
print("  Owner: full control. Can manage: run/edit. Can view: read-only.")
print("  Service principals: recommended for production jobs.")

print("\n--- Level 10: Teach Lakeflow Jobs ---")
print("""
"Lakeflow Jobs = Databricks native orchestrator.
  Job = container with tasks, schedule, and alerts.
  Tasks form a DAG (depends_on for ordering, parallel by default).
  Parameters: pass to notebooks via widgets.
  Task values: pass data between tasks (dbutils.jobs.taskValues).
  Clusters: use job clusters (auto-terminate) for cost savings.
  Always configure: timeout, retries, and failure alerts.
  Creation: UI, CLI, REST API, or DABs (IaC)."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 96")
print("="*70)