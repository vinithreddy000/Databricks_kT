# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 94: dbutils — Databricks Utilities
# MAGIC ## Module 16: dbutils & Multi-Language
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 55 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **dbutils** is Databricks' built-in utility library that provides helper functions for working with files, secrets, widgets (parameters), notebooks, and more. It's available automatically in every Databricks notebook — no import needed.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Think of `dbutils` as a **Swiss Army knife** for your notebook:
# MAGIC - **dbutils.fs** = A file manager (list, copy, move, delete files in cloud storage)
# MAGIC - **dbutils.secrets** = A password vault (securely access API keys, connection strings)
# MAGIC - **dbutils.widgets** = A control panel (create dropdown menus and text inputs for parameters)
# MAGIC - **dbutils.notebook** = A workflow coordinator (run other notebooks, pass values between them)
# MAGIC
# MAGIC ### Key Modules:
# MAGIC | Module | Purpose | Most Used Methods |
# MAGIC |--------|---------|------------------|
# MAGIC | `dbutils.fs` | File system operations | ls, cp, mv, rm, head, put, mkdirs |
# MAGIC | `dbutils.secrets` | Access secrets securely | get, list, listScopes |
# MAGIC | `dbutils.widgets` | Parameterize notebooks | text, dropdown, get, remove |
# MAGIC | `dbutils.notebook` | Run other notebooks | run, exit |
# MAGIC | `dbutils.library` | Manage libraries | (deprecated → use %pip) |
# MAGIC | `dbutils.credentials` | Identity info | (Unity Catalog managed) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC dbutils Architecture:
# MAGIC
# MAGIC   ┌──────────────────────────────────────────────────────────┐
# MAGIC   │                       dbutils                              │
# MAGIC   ├──────────────┬──────────────┬──────────────┬──────────────┤
# MAGIC   │  dbutils.fs  │ dbutils.secrets│ dbutils.widgets│dbutils.notebook│
# MAGIC   ├──────────────┼──────────────┼──────────────┼──────────────┤
# MAGIC   │ ls, cp, mv,  │ get, list,     │ text, dropdown,│ run, exit      │
# MAGIC   │ rm, head,    │ listScopes     │ get, remove,   │               │
# MAGIC   │ put, mkdirs  │               │ getAll         │               │
# MAGIC   └──────────────┴──────────────┴──────────────┴──────────────┘
# MAGIC
# MAGIC dbutils.fs paths:
# MAGIC   DBFS:    "dbfs:/mnt/data/file.csv"     (legacy mount-based)
# MAGIC   Volumes: "/Volumes/catalog/schema/vol/" (recommended, Unity Catalog)
# MAGIC   abfss:   "abfss://container@account.dfs.core.windows.net/path"
# MAGIC
# MAGIC dbutils.secrets flow:
# MAGIC   1. Admin creates a Secret Scope (Azure Key Vault-backed or Databricks-backed).
# MAGIC   2. Admin adds secrets to the scope (e.g., "db-password", "api-key").
# MAGIC   3. Notebook uses: dbutils.secrets.get(scope="my-scope", key="db-password")
# MAGIC   4. Value is NEVER displayed in logs (shows [REDACTED]).
# MAGIC
# MAGIC dbutils.widgets flow:
# MAGIC   1. Define widget: dbutils.widgets.text("env", "dev", "Environment")
# MAGIC   2. Widget appears at top of notebook (text input or dropdown).
# MAGIC   3. Get value: env = dbutils.widgets.get("env")  # Returns "dev"
# MAGIC   4. Use in code: df = spark.table(f"{env}_catalog.schema.table")
# MAGIC   5. Lakeflow Jobs passes parameters to widgets automatically.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: Beginner Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTION 3 — BEGINNER: dbutils Fundamentals")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Discover all dbutils modules (self-documenting)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Exploring dbutils (built-in help)")
print("-"*60)

# dbutils.help() shows ALL available modules.
print("\nAvailable dbutils modules:")
dbutils.help()  # Lists: fs, secrets, widgets, notebook, library, etc.

# Get help for a specific module.
print("\ndbutils.fs methods:")
dbutils.fs.help()  # Lists: ls, cp, mv, rm, head, put, mkdirs, etc.

print("\n✓ dbutils.help() is your starting point for discovery.")
print("  Every module also has .help() for method-level docs.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: dbutils.fs (file system operations)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: dbutils.fs — file system operations")
print("-"*60)

# Create a temp directory for demos.
base_path = "/tmp/dbutils_demo"  # DBFS path.
dbutils.fs.mkdirs(base_path)  # Create directory (like mkdir -p).
print(f"\nCreated directory: {base_path}")

# Write a file.
dbutils.fs.put(
    f"{base_path}/hello.txt",      # Path to create.
    "Hello from dbutils!\nLine 2",  # Content to write.
    overwrite=True                   # Overwrite if exists.
)
print("Wrote hello.txt")

# List files in directory.
print("\nFiles in directory:")
files = dbutils.fs.ls(base_path)  # Returns list of FileInfo objects.
for f in files:
    print(f"  {f.name:20s} size={f.size} bytes, isDir={f.isDir()}")

# Read first N bytes of a file.
print("\nFile content (first 100 chars):")
content = dbutils.fs.head(f"{base_path}/hello.txt", 100)  # Read first 100 chars.
print(f"  '{content}'")

# Copy a file.
dbutils.fs.cp(
    f"{base_path}/hello.txt",     # Source.
    f"{base_path}/hello_copy.txt"  # Destination.
)
print("\nCopied hello.txt → hello_copy.txt")

# List again to confirm.
print("Files after copy:")
for f in dbutils.fs.ls(base_path):
    print(f"  {f.name}")

print("\n✓ dbutils.fs: ls, cp, mv, rm, head, put, mkdirs.")
print("  Works with DBFS, UC Volumes, and cloud storage paths.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: dbutils.widgets (parameterize notebooks)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: dbutils.widgets — notebook parameters")
print("-"*60)

# Create a text widget (appears at top of notebook).
dbutils.widgets.text(
    "environment",  # Widget name (used in code).
    "dev",          # Default value.
    "Environment"   # Label shown in UI.
)

# Create a dropdown widget.
dbutils.widgets.dropdown(
    "region",                              # Widget name.
    "us-east",                             # Default value.
    ["us-east", "us-west", "eu-west"],     # Choices.
    "Region"                               # Label.
)

# Get widget values in code.
env = dbutils.widgets.get("environment")  # Returns current value.
region = dbutils.widgets.get("region")    # Returns selected value.
print(f"\n  environment = '{env}'")
print(f"  region      = '{region}'")

# Use in dynamic code.
table_prefix = f"{env}_catalog"  # e.g., "dev_catalog" or "prod_catalog".
print(f"  Dynamic table: {table_prefix}.schema.my_table")

print("\n✓ Widgets create UI controls at the top of the notebook.")
print("  Lakeflow Jobs passes parameters to widgets automatically.")
print("  Types: text, dropdown, combobox, multiselect.")

# Cleanup widgets.
dbutils.widgets.remove("environment")  # Remove specific widget.
dbutils.widgets.remove("region")       # Remove specific widget.
print("  (Widgets removed for cleanup.)")

# COMMAND ----------

# DBTITLE 1,Sections 4-5: Intermediate and Advanced
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4-5 — INTERMEDIATE & ADVANCED
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("SECTIONS 4-5: Intermediate & Advanced dbutils")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: dbutils.secrets (secure credential access)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: dbutils.secrets — secure credential management")
print("-"*60)

# List available secret scopes.
print("\nAvailable secret scopes:")
try:
    scopes = dbutils.secrets.listScopes()  # List all scopes.
    for scope in scopes:
        print(f"  Scope: {scope.name}")  # Print scope name.
except Exception as e:
    print(f"  (No scopes available or permission denied: {e})")

# How to USE secrets (syntax demo — won't run without a real scope).
print("\nUsage pattern:")
print("  # Get a secret value (never displayed in logs!).")
print('  password = dbutils.secrets.get(scope="my-scope", key="db-password")')
print('  api_key = dbutils.secrets.get(scope="my-scope", key="api-key")')
print("")
print("  # Use in JDBC connection.")
print('  jdbc_url = f"jdbc:sqlserver://server:1433;database=mydb"')
print('  df = spark.read.jdbc(jdbc_url, "table", properties={"password": password})')
print("")
print("✓ Secrets are NEVER printed in cell output (shows [REDACTED]).")
print("  Backed by Azure Key Vault or Databricks secret store.")
print("  Admin creates scopes; users consume with .get().")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: dbutils.notebook (orchestrate notebooks)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: dbutils.notebook — run other notebooks")
print("-"*60)

# Syntax for running another notebook.
print("\nRun a child notebook:")
print('  result = dbutils.notebook.run(')
print('      "/Users/user@company.com/child_notebook",  # Path to notebook.')
print('      timeout_seconds=300,                        # Max 5 minutes.')
print('      arguments={"param1": "value1", "env": "prod"}  # Pass parameters.')
print('  )')
print('  # result = string returned by child\'s dbutils.notebook.exit("...").')
print("")
print("Child notebook pattern:")
print('  # child_notebook:')
print('  env = dbutils.widgets.get("env")  # Receive parameter.')
print('  # ... do work ...')
print('  dbutils.notebook.exit("SUCCESS: processed 1000 rows")  # Return value.')
print("")
print("✓ dbutils.notebook.run(): synchronous, waits for child to finish.")
print("  Returns the string passed to exit(). Timeout raises exception.")
print("  Use for: ETL orchestration, multi-step workflows.")
print("  For complex orchestration, prefer Lakeflow Jobs (DAG-based).")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Advanced dbutils.fs patterns
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Advanced file operations")
print("-"*60)

base_path = "/tmp/dbutils_demo"  # Reuse demo directory.

# Recursive listing (get all files including subdirectories).
def list_all_files(path):
    """Recursively list all files in a directory."""
    all_files = []  # Collect results.
    try:
        items = dbutils.fs.ls(path)  # List current directory.
        for item in items:
            if item.isDir():  # If directory, recurse.
                all_files.extend(list_all_files(item.path))  # Recursive call.
            else:
                all_files.append(item)  # Add file to results.
    except Exception as e:
        print(f"  Error listing {path}: {e}")
    return all_files

all_files = list_all_files(base_path)  # Get all files recursively.
print(f"\nAll files (recursive):")
for f in all_files:
    print(f"  {f.path} ({f.size} bytes)")

# Move (rename) a file.
dbutils.fs.mv(
    f"{base_path}/hello_copy.txt",    # Source.
    f"{base_path}/renamed_file.txt"   # New name/location.
)
print(f"\nMoved hello_copy.txt → renamed_file.txt")

# Delete a file.
dbutils.fs.rm(f"{base_path}/renamed_file.txt")  # Remove single file.
print("Deleted renamed_file.txt")

# Delete directory recursively.
dbutils.fs.rm(base_path, recurse=True)  # Remove dir + all contents.
print(f"Deleted entire {base_path}/ directory")

print("\n✓ Advanced patterns: recursive listing, bulk operations.")
print("  rm(path, recurse=True) deletes directories and all contents.")
print("  cp(src, dst, recurse=True) copies entire directory trees.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 7: Widget-driven ETL pattern (production)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 7: Widget-driven ETL pattern")
print("-"*60)

# Create widgets that a Lakeflow Job would populate.
dbutils.widgets.text("start_date", "2024-01-01", "Start Date")
dbutils.widgets.text("end_date", "2024-01-31", "End Date")
dbutils.widgets.dropdown("mode", "incremental", ["full", "incremental"], "Load Mode")

# Get values (from UI or from Job parameters).
start_date = dbutils.widgets.get("start_date")  # "2024-01-01".
end_date = dbutils.widgets.get("end_date")      # "2024-01-31".
mode = dbutils.widgets.get("mode")              # "incremental".

print(f"\n  ETL Configuration:")
print(f"    start_date = {start_date}")
print(f"    end_date   = {end_date}")
print(f"    mode       = {mode}")

# Use in dynamic queries.
if mode == "full":
    query = "SELECT * FROM source_table"  # Full load.
else:
    query = f"SELECT * FROM source_table WHERE date BETWEEN '{start_date}' AND '{end_date}'"

print(f"    Generated query: {query}")

print("\n✓ Production pattern: widgets parameterize notebooks.")
print("  Jobs pass values via 'Parameters' in task config.")
print("  Same notebook works for dev (manual) and prod (automated).")

# Cleanup.
dbutils.widgets.removeAll()  # Remove all widgets.
print("  (All widgets removed.)")

# COMMAND ----------

# DBTITLE 1,Section 6: Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Printing secrets (they're ALWAYS redacted)
# MAGIC ```python
# MAGIC # BAD: Trying to see the secret value.
# MAGIC password = dbutils.secrets.get(scope="my-scope", key="password")
# MAGIC print(password)  # Prints: [REDACTED] (not the actual value!)
# MAGIC
# MAGIC # GOOD: Just use it directly in connections.
# MAGIC jdbc_props = {"password": password}  # Works internally, never displayed.
# MAGIC ```
# MAGIC **Why**: Databricks redacts secrets in ALL output to prevent accidental leaks.
# MAGIC
# MAGIC ### Mistake 2: Using dbutils.fs.ls on non-existent paths
# MAGIC ```python
# MAGIC # BAD: No error handling.
# MAGIC files = dbutils.fs.ls("/path/that/does/not/exist")  # Throws Exception!
# MAGIC
# MAGIC # GOOD: Wrap in try/except.
# MAGIC try:
# MAGIC     files = dbutils.fs.ls("/my/path")
# MAGIC except Exception as e:
# MAGIC     print(f"Path not found: {e}")
# MAGIC     files = []
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Forgetting recurse=True when deleting directories
# MAGIC ```python
# MAGIC # BAD: Trying to delete a non-empty directory.
# MAGIC dbutils.fs.rm("/my/directory")  # Fails if directory has files!
# MAGIC
# MAGIC # GOOD: Use recurse=True for directories.
# MAGIC dbutils.fs.rm("/my/directory", recurse=True)  # Deletes everything.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Widget name mismatch between create and get
# MAGIC ```python
# MAGIC # BAD: Different names in create vs get.
# MAGIC dbutils.widgets.text("start_date", "2024-01-01", "Start Date")
# MAGIC date = dbutils.widgets.get("startDate")  # ERROR! Name is "start_date" not "startDate"!
# MAGIC
# MAGIC # GOOD: Use exact same name string.
# MAGIC dbutils.widgets.text("start_date", "2024-01-01", "Start Date")
# MAGIC date = dbutils.widgets.get("start_date")  # Matches exactly.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Using dbutils.notebook.run for heavy orchestration
# MAGIC ```python
# MAGIC # BAD: Complex workflow with nested notebook.run calls.
# MAGIC result1 = dbutils.notebook.run("/step1", 600)
# MAGIC result2 = dbutils.notebook.run("/step2", 600)  # Sequential, no parallelism!
# MAGIC result3 = dbutils.notebook.run("/step3", 600)  # No retry, no monitoring.
# MAGIC
# MAGIC # GOOD: Use Lakeflow Jobs for complex workflows.
# MAGIC # Jobs provide: parallelism, retries, monitoring, alerts, dependencies.
# MAGIC # Reserve notebook.run for simple 2-3 step sequences.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7: Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

print("="*70)
print("HOMEWORK — dbutils")
print("="*70)

# Level 1: Explore available utilities.
print("\n--- Level 1: Explore dbutils ---")
dbutils.help()  # See all available modules.
# WHY: Self-documenting. Always start here.

# Level 2: Create and list files.
print("\n--- Level 2: File operations ---")
dbutils.fs.mkdirs("/tmp/hw_demo")  # Create directory.
dbutils.fs.put("/tmp/hw_demo/test.txt", "homework data", overwrite=True)  # Write file.
print(f"Files: {[f.name for f in dbutils.fs.ls('/tmp/hw_demo')]}")
dbutils.fs.rm("/tmp/hw_demo", recurse=True)  # Cleanup.
# WHY: Core file management for ETL, data movement.

# Level 3: Create and read a widget.
print("\n--- Level 3: Widgets ---")
dbutils.widgets.text("name", "World", "Your Name")  # Create widget.
greeting = dbutils.widgets.get("name")  # Read value.
print(f"Hello, {greeting}!")  # Use value.
dbutils.widgets.remove("name")  # Cleanup.
# WHY: Widgets parameterize notebooks for reuse across envs.

# Level 4: List secret scopes.
print("\n--- Level 4: Secrets ---")
try:
    scopes = dbutils.secrets.listScopes()  # List scopes.
    print(f"Scopes: {[s.name for s in scopes]}")
except Exception:
    print("No accessible scopes (normal in demo environments).")
# WHY: Secrets keep credentials out of code.

# Level 5: Dynamic path construction.
print("\n--- Level 5: Dynamic paths ---")
from datetime import date
today = date.today().strftime("%Y/%m/%d")  # 2024/01/15.
path = f"/Volumes/catalog/schema/vol/data/{today}/"  # Date-partitioned path.
print(f"Today's data path: {path}")
# WHY: Production ETL uses dynamic date-based paths.

# Level 6: Recursive file listing.
print("\n--- Level 6: Recursive listing ---")
def count_files(path):
    """Count files recursively."""
    count = 0
    try:
        for item in dbutils.fs.ls(path):
            if item.isDir():
                count += count_files(item.path)  # Recurse.
            else:
                count += 1  # Count file.
    except Exception:
        pass
    return count

print(f"Files in /tmp/: {count_files('/tmp/')}")
# WHY: No built-in recursive ls; you must implement it.

# Levels 7-10: Conceptual.
print("\n--- Level 7: Widget types ---")
print("  text: free-form input. dropdown: select from list.")
print("  combobox: select OR type custom. multiselect: pick multiple.")

print("\n--- Level 8: dbutils.notebook.run vs Jobs ---")
print("  notebook.run: simple, sequential, same cluster.")
print("  Jobs: parallel tasks, retries, alerts, different clusters.")

print("\n--- Level 9: Secret scopes ---")
print("  Azure Key Vault-backed: secrets stored in AKV, referenced in Databricks.")
print("  Databricks-backed: secrets stored inside Databricks.")
print("  Both: accessed via dbutils.secrets.get(scope, key).")

print("\n--- Level 10: Teach dbutils ---")
print("""
"dbutils = built-in Databricks utilities (no import needed).
  .fs: file operations (ls, cp, mv, rm, head, put, mkdirs).
  .secrets: access credentials securely (get, list, listScopes).
  .widgets: parameterize notebooks (text, dropdown, get, remove).
  .notebook: run other notebooks (run, exit).
  Key rules:
    - Secrets are always [REDACTED] in output.
    - Widgets enable dev/prod with same notebook.
    - Use Jobs for complex orchestration, notebook.run for simple."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 94")
print("="*70)