# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 93: Python Libraries in Databricks
# MAGIC ## Module 15: PySpark Pandas & Python Integration
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 40 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC Databricks clusters come pre-installed with hundreds of Python libraries (pandas, numpy, scikit-learn, matplotlib, etc.). But you'll often need **additional libraries** for specific tasks. This notebook covers all the ways to install, manage, and use Python packages in Databricks.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Your cluster is like a **toolbox**. It comes with common tools (hammer, screwdriver). But sometimes you need a specialized tool (laser level). You can:
# MAGIC - **%pip install** = Buy the tool for THIS notebook session only.
# MAGIC - **Cluster library** = Stock the tool in the workshop permanently.
# MAGIC - **Init script** = Automatically set up tools when building a new workshop.
# MAGIC
# MAGIC ### Installation Methods (ranked by preference):
# MAGIC | Method | Scope | Persistence | Use Case |
# MAGIC |--------|-------|-------------|----------|
# MAGIC | `%pip install` | Notebook | Session only | Quick testing, notebook-specific |
# MAGIC | Cluster libraries | Cluster | Until cluster restart | Team-wide packages |
# MAGIC | Requirements.txt | Cluster | Until restart | Reproducible environments |
# MAGIC | Init scripts | Cluster | Every restart | System-level setup |
# MAGIC | Unity Catalog Volumes | Workspace | Permanent | Custom .whl files |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Library Installation in Databricks:
# MAGIC
# MAGIC   %pip install (NOTEBOOK-SCOPED):
# MAGIC     - Installs for THIS notebook only (other notebooks unaffected).
# MAGIC     - Requires Python process restart (automatic in Databricks).
# MAGIC     - Must be in its own cell (first line = %pip).
# MAGIC     - Available immediately after cell completes.
# MAGIC     - Lost when cluster restarts.
# MAGIC
# MAGIC   Cluster Libraries (CLUSTER-SCOPED):
# MAGIC     - Installed on ALL nodes of the cluster.
# MAGIC     - Available to all notebooks on that cluster.
# MAGIC     - Set via: Cluster → Libraries tab → Install New.
# MAGIC     - Supports: PyPI, Maven, CRAN, JAR, Wheel.
# MAGIC
# MAGIC   Init Scripts (CLUSTER-SCOPED, persistent):
# MAGIC     - Shell scripts that run on EVERY cluster start.
# MAGIC     - For system dependencies, apt-get, custom setup.
# MAGIC     - Location: /Volumes/catalog/schema/vol/init.sh or DBFS.
# MAGIC
# MAGIC Pre-installed Libraries (Databricks Runtime 17.3):
# MAGIC   Already available (no installation needed):
# MAGIC     pandas, numpy, scipy, scikit-learn, matplotlib,
# MAGIC     seaborn, plotly, requests, beautifulsoup4,
# MAGIC     mlflow, delta-spark, pyarrow, xgboost,
# MAGIC     transformers, torch (on ML Runtime)
# MAGIC
# MAGIC Best Practices:
# MAGIC   1. Use %pip for one-off packages in notebooks.
# MAGIC   2. Pin versions: %pip install pandas==2.1.0 (reproducibility).
# MAGIC   3. Use requirements.txt for team consistency.
# MAGIC   4. Never %pip install packages already in the runtime (conflicts!).
# MAGIC   5. Check what's installed: %pip list | grep package_name
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: Beginner Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

import sys  # System module for Python version info.
import importlib  # For dynamic module importing and version checks.

print("="*70)
print("SECTION 3 — BEGINNER: Checking & Using Pre-installed Libraries")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Check what's already installed
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: What Python packages are already available?")
print("-"*60)

print(f"\nPython version: {sys.version}")  # Runtime Python version.

# Check if popular packages are installed.
packages_to_check = [
    'pandas', 'numpy', 'sklearn', 'matplotlib',
    'mlflow', 'xgboost', 'requests', 'plotly'
]

print("\nPre-installed packages (no %pip needed):")
for pkg in packages_to_check:  # Loop through common packages.
    try:
        mod = importlib.import_module(pkg)  # Try to import.
        version = getattr(mod, '__version__', 'installed')  # Get version.
        print(f"  ✓ {pkg:15s} = {version}")  # Package found.
    except ImportError:
        print(f"  ✗ {pkg:15s} = NOT installed")  # Package missing.

print("\n✓ Databricks Runtime includes 100s of packages out of the box.")
print("  No need to install pandas, numpy, sklearn, mlflow, etc.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Using common libraries together
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Using popular pre-installed libraries")
print("-"*60)

import numpy as np  # NumPy: array operations.
import pandas as pd  # Pandas: data manipulation.
import matplotlib.pyplot as plt  # Matplotlib: plotting.
from sklearn.linear_model import LinearRegression  # scikit-learn.
import mlflow  # MLflow: experiment tracking.

print(f"\n✓ numpy {np.__version__}: Array math, linear algebra")
print(f"✓ pandas {pd.__version__}: DataFrames, CSV, data cleaning")
print(f"✓ matplotlib: Inline charts in notebooks")
print(f"✓ scikit-learn: ML models on small (collected) data")
print(f"✓ mlflow {mlflow.__version__}: Track experiments, log models")

# Quick demo: Spark for big data, pandas for small result.
spark_df = spark.range(1000000).selectExpr("id % 10 as group", "rand() * 100 as value")  # 1M rows.
summary_pd = spark_df.groupBy("group").avg("value").toPandas()  # Collect small result.
print(f"\nSpark processed 1M rows → collected {len(summary_pd)} row summary to pandas.")
print(summary_pd.head())  # Print pandas DataFrame.

print("\n✓ Pattern: Spark (big data) → .toPandas() (small summary) → plot/model.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: %pip install syntax reference
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: %pip install — all patterns")
print("-"*60)

# NOTE: %pip commands must be in their OWN cell (shown here as print for reference).
print("""
%pip commands (each goes in its OWN cell!):

  %pip install faker                  # Install latest from PyPI.
  %pip install faker==22.0.0          # Pin exact version (production!).
  %pip install faker tqdm rich        # Multiple packages at once.
  %pip install -r /Workspace/Users/you/requirements.txt  # From file.
  %pip install /Volumes/cat/schema/vol/custom-1.0.whl    # From Volume.
  %pip install --upgrade pandas       # Upgrade existing package.
  %pip install mlflow[gateway]        # Install with extras.
  %pip uninstall faker -y             # Uninstall (use -y to skip prompt).
  %pip show pandas                    # Show package info + version.
  %pip list                           # List ALL installed packages.

Rules:
  1. %pip MUST be the FIRST line in its cell.
  2. No other code in the same cell (not even comments before it).
  3. Python process restarts after %pip → variables are lost.
  4. Re-run earlier cells after %pip to restore variables.
  5. Installs on ALL nodes (driver + workers) automatically.
""")

# COMMAND ----------

# DBTITLE 1,Sections 4-5: Intermediate and Advanced
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4-5 — INTERMEDIATE & ADVANCED
# ═══════════════════════════════════════════════════════════════════

import importlib  # For version checking.
import subprocess  # For running pip commands programmatically.

print("="*70)
print("SECTIONS 4-5: Intermediate & Advanced Library Management")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: Using libraries inside UDFs on workers
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: Using packages in UDFs (distributed on workers)")
print("-"*60)

from pyspark.sql.functions import pandas_udf, col  # Spark imports.
import pandas as pd  # Pandas for type hints.

# Libraries installed via %pip are available on ALL workers.
# This UDF uses numpy (pre-installed) on every worker node.
@pandas_udf("double")
def advanced_calc(values: pd.Series) -> pd.Series:
    """Uses numpy on workers — works because numpy is pre-installed."""
    import numpy as np  # Import inside UDF (available on all workers).
    return pd.Series(np.log1p(values))  # log(1 + x), handles 0 safely.

# Apply to Spark DataFrame (executes on workers, not driver).
df = spark.range(10).select((col("id") * 10.0).alias("value"))  # Test data.
result = df.withColumn("log_value", advanced_calc(col("value")))  # UDF on workers.
print("\nUDF using numpy on workers:")
display(result)  # display() for output.

print("\n✓ %pip installed packages are on ALL nodes (driver + workers).")
print("  Import inside the UDF function body for clarity.")
print("  Pre-installed packages (numpy, pandas, scipy) always available.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: Programmatic package inspection
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: Check package versions programmatically")
print("-"*60)

import pkg_resources  # Package metadata.

# Get version info for specific packages.
packages_of_interest = ['pandas', 'numpy', 'pyarrow', 'mlflow', 'delta-spark']

print("\nInstalled versions (from pkg_resources):")
for pkg_name in packages_of_interest:  # Check each.
    try:
        version = pkg_resources.get_distribution(pkg_name).version  # Get version.
        print(f"  {pkg_name:15s} = {version}")
    except pkg_resources.DistributionNotFound:
        print(f"  {pkg_name:15s} = NOT FOUND")

# Check total number of installed packages.
all_packages = [d for d in pkg_resources.working_set]  # All installed.
print(f"\n  Total installed packages: {len(all_packages)}")
print(f"  (Databricks Runtime comes loaded!)")

print("\n✓ Use pkg_resources for programmatic version checks.")
print("  Use %pip show <package> in a cell for interactive inspection.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Requirements.txt for reproducibility
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: requirements.txt for team reproducibility")
print("-"*60)

# Example requirements.txt content.
requirements_content = """# requirements.txt for our project
# Pin exact versions for reproducibility!
faker==22.0.0
tqdm==4.66.1
rich==13.7.0
great-expectations==0.18.8
"""

print("\nSample requirements.txt:")
print(requirements_content)

print("Usage:")
print("  1. Store in workspace: /Workspace/Users/you/project/requirements.txt")
print("  2. Or in a UC Volume: /Volumes/catalog/schema/vol/requirements.txt")
print("  3. Install: %pip install -r /Workspace/Users/you/project/requirements.txt")
print("  4. Generate from current env: %pip freeze > requirements.txt")

print("\n✓ Pin versions to avoid 'it worked yesterday' surprises.")
print("  Share requirements.txt with your team for identical environments.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 7: Init Scripts (cluster-level setup)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 7: Init Scripts (advanced cluster setup)")
print("-"*60)

# Example init script content.
init_script_content = """#!/bin/bash
# This script runs on EVERY cluster start (all nodes).

# Install system-level dependencies (apt-get).
apt-get update && apt-get install -y libgdal-dev

# Install Python packages that need system deps.
pip install GDAL==3.6.2

# Set environment variables.
export MY_API_KEY="abc123"
export ENVIRONMENT="production"

echo "Init script completed successfully!"
"""

print("\nSample init script (init_setup.sh):")
print(init_script_content)

print("Setup:")
print("  1. Save script to UC Volume: /Volumes/catalog/schema/vol/init_setup.sh")
print("  2. Cluster config → Advanced → Init Scripts → Add path.")
print("  3. Script runs automatically on every cluster start/restart.")
print("\n  Use for: system libs (apt-get), ODBC drivers, env vars, certificates.")
print("  NOT for: Python packages (use %pip or cluster libraries instead).")

# COMMAND ----------

# DBTITLE 1,Section 6: Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Code in the same cell as %pip
# MAGIC ```python
# MAGIC # BAD: %pip shares a cell with other code.
# MAGIC import pandas as pd  # This line makes %pip fail!
# MAGIC %pip install faker
# MAGIC
# MAGIC # GOOD: %pip must be ALONE in its cell.
# MAGIC # Cell 1:
# MAGIC %pip install faker
# MAGIC
# MAGIC # Cell 2 (separate):
# MAGIC import faker
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Using !pip instead of %pip
# MAGIC ```python
# MAGIC # BAD: !pip only installs on the driver node.
# MAGIC !pip install geopy  # Workers DON'T get it! UDFs will fail.
# MAGIC
# MAGIC # GOOD: %pip installs on ALL nodes (driver + workers).
# MAGIC %pip install geopy  # Available everywhere.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Not re-running cells after %pip
# MAGIC ```python
# MAGIC # After %pip, Python restarts. All variables are GONE!
# MAGIC # Cell 1: df = spark.range(100)  # ← This variable is lost after %pip.
# MAGIC # Cell 2: %pip install faker     # ← Python restarts here.
# MAGIC # Cell 3: display(df)            # ← ERROR! df no longer exists.
# MAGIC
# MAGIC # GOOD: Re-run Cell 1 after %pip completes, then run Cell 3.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Overriding pre-installed package versions
# MAGIC ```python
# MAGIC # BAD: Installing a different version of a core package.
# MAGIC %pip install pandas==1.5.0  # Conflicts with runtime's pandas 2.x!
# MAGIC # Can break Spark's internal pandas integration.
# MAGIC
# MAGIC # GOOD: Check current version first.
# MAGIC %pip show pandas  # See what version is installed.
# MAGIC # Only override if you truly need it and test thoroughly.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not pinning versions in production
# MAGIC ```python
# MAGIC # BAD: No version pin = different results each week.
# MAGIC %pip install great-expectations  # Could be 0.17 today, 0.18 tomorrow.
# MAGIC
# MAGIC # GOOD: Always pin for production notebooks.
# MAGIC %pip install great-expectations==0.18.8  # Reproducible!
# MAGIC # Or use: %pip install -r requirements.txt (with pinned versions).
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7: Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

import importlib  # For package checks.
import pkg_resources  # For version info.
import sys  # System info.

print("="*70)
print("HOMEWORK — Python Libraries in Databricks")
print("="*70)

# Level 1: Check Python version.
print("\n--- Level 1: Python version ---")
print(f"Python: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
# WHY: Know your Python version for compatibility.

# Level 2: Check if a package is installed.
print("\n--- Level 2: Check package existence ---")
try:
    import numpy as np  # Try import.
    print(f"✓ numpy installed: {np.__version__}")
except ImportError:
    print("✗ numpy NOT installed")
# WHY: Check before coding to avoid runtime ImportError.

# Level 3: Get version of any package.
print("\n--- Level 3: Get version programmatically ---")
version = pkg_resources.get_distribution("pandas").version  # Get version.
print(f"pandas version: {version}")
# WHY: Verify version matches your requirements.

# Level 4: Count installed packages.
print("\n--- Level 4: Count all packages ---")
all_pkgs = list(pkg_resources.working_set)  # Get all.
print(f"Total packages in this runtime: {len(all_pkgs)}")
print(f"First 5: {[p.project_name for p in sorted(all_pkgs)[:5]]}")
# WHY: Understand what's already available before installing.

# Level 5: Import and use a library in one cell.
print("\n--- Level 5: Quick library usage ---")
import json  # Standard library, always available.
data = {"name": "Databricks", "version": 17.3}  # Python dict.
print(f"JSON: {json.dumps(data, indent=2)}")  # Convert to JSON string.
# WHY: Many standard library modules are useful without %pip.

# Level 6: Conditional import pattern.
print("\n--- Level 6: Conditional import (safe fallback) ---")
try:
    from tqdm import tqdm  # Try importing tqdm.
    print("✓ tqdm available — can use progress bars.")
except ImportError:
    print("✗ tqdm not installed — run: %pip install tqdm")
    tqdm = None  # Set to None for conditional use later.
# WHY: Graceful degradation when package might not be installed.

# Levels 7-10: Conceptual.
print("\n--- Level 7: %pip vs Cluster Libraries vs Init Scripts ---")
print("  %pip: Notebook-scoped, session only, quick install.")
print("  Cluster libs: All notebooks on cluster, UI-configured.")
print("  Init scripts: System deps, runs on every start.")

print("\n--- Level 8: Production best practices ---")
print("  1. Pin ALL versions in requirements.txt.")
print("  2. Test on same DBR version as production.")
print("  3. Use cluster libraries for team-shared packages.")
print("  4. Use %pip for notebook-specific experiments.")

print("\n--- Level 9: Troubleshooting ---")
print("  'ModuleNotFoundError' → Package not installed. Use %pip install.")
print("  'ImportError' after %pip → Re-run import cell (Python restarted).")
print("  'Version conflict' → Use %pip show pkg to check current version.")
print("  UDF fails on workers → Used !pip instead of %pip (use %pip!).")

print("\n--- Level 10: Teach library management ---")
print("""
"Python libraries in Databricks:
  Pre-installed: pandas, numpy, sklearn, mlflow, matplotlib, etc.
  Install more: %pip install package==version (in own cell).
  %pip installs on ALL nodes (driver + workers).
  Python restarts after %pip → re-run earlier cells.
  Never use !pip (driver-only). Always use %pip.
  For production: pin versions, use requirements.txt.
  For system deps: use init scripts (bash, apt-get)."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 93")
print("✓ MODULE 15 COMPLETE!")
print("="*70)