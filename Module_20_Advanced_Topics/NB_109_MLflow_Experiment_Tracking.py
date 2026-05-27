# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 109: MLflow Experiment Tracking & Model Registry
# MAGIC ## Module 20: Advanced Topics
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **MLflow** is the open-source platform (built into Databricks) for managing the ML lifecycle: tracking experiments, packaging models, and deploying them. It answers: "Which hyperparameters produced the best model?" and "Which model version is currently serving in production?"
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC A **scientist's lab notebook** that automatically records every experiment: what recipe was used (parameters), what happened (metrics), and the final product (model artifact). Plus a **museum catalog** (model registry) that tracks which version of each artwork is on display (production).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC MLflow Components:
# MAGIC
# MAGIC   1. TRACKING: Log params, metrics, artifacts for each experiment run.
# MAGIC   2. MODELS: Package models with dependencies and signature.
# MAGIC   3. REGISTRY: Version models, stage them (Staging → Production).
# MAGIC   4. SERVING: Deploy models as REST endpoints.
# MAGIC
# MAGIC Code Pattern:
# MAGIC   import mlflow
# MAGIC
# MAGIC   mlflow.set_experiment("/Users/me/my_experiment")
# MAGIC
# MAGIC   with mlflow.start_run(run_name="rf_v1"):
# MAGIC       # Log parameters.
# MAGIC       mlflow.log_param("n_estimators", 100)
# MAGIC       mlflow.log_param("max_depth", 5)
# MAGIC       # Train model.
# MAGIC       model = train(params)
# MAGIC       # Log metrics.
# MAGIC       mlflow.log_metric("auc", 0.92)
# MAGIC       mlflow.log_metric("f1", 0.87)
# MAGIC       # Log model to UC.
# MAGIC       mlflow.sklearn.log_model(model, "model",
# MAGIC           registered_model_name="catalog.schema.my_model")
# MAGIC
# MAGIC Unity Catalog Model Registry:
# MAGIC   catalog.schema.model_name
# MAGIC     Version 1: AUC=0.85 (archived)
# MAGIC     Version 2: AUC=0.92 (champion ← alias)
# MAGIC     Version 3: AUC=0.90 (challenger)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: MLflow Examples
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7 — MLFLOW EXPERIMENT TRACKING
# ═══════════════════════════════════════════════════════════════════

import mlflow  # MLflow is pre-installed in Databricks.
from sklearn.ensemble import RandomForestClassifier  # sklearn model.
from sklearn.datasets import make_classification  # Synthetic data.
from sklearn.model_selection import train_test_split  # Split.
from sklearn.metrics import accuracy_score, f1_score  # Metrics.

print("="*70)
print("SECTIONS 3-7: MLflow Experiment Tracking")
print("="*70)

# ─── EXAMPLE 1: Basic experiment tracking ───
print("\n" + "-"*60)
print("EXAMPLE 1: Track an ML experiment")
print("-"*60)

# Create synthetic classification data.
X, y = make_classification(n_samples=1000, n_features=10, random_state=42)  # Synthetic.
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Set experiment (auto-creates if doesn't exist).
mlflow.set_experiment("/Users/sin1hyd@bosch.com/experiments/KT_demo")  # Experiment path.

# Run 1: Train with specific hyperparameters.
with mlflow.start_run(run_name="rf_100_trees"):  # Start tracking.
    # Log parameters.
    n_est, max_d = 100, 5  # Hyperparameters.
    mlflow.log_param("n_estimators", n_est)  # Log param.
    mlflow.log_param("max_depth", max_d)     # Log param.
    mlflow.log_param("model_type", "RandomForest")  # Log param.
    
    # Train model.
    model = RandomForestClassifier(n_estimators=n_est, max_depth=max_d, random_state=42)
    model.fit(X_train, y_train)  # Train.
    
    # Evaluate.
    y_pred = model.predict(X_test)  # Predict.
    acc = accuracy_score(y_test, y_pred)  # Accuracy.
    f1 = f1_score(y_test, y_pred)  # F1.
    
    # Log metrics.
    mlflow.log_metric("accuracy", acc)  # Log metric.
    mlflow.log_metric("f1_score", f1)   # Log metric.
    
    # Log model.
    mlflow.sklearn.log_model(model, "model")  # Save model artifact.
    
    print(f"  Run logged: accuracy={acc:.4f}, f1={f1:.4f}")
    print(f"  Run ID: {mlflow.active_run().info.run_id}")

print("\n✓ Check the Experiments UI (left sidebar) to see logged runs.")
print("  Each run shows: parameters, metrics, model artifact, duration.")

# ─── EXAMPLE 2: Compare multiple runs ───
print("\n" + "-"*60)
print("EXAMPLE 2: Hyperparameter sweep (multiple runs)")
print("-"*60)

# Try different hyperparameters.
configs = [
    {"n_estimators": 50, "max_depth": 3},
    {"n_estimators": 100, "max_depth": 5},
    {"n_estimators": 200, "max_depth": 10},
]

best_f1, best_run_id = 0, None  # Track best.

for config in configs:  # Try each config.
    with mlflow.start_run(run_name=f"rf_{config['n_estimators']}_{config['max_depth']}"):
        mlflow.log_params(config)  # Log all params at once.
        
        clf = RandomForestClassifier(**config, random_state=42)  # Create model.
        clf.fit(X_train, y_train)  # Train.
        preds = clf.predict(X_test)  # Predict.
        
        acc = accuracy_score(y_test, preds)  # Evaluate.
        f1 = f1_score(y_test, preds)
        mlflow.log_metrics({"accuracy": acc, "f1_score": f1})  # Log metrics.
        mlflow.sklearn.log_model(clf, "model")  # Log model.
        
        if f1 > best_f1:  # Track best.
            best_f1 = f1
            best_run_id = mlflow.active_run().info.run_id
        
        print(f"  Config {config}: F1={f1:.4f}")

print(f"\n  Best run: {best_run_id} (F1={best_f1:.4f})")
print("  → Compare all runs in the Experiments UI (table + charts).")

# ─── EXAMPLE 3: Register model to Unity Catalog ───
print("\n" + "-"*60)
print("EXAMPLE 3: Model Registry (Unity Catalog)")
print("-"*60)

print("""
Register best model to UC Model Registry:

  # Register during logging.
  mlflow.sklearn.log_model(
      model, "model",
      registered_model_name="catalog.ml_schema.churn_predictor"  # UC path.
  )

  # Or register after the fact.
  mlflow.register_model(
      f"runs:/{best_run_id}/model",
      "catalog.ml_schema.churn_predictor"
  )

  # Set alias (replaces stages in UC).
  from mlflow import MlflowClient
  client = MlflowClient()
  client.set_registered_model_alias(
      "catalog.ml_schema.churn_predictor",
      alias="champion",       # Production model.
      version=2               # Version number.
  )

  # Load model by alias for inference.
  model = mlflow.sklearn.load_model("models:/catalog.ml_schema.churn_predictor@champion")
  predictions = model.predict(new_data)
""")

# ─── HOMEWORK ───
print("\n" + "="*70)
print("HOMEWORK — MLflow")
print("="*70)
print("  Level 1-3: start_run, log_param, log_metric, log_model.")
print("  Level 4-6: Hyperparameter sweep, compare runs in UI.")
print("  Level 7-10: Register to UC, aliases, model serving endpoints.")
print("\n" + "="*70)
print("✓ HOMEWORK COMPLETED — Notebook 109")
print("="*70)