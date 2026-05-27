# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 90: ML Pipelines, Cross-Validation & Hyperparameter Tuning
# MAGIC ## Module 14: Machine Learning
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **ML Pipelines** chain preprocessing + model training into a single reusable object. **Cross-validation** evaluates models robustly by training on multiple data folds. **Hyperparameter tuning** automatically finds the best model settings.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC - **Pipeline** = A car assembly line: raw metal goes in, finished car comes out. Same line works for every car.
# MAGIC - **Cross-validation** = Testing a recipe 5 times with different taste-testers. More reliable than asking one person.
# MAGIC - **Hyperparameter tuning** = Trying 100 recipe variations and picking the one that got the best average rating.
# MAGIC
# MAGIC ### Key Components:
# MAGIC | Component | Purpose |
# MAGIC |-----------|--------|
# MAGIC | `Pipeline` | Chain transformers + estimator into one workflow |
# MAGIC | `CrossValidator` | K-fold cross-validation (train K times, average metrics) |
# MAGIC | `TrainValidationSplit` | Single train/validation split (faster than CV) |
# MAGIC | `ParamGridBuilder` | Define hyperparameter search space |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Cross-Validation (K=5 Folds):
# MAGIC
# MAGIC   Data split into 5 equal folds:
# MAGIC   [Fold1] [Fold2] [Fold3] [Fold4] [Fold5]
# MAGIC
# MAGIC   Iteration 1: Train on [2,3,4,5], Validate on [1] → Score 0.82
# MAGIC   Iteration 2: Train on [1,3,4,5], Validate on [2] → Score 0.85
# MAGIC   Iteration 3: Train on [1,2,4,5], Validate on [3] → Score 0.79
# MAGIC   Iteration 4: Train on [1,2,3,5], Validate on [4] → Score 0.83
# MAGIC   Iteration 5: Train on [1,2,3,4], Validate on [5] → Score 0.81
# MAGIC   
# MAGIC   Final score = average = 0.82 (much more reliable than single split!)
# MAGIC
# MAGIC Hyperparameter Tuning:
# MAGIC
# MAGIC   ParamGrid = {
# MAGIC     maxDepth: [3, 5, 10],
# MAGIC     numTrees: [50, 100, 200]
# MAGIC   }
# MAGIC   Total combinations: 3 × 3 = 9
# MAGIC   With 5-fold CV: 9 × 5 = 45 model trainings!
# MAGIC
# MAGIC   CrossValidator tries ALL combinations and picks the best.
# MAGIC
# MAGIC Code Pattern:
# MAGIC
# MAGIC   # 1. Build pipeline.
# MAGIC   pipeline = Pipeline(stages=[assembler, scaler, classifier])
# MAGIC
# MAGIC   # 2. Define param grid.
# MAGIC   paramGrid = ParamGridBuilder() \\
# MAGIC       .addGrid(classifier.maxDepth, [3, 5, 10]) \\
# MAGIC       .addGrid(classifier.numTrees, [50, 100]) \\
# MAGIC       .build()
# MAGIC
# MAGIC   # 3. Cross-validate.
# MAGIC   cv = CrossValidator(
# MAGIC       estimator=pipeline,
# MAGIC       estimatorParamMaps=paramGrid,
# MAGIC       evaluator=BinaryClassificationEvaluator(),
# MAGIC       numFolds=5
# MAGIC   )
# MAGIC
# MAGIC   # 4. Fit (runs all combinations × all folds).
# MAGIC   cv_model = cv.fit(train_df)  # Returns best model automatically.
# MAGIC
# MAGIC   # 5. Use best model.
# MAGIC   predictions = cv_model.transform(test_df)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: Pipeline and Tuning Examples
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7: PIPELINES & HYPERPARAMETER TUNING
# ═══════════════════════════════════════════════════════════════════

from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler  # Features.
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier  # Models.
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator  # Eval.
from pyspark.ml import Pipeline  # Pipeline.
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, TrainValidationSplit  # Tuning.
from pyspark.sql.functions import col, rand, when, lit  # Functions.

print("="*70)
print("SECTIONS 3-5: ML Pipelines & Hyperparameter Tuning")
print("="*70)

# ─── Create dataset ───
df = spark.range(2000).select(
    (rand() * 50 + 20).alias("age"),
    (rand() * 100000 + 20000).alias("income"),
    (rand() * 5).alias("score"),
    when(col("id") % 3 == 0, "A").when(col("id") % 3 == 1, "B").otherwise("C").alias("category"),
    when(rand() > 0.5, 1.0).otherwise(0.0).alias("label")
)

# ─── EXAMPLE 1: Complete ML Pipeline ───
print("\n" + "-"*60)
print("EXAMPLE 1: End-to-End ML Pipeline")
print("-"*60)

# Define pipeline stages.
indexer = StringIndexer(inputCol="category", outputCol="cat_idx")
encoder = OneHotEncoder(inputCol="cat_idx", outputCol="cat_vec")
assembler = VectorAssembler(inputCols=["age", "income", "score", "cat_vec"], outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50, seed=42)

# Chain into pipeline.
pipeline = Pipeline(stages=[indexer, encoder, assembler, scaler, rf])

# Split.
train, test = df.randomSplit([0.7, 0.3], seed=42)
print(f"Train: {train.count()} | Test: {test.count()}")

# Fit pipeline (fits ALL stages: indexer, encoder, assembler, scaler, model).
pipeline_model = pipeline.fit(train)

# Predict.
predictions = pipeline_model.transform(test)

# Evaluate.
evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)
print(f"\nPipeline AUC: {auc:.4f}")
print("✓ One .fit() trains the entire pipeline (preprocessing + model).")
print("  One .transform() applies everything to new data.")

# ─── EXAMPLE 2: CrossValidator with ParamGrid ───
print("\n" + "-"*60)
print("EXAMPLE 2: Cross-Validation with Hyperparameter Grid")
print("-"*60)

# Define a smaller pipeline for tuning (faster demo).
small_assembler = VectorAssembler(inputCols=["age", "income", "score"], outputCol="features")
rf_tune = RandomForestClassifier(featuresCol="features", labelCol="label", seed=42)
small_pipeline = Pipeline(stages=[small_assembler, rf_tune])

# Build parameter grid.
paramGrid = (
    ParamGridBuilder()
    .addGrid(rf_tune.numTrees, [20, 50, 100])  # Try 3 values.
    .addGrid(rf_tune.maxDepth, [3, 5, 7])       # Try 3 values.
    .build()  # Total: 3×3 = 9 combinations.
)

print(f"Parameter combinations to try: {len(paramGrid)}")

# Cross-validator (3-fold for speed; use 5 in production).
cv = CrossValidator(
    estimator=small_pipeline,          # Pipeline to tune.
    estimatorParamMaps=paramGrid,       # Parameter combinations.
    evaluator=evaluator,                # How to score.
    numFolds=3,                         # 3-fold CV.
    parallelism=4,                      # Parallel model training!
    seed=42
)

print("Running cross-validation (9 combos × 3 folds = 27 trainings)...")
cv_model = cv.fit(train)  # Finds best model automatically.

# Best model results.
best_auc = evaluator.evaluate(cv_model.transform(test))
print(f"\nBest model AUC on test: {best_auc:.4f}")

# Get best parameters.
best_rf = cv_model.bestModel.stages[-1]  # Last stage = RandomForest.
print(f"Best numTrees: {best_rf.getNumTrees}")
print(f"Best maxDepth: {best_rf.getOrDefault('maxDepth')}")

# CV scores for all combinations.
print(f"\nAll CV scores (avg across folds):")
for params, score in zip(paramGrid, cv_model.avgMetrics):
    print(f"  {params} → AUC={score:.4f}")

# ─── EXAMPLE 3: TrainValidationSplit (faster alternative) ───
print("\n" + "-"*60)
print("EXAMPLE 3: TrainValidationSplit (70/30, no folds)")
print("-"*60)

tvs = TrainValidationSplit(
    estimator=small_pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    trainRatio=0.8,  # 80% train, 20% validation.
    seed=42
)

tvs_model = tvs.fit(train)  # Only 9 trainings (no folds!).
tvs_auc = evaluator.evaluate(tvs_model.transform(test))
print(f"TrainValidationSplit best AUC: {tvs_auc:.4f}")
print("✓ Faster than CV (no folds), but less robust.")
print("  Use for quick exploration. Use CV for final model selection.")

# ─── EXAMPLE 4: Save and Load Pipeline ───
print("\n" + "-"*60)
print("EXAMPLE 4: Save/Load pipeline for production")
print("-"*60)

# Save.
model_path = "/tmp/delta_kt/ml_pipeline_model"
pipeline_model.save(model_path)
print(f"Pipeline saved to: {model_path}")

# Load.
from pyspark.ml import PipelineModel
loaded_model = PipelineModel.load(model_path)

# Predict with loaded model.
loaded_preds = loaded_model.transform(test)
loaded_auc = evaluator.evaluate(loaded_preds)
print(f"Loaded model AUC: {loaded_auc:.4f} (same as before)")
print("✓ Saved pipeline includes ALL stages (preprocessing + model).")
print("  Deploy by loading and calling .transform(new_data).")

# ─── SECTION 6 & 7 ───
print("\n" + "="*70)
print("SECTION 6 — COMMON MISTAKES")
print("="*70)
print("""
1. Tuning on test data (data leakage!). CV only touches train set.
2. Too many parameters in grid (exponential explosion: 5×5×5 = 125 combos!).
3. numFolds=10 on large data = very slow. Use 3-5 for big datasets.
4. Not using parallelism in CrossValidator (default is serial = slow).
5. Not saving the best model after tuning (losing hours of computation).
""")

print("="*70)
print("SECTION 7 — HOMEWORK")
print("="*70)
print("""
Level 1: Build a Pipeline with 3 stages.
Level 2: Split data 70/30 and train.
Level 3: ParamGrid with 2 hyperparameters.
Level 4: CrossValidator with 3 folds.
Level 5: Extract best parameters from cv_model.
Level 6: Compare CrossValidator vs TrainValidationSplit.
Level 7: Save and load a pipeline.
Level 8: Use parallelism=4 in CrossValidator (4x faster).
Level 9: MLflow logging (track experiments automatically).
  import mlflow
  with mlflow.start_run():
      cv_model = cv.fit(train)
      mlflow.log_metric("auc", best_auc)
      mlflow.spark.log_model(cv_model.bestModel, "model")

Level 10: Teach ML pipelines:
  "Pipeline: chain preprocessing + model into one .fit()/.transform().
   CrossValidator: K-fold CV + param grid = finds best model.
   ParamGridBuilder: define search space for hyperparameters.
   Save pipeline = deploy to production with one .transform() call.
   Always: tune on train only, evaluate on held-out test."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 90")
print("✓ MODULE 14 (MACHINE LEARNING) COMPLETE! All 5 notebooks (86-90) done.")
print("="*70)