# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 88: Classification & Regression with PySpark MLlib
# MAGIC ## Module 14: Machine Learning
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 55 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Classification** predicts a CATEGORY (spam/not-spam, churn/stay, pass/fail).
# MAGIC **Regression** predicts a NUMBER (price, temperature, sales amount).
# MAGIC
# MAGIC PySpark MLlib provides distributed implementations of all major algorithms that work on billions of rows.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC - **Classification** = A doctor diagnosing: "Patient has Disease A or Disease B" (categories)
# MAGIC - **Regression** = An appraiser estimating: "This house is worth $425,000" (continuous value)
# MAGIC
# MAGIC ### MLlib Algorithms:
# MAGIC | Algorithm | Type | Use Case |
# MAGIC |-----------|------|----------|
# MAGIC | LogisticRegression | Classification | Binary/multiclass, interpretable |
# MAGIC | DecisionTreeClassifier | Classification | Non-linear, interpretable |
# MAGIC | RandomForestClassifier | Classification | Ensemble, robust, handles noise |
# MAGIC | GBTClassifier | Classification | Best accuracy for tabular data |
# MAGIC | LinearRegression | Regression | Simple, fast, interpretable |
# MAGIC | DecisionTreeRegressor | Regression | Non-linear relationships |
# MAGIC | RandomForestRegressor | Regression | Robust, handles noise |
# MAGIC | GBTRegressor | Regression | Best accuracy for regression |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC ML Workflow:
# MAGIC
# MAGIC   [Raw Data] → [Feature Engineering] → [Train/Test Split] → [Train Model] → [Evaluate]
# MAGIC                 (NB 86)                  70% / 30%          .fit(train)     metrics
# MAGIC
# MAGIC   Classification metrics:
# MAGIC     Accuracy:  correct / total
# MAGIC     Precision: true positives / (true pos + false pos)
# MAGIC     Recall:    true positives / (true pos + false neg)
# MAGIC     F1:        harmonic mean of precision and recall
# MAGIC     AUC-ROC:   area under the ROC curve (0.5=random, 1.0=perfect)
# MAGIC
# MAGIC   Regression metrics:
# MAGIC     RMSE:  Root Mean Squared Error (lower = better)
# MAGIC     MAE:   Mean Absolute Error (lower = better)
# MAGIC     R²:    Coefficient of Determination (1.0=perfect, 0=baseline)
# MAGIC
# MAGIC   Code Pattern (same for ALL models):
# MAGIC     # 1. Prepare features.
# MAGIC     assembler = VectorAssembler(inputCols=[...], outputCol="features")
# MAGIC     df_ready = assembler.transform(df)
# MAGIC
# MAGIC     # 2. Split.
# MAGIC     train, test = df_ready.randomSplit([0.7, 0.3], seed=42)
# MAGIC
# MAGIC     # 3. Train.
# MAGIC     model = LogisticRegression(featuresCol="features", labelCol="label").fit(train)
# MAGIC
# MAGIC     # 4. Predict.
# MAGIC     predictions = model.transform(test)
# MAGIC
# MAGIC     # 5. Evaluate.
# MAGIC     evaluator = BinaryClassificationEvaluator(labelCol="label")
# MAGIC     auc = evaluator.evaluate(predictions)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: Classification and Regression Examples
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7: CLASSIFICATION & REGRESSION
# ═══════════════════════════════════════════════════════════════════

from pyspark.ml.feature import VectorAssembler  # Feature assembler.
from pyspark.ml.classification import (
    LogisticRegression, DecisionTreeClassifier,
    RandomForestClassifier, GBTClassifier
)  # Classification models.
from pyspark.ml.regression import (
    LinearRegression, RandomForestRegressor, GBTRegressor
)  # Regression models.
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    RegressionEvaluator
)  # Evaluators.
from pyspark.sql.functions import col, rand, expr, when  # Functions.

print("="*70)
print("SECTIONS 3-5: Classification & Regression")
print("="*70)

# ─── Create sample datasets ───
# Classification dataset (binary: label 0 or 1).
clf_data = spark.range(1000).select(
    col("id"),
    (rand() * 50 + 20).alias("age"),
    (rand() * 100000).alias("income"),
    (rand() * 5).alias("score"),
    when(rand() > 0.5, 1.0).otherwise(0.0).alias("label")  # Binary label.
)

# Regression dataset (label is continuous).
reg_data = spark.range(1000).select(
    col("id"),
    (rand() * 2000 + 500).alias("sqft"),
    (rand() * 5 + 1).alias("bedrooms"),
    (rand() * 50 + 1970).cast("int").alias("year_built"),
    (rand() * 500000 + 100000).alias("price")  # Continuous label.
)

# ─── EXAMPLE 1: Logistic Regression (Classification) ───
print("\n" + "-"*60)
print("EXAMPLE 1: Logistic Regression (Binary Classification)")
print("-"*60)

# Prepare features.
assembler = VectorAssembler(inputCols=["age", "income", "score"], outputCol="features")
clf_ready = assembler.transform(clf_data).select("features", "label")

# Split.
train_clf, test_clf = clf_ready.randomSplit([0.7, 0.3], seed=42)
print(f"Train: {train_clf.count()} rows | Test: {test_clf.count()} rows")

# Train.
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
lr_model = lr.fit(train_clf)

# Predict.
predictions = lr_model.transform(test_clf)
print("\nPredictions sample:")
predictions.select("features", "label", "prediction", "probability").show(5, truncate=40)

# Evaluate.
evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)
print(f"AUC-ROC: {auc:.4f} (0.5=random, 1.0=perfect)")

# Multi-class evaluator for accuracy.
acc_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
accuracy = acc_eval.evaluate(predictions)
print(f"Accuracy: {accuracy:.4f}")

# ─── EXAMPLE 2: Random Forest Classifier ───
print("\n" + "-"*60)
print("EXAMPLE 2: Random Forest Classifier")
print("-"*60)

rf = RandomForestClassifier(
    featuresCol="features", labelCol="label",
    numTrees=50, maxDepth=5, seed=42
)
rf_model = rf.fit(train_clf)
rf_preds = rf_model.transform(test_clf)

rf_auc = evaluator.evaluate(rf_preds)
print(f"Random Forest AUC: {rf_auc:.4f}")
print(f"Feature importances: {rf_model.featureImportances}")
print("  (Shows which features matter most for predictions.)")

# ─── EXAMPLE 3: Linear Regression ───
print("\n" + "-"*60)
print("EXAMPLE 3: Linear Regression (Predict house price)")
print("-"*60)

assembler_reg = VectorAssembler(inputCols=["sqft", "bedrooms", "year_built"], outputCol="features")
reg_ready = assembler_reg.transform(reg_data).select("features", col("price").alias("label"))
train_reg, test_reg = reg_ready.randomSplit([0.7, 0.3], seed=42)

lr_reg = LinearRegression(featuresCol="features", labelCol="label", maxIter=10)
lr_reg_model = lr_reg.fit(train_reg)
reg_preds = lr_reg_model.transform(test_reg)

print("Predictions:")
reg_preds.select("features", "label", "prediction").show(5, truncate=40)

# Evaluate regression.
reg_evaluator = RegressionEvaluator(labelCol="label")
rmse = reg_evaluator.setMetricName("rmse").evaluate(reg_preds)
r2 = reg_evaluator.setMetricName("r2").evaluate(reg_preds)
print(f"RMSE: {rmse:.2f} (lower=better)")
print(f"R²:   {r2:.4f} (1.0=perfect fit)")
print(f"\nCoefficients: {lr_reg_model.coefficients}")
print(f"Intercept: {lr_reg_model.intercept:.2f}")

# ─── SECTION 6 & 7 ───
print("\n" + "="*70)
print("SECTION 6 — COMMON MISTAKES")
print("="*70)
print("""
1. Not splitting train/test (overfitting on same data you evaluate on).
2. Evaluating classification with accuracy alone (misleading for imbalanced data).
   Fix: Use AUC-ROC, F1, precision/recall.
3. Not scaling features before logistic regression or SVM.
4. Using random data for model demo (results will be ~0.5 AUC = random).
5. Not setting seed in randomSplit (non-reproducible results).
""")

print("="*70)
print("SECTION 7 — HOMEWORK")
print("="*70)
print("""
Level 1: Train a LogisticRegression and evaluate AUC.
Level 2: Train a RandomForest with numTrees=100.
Level 3: Compare LR vs RF on same data.
Level 4: Train LinearRegression and evaluate RMSE + R².
Level 5: Use GBTClassifier (best accuracy for tabular data).
Level 6: Feature importances from RandomForest.
Level 7: hyperparameter tuning (next notebook!).
Level 8: Handle imbalanced data (weightCol parameter).
Level 9: Cross-validation (next notebook!).
Level 10: Teach classification vs regression:
  "Classification: predict a category (yes/no, A/B/C).
   Regression: predict a number (price, temperature).
   Workflow: features → split → fit → predict → evaluate.
   Always split BEFORE fitting. Never evaluate on training data."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 88")
print("="*70)