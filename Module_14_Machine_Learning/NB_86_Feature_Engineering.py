# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 86: Feature Engineering with PySpark MLlib
# MAGIC ## Module 14: Machine Learning
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 55 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Feature engineering** is the process of transforming raw data into numerical representations that machine learning algorithms can understand. It's often said that 80% of ML success comes from good features, not fancy models.
# MAGIC
# MAGIC PySpark MLlib provides **Transformers** and **Estimators** that handle feature engineering at massive scale — millions or billions of rows processed in parallel.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine teaching a child to recognize fruits:
# MAGIC - **Raw data**: "It's round, red, small, has a stem" (text description)
# MAGIC - **Feature engineering**: Convert to numbers the model understands:
# MAGIC   - Shape: round=1, oval=2, long=3
# MAGIC   - Color: red=0.9, green=0.3, yellow=0.7 (RGB values)
# MAGIC   - Size: 3.5 cm diameter
# MAGIC   - Has_stem: 1 (yes/no)
# MAGIC - **Model input**: [1, 0.9, 0.0, 0.0, 3.5, 1] — a **feature vector**
# MAGIC
# MAGIC MLlib requires ALL features combined into a single **Vector column** called "features".
# MAGIC
# MAGIC ### Key MLlib Transformers:
# MAGIC | Transformer | Purpose | Input → Output |
# MAGIC |------------|---------|----------------|
# MAGIC | `VectorAssembler` | Combine columns into vector | Multiple cols → 1 vector |
# MAGIC | `StringIndexer` | Category → numeric index | "red" → 0.0, "blue" → 1.0 |
# MAGIC | `OneHotEncoder` | Index → binary vector | 0.0 → [1,0,0], 1.0 → [0,1,0] |
# MAGIC | `StandardScaler` | Normalize to mean=0, std=1 | Raw values → standardized |
# MAGIC | `MinMaxScaler` | Scale to [0,1] range | Raw → [0,1] |
# MAGIC | `Bucketizer` | Continuous → discrete bins | 25.5 → "20-30" bucket |
# MAGIC | `Imputer` | Fill missing values | NULL → mean/median |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC MLlib Feature Engineering Pipeline:
# MAGIC
# MAGIC   Raw DataFrame              Transformers                 Model-Ready DataFrame
# MAGIC   ─────────────              ────────────                 ────────────────────
# MAGIC   | age | city  | income |   StringIndexer(city)         | features      | label |
# MAGIC   | 25  | NYC   | 50000  |   OneHotEncoder(city_idx)     | [25,1,0,50K]  |  1    |
# MAGIC   | 30  | LA    | 60000  |   VectorAssembler(all)        | [30,0,1,60K]  |  0    |
# MAGIC   | 35  | NYC   | 70000  |   StandardScaler(features)    | [35,1,0,70K]  |  1    |
# MAGIC
# MAGIC   Step-by-step:
# MAGIC     1. StringIndexer: "NYC"→0.0, "LA"→1.0 (categorical to numeric)
# MAGIC     2. OneHotEncoder: 0.0→[1,0], 1.0→[0,1] (avoid ordinal assumption)
# MAGIC     3. VectorAssembler: Combine [age, city_vec, income] into one vector
# MAGIC     4. StandardScaler: Normalize all features to same scale
# MAGIC     5. Output: Single 'features' column + 'label' column → ready for model
# MAGIC
# MAGIC   Estimator vs Transformer:
# MAGIC     Estimator: Learns from data (needs .fit()). Returns a Transformer.
# MAGIC       Example: StringIndexer.fit(df) learns the mapping, returns StringIndexerModel.
# MAGIC     Transformer: Applies a fixed transformation (just .transform()).
# MAGIC       Example: StringIndexerModel.transform(df) applies the learned mapping.
# MAGIC
# MAGIC   Pipeline: Chain multiple stages into a single reusable workflow.
# MAGIC     pipeline = Pipeline(stages=[indexer, encoder, assembler, scaler])
# MAGIC     model = pipeline.fit(train_df)        # Fit all stages.
# MAGIC     result = model.transform(test_df)     # Apply all stages.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: Beginner Examples
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — BEGINNER EXAMPLES
# ═══════════════════════════════════════════════════════════════════

from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, OneHotEncoder,
    StandardScaler, MinMaxScaler, Imputer, Bucketizer
)  # Feature engineering imports.
from pyspark.ml import Pipeline  # Pipeline import.
from pyspark.sql.functions import col, rand, expr, when  # SQL functions.

print("="*70)
print("SECTION 3 — BEGINNER EXAMPLES: Feature Engineering Basics")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: VectorAssembler (combine columns into feature vector)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: VectorAssembler — combine numeric columns")
print("-"*60)

# Create sample data.
data = spark.createDataFrame([
    (25, 50000.0, 3.5, 1),
    (30, 60000.0, 4.2, 0),
    (35, 70000.0, 2.8, 1),
    (28, 55000.0, 3.9, 0),
    (40, 80000.0, 4.5, 1)
], ["age", "income", "gpa", "label"])

print("\nRaw data:")
data.show()

# VectorAssembler: combine age, income, gpa into one 'features' vector.
assembler = VectorAssembler(
    inputCols=["age", "income", "gpa"],  # Columns to combine.
    outputCol="features"                  # Output vector column name.
)

assembled = assembler.transform(data)  # Apply transformation.
print("After VectorAssembler:")
assembled.select("age", "income", "gpa", "features", "label").show(truncate=False)

print("✓ 'features' column = dense vector of [age, income, gpa].")
print("  This is what MLlib models expect as input.")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: StringIndexer (categorical → numeric)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: StringIndexer — convert categories to numbers")
print("-"*60)

# Data with categorical column.
city_data = spark.createDataFrame([
    ("NYC", 50000), ("LA", 60000), ("NYC", 55000),
    ("Chicago", 45000), ("LA", 70000), ("Chicago", 48000)
], ["city", "income"])

print("\nRaw categorical data:")
city_data.show()

# StringIndexer: assigns numeric index based on frequency.
# Most frequent gets 0, next gets 1, etc.
indexer = StringIndexer(
    inputCol="city",        # Input: string column.
    outputCol="city_index"  # Output: numeric index.
)

indexer_model = indexer.fit(city_data)  # Fit: learns the mapping.
indexed = indexer_model.transform(city_data)  # Transform: applies mapping.

print("After StringIndexer:")
indexed.show()
print(f"Mapping: {indexer_model.labels}")  # Shows [most_frequent, ..., least_frequent].
print("✓ NYC(2x)→1.0, LA(2x)→0.0, Chicago(2x)→2.0 (alphabetic tiebreaker).")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: OneHotEncoder (index → binary vector)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: OneHotEncoder — avoid ordinal assumption")
print("-"*60)

# OneHotEncoder takes the indexed column and creates binary vectors.
encoder = OneHotEncoder(
    inputCol="city_index",     # Input: numeric index from StringIndexer.
    outputCol="city_encoded"   # Output: sparse binary vector.
)

encoded = encoder.fit(indexed).transform(indexed)  # Fit + transform.
print("After OneHotEncoder:")
encoded.select("city", "city_index", "city_encoded").show(truncate=False)

print("✓ OneHot avoids the model thinking Chicago(2) > LA(0).")
print("  Each category gets its own binary flag.")
print("  Note: Uses N-1 encoding (last category is all zeros — reference).")

# COMMAND ----------

# DBTITLE 1,Section 4-5: Intermediate and Advanced
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 4-5 — INTERMEDIATE & ADVANCED
# ═══════════════════════════════════════════════════════════════════

from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, OneHotEncoder,
    StandardScaler, MinMaxScaler, Imputer, Bucketizer
)  # Re-import for clarity.
from pyspark.ml import Pipeline  # Pipeline.
from pyspark.sql.functions import col, rand, when, lit  # Functions.

print("="*70)
print("SECTIONS 4-5: Intermediate & Advanced Feature Engineering")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 4: StandardScaler (normalize features)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 4: StandardScaler — normalize to mean=0, std=1")
print("-"*60)

# Create data with very different scales.
scale_data = spark.createDataFrame([
    (25, 50000.0), (30, 60000.0), (35, 70000.0),
    (28, 55000.0), (40, 80000.0), (22, 42000.0)
], ["age", "income"])

# Step 1: Assemble into vector (StandardScaler needs vector input).
assembler = VectorAssembler(inputCols=["age", "income"], outputCol="raw_features")
assembled = assembler.transform(scale_data)

# Step 2: StandardScaler.
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="scaled_features",
    withMean=True,   # Subtract mean (center at 0).
    withStd=True     # Divide by std (unit variance).
)

scaler_model = scaler.fit(assembled)  # Learns mean and std.
scaled = scaler_model.transform(assembled)

print("\nBefore scaling (different scales):")
assembled.select("age", "income", "raw_features").show(truncate=False)
print("After StandardScaler (same scale):")
scaled.select("scaled_features").show(truncate=False)

print("✓ Now age and income are on the same scale.")
print("  This is important for distance-based algorithms (KNN, SVM, K-Means).")
print(f"  Mean: {scaler_model.mean}")
print(f"  Std:  {scaler_model.std}")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 5: Imputer (handle missing values)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 5: Imputer — fill missing values with mean/median")
print("-"*60)

# Create data with NULLs.
missing_data = spark.createDataFrame([
    (25.0, 50000.0), (None, 60000.0), (35.0, None),
    (28.0, 55000.0), (None, 80000.0), (22.0, 42000.0)
], ["age", "income"])

print("\nData with missing values:")
missing_data.show()

# Imputer fills NULLs with mean (default) or median.
imputer = Imputer(
    inputCols=["age", "income"],           # Columns with missing values.
    outputCols=["age_imp", "income_imp"],  # Imputed output columns.
    strategy="mean"                        # or "median", "mode".
)

imputer_model = imputer.fit(missing_data)  # Learns means.
imputed = imputer_model.transform(missing_data)

print("After Imputer (NULLs replaced with mean):")
imputed.select("age", "age_imp", "income", "income_imp").show()
print(f"  Learned means: age={imputer_model.surrogateDF.collect()}")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 6: Complete Pipeline (all steps chained)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 6: Complete Feature Engineering Pipeline")
print("-"*60)

# Realistic dataset.
df = spark.createDataFrame([
    (25, "NYC", 50000.0, 3.5, 1),
    (30, "LA", 60000.0, 4.2, 0),
    (35, "NYC", 70000.0, 2.8, 1),
    (28, "Chicago", 55000.0, 3.9, 0),
    (40, "LA", 80000.0, 4.5, 1),
    (22, "Chicago", 42000.0, 3.1, 0),
    (33, "NYC", 65000.0, 3.7, 1),
    (29, "LA", 58000.0, 4.0, 0)
], ["age", "city", "income", "gpa", "label"])

print("\nRaw data:")
df.show()

# Build a complete pipeline.
stages = []

# Stage 1: Index the city column.
city_indexer = StringIndexer(inputCol="city", outputCol="city_idx")
stages.append(city_indexer)

# Stage 2: One-hot encode the indexed city.
city_encoder = OneHotEncoder(inputCol="city_idx", outputCol="city_vec")
stages.append(city_encoder)

# Stage 3: Assemble all features into one vector.
feature_assembler = VectorAssembler(
    inputCols=["age", "income", "gpa", "city_vec"],  # Mix numeric + encoded.
    outputCol="raw_features"
)
stages.append(feature_assembler)

# Stage 4: Scale features.
feature_scaler = StandardScaler(
    inputCol="raw_features", outputCol="features",
    withMean=True, withStd=True
)
stages.append(feature_scaler)

# Create and fit the pipeline.
pipeline = Pipeline(stages=stages)
pipeline_model = pipeline.fit(df)  # Fits all stages in order.

# Transform data.
result = pipeline_model.transform(df)
print("After complete pipeline:")
result.select("age", "city", "income", "gpa", "features", "label").show(truncate=False)

print("✓ Complete pipeline: Index → Encode → Assemble → Scale.")
print("  Save pipeline_model to reuse on new data (ensures consistency).")
print("  pipeline_model.save('/models/feature_pipeline')")
print("  loaded = PipelineModel.load('/models/feature_pipeline')")

# COMMAND ----------

# DBTITLE 1,Section 6 - Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Forgetting VectorAssembler (model gets wrong input)
# MAGIC ```python
# MAGIC # BAD: Passing raw columns directly to model.
# MAGIC model.fit(df.select("age", "income", "label"))  # ERROR! Models need 'features' vector.
# MAGIC
# MAGIC # GOOD: Always assemble into a vector column first.
# MAGIC assembler = VectorAssembler(inputCols=["age", "income"], outputCol="features")
# MAGIC df_ready = assembler.transform(df)
# MAGIC model.fit(df_ready)  # Works!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Using StringIndexer output directly (ordinal assumption)
# MAGIC ```python
# MAGIC # BAD: StringIndexer gives NYC=0, LA=1, Chicago=2.
# MAGIC # Model thinks Chicago(2) > LA(1) > NYC(0) — wrong for categories!
# MAGIC assembler = VectorAssembler(inputCols=["city_index"], outputCol="features")
# MAGIC
# MAGIC # GOOD: Always OneHotEncode after StringIndexer for nominal categories.
# MAGIC indexer = StringIndexer(inputCol="city", outputCol="city_idx")
# MAGIC encoder = OneHotEncoder(inputCol="city_idx", outputCol="city_vec")
# MAGIC assembler = VectorAssembler(inputCols=["city_vec"], outputCol="features")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Fitting transformers on test data (data leakage)
# MAGIC ```python
# MAGIC # BAD: Fit scaler on ALL data including test set.
# MAGIC scaler_model = scaler.fit(all_data)  # Test data info leaks into training!
# MAGIC
# MAGIC # GOOD: Fit ONLY on training data, transform both.
# MAGIC scaler_model = scaler.fit(train_df)  # Learn from train only.
# MAGIC train_scaled = scaler_model.transform(train_df)
# MAGIC test_scaled = scaler_model.transform(test_df)  # Apply same params to test.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Not handling NULLs before assembling
# MAGIC ```python
# MAGIC # BAD: VectorAssembler fails or produces NaN vectors with NULL values.
# MAGIC assembler.transform(df_with_nulls)  # Error or garbage output!
# MAGIC
# MAGIC # GOOD: Impute or drop NULLs first.
# MAGIC imputer = Imputer(inputCols=["age"], outputCols=["age_clean"], strategy="median")
# MAGIC df_clean = imputer.fit(df).transform(df)
# MAGIC assembler.transform(df_clean)  # Safe.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not saving the pipeline model (can't reproduce on new data)
# MAGIC ```python
# MAGIC # BAD: Transform training data, then manually redo steps on new data.
# MAGIC # You'll forget a step or use different parameters!
# MAGIC
# MAGIC # GOOD: Save the fitted pipeline. Load and apply to any new data.
# MAGIC pipeline_model.save("/models/my_feature_pipeline")
# MAGIC # Later:
# MAGIC from pyspark.ml import PipelineModel
# MAGIC loaded = PipelineModel.load("/models/my_feature_pipeline")
# MAGIC new_features = loaded.transform(new_raw_data)  # Exact same transformations!
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7 - Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK (Levels 1–10)
# ═══════════════════════════════════════════════════════════════════

from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler  # Imports.
from pyspark.ml import Pipeline  # Pipeline.
from pyspark.sql.functions import col  # Functions.

print("="*70)
print("HOMEWORK — Feature Engineering")
print("="*70)

# Level 1: VectorAssembler.
print("\n--- Level 1: VectorAssembler ---")
df1 = spark.createDataFrame([(1,2.0,3.0),(4,5.0,6.0),(7,8.0,9.0)], ["a","b","c"])
assembler = VectorAssembler(inputCols=["a","b","c"], outputCol="features")
assembler.transform(df1).select("features").show(truncate=False)
# WHY: All MLlib models require a single 'features' vector column.

# Level 2: StringIndexer.
print("\n--- Level 2: StringIndexer ---")
df2 = spark.createDataFrame([("cat",),("dog",),("cat",),("bird",)], ["animal"])
idx = StringIndexer(inputCol="animal", outputCol="animal_idx").fit(df2)
idx.transform(df2).show()
print(f"Labels: {idx.labels}")
# WHY: ML models need numbers, not strings. Most frequent → 0.

# Level 3: OneHotEncoder.
print("\n--- Level 3: OneHotEncoder ---")
df3 = idx.transform(df2)
enc = OneHotEncoder(inputCol="animal_idx", outputCol="animal_vec").fit(df3)
enc.transform(df3).select("animal", "animal_idx", "animal_vec").show(truncate=False)
# WHY: Prevents model from assuming ordinal relationship between categories.

# Level 4: StandardScaler.
print("\n--- Level 4: StandardScaler ---")
df4 = spark.createDataFrame([(1.0,1000.0),(2.0,2000.0),(3.0,3000.0)], ["x","y"])
assm = VectorAssembler(inputCols=["x","y"], outputCol="raw").transform(df4)
scl = StandardScaler(inputCol="raw", outputCol="scaled", withMean=True, withStd=True)
scl.fit(assm).transform(assm).select("raw", "scaled").show(truncate=False)
# WHY: Puts all features on same scale (critical for distance-based models).

# Level 5: Pipeline.
print("\n--- Level 5: Build a Pipeline ---")
df5 = spark.createDataFrame([
    (25, "A", 100.0, 1), (30, "B", 200.0, 0), (35, "A", 150.0, 1)
], ["age", "cat", "val", "label"])
pipe = Pipeline(stages=[
    StringIndexer(inputCol="cat", outputCol="cat_idx"),
    OneHotEncoder(inputCol="cat_idx", outputCol="cat_vec"),
    VectorAssembler(inputCols=["age", "val", "cat_vec"], outputCol="features")
])
result5 = pipe.fit(df5).transform(df5)
result5.select("features", "label").show(truncate=False)
# WHY: Pipeline chains all steps. Fit once, apply to train AND test.

# Level 6-10: Conceptual.
print("\n--- Level 6: When to scale? ---")
print("Scale for: KNN, SVM, K-Means, Neural Networks.")
print("Not needed for: Decision Trees, Random Forests, XGBoost.")

print("\n--- Level 7: Data leakage prevention ---")
print("ALWAYS fit on training data only. Transform both train and test.")

print("\n--- Level 8: Handling many categories (100+) ---")
print("Options: Target encoding, hash encoding, or keep top-N + 'other' bucket.")

print("\n--- Level 9: Save/load pipeline ---")
print("pipeline_model.save('/path') → PipelineModel.load('/path')")
print("Ensures exact same transformations on new data.")

print("\n--- Level 10: Teach feature engineering ---")
print("""
"Feature engineering: turn raw data into numbers for ML.
  VectorAssembler: combine columns into one 'features' vector.
  StringIndexer + OneHotEncoder: handle categories.
  StandardScaler: normalize scales.
  Pipeline: chain all steps, fit on train, apply to train+test.
  Save pipelines to ensure reproducibility on new data."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 86")
print("="*70)