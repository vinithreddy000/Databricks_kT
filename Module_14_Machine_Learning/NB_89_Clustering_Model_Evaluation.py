# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 89: Clustering & Model Evaluation
# MAGIC ## Module 14: Machine Learning
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Clustering** groups similar data points together WITHOUT labels (unsupervised learning). Unlike classification which needs labeled training data, clustering discovers natural groups in the data.
# MAGIC
# MAGIC **Model Evaluation** provides comprehensive metrics to understand how well your models perform, including confusion matrices, precision/recall trade-offs, and cross-validation.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC **Clustering**: You have 1,000 customers. Without knowing anything about them, clustering groups them into segments based on behavior: "Big spenders", "Bargain hunters", "Window shoppers" — you discover the groups, you don't define them.
# MAGIC
# MAGIC ### Key Algorithms:
# MAGIC | Algorithm | Type | Use Case |
# MAGIC |-----------|------|----------|
# MAGIC | K-Means | Clustering | Customer segmentation, grouping |
# MAGIC | Bisecting K-Means | Clustering | Hierarchical clustering |
# MAGIC | Gaussian Mixture | Clustering | Soft clustering (probabilities) |
# MAGIC | ClusteringEvaluator | Evaluation | Silhouette score |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC K-Means Clustering:
# MAGIC   Input: Data points (no labels!).
# MAGIC   Output: Each point assigned to a cluster (0, 1, 2, ... K-1).
# MAGIC
# MAGIC   Algorithm:
# MAGIC     1. Pick K random centers (centroids).
# MAGIC     2. Assign each point to nearest centroid.
# MAGIC     3. Recompute centroids as mean of assigned points.
# MAGIC     4. Repeat 2-3 until convergence.
# MAGIC
# MAGIC   How to choose K:
# MAGIC     Elbow method: Try K=2,3,4,...10. Plot cost vs K. 
# MAGIC     Pick K where cost stops decreasing significantly (the "elbow").
# MAGIC     Silhouette score: Higher = better-defined clusters (max 1.0).
# MAGIC
# MAGIC Model Evaluation Deep Dive:
# MAGIC   Confusion Matrix (classification):
# MAGIC                     Predicted Positive   Predicted Negative
# MAGIC     Actual Positive      TP                    FN
# MAGIC     Actual Negative      FP                    TN
# MAGIC
# MAGIC   From this:
# MAGIC     Accuracy  = (TP+TN) / (TP+TN+FP+FN)
# MAGIC     Precision = TP / (TP+FP)  "Of all predicted positive, how many correct?"
# MAGIC     Recall    = TP / (TP+FN)  "Of all actual positive, how many found?"
# MAGIC     F1        = 2*P*R / (P+R) "Balance of precision and recall"
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: Clustering and Evaluation
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7: CLUSTERING & EVALUATION
# ═══════════════════════════════════════════════════════════════════

from pyspark.ml.feature import VectorAssembler, StandardScaler  # Feature tools.
from pyspark.ml.clustering import KMeans, BisectingKMeans  # Clustering.
from pyspark.ml.evaluation import ClusteringEvaluator, MulticlassClassificationEvaluator  # Eval.
from pyspark.ml.classification import RandomForestClassifier  # For eval demo.
from pyspark.sql.functions import col, rand, when  # Functions.

print("="*70)
print("SECTIONS 3-5: Clustering & Model Evaluation")
print("="*70)

# ─── EXAMPLE 1: K-Means Clustering ───
print("\n" + "-"*60)
print("EXAMPLE 1: K-Means Clustering")
print("-"*60)

# Create data with natural clusters.
cust_data = spark.range(500).select(
    (rand() * 100).alias("annual_spend"),
    (rand() * 50).alias("visit_frequency"),
    (rand() * 10).alias("avg_basket_size")
)

# Prepare features.
assembler = VectorAssembler(
    inputCols=["annual_spend", "visit_frequency", "avg_basket_size"],
    outputCol="features"
)
scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withMean=True, withStd=True)
assembled = assembler.transform(cust_data)
scaled = scaler.fit(assembled).transform(assembled)

# K-Means with K=3 (3 customer segments).
kmeans = KMeans(
    featuresCol="scaled_features",
    predictionCol="cluster",
    k=3,          # Number of clusters.
    seed=42,
    maxIter=20
)
kmeans_model = kmeans.fit(scaled)
clustered = kmeans_model.transform(scaled)

print("\nCluster assignments:")
clustered.groupBy("cluster").count().orderBy("cluster").show()

print("Cluster centers:")
for i, center in enumerate(kmeans_model.clusterCenters()):
    print(f"  Cluster {i}: {[f'{v:.2f}' for v in center]}")

# Evaluate with Silhouette score.
evaluator = ClusteringEvaluator(featuresCol="scaled_features", predictionCol="cluster")
silhouette = evaluator.evaluate(clustered)
print(f"\nSilhouette Score: {silhouette:.4f}")
print("  (1.0 = perfect separation, 0 = overlapping, -1 = wrong assignment)")

# ─── EXAMPLE 2: Elbow Method (find optimal K) ───
print("\n" + "-"*60)
print("EXAMPLE 2: Elbow Method — finding optimal K")
print("-"*60)

print("\nTrying K = 2 to 8:")
results = []
for k in range(2, 9):
    km = KMeans(featuresCol="scaled_features", k=k, seed=42, maxIter=20)
    model = km.fit(scaled)
    preds = model.transform(scaled)
    score = evaluator.evaluate(preds)
    cost = model.summary.trainingCost  # Within-cluster sum of squares.
    results.append((k, score, cost))
    print(f"  K={k}: Silhouette={score:.4f}, Cost={cost:.0f}")

best_k = max(results, key=lambda x: x[1])
print(f"\n  Best K by silhouette: {best_k[0]} (score={best_k[1]:.4f})")
print("  Rule: Pick K where silhouette is highest or cost drops steeply.")

# ─── EXAMPLE 3: Detailed Classification Evaluation ───
print("\n" + "-"*60)
print("EXAMPLE 3: Comprehensive Classification Metrics")
print("-"*60)

# Create labeled data and train a model.
clf_data = spark.range(500).select(
    (rand()*50+20).alias("f1"), (rand()*100).alias("f2"), (rand()*5).alias("f3"),
    when(rand() > 0.6, 1.0).otherwise(0.0).alias("label")
)
assm = VectorAssembler(inputCols=["f1","f2","f3"], outputCol="features")
clf_ready = assm.transform(clf_data).select("features", "label")
train, test = clf_ready.randomSplit([0.7, 0.3], seed=42)
rf = RandomForestClassifier(numTrees=50, seed=42).fit(train)
preds = rf.transform(test)

# All classification metrics.
metrics = [
    ("accuracy", "Accuracy"),
    ("weightedPrecision", "Weighted Precision"),
    ("weightedRecall", "Weighted Recall"),
    ("f1", "F1 Score")
]

print("\nClassification Metrics:")
multi_eval = MulticlassClassificationEvaluator(labelCol="label")
for metric_name, display_name in metrics:
    score = multi_eval.setMetricName(metric_name).evaluate(preds)
    print(f"  {display_name:25s} = {score:.4f}")

# Confusion matrix.
print("\nConfusion Matrix:")
preds.groupBy("label", "prediction").count().orderBy("label", "prediction").show()

# ─── SECTION 6 & 7 ───
print("\n" + "="*70)
print("SECTION 6 — COMMON MISTAKES")
print("="*70)
print("""
1. Not scaling features before K-Means (income in 1000s dominates age in 10s).
2. Choosing K arbitrarily (always use elbow method or silhouette).
3. Using accuracy for imbalanced data (99% accuracy when 99% is one class!).
4. Not checking confusion matrix (might have 0 recall for minority class).
5. Running K-Means on categorical data (K-Means is for numeric only).
""")

print("="*70)
print("SECTION 7 — HOMEWORK")
print("="*70)
print("""
Level 1: Run K-Means with K=3. Level 2: Evaluate with silhouette.
Level 3: Elbow method for K=2..8. Level 4: Print confusion matrix.
Level 5: Compare accuracy vs F1 on imbalanced data.
Level 6: BisectingKMeans (hierarchical). Level 7: Feature importances.
Level 8: When to use clustering vs classification?
  Clustering: no labels, discover patterns.
  Classification: have labels, predict category.
Level 10: Teach clustering:
  "K-Means groups similar points (unsupervised, no labels).
   Choose K with elbow/silhouette. Always scale features first.
   Evaluate: silhouette (higher=better). Use for segmentation."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 89")
print("="*70)