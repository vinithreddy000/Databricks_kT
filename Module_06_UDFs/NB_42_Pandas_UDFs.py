# Databricks notebook source
# DBTITLE 1,NB_42 Header
# MAGIC %md
# MAGIC # NB_42 — Pandas UDFs (All Types)
# MAGIC
# MAGIC **Module 6: User-Defined Functions** | Notebook 42 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Why Pandas UDFs are 10-100x faster than regular UDFs
# MAGIC * Apache Arrow: the secret behind vectorized UDFs
# MAGIC * Series → Series (scalar pandas UDF)
# MAGIC * Iterator of Series → Iterator of Series (batched processing)
# MAGIC * Series → Scalar (grouped aggregate)
# MAGIC * applyInPandas() — grouped map
# MAGIC * mapInPandas() — map operations
# MAGIC * Performance comparison: Regular UDF vs Pandas UDF vs Built-in
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐⭐ (Best of both worlds: custom logic + speed)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are Pandas UDFs?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are Pandas UDFs? (Real-World Analogy)
# MAGIC
# MAGIC ### 🚚 The Batch Delivery Truck
# MAGIC
# MAGIC Regular UDFs deliver items one-by-one (row-by-row). Pandas UDFs load an entire TRUCK (batch) and deliver all at once:
# MAGIC
# MAGIC | Delivery Method | UDF Type | Speed |
# MAGIC |---|---|---|
# MAGIC | Bicycle (one item) | Regular Python UDF | Slow (row-by-row serialization) |
# MAGIC | Delivery truck (batch) | Pandas UDF (vectorized) | Fast (Apache Arrow batches) |
# MAGIC | Factory built-in conveyor | Built-in function | Fastest (JVM-native) |
# MAGIC
# MAGIC ### Why Pandas UDFs Are Faster
# MAGIC 1. **Apache Arrow:** Zero-copy columnar data transfer (no pickle serialization)
# MAGIC 2. **Vectorized:** Operations on entire pandas Series (not row-by-row)
# MAGIC 3. **NumPy/pandas optimized:** Leverage C-optimized libraries
# MAGIC 4. **Batch processing:** Amortize Python overhead across many rows
# MAGIC
# MAGIC ### The 5 Types of Pandas UDFs
# MAGIC | Type | Input → Output | Use Case |
# MAGIC |---|---|---|
# MAGIC | Series → Series | Column → Column | Element-wise transforms |
# MAGIC | Iterator[Series] → Iterator[Series] | Batched column | Large model loading |
# MAGIC | Series → Scalar | Column → Single value | Custom aggregations |
# MAGIC | applyInPandas | Group → DataFrame | Complex per-group logic |
# MAGIC | mapInPandas | Partition → DataFrame | Full partition processing |

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Pandas UDFs Work
# MAGIC %md
# MAGIC ## SECTION 2 — How Pandas UDFs Work (Internal Mechanics)
# MAGIC
# MAGIC ### Arrow-Based Data Transfer
# MAGIC ```
# MAGIC ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
# MAGIC │ JVM (Spark) │ ───► │ Arrow Batch │ ───► │ Python      │
# MAGIC │ Partition   │      │ (columnar   │      │ pandas      │
# MAGIC │ 1000s rows  │      │  zero-copy)  │      │ Series/DF   │
# MAGIC └─────────────┘      └─────────────┘      └─────────────┘
# MAGIC
# MAGIC                    BATCH transfer (1000s of rows at once)
# MAGIC                    vs Regular UDF: one row at a time
# MAGIC ```
# MAGIC
# MAGIC ### Comparison
# MAGIC ```
# MAGIC Regular UDF:  Row1 → Python → Back. Row2 → Python → Back. (N roundtrips)
# MAGIC Pandas UDF:   [Row1..Row1000] → Python as pd.Series → Back. (1 roundtrip per batch)
# MAGIC ```
# MAGIC
# MAGIC ### Key Rules
# MAGIC 1. Input/output are pandas Series or DataFrames (NOT Python scalars)
# MAGIC 2. Use `@pandas_udf` decorator with type hints
# MAGIC 3. Arrow batch size controlled by `spark.sql.execution.arrow.maxRecordsPerBatch`
# MAGIC 4. NULL → NaN in pandas (use `.isna()` to check)
# MAGIC 5. Return Series MUST have same length as input Series (for Series→Series)

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Series to Series (scalar)
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Series to Series (Scalar Pandas UDF)
# ============================================================
# Real-world: Element-wise transformations using pandas vectorization.

from pyspark.sql import SparkSession  # Import SparkSession.
from pyspark.sql.functions import col, pandas_udf  # Import pandas_udf.
from pyspark.sql.types import StringType, DoubleType, IntegerType  # Types.
import pandas as pd  # Import pandas.

spark = SparkSession.builder.getOrCreate()  # Get active session.

# === Type 1: Series -> Series (most common) ===
# Signature: pd.Series -> pd.Series (same length!)

@pandas_udf(StringType())  # Decorator with return type.
def pandas_title_case(names: pd.Series) -> pd.Series:  # Type hints.
    """Vectorized title case conversion."""
    return names.str.title()  # pandas vectorized string operation.

@pandas_udf(DoubleType())  # Double return.
def pandas_celsius_to_fahrenheit(temps: pd.Series) -> pd.Series:  # Type hints.
    """Vectorized temperature conversion."""
    return temps * 9 / 5 + 32  # Vectorized math (no loop!).

@pandas_udf(IntegerType())  # Int return.
def pandas_word_count(texts: pd.Series) -> pd.Series:  # Type hints.
    """Count words using vectorized string operations."""
    return texts.str.split().str.len()  # Vectorized: split then count.

# Apply pandas UDFs.
print("=== Series → Series (Scalar Pandas UDF) ===")  # Print heading.
people = spark.createDataFrame([
    (1, "alice smith", 37.0, "Hello world from PySpark"),
    (2, "bob jones", 22.5, "Pandas UDFs are vectorized and fast"),
    (3, "charlie brown", 100.0, "Single word"),
    (4, None, None, None),  # NULL handling.
], ["id", "name", "temp_c", "text"])  # Sample data.

people.select(
    col("id"),  # Keep id.
    col("name"),  # Original.
    pandas_title_case(col("name")).alias("title_name"),  # Vectorized title.
    col("temp_c"),  # Original temp.
    pandas_celsius_to_fahrenheit(col("temp_c")).alias("temp_f"),  # Vectorized convert.
    pandas_word_count(col("text")).alias("words"),  # Vectorized count.
).show(truncate=False)  # Display.

# Multiple columns using struct (pass multiple columns).
print("=== Multi-Column Pandas UDF ===")  # Print heading.

@pandas_udf(DoubleType())  # Double return.
def bmi_calculator(weight: pd.Series, height: pd.Series) -> pd.Series:  # Two inputs.
    """Calculate BMI from weight(kg) and height(m)."""
    return (weight / (height ** 2)).round(1)  # Vectorized BMI.

health = spark.createDataFrame([
    ("Alice", 60.0, 1.65), ("Bob", 85.0, 1.80), ("Charlie", 70.0, 1.75)
], ["name", "weight_kg", "height_m"])  # Health data.

health.select(
    col("name"),  # Keep.
    col("weight_kg"), col("height_m"),  # Keep.
    bmi_calculator(col("weight_kg"), col("height_m")).alias("bmi"),  # Vectorized BMI.
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Iterator of Series (batched)
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Iterator of Series (Batched)
# ============================================================
# Real-world: Load expensive model ONCE, apply to all batches.

from pyspark.sql.functions import col, pandas_udf  # Imports.
from pyspark.sql.types import StringType, DoubleType  # Types.
import pandas as pd  # Pandas.
from typing import Iterator  # Type hint.

# === Type 2: Iterator[Series] -> Iterator[Series] ===
# Key advantage: initialize expensive resources ONCE (model, connection, etc.)

@pandas_udf(StringType())  # String return.
def batched_classify(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """Classify text in batches. Model loaded once."""
    # This code runs ONCE at start (model loading simulation).
    keywords = {  # Simple keyword classifier (simulates loaded model).
        "urgent": "HIGH",
        "error": "HIGH",
        "warning": "MEDIUM",
        "info": "LOW",
    }  # Loaded once.
    
    # Process each batch.
    for batch in batch_iter:  # Iterate batches.
        def classify_text(text):  # Classification logic.
            if pd.isna(text):  # Handle NULL/NaN.
                return None  # Return None.
            text_lower = text.lower()  # Lowercase.
            for keyword, level in keywords.items():  # Check keywords.
                if keyword in text_lower:  # Match found.
                    return level  # Return level.
            return "INFO"  # Default.
        yield batch.apply(classify_text)  # Apply to batch, yield result.

# Apply batched UDF.
print("=== Iterator[Series] → Iterator[Series] ===")  # Print heading.
logs = spark.createDataFrame([
    (1, "URGENT: Server down!"),
    (2, "Error in module X"),
    (3, "Warning: disk space low"),
    (4, "Info: backup complete"),
    (5, "Regular log message"),
    (6, None),
], ["id", "message"])  # Log messages.

logs.select(
    col("id"),  # Keep.
    col("message"),  # Original.
    batched_classify(col("message")).alias("severity"),  # Classify.
).show(truncate=False)  # Display.

print("""When to use Iterator[Series] -> Iterator[Series]:
- Loading ML models (load once, predict many batches)
- Database connections (connect once, query per batch)
- Any expensive initialization that should happen ONCE per partition
- Processing that benefits from batch-level context""")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Series to Scalar (grouped aggregate)
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Series to Scalar (Grouped Aggregate)
# ============================================================
# Real-world: Custom aggregation functions per group.

from pyspark.sql.functions import col, pandas_udf  # Imports.
from pyspark.sql.types import DoubleType, StringType  # Types.
import pandas as pd  # Pandas.
import numpy as np  # NumPy.

# === Type 3: Series -> Scalar (Grouped Aggregate) ===
# Used with groupBy().agg() — reduces a column to one value per group.

@pandas_udf(DoubleType())  # Returns single double per group.
def pandas_median(values: pd.Series) -> float:  # Series -> scalar.
    """Compute median (not available as built-in!)."""
    return float(values.median())  # Pandas median.

@pandas_udf(DoubleType())  # Returns single double per group.
def pandas_iqr(values: pd.Series) -> float:  # Series -> scalar.
    """Compute Interquartile Range."""
    q75 = values.quantile(0.75)  # 75th percentile.
    q25 = values.quantile(0.25)  # 25th percentile.
    return float(q75 - q25)  # IQR.

@pandas_udf(DoubleType())  # Returns single double per group.
def pandas_coefficient_of_variation(values: pd.Series) -> float:  # CV.
    """Compute Coefficient of Variation (std/mean * 100)."""
    if values.mean() == 0:  # Avoid division by zero.
        return 0.0  # Return 0.
    return float(values.std() / values.mean() * 100)  # CV percentage.

# Apply grouped aggregate pandas UDFs.
print("=== Series → Scalar (Grouped Aggregate) ===")  # Print heading.
sales = spark.createDataFrame([
    ("Electronics", 1200), ("Electronics", 800), ("Electronics", 1500),
    ("Electronics", 950), ("Electronics", 2000),
    ("Books", 25), ("Books", 35), ("Books", 15), ("Books", 45), ("Books", 30),
    ("Clothing", 80), ("Clothing", 120), ("Clothing", 95),
], ["category", "price"])  # Sales data.

sales.groupBy("category").agg(
    pandas_median(col("price")).alias("median_price"),  # Custom median.
    pandas_iqr(col("price")).alias("iqr"),  # Custom IQR.
    pandas_coefficient_of_variation(col("price")).alias("cv_pct"),  # Custom CV.
).show(truncate=False)  # Display.

print("""Grouped Aggregate Pandas UDFs are perfect for:
- Median (not directly available as built-in)
- Custom percentiles
- Coefficient of variation
- Weighted averages
- Any statistical measure not in Spark's built-in library""")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: applyInPandas (grouped map)
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: applyInPandas (Grouped Map)
# ============================================================
# Real-world: Complex per-group transformations returning DataFrames.

from pyspark.sql.functions import col  # Import col.
from pyspark.sql.types import (  # Import types.
    StructType, StructField, StringType, DoubleType, IntegerType
)  # End types.
import pandas as pd  # Pandas.
import numpy as np  # NumPy.

# applyInPandas: each group becomes a pandas DataFrame.
# Function signature: pd.DataFrame -> pd.DataFrame

# Example 1: Z-score normalization per group.
def normalize_per_group(pdf: pd.DataFrame) -> pd.DataFrame:
    """Z-score normalize values within each group."""
    mean = pdf["value"].mean()  # Group mean.
    std = pdf["value"].std()  # Group std.
    pdf["z_score"] = ((pdf["value"] - mean) / std).round(3) if std > 0 else 0  # Z-score.
    pdf["group_mean"] = round(mean, 2)  # Add group mean.
    pdf["group_std"] = round(std, 2)  # Add group std.
    return pdf  # Return enriched DataFrame.

# Define output schema (MUST match output DataFrame columns).
normalize_schema = StructType([
    StructField("group", StringType()),  # Group column.
    StructField("id", IntegerType()),  # ID column.
    StructField("value", DoubleType()),  # Original value.
    StructField("z_score", DoubleType()),  # Computed z-score.
    StructField("group_mean", DoubleType()),  # Group mean.
    StructField("group_std", DoubleType()),  # Group std.
])  # End schema.

print("=== applyInPandas: Z-Score Per Group ===")  # Print heading.
data = spark.createDataFrame([
    ("A", 1, 10.0), ("A", 2, 20.0), ("A", 3, 30.0), ("A", 4, 40.0),
    ("B", 5, 100.0), ("B", 6, 200.0), ("B", 7, 300.0), ("B", 8, 400.0),
], ["group", "id", "value"])  # Grouped data.

result = data.groupBy("group").applyInPandas(
    normalize_per_group,  # Function to apply per group.
    schema=normalize_schema,  # Output schema.
)
result.show(truncate=False)  # Display.

# Example 2: Top-N per group.
def top_n_per_group(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return top 2 records per group by value."""
    return pdf.nlargest(2, "value")  # Top 2.

top_n_schema = StructType([
    StructField("group", StringType()),
    StructField("id", IntegerType()),
    StructField("value", DoubleType()),
])

print("=== applyInPandas: Top-2 Per Group ===")  # Print heading.
data.groupBy("group").applyInPandas(
    top_n_per_group, schema=top_n_schema
).show(truncate=False)  # Display top 2 per group.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: mapInPandas (partition map)
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: mapInPandas (Partition Map)
# ============================================================
# Real-world: Process entire partitions as pandas DataFrames.

from pyspark.sql.functions import col  # Import col.
from pyspark.sql.types import (  # Types.
    StructType, StructField, StringType, IntegerType, DoubleType
)  # End types.
import pandas as pd  # Pandas.
from typing import Iterator  # Type hint.

# mapInPandas: process partitions as pandas DataFrames.
# Signature: Iterator[pd.DataFrame] -> Iterator[pd.DataFrame]

def add_partition_stats(batch_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Add partition-level statistics to each row."""
    for pdf in batch_iter:  # Iterate over batches.
        if len(pdf) > 0:  # Non-empty batch.
            pdf["batch_size"] = len(pdf)  # How many rows in this batch.
            pdf["batch_mean"] = pdf["value"].mean().round(2)  # Batch mean.
            pdf["batch_max"] = pdf["value"].max()  # Batch max.
            pdf["pct_of_batch"] = (pdf["value"] / pdf["value"].sum() * 100).round(1)  # Percentage.
        yield pdf  # Yield enriched batch.

# Output schema.
map_schema = StructType([
    StructField("id", IntegerType()),  # Original.
    StructField("name", StringType()),  # Original.
    StructField("value", DoubleType()),  # Original.
    StructField("batch_size", IntegerType()),  # Added.
    StructField("batch_mean", DoubleType()),  # Added.
    StructField("batch_max", DoubleType()),  # Added.
    StructField("pct_of_batch", DoubleType()),  # Added.
])  # End schema.

print("=== mapInPandas: Partition-Level Enrichment ===")  # Print heading.
df = spark.createDataFrame([
    (1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Charlie", 300.0),
    (4, "Diana", 150.0), (5, "Eve", 250.0), (6, "Frank", 50.0),
], ["id", "name", "value"])  # Data.

result = df.mapInPandas(add_partition_stats, schema=map_schema)  # Apply.
result.show(truncate=False)  # Display.

# Example 2: Filtering with complex logic.
def remove_outliers(batch_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Remove outliers (> 2 std from mean) per batch."""
    for pdf in batch_iter:  # Iterate.
        if len(pdf) > 0 and "value" in pdf.columns:  # Valid batch.
            mean = pdf["value"].mean()  # Mean.
            std = pdf["value"].std()  # Std.
            if std > 0:  # Has variation.
                mask = (pdf["value"] - mean).abs() <= 2 * std  # Within 2 std.
                yield pdf[mask]  # Keep non-outliers.
            else:
                yield pdf  # No variation, keep all.
        else:
            yield pdf  # Empty batch, pass through.

filter_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("value", DoubleType()),
])

print("=== mapInPandas: Remove Outliers ===")  # Print heading.
outlier_df = spark.createDataFrame([
    (1, "A", 10.0), (2, "B", 12.0), (3, "C", 11.0),
    (4, "D", 100.0),  # Outlier!
    (5, "E", 9.0), (6, "F", 13.0),
], ["id", "name", "value"]).repartition(1)  # Single partition for demo.

outlier_df.mapInPandas(remove_outliers, schema=filter_schema).show()  # Outlier removed.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Performance comparison
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Performance Comparison
# ============================================================
# Real-world: Benchmarking Regular UDF vs Pandas UDF vs Built-in.

from pyspark.sql.functions import col, udf, pandas_udf, upper, length  # Imports.
from pyspark.sql.types import StringType, IntegerType  # Types.
import pandas as pd  # Pandas.
import time  # Timing.

# Create benchmark data.
bench_df = spark.range(200000).select(  # 200K rows.
    col("id"),  # Keep.
    (col("id").cast("string")).alias("text"),  # Text column.
)
bench_df.cache()  # Cache.
bench_df.count()  # Materialize.

# === Method 1: Regular Python UDF ===
@udf(returnType=IntegerType())  # Regular UDF.
def regular_udf_len(s):
    """Regular UDF: string length."""
    return len(s) if s else 0  # Length.

# === Method 2: Pandas UDF (vectorized) ===
@pandas_udf(IntegerType())  # Pandas UDF.
def pandas_udf_len(s: pd.Series) -> pd.Series:
    """Pandas UDF: string length (vectorized)."""
    return s.str.len().fillna(0).astype(int)  # Vectorized length.

# === Method 3: Built-in function ===
# Just use: length(col("text"))

# Benchmark.
print("=== Performance Benchmark: 200K Rows ===")  # Print heading.

# Built-in.
start = time.time()  # Timer.
bench_df.select(length(col("text")).alias("len")).collect()  # Execute.
builtin_time = time.time() - start  # Elapsed.

# Pandas UDF.
start = time.time()  # Timer.
bench_df.select(pandas_udf_len(col("text")).alias("len")).collect()  # Execute.
pandas_time = time.time() - start  # Elapsed.

# Regular UDF.
start = time.time()  # Timer.
bench_df.select(regular_udf_len(col("text")).alias("len")).collect()  # Execute.
regular_time = time.time() - start  # Elapsed.

# Results.
print(f"Built-in length():    {builtin_time:.3f}s (baseline)")  # Display.
print(f"Pandas UDF:           {pandas_time:.3f}s ({pandas_time/builtin_time:.1f}x built-in)")  # Display.
print(f"Regular UDF:          {regular_time:.3f}s ({regular_time/builtin_time:.1f}x built-in)")  # Display.
print(f"\nPandas UDF is ~{regular_time/pandas_time:.1f}x faster than Regular UDF!")  # Ratio.

print("\n=== Performance Hierarchy ===")  # Print heading.
print("1. Built-in functions  (fastest - Catalyst + Photon)")
print("2. Pandas UDF          (vectorized - Arrow batches)")
print("3. Regular Python UDF  (slowest - row-by-row pickle)")

bench_df.unpersist()  # Cleanup.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: ML model inference with Pandas UDF
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: ML Model Inference with Pandas UDF
# ============================================================
# Real-world: Apply trained model for prediction using vectorized UDF.

from pyspark.sql.functions import col, pandas_udf, struct  # Imports.
from pyspark.sql.types import DoubleType, StringType  # Types.
import pandas as pd  # Pandas.
import numpy as np  # NumPy.
from typing import Iterator, Tuple  # Types.

# Simulate a "trained model" (in production, load from MLflow/pickle).
class SimpleModel:
    """Simulated ML model for demonstration."""
    def __init__(self):  # Initialize.
        # These would be learned parameters.
        self.coefficients = np.array([0.3, 0.5, 0.2])  # Feature weights.
        self.intercept = 10.0  # Bias.
        self.threshold = 50.0  # Classification threshold.
    
    def predict(self, features: np.ndarray) -> np.ndarray:  # Predict.
        """Predict score from features."""
        scores = features @ self.coefficients + self.intercept  # Linear model.
        return scores  # Return predictions.
    
    def classify(self, scores: np.ndarray) -> np.ndarray:  # Classify.
        """Classify based on threshold."""
        return np.where(scores >= self.threshold, "HIGH", "LOW")  # Binary.

# Iterator pattern: load model ONCE, predict on all batches.
@pandas_udf(DoubleType())  # Returns predictions.
def predict_score(
    batch_iter: Iterator[Tuple[pd.Series, pd.Series, pd.Series]]
) -> Iterator[pd.Series]:
    """Apply model to predict scores. Model loaded once."""
    model = SimpleModel()  # Load model ONCE (expensive in real scenario).
    for feature1, feature2, feature3 in batch_iter:  # Process batches.
        # Stack features into matrix.
        features = np.column_stack([  # Create feature matrix.
            feature1.fillna(0).values,  # Feature 1.
            feature2.fillna(0).values,  # Feature 2.
            feature3.fillna(0).values,  # Feature 3.
        ])  # Shape: (batch_size, 3).
        predictions = model.predict(features)  # Batch predict.
        yield pd.Series(predictions.round(2))  # Yield predictions.

# Apply model.
print("=== ML Model Inference with Pandas UDF ===")  # Print heading.
customers = spark.createDataFrame([
    (1, "Alice", 80.0, 60.0, 90.0),
    (2, "Bob", 30.0, 20.0, 40.0),
    (3, "Charlie", 95.0, 85.0, 70.0),
    (4, "Diana", 50.0, 50.0, 50.0),
    (5, "Eve", 10.0, 5.0, 15.0),
], ["id", "name", "engagement", "spending", "tenure"])  # Customer features.

customers.select(
    col("id"), col("name"),  # Keep context.
    col("engagement"), col("spending"), col("tenure"),  # Features.
    predict_score(col("engagement"), col("spending"), col("tenure")).alias("predicted_score"),  # Prediction.
).show(truncate=False)  # Display predictions.

print("""This pattern is ideal for:
- Loading trained ML models (sklearn, XGBoost, PyTorch)
- Batch inference on millions of rows
- Model loaded ONCE per partition worker
- 10-100x faster than row-by-row regular UDF""")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Time-series with applyInPandas
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Time-Series with applyInPandas
# ============================================================
# Real-world: Per-device time-series analysis using full pandas power.

from pyspark.sql.functions import col  # Import col.
from pyspark.sql.types import (  # Types.
    StructType, StructField, StringType, DoubleType, IntegerType
)  # End types.
import pandas as pd  # Pandas.
import numpy as np  # NumPy.

# Time-series processing per device.
def compute_rolling_stats(pdf: pd.DataFrame) -> pd.DataFrame:
    """Compute rolling statistics per device."""
    pdf = pdf.sort_values("timestamp")  # Sort by time.
    # Rolling calculations (pandas window functions).
    pdf["rolling_mean_3"] = pdf["value"].rolling(window=3, min_periods=1).mean().round(2)  # 3-point mean.
    pdf["rolling_std_3"] = pdf["value"].rolling(window=3, min_periods=1).std().round(2)  # 3-point std.
    pdf["pct_change"] = (pdf["value"].pct_change() * 100).round(1)  # Percent change.
    # Anomaly detection: value > mean + 2*std.
    overall_mean = pdf["value"].mean()  # Device mean.
    overall_std = pdf["value"].std()  # Device std.
    pdf["is_anomaly"] = (  # Flag anomalies.
        (pdf["value"] > overall_mean + 2 * overall_std) |
        (pdf["value"] < overall_mean - 2 * overall_std)
    ).astype(int)  # 1 = anomaly, 0 = normal.
    return pdf  # Return enriched.

# Output schema.
ts_schema = StructType([
    StructField("device", StringType()),  # Group key.
    StructField("timestamp", IntegerType()),  # Time.
    StructField("value", DoubleType()),  # Original.
    StructField("rolling_mean_3", DoubleType()),  # Rolling mean.
    StructField("rolling_std_3", DoubleType()),  # Rolling std.
    StructField("pct_change", DoubleType()),  # Pct change.
    StructField("is_anomaly", IntegerType()),  # Anomaly flag.
])  # End schema.

print("=== Time-Series Analysis per Device ===")  # Print heading.
iot_data = spark.createDataFrame([
    ("sensor-A", 1, 23.5), ("sensor-A", 2, 24.0), ("sensor-A", 3, 23.8),
    ("sensor-A", 4, 50.0),  # Anomaly!
    ("sensor-A", 5, 24.2), ("sensor-A", 6, 23.9),
    ("sensor-B", 1, 100.0), ("sensor-B", 2, 102.0), ("sensor-B", 3, 99.0),
    ("sensor-B", 4, 101.0), ("sensor-B", 5, 98.0), ("sensor-B", 6, 103.0),
], ["device", "timestamp", "value"])  # IoT data.

result = iot_data.groupBy("device").applyInPandas(
    compute_rolling_stats,  # Apply per group.
    schema=ts_schema,  # Output schema.
)

result.orderBy("device", "timestamp").show(truncate=False)  # Display.

print("""applyInPandas is perfect for:
- Time-series: rolling windows, ARIMA, interpolation
- Statistical tests per group (t-test, correlation)
- Complex transformations requiring sort order
- Any logic that benefits from full pandas DataFrame API""")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production Pandas UDF patterns
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Production Pandas UDF Patterns
# ============================================================
# Real-world: Best practices and reusable patterns.

from pyspark.sql.functions import col, pandas_udf, lit  # Imports.
from pyspark.sql.types import (  # Types.
    StringType, DoubleType, ArrayType, IntegerType,
    StructType, StructField
)  # End types.
import pandas as pd  # Pandas.
import numpy as np  # NumPy.

# === Pattern 1: Pandas UDF with error handling ===
@pandas_udf(DoubleType())  # Double return.
def safe_log_transform(values: pd.Series) -> pd.Series:
    """Log transform with safe handling of zero/negative."""
    # Replace zero/negative with NaN, then log.
    safe_values = values.where(values > 0, other=np.nan)  # Zero/neg -> NaN.
    return np.log(safe_values)  # Log (NaN stays NaN -> becomes NULL).

print("=== Pattern 1: Safe Log Transform ===")  # Print heading.
nums = spark.createDataFrame([
    (1.0,), (10.0,), (100.0,), (0.0,), (-5.0,), (None,)
], "value DOUBLE")  # Numbers.

nums.select(
    col("value"),  # Original.
    safe_log_transform(col("value")).alias("log_value"),  # Safe log.
).show()  # Display.

# === Pattern 2: String cleaning pipeline ===
@pandas_udf(StringType())  # String return.
def clean_text_pipeline(texts: pd.Series) -> pd.Series:
    """Vectorized text cleaning pipeline."""
    return (
        texts
        .str.strip()  # Remove whitespace.
        .str.lower()  # Lowercase.
        .str.replace(r'[^a-z0-9\s]', '', regex=True)  # Remove special chars.
        .str.replace(r'\s+', ' ', regex=True)  # Collapse whitespace.
    )  # Chained pandas string operations.

print("=== Pattern 2: Text Cleaning Pipeline ===")  # Print heading.
dirty = spark.createDataFrame([
    ("  Hello, World!  ",), ("UPPER   CASE!!!",), ("special@#$chars",), (None,)
], ["text"])  # Dirty text.

dirty.select(
    col("text"),  # Original.
    clean_text_pipeline(col("text")).alias("cleaned"),  # Cleaned.
).show(truncate=False)  # Display.

# === Pattern 3: Feature engineering with multiple outputs ===
def compute_features(pdf: pd.DataFrame) -> pd.DataFrame:
    """Compute multiple features per group."""
    result = pd.DataFrame({  # Build result.
        "category": [pdf["category"].iloc[0]],  # Group key.
        "count": [len(pdf)],  # Row count.
        "mean_price": [pdf["price"].mean().round(2)],  # Mean.
        "median_price": [float(pdf["price"].median())],  # Median.
        "price_range": [float(pdf["price"].max() - pdf["price"].min())],  # Range.
        "cv": [float(pdf["price"].std() / pdf["price"].mean() * 100) if pdf["price"].mean() > 0 else 0],  # CV.
    })
    return result  # Return summary.

feature_schema = StructType([
    StructField("category", StringType()),
    StructField("count", IntegerType()),
    StructField("mean_price", DoubleType()),
    StructField("median_price", DoubleType()),
    StructField("price_range", DoubleType()),
    StructField("cv", DoubleType()),
])

print("=== Pattern 3: Group Feature Engineering ===")  # Print heading.
products = spark.createDataFrame([
    ("Electronics", 999.0), ("Electronics", 499.0), ("Electronics", 1499.0),
    ("Books", 15.0), ("Books", 25.0), ("Books", 35.0), ("Books", 20.0),
    ("Clothing", 50.0), ("Clothing", 80.0),
], ["category", "price"])  # Products.

products.groupBy("category").applyInPandas(
    compute_features, schema=feature_schema
).show(truncate=False)  # Display.

print("✅ Pandas UDFs mastery complete!")  # Done.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Pandas UDFs
# MAGIC
# MAGIC ### Mistake 1: Input/output length mismatch (Series→Series)
# MAGIC ```python
# MAGIC # WRONG — output Series has different length than input!
# MAGIC @pandas_udf(DoubleType())
# MAGIC def bad_udf(s: pd.Series) -> pd.Series:
# MAGIC     return s.dropna()  # SHORTER than input! Error!
# MAGIC
# MAGIC # CORRECT — same length, use fillna instead.
# MAGIC @pandas_udf(DoubleType())
# MAGIC def good_udf(s: pd.Series) -> pd.Series:
# MAGIC     return s.fillna(0)  # Same length as input.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Wrong schema in applyInPandas
# MAGIC ```python
# MAGIC # The schema MUST exactly match the columns returned by your function.
# MAGIC # Missing/extra columns = runtime error.
# MAGIC # Column ORDER matters. Column NAMES must match schema field names.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Forgetting NULL → NaN conversion
# MAGIC ```python
# MAGIC # In Pandas UDFs, Spark NULL becomes pandas NaN (not None!).
# MAGIC # Check with: pd.isna(value) or series.isna()
# MAGIC # NOT: value is None (this won't catch NaN!)
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Using regular UDF syntax
# MAGIC ```python
# MAGIC # WRONG — @udf is row-by-row!
# MAGIC @udf(returnType=StringType())
# MAGIC def slow_func(x): return x.upper()
# MAGIC
# MAGIC # CORRECT — @pandas_udf is vectorized!
# MAGIC @pandas_udf(StringType())
# MAGIC def fast_func(s: pd.Series) -> pd.Series:
# MAGIC     return s.str.upper()  # Vectorized!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not specifying type hints
# MAGIC ```python
# MAGIC # Modern pandas UDFs require type hints to determine behavior.
# MAGIC # Series -> Series = scalar UDF
# MAGIC # Iterator[Series] -> Iterator[Series] = batched
# MAGIC # Without hints, Spark may guess wrong type.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Pandas UDF Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Create a Series→Series pandas UDF that lowercases strings.
# MAGIC 2. Create a grouped aggregate pandas UDF for computing median.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Add NULL handling with `.fillna()` to the Series UDF.
# MAGIC 4. Change the aggregate UDF to compute 90th percentile.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Chain multiple pandas string operations in one UDF.
# MAGIC 6. Use applyInPandas to compute rolling average per group.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Build a feature scaler: min-max normalization per group using applyInPandas.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build an anomaly detector: per-device z-score flagging with rolling windows.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design an ML inference pipeline: load model once, predict on batches.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Benchmark: Regular UDF vs Pandas UDF vs Built-in on string/math/date operations.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Test: empty groups, all-NULL columns, very large batches, type mismatches.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build a complete feature engineering pipeline: numeric scaling, text cleaning, categorical encoding.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create a guide: "Which Pandas UDF type for which scenario?"

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all.
from pyspark.sql.types import *  # All types.
import pandas as pd  # Pandas.
import numpy as np  # NumPy.

# --- Level 1: Basic Series -> Series ---
print("=== Level 1: Lowercase Pandas UDF ===")  # Print heading.

@pandas_udf(StringType())  # Vectorized.
def pandas_lower(s: pd.Series) -> pd.Series:
    """Vectorized lowercase."""
    return s.str.lower()  # Vectorized string op.

spark.createDataFrame([("HELLO",), ("WORLD",), (None,)], ["word"]).select(
    col("word"),  # Original.
    pandas_lower(col("word")).alias("lowered"),  # Lowered.
).show()  # Display.

# --- Level 2: 90th percentile aggregate ---
print("=== Level 2: 90th Percentile ===")  # Print heading.

@pandas_udf(DoubleType())  # Aggregate.
def p90(values: pd.Series) -> float:
    """90th percentile."""
    return float(values.quantile(0.9))  # 90th percentile.

sales = spark.createDataFrame([
    ("A", 10), ("A", 20), ("A", 30), ("A", 100),
    ("B", 5), ("B", 15), ("B", 25),
], ["group", "value"])  # Sales.

sales.groupBy("group").agg(
    p90(col("value")).alias("p90_value"),  # 90th percentile.
).show()  # Display.

# --- Level 4: Min-max normalization per group ---
print("=== Level 4: Min-Max Normalization ===")  # Print heading.

def minmax_normalize(pdf: pd.DataFrame) -> pd.DataFrame:
    """Min-max normalize value column per group."""
    min_val = pdf["value"].min()  # Group min.
    max_val = pdf["value"].max()  # Group max.
    range_val = max_val - min_val  # Range.
    pdf["normalized"] = ((pdf["value"] - min_val) / range_val).round(3) if range_val > 0 else 0.5  # Normalize.
    return pdf  # Return.

norm_schema = StructType([
    StructField("group", StringType()),
    StructField("id", IntegerType()),
    StructField("value", DoubleType()),
    StructField("normalized", DoubleType()),
])

data = spark.createDataFrame([
    ("A", 1, 10.0), ("A", 2, 50.0), ("A", 3, 30.0),
    ("B", 4, 100.0), ("B", 5, 200.0), ("B", 6, 150.0),
], ["group", "id", "value"])

data.groupBy("group").applyInPandas(
    minmax_normalize, schema=norm_schema
).orderBy("group", "id").show()  # Display.

# --- Level 8: Edge cases ---
print("=== Level 8: Edge Cases ===")  # Print heading.

@pandas_udf(DoubleType())  # Safe UDF.
def safe_divide_pandas(a: pd.Series, b: pd.Series) -> pd.Series:
    """Safe vectorized division."""
    result = a / b  # May produce inf.
    result = result.replace([np.inf, -np.inf], np.nan)  # inf -> NaN -> NULL.
    return result  # Return.

edge = spark.createDataFrame([
    (10.0, 2.0), (5.0, 0.0), (None, 3.0), (8.0, None)
], "a DOUBLE, b DOUBLE")

edge.select(
    col("a"), col("b"),  # Inputs.
    safe_divide_pandas(col("a"), col("b")).alias("result"),  # Safe divide.
).show()  # Display.

print("✅ All homework solutions complete!")  # Completion.