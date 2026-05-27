# Databricks notebook source
# DBTITLE 1,NB_43 Header
# MAGIC %md
# MAGIC # NB_43 — Data Profiling
# MAGIC
# MAGIC **Module 7: Data Cleaning & Quality** | Notebook 43 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * What data profiling is and why it's the first step
# MAGIC * describe() and summary() for quick statistics
# MAGIC * Column-level profiling: types, nulls, distinct, min/max/mean
# MAGIC * Distribution analysis: histograms, quantiles, skewness
# MAGIC * Correlation and relationship discovery
# MAGIC * Pattern detection: regex for format validation
# MAGIC * Building a reusable DataProfiler class
# MAGIC * Automated profiling reports
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Foundation for all cleaning tasks)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is Data Profiling?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is Data Profiling? (Real-World Analogy)
# MAGIC
# MAGIC ### 🔍 The Home Inspector
# MAGIC
# MAGIC Before buying a house, an inspector checks everything. Data profiling is the same for your dataset:
# MAGIC
# MAGIC | Home Inspection | Data Profiling | What You Learn |
# MAGIC |---|---|---|
# MAGIC | Count rooms | `df.count()`, `len(df.columns)` | Dataset dimensions |
# MAGIC | Check for damage | NULL counts, invalid values | Data quality issues |
# MAGIC | Measure dimensions | min, max, mean, std | Value distributions |
# MAGIC | Check plumbing | Data types, format validation | Schema correctness |
# MAGIC | Look for duplicates | Distinct counts, duplicate rows | Uniqueness |
# MAGIC | Neighborhood context | Correlations, relationships | Column dependencies |
# MAGIC
# MAGIC ### Why Profile Before Cleaning?
# MAGIC 1. **Discover unknowns:** Find issues you didn't know existed
# MAGIC 2. **Prioritize effort:** Focus on columns with worst quality
# MAGIC 3. **Set baselines:** Know what "clean" should look like
# MAGIC 4. **Validate assumptions:** Is the data what you expected?
# MAGIC 5. **Document:** Create a data dictionary for the team

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Data Profiling Works
# MAGIC %md
# MAGIC ## SECTION 2 — How Data Profiling Works
# MAGIC
# MAGIC ### Profiling Dimensions
# MAGIC ```
# MAGIC ┌─────────────────┬─────────────────┬─────────────────┐
# MAGIC │ STRUCTURE       │ QUALITY          │ STATISTICS       │
# MAGIC │ row count       │ null count/%     │ min/max/mean     │
# MAGIC │ column count    │ duplicate count  │ stddev/variance  │
# MAGIC │ data types      │ invalid formats  │ quantiles        │
# MAGIC │ schema          │ out-of-range     │ skewness/kurtosis│
# MAGIC ├─────────────────┼─────────────────┼─────────────────┤
# MAGIC │ UNIQUENESS      │ PATTERNS         │ RELATIONSHIPS    │
# MAGIC │ distinct count  │ regex validation │ correlations     │
# MAGIC │ unique ratio    │ format detection │ dependencies     │
# MAGIC │ top-N values    │ length stats     │ join candidates  │
# MAGIC └─────────────────┴─────────────────┴─────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Profiling Strategy
# MAGIC 1. **Start broad:** Row count, column count, types
# MAGIC 2. **Go column-by-column:** Nulls, distinct, stats per column
# MAGIC 3. **Check distributions:** Identify skew, outliers
# MAGIC 4. **Validate formats:** Regex patterns for strings
# MAGIC 5. **Find relationships:** Correlations between numeric columns
# MAGIC 6. **Generate report:** Summarize findings for action

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Quick profiling with describe/summary
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Quick Profiling
# ============================================================
# Real-world: First look at any new dataset.

from pyspark.sql import SparkSession  # Import SparkSession.
from pyspark.sql.functions import (  # Import profiling functions.
    col, count, countDistinct, sum as spark_sum, avg, min as spark_min,
    max as spark_max, stddev, when, isnan, isnull, lit, round as spark_round,
    length, desc
)  # End imports.

spark = SparkSession.builder.getOrCreate()  # Get active session.

# Create realistic messy dataset.
import random  # Random for data generation.
random.seed(42)  # Reproducible.

data = []
for i in range(100):  # 100 rows.
    data.append((
        i + 1,  # id.
        random.choice(["Alice", "Bob", "Charlie", "Diana", None, ""]),  # name.
        random.choice(["M", "F", "Other", None, "X", "unknown"]),  # gender.
        random.randint(18, 80) if random.random() > 0.1 else None,  # age.
        round(random.uniform(20000, 150000), 2) if random.random() > 0.05 else None,  # salary.
        random.choice(["Engineering", "Marketing", "Sales", "HR", None]),  # dept.
        f"{random.choice(['alice','bob','bad'])}{'' if random.random() > 0.2 else '@'}{'co.com' if random.random() > 0.3 else ''}",  # email.
        f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",  # join_date.
    ))

df = spark.createDataFrame(data, ["id", "name", "gender", "age", "salary", "dept", "email", "join_date"])  # Create DataFrame.

# Quick profiling.
print("=== Dataset Overview ===")  # Print heading.
print(f"Rows: {df.count()}")  # Row count.
print(f"Columns: {len(df.columns)}")  # Column count.
print(f"Column names: {df.columns}")  # Column list.

# Schema.
print("\n=== Schema ===")  # Print heading.
df.printSchema()  # Display schema.

# describe() — basic stats for all columns.
print("=== describe() — Quick Statistics ===")  # Print heading.
df.describe().show(truncate=False)  # count, mean, stddev, min, max.

# summary() — extended with percentiles.
print("=== summary() — Extended Statistics ===")  # Print heading.
df.select("age", "salary").summary(
    "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
).show(truncate=False)  # Display extended stats.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Column-level null and distinct profiling
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Column-Level Profiling
# ============================================================
# Real-world: Understanding data quality per column.

from pyspark.sql.functions import (  # Import functions.
    col, count, countDistinct, sum as spark_sum, when, isnull,
    isnan, lit, round as spark_round, trim
)  # End imports.

total_rows = df.count()  # Total row count.

# NULL and distinct analysis per column.
print("=== Column Quality Profile ===")  # Print heading.
print(f"{'Column':<12} {'Type':<8} {'Nulls':<8} {'Null%':<8} {'Empty':<8} {'Distinct':<10} {'Uniq%':<8}")  # Header.
print("-" * 72)  # Separator.

for col_name in df.columns:  # Iterate columns.
    col_type = str(df.schema[col_name].dataType)  # Data type.
    null_count = df.filter(col(col_name).isNull()).count()  # NULL count.
    null_pct = round(null_count / total_rows * 100, 1)  # NULL percentage.
    # Empty string count (for string columns).
    if "String" in col_type:
        empty_count = df.filter((trim(col(col_name)) == "") & col(col_name).isNotNull()).count()
    else:
        empty_count = 0
    distinct_count = df.select(col_name).distinct().count()  # Distinct values.
    unique_pct = round(distinct_count / total_rows * 100, 1)  # Uniqueness.
    print(f"{col_name:<12} {col_type[:7]:<8} {null_count:<8} {null_pct:<8} {empty_count:<8} {distinct_count:<10} {unique_pct:<8}")  # Print row.

# Top values per categorical column.
print("\n=== Top Values for Categorical Columns ===")  # Print heading.
for col_name in ["name", "gender", "dept"]:  # Categorical columns.
    print(f"\n--- {col_name} ---")  # Column header.
    df.groupBy(col_name).count().orderBy(desc("count")).show(10, truncate=False)  # Top values.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Numeric distribution profiling
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Numeric Distribution Profiling
# ============================================================
# Real-world: Understanding value ranges and distributions.

from pyspark.sql.functions import (  # Import functions.
    col, avg, stddev, min as spark_min, max as spark_max,
    percentile_approx, skewness, kurtosis, count, when,
    round as spark_round, expr
)  # End imports.

# Detailed numeric profiling.
print("=== Numeric Column Deep Profile ===")  # Print heading.
numeric_cols = ["age", "salary"]  # Numeric columns.

for col_name in numeric_cols:  # Iterate numeric columns.
    print(f"\n{'='*50}")  # Separator.
    print(f"  Column: {col_name}")  # Column name.
    print(f"{'='*50}")  # Separator.
    
    stats = df.select(
        count(col(col_name)).alias("non_null_count"),  # Non-null count.
        spark_round(avg(col(col_name)), 2).alias("mean"),  # Mean.
        spark_round(stddev(col(col_name)), 2).alias("stddev"),  # Stddev.
        spark_min(col(col_name)).alias("min"),  # Minimum.
        spark_max(col(col_name)).alias("max"),  # Maximum.
        spark_round(skewness(col(col_name)), 3).alias("skewness"),  # Skewness.
        spark_round(kurtosis(col(col_name)), 3).alias("kurtosis"),  # Kurtosis.
        percentile_approx(col(col_name), 0.25).alias("p25"),  # 25th percentile.
        percentile_approx(col(col_name), 0.50).alias("median"),  # Median.
        percentile_approx(col(col_name), 0.75).alias("p75"),  # 75th percentile.
    ).first()  # Collect as Row.
    
    for key in stats.asDict():  # Print each stat.
        print(f"  {key:<15}: {stats[key]}")  # Display.

# Value range buckets.
print("\n=== Age Distribution Buckets ===")  # Print heading.
df.filter(col("age").isNotNull()).select(
    when(col("age") < 25, "18-24")
        .when(col("age") < 35, "25-34")
        .when(col("age") < 45, "35-44")
        .when(col("age") < 55, "45-54")
        .otherwise("55+").alias("age_bucket"),
).groupBy("age_bucket").count().orderBy("age_bucket").show()  # Display buckets.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Pattern and format profiling
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Pattern and Format Profiling
# ============================================================
# Real-world: Detecting format issues in string columns.

from pyspark.sql.functions import (  # Import functions.
    col, length, when, regexp_extract, trim, count, sum as spark_sum,
    round as spark_round, lit
)  # End imports.

# Email format validation.
print("=== Email Format Validation ===")  # Print heading.
email_profile = df.select(
    count("*").alias("total"),  # Total.
    spark_sum(col("email").isNull().cast("int")).alias("null_emails"),  # NULLs.
    spark_sum(col("email").rlike(r'^[\w.]+@[\w.]+\.[a-zA-Z]{2,}$').cast("int")).alias("valid_emails"),  # Valid format.
    spark_sum((~col("email").rlike(r'^[\w.]+@[\w.]+\.[a-zA-Z]{2,}$') & col("email").isNotNull()).cast("int")).alias("invalid_emails"),  # Invalid.
)
email_profile.show(truncate=False)  # Display.

# Show invalid email examples.
print("=== Invalid Email Examples ===")  # Print heading.
df.filter(
    ~col("email").rlike(r'^[\w.]+@[\w.]+\.[a-zA-Z]{2,}$') & col("email").isNotNull()
).select("id", "email").show(10, truncate=False)  # Show bad emails.

# String length profiling.
print("=== String Length Analysis ===")  # Print heading.
for col_name in ["name", "email"]:  # String columns.
    df.filter(col(col_name).isNotNull()).select(
        lit(col_name).alias("column"),  # Column name.
        spark_round(avg(length(col(col_name))), 1).alias("avg_length"),  # Average.
        spark_min(length(col(col_name))).alias("min_length"),  # Min.
        spark_max(length(col(col_name))).alias("max_length"),  # Max.
    ).show(truncate=False)  # Display.

# Gender value standardization check.
print("=== Gender Value Audit ===")  # Print heading.
df.groupBy("gender").count().orderBy(desc("count")).show()  # Show all values.
print("Expected: M, F, Other. Found non-standard values above.")  # Note.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Correlation and relationships
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Correlation and Relationships
# ============================================================
# Real-world: Finding relationships between columns.

from pyspark.sql.functions import (  # Import functions.
    col, corr, count, countDistinct, when, lit, round as spark_round
)  # End imports.

# Correlation matrix for numeric columns.
print("=== Correlation Matrix ===")  # Print heading.
numeric_cols = ["id", "age", "salary"]  # Numeric columns.

# Build correlation matrix.
corr_data = []  # Accumulator.
for c1 in numeric_cols:  # Row.
    row = {"column": c1}  # Start row.
    for c2 in numeric_cols:  # Column.
        correlation = df.select(corr(col(c1), col(c2))).first()[0]  # Compute.
        row[c2] = round(correlation, 3) if correlation else None  # Add.
    corr_data.append(row)  # Append row.

# Display as DataFrame.
corr_df = spark.createDataFrame(corr_data)  # Create DataFrame.
corr_df.show(truncate=False)  # Display correlation matrix.

# Functional dependency check: does dept determine salary range?
print("=== Salary by Department ===")  # Print heading.
df.filter(col("salary").isNotNull() & col("dept").isNotNull()).groupBy("dept").agg(
    count("*").alias("count"),  # Count.
    spark_round(avg("salary"), 0).alias("avg_salary"),  # Avg salary.
    spark_round(spark_min("salary"), 0).alias("min_salary"),  # Min.
    spark_round(spark_max("salary"), 0).alias("max_salary"),  # Max.
).show(truncate=False)  # Display.

# Uniqueness check: is ID a valid primary key?
print("=== Primary Key Candidates ===")  # Print heading.
for col_name in df.columns:  # Check each column.
    distinct = df.select(col_name).distinct().count()  # Distinct count.
    is_unique = distinct == total_rows  # All unique?
    has_nulls = df.filter(col(col_name).isNull()).count() > 0  # Has NULLs?
    if is_unique and not has_nulls:  # Valid PK.
        print(f"  ✅ {col_name}: Valid primary key (unique, no NULLs)")  # Valid.
    elif is_unique:  # Unique but has NULLs.
        print(f"  ⚠️ {col_name}: Unique but has NULLs")  # Warning.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Automated profiling report
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Automated Profiling Report
# ============================================================
# Real-world: One-function profiling for any DataFrame.

from pyspark.sql.functions import *  # Import all.
from pyspark.sql.types import StringType, NumericType  # Type checks.

def auto_profile(df, sample_size=5):
    """Generate comprehensive profile for any DataFrame."""
    total = df.count()  # Total rows.
    print(f"\n{'='*60}")  # Header.
    print(f"  AUTOMATED DATA PROFILE")
    print(f"{'='*60}")  # Header.
    print(f"  Rows: {total:,}")  # Row count.
    print(f"  Columns: {len(df.columns)}")  # Column count.
    
    # Duplicate rows.
    distinct_rows = df.distinct().count()  # Distinct rows.
    dupes = total - distinct_rows  # Duplicate count.
    print(f"  Duplicate rows: {dupes} ({round(dupes/total*100, 1)}%)")
    print(f"{'='*60}\n")  # Separator.
    
    # Per-column profile.
    print(f"{'Column':<12} {'Type':<10} {'Non-Null':<10} {'Null%':<7} {'Distinct':<10} {'Top Value'}")  # Header.
    print("-" * 75)  # Separator.
    
    for field in df.schema.fields:  # Iterate schema.
        col_name = field.name  # Column name.
        col_type = str(field.dataType)[:9]  # Type (truncated).
        non_null = df.filter(col(col_name).isNotNull()).count()  # Non-null.
        null_pct = round((total - non_null) / total * 100, 1)  # Null %.
        distinct = df.select(col_name).distinct().count()  # Distinct.
        # Top value.
        top = df.filter(col(col_name).isNotNull()).groupBy(col_name).count().orderBy(desc("count")).first()
        top_val = f"{top[0]} ({top[1]}x)" if top else "N/A"  # Format top.
        print(f"{col_name:<12} {col_type:<10} {non_null:<10} {null_pct:<7} {distinct:<10} {top_val}")  # Print.
    
    # Quality score.
    total_cells = total * len(df.columns)  # Total cells.
    null_cells = sum(df.filter(col(c).isNull()).count() for c in df.columns)  # Null cells.
    quality_score = round((1 - null_cells / total_cells) * 100, 1)  # Score.
    print(f"\n  📊 Overall Completeness Score: {quality_score}%")  # Score.
    print(f"{'='*60}")  # Footer.

# Apply automated profiling.
auto_profile(df)  # Run profile.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: DataProfiler class
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Reusable DataProfiler Class
# ============================================================
# Real-world: Production-grade profiling utility.

from pyspark.sql.functions import *  # Import all.
from pyspark.sql import DataFrame  # Type hint.

class DataProfiler:
    """Reusable data profiling class for PySpark DataFrames."""
    
    def __init__(self, df: DataFrame):  # Constructor.
        self.df = df  # Store DataFrame.
        self.total_rows = df.count()  # Cache row count.
        self.columns = df.columns  # Column list.
        self.schema = df.schema  # Schema.
    
    def null_report(self) -> DataFrame:  # NULL report.
        """Generate NULL report for all columns."""
        exprs = []  # Expression list.
        for c in self.columns:  # Iterate columns.
            exprs.append(sum(col(c).isNull().cast("int")).alias(f"{c}_nulls"))  # Count NULLs.
            exprs.append(round(sum(col(c).isNull().cast("int")) * 100.0 / lit(self.total_rows), 1).alias(f"{c}_pct"))  # Pct.
        return self.df.select(*exprs)  # Return report.
    
    def distinct_report(self) -> dict:  # Distinct report.
        """Count distinct values per column."""
        result = {}  # Accumulator.
        for c in self.columns:  # Iterate.
            result[c] = self.df.select(c).distinct().count()  # Count.
        return result  # Return dict.
    
    def numeric_stats(self, columns=None) -> DataFrame:  # Numeric stats.
        """Compute detailed stats for numeric columns."""
        cols = columns or [f.name for f in self.schema.fields if "Int" in str(f.dataType) or "Double" in str(f.dataType) or "Long" in str(f.dataType)]  # Auto-detect.
        stats_exprs = []  # Expressions.
        for c in cols:  # Iterate numeric columns.
            stats_exprs.extend([
                round(avg(c), 2).alias(f"{c}_mean"),  # Mean.
                round(stddev(c), 2).alias(f"{c}_std"),  # Std.
                min(c).alias(f"{c}_min"),  # Min.
                max(c).alias(f"{c}_max"),  # Max.
                round(skewness(c), 3).alias(f"{c}_skew"),  # Skewness.
            ])  # End expressions.
        return self.df.select(*stats_exprs)  # Return stats.
    
    def quality_score(self) -> float:  # Quality score.
        """Compute overall data completeness score."""
        total_cells = self.total_rows * len(self.columns)  # Total cells.
        null_cells = sum(self.df.filter(col(c).isNull()).count() for c in self.columns)  # Null cells.
        return round((1 - null_cells / total_cells) * 100, 1)  # Percentage.

# Apply DataProfiler.
print("=== DataProfiler in Action ===")  # Print heading.
profiler = DataProfiler(df)  # Create profiler.

print(f"Total rows: {profiler.total_rows}")  # Row count.
print(f"Quality score: {profiler.quality_score()}%")  # Quality.

print("\n=== NULL Report ===")  # Print heading.
profiler.null_report().show(truncate=False)  # Display NULLs.

print("\n=== Distinct Values ===")  # Print heading.
for col_name, distinct in profiler.distinct_report().items():  # Iterate.
    print(f"  {col_name}: {distinct} distinct values")  # Display.

print("\n=== Numeric Stats ===")  # Print heading.
profiler.numeric_stats().show(truncate=False)  # Display stats.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Cross-column validation
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Cross-Column Validation
# ============================================================
# Real-world: Business rules that span multiple columns.

from pyspark.sql.functions import (  # Import functions.
    col, when, count, sum as spark_sum, round as spark_round, lit
)  # End imports.

# Create data with cross-column issues.
hr_data = spark.createDataFrame([
    (1, "Alice", "Manager", 120000, 15, "Engineering"),
    (2, "Bob", "Intern", 95000, 0, "Sales"),  # Intern with high salary!
    (3, "Charlie", "Director", 45000, 20, "Marketing"),  # Director with low salary!
    (4, "Diana", "Engineer", 80000, 3, None),  # Missing dept.
    (5, "Eve", "Manager", 110000, -2, "HR"),  # Negative years!
    (6, "Frank", None, 70000, 5, "Engineering"),  # Missing title.
    (7, "Grace", "VP", 200000, 25, "Engineering"),  # Valid.
], ["id", "name", "title", "salary", "years_exp", "department"])  # HR data.

# Cross-column business rules.
print("=== Cross-Column Validation Rules ===")  # Print heading.
validation = hr_data.select(
    col("id"), col("name"), col("title"), col("salary"), col("years_exp"),  # Context.
    # Rule 1: Interns shouldn't earn > $60K.
    when((col("title") == "Intern") & (col("salary") > 60000), "FAIL")
        .otherwise("PASS").alias("rule_intern_salary"),
    # Rule 2: Directors should earn > $80K.
    when((col("title") == "Director") & (col("salary") < 80000), "FAIL")
        .otherwise("PASS").alias("rule_director_salary"),
    # Rule 3: Years of experience must be non-negative.
    when(col("years_exp") < 0, "FAIL")
        .otherwise("PASS").alias("rule_positive_years"),
    # Rule 4: Title and department both required.
    when(col("title").isNull() | col("department").isNull(), "FAIL")
        .otherwise("PASS").alias("rule_required_fields"),
)

validation.show(truncate=False)  # Display validation.

# Summary: how many failures per rule.
print("=== Rule Failure Summary ===")  # Print heading.
for rule_col in [c for c in validation.columns if c.startswith("rule_")]:
    fail_count = validation.filter(col(rule_col) == "FAIL").count()  # Count failures.
    print(f"  {rule_col}: {fail_count} failures")  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production profiling pipeline
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Production Profiling Pipeline
# ============================================================
# Real-world: Automated profiling that feeds into data quality dashboards.

from pyspark.sql.functions import *  # Import all.
from pyspark.sql.types import *  # All types.
from datetime import datetime  # Date.

def generate_profile_report(df, dataset_name="unknown"):
    """Generate a structured profiling report as a DataFrame."""
    total = df.count()  # Total rows.
    report_rows = []  # Accumulator.
    
    for field in df.schema.fields:  # Iterate columns.
        c = field.name  # Column name.
        col_type = str(field.dataType)  # Type.
        
        # Compute metrics.
        non_null = df.filter(col(c).isNotNull()).count()  # Non-null.
        null_count = total - non_null  # Null count.
        distinct = df.select(c).distinct().count()  # Distinct.
        
        # Type-specific stats.
        if "Int" in col_type or "Double" in col_type or "Long" in col_type:
            stats = df.select(avg(c), stddev(c), min(c), max(c)).first()  # Stats.
            mean_val = str(round(stats[0], 2)) if stats[0] else "N/A"  # Mean.
            std_val = str(round(stats[1], 2)) if stats[1] else "N/A"  # Std.
            min_val = str(stats[2])  # Min.
            max_val = str(stats[3])  # Max.
        else:
            mean_val = "N/A"  # Not numeric.
            std_val = "N/A"  # Not numeric.
            top_val = df.filter(col(c).isNotNull()).groupBy(c).count().orderBy(desc("count")).first()
            min_val = str(top_val[0])[:20] if top_val else "N/A"  # Top value.
            max_val = "N/A"  # Not applicable.
        
        report_rows.append((
            dataset_name,  # Dataset.
            c,  # Column.
            col_type,  # Type.
            total,  # Total rows.
            non_null,  # Non-null.
            null_count,  # Nulls.
            round(null_count / total * 100, 1),  # Null %.
            distinct,  # Distinct.
            round(distinct / total * 100, 1),  # Unique %.
            mean_val,  # Mean or top value.
            std_val,  # Std.
        ))
    
    # Create report DataFrame.
    report_schema = ["dataset", "column", "type", "total_rows", "non_null",
                     "null_count", "null_pct", "distinct", "unique_pct", "mean_or_top", "stddev"]
    report_df = spark.createDataFrame(report_rows, report_schema)  # Create.
    return report_df  # Return structured report.

# Generate report.
print("=== Structured Profiling Report ===")  # Print heading.
report = generate_profile_report(df, "employee_data")  # Generate.
report.show(truncate=False)  # Display.

# Flag problematic columns (>10% null or <5 distinct for string).
print("=== ⚠️ Columns Needing Attention ===")  # Print heading.
report.filter(
    (col("null_pct") > 10) | ((col("type").contains("String")) & (col("distinct") < 5))
).select("column", "null_pct", "distinct", "type").show(truncate=False)  # Problem cols.

print("✅ Data Profiling mastery complete!")  # Done.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes in Data Profiling
# MAGIC
# MAGIC ### Mistake 1: Profiling on the full dataset when sample suffices
# MAGIC ```python
# MAGIC # WRONG for exploration — profiling 100M rows takes forever!
# MAGIC df.describe().show()  # Full scan!
# MAGIC
# MAGIC # BETTER — sample first for exploration.
# MAGIC df.sample(0.01).describe().show()  # 1% sample for quick look.
# MAGIC # Use full dataset only for final quality report.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Confusing NULL with empty string
# MAGIC ```python
# MAGIC # count(col) skips NULLs but counts empty strings!
# MAGIC # A column could show 0% NULL but have 20% empty strings.
# MAGIC # Always check BOTH:
# MAGIC df.filter((col("name").isNull()) | (trim(col("name")) == "")).count()
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Ignoring data types
# MAGIC ```python
# MAGIC # A "numeric" column stored as STRING won't show in describe()!
# MAGIC # Always check schema first: df.printSchema()
# MAGIC # Then cast: df.withColumn("age", col("age").cast("int"))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Not checking for disguised NULLs
# MAGIC ```python
# MAGIC # Values like "N/A", "null", "undefined", -999 are disguised NULLs.
# MAGIC # Profile for these BEFORE computing statistics:
# MAGIC df.filter(col("value").isin("N/A", "null", "none", "-1", "unknown")).count()
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Running expensive profiling repeatedly
# MAGIC ```python
# MAGIC # Cache the DataFrame before profiling multiple columns!
# MAGIC df.cache()  # Cache first.
# MAGIC # Then run all your profiling queries.
# MAGIC # Unpersist when done: df.unpersist()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Data Profiling Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Use `describe()` and `summary()` on a DataFrame.
# MAGIC 2. Count NULLs and distinct values per column.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Add percentile analysis (10th, 25th, 50th, 75th, 90th).
# MAGIC 4. Profile string length statistics.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Combine NULL profiling + distinct counts + type analysis into one report.
# MAGIC 6. Use regex to validate email, phone, and date formats.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Profile a dataset you've never seen: discover types, ranges, patterns.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build a complete profiling pipeline: input any DataFrame, output structured report.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design a DataProfiler class with methods: null_report, type_report, stats_report, quality_score.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Profile 10M rows efficiently: use sampling, caching, and column batching.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Handle: all-NULL columns, single-value columns, very high cardinality, mixed types.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build automated quality gates: profile + assert rules + fail pipeline on violations.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create a "Data Profiling Checklist" for onboarding new datasets.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all.

# --- Level 1: Basic profiling ---
print("=== Level 1: describe + summary ===")  # Print heading.
df.describe().show(truncate=False)  # Basic stats.
df.select("age", "salary").summary("count", "25%", "50%", "75%").show()  # Percentiles.

# --- Level 2: Percentile analysis ---
print("=== Level 2: Detailed Percentiles ===")  # Print heading.
df.select(
    percentile_approx("age", 0.1).alias("p10"),  # 10th.
    percentile_approx("age", 0.25).alias("p25"),  # 25th.
    percentile_approx("age", 0.5).alias("p50"),  # 50th.
    percentile_approx("age", 0.75).alias("p75"),  # 75th.
    percentile_approx("age", 0.9).alias("p90"),  # 90th.
).show()  # Display.

# --- Level 5: Mini project ---
print("=== Level 5: One-Line Profile ===")  # Print heading.
def quick_profile(df):
    """One-function complete profile."""
    total = df.count()  # Total.
    print(f"Rows: {total}, Cols: {len(df.columns)}")  # Overview.
    print(f"Duplicates: {total - df.distinct().count()}")  # Dupes.
    for c in df.columns:  # Per column.
        nulls = df.filter(col(c).isNull()).count()  # NULLs.
        distinct = df.select(c).distinct().count()  # Distinct.
        print(f"  {c}: {nulls} nulls ({round(nulls/total*100,1)}%), {distinct} distinct")  # Print.

quick_profile(df)  # Run.

# --- Level 9: Quality gate ---
print("\n=== Level 9: Quality Gate ===")  # Print heading.
def quality_gate(df, max_null_pct=15.0, min_rows=10):
    """Fail if quality thresholds breached."""
    total = df.count()  # Total.
    if total < min_rows:  # Too few rows.
        print(f"❌ FAIL: Only {total} rows (need {min_rows}+)")
        return False
    failures = []  # Track.
    for c in df.columns:  # Check each.
        null_pct = df.filter(col(c).isNull()).count() / total * 100
        if null_pct > max_null_pct:  # Exceeds threshold.
            failures.append((c, round(null_pct, 1)))  # Record.
    if failures:
        print(f"❌ QUALITY GATE FAILED! {len(failures)} columns exceed {max_null_pct}% null:")
        for c, pct in failures: print(f"   {c}: {pct}%")
        return False
    print(f"✅ QUALITY GATE PASSED!")
    return True

quality_gate(df, max_null_pct=15.0)  # Run gate.

print("\n✅ All homework solutions complete!")  # Done.