# Databricks and PySpark - Complete Knowledge Transfer Guide

> **111 Notebooks | 20 Modules | Beginner to Expert**
> Platform: Databricks (Azure) | Runtime: DBR 17.3+
> Language: PySpark / Spark SQL

---

## How to Read This Guide

This document explains every concept in **simple English** with real-world analogies. No jargon without explanation. Think of it as a friendly textbook you can read on the bus.

**Convert to PDF/Word:**
```bash
pandoc Databricks_KT_Complete_Reference_Guide.md -o Guide.pdf
pandoc Databricks_KT_Complete_Reference_Guide.md -o Guide.docx
```

---

# MODULE 1: Spark Fundamentals (NB 1-8)

## NB 1: What is Apache Spark?

**Simple English:** Spark is a tool that processes huge amounts of data by splitting the work across many computers at once. Instead of one computer reading 1 TB of data alone (takes hours), Spark splits it across 100 computers (takes minutes).

**Real-World Analogy:** Imagine counting all books in a massive library. One person takes weeks. But if you hire 100 people and give each person one shelf, the whole library is counted in an hour. Spark is the manager who divides the work and collects results.

**Key Concepts:**
- **Driver**: The manager computer that plans the work
- **Executors**: The worker computers that do the actual processing
- **Cluster**: The group of computers working together
- **Partition**: One chunk of data assigned to one worker

```
Your Code --> Driver (plans) --> Executors (work) --> Results back to Driver
```

## NB 2: Databricks Platform Overview

**Simple English:** Databricks is a cloud platform that makes Spark easy to use. It gives you notebooks (like Google Docs for code), clusters (computers you can rent), and tools to manage everything.

**Key Components:**
- **Workspace**: Your home folder for notebooks, files, dashboards
- **Cluster**: Rented computers that run your code (pay per hour)
- **Notebook**: Interactive document where you write and run code
- **Unity Catalog**: Security system that controls who sees what data
- **Jobs**: Scheduled tasks that run automatically

## NB 3: SparkSession - Your Entry Point

**Simple English:** SparkSession is the "key" that connects your code to the Spark engine. In Databricks, it is already created for you as a variable called `spark`.

```python
# Already available in Databricks - no need to create!
spark  # This is your SparkSession

# Use it to read data, run SQL, create DataFrames
df = spark.table("my_catalog.my_schema.my_table")
df = spark.sql("SELECT * FROM employees")
```

## NB 4: RDDs vs DataFrames vs Datasets

**Simple English:**
- **RDD** (old way): Like a raw list of items. No structure. Hard to optimize.
- **DataFrame** (modern way): Like an Excel spreadsheet with named columns. Spark optimizes automatically.
- **Dataset** (Scala/Java only): DataFrame + type safety. Not used in Python.

**Rule:** Always use DataFrames in PySpark. RDDs are for very rare edge cases.

## NB 5: Lazy Evaluation

**Simple English:** Spark does NOT run your code immediately. It just takes notes (builds a plan). Only when you ask for a result (like count, show, write) does it actually do the work.

**Analogy:** Like a waiter in a restaurant. You say "I want soup, salad, steak." The waiter writes it all down (lazy). Only when you say "That is my order" (action) does the kitchen start cooking.

- **Transformations** (lazy - just builds plan): filter, select, join, groupBy
- **Actions** (eager - triggers execution): count, show, collect, write, display

```python
# These lines do NOTHING yet (just planning):
df2 = df.filter(col("age") > 25)      # Lazy
df3 = df2.select("name", "salary")     # Lazy
df4 = df3.groupBy("dept").avg("salary") # Lazy

# THIS triggers everything above to actually run:
df4.show()  # Action! Now Spark executes the whole plan.
```

## NB 6: Narrow vs Wide Transformations

**Simple English:**
- **Narrow**: Each partition works alone. No data movement. FAST.
  - Examples: filter, select, map
- **Wide**: Data must move between partitions (shuffle). SLOW.
  - Examples: groupBy, join, orderBy, distinct

**Analogy:** Narrow = each student grades their own paper. Wide = students must swap papers to compare answers (lots of passing around).

## NB 7: Spark Architecture Deep Dive

```
Your Notebook
     |
     v
[Driver Node] -- Plans the work (creates DAG)
     |
     v
[Cluster Manager] -- Assigns work to executors
     |
     +---> [Executor 1] processes partitions 1-3
     +---> [Executor 2] processes partitions 4-6
     +---> [Executor 3] processes partitions 7-9
     |
     v
Results collected back to Driver
```

**Key terms:**
- **Job**: One action (like .count()) creates one job
- **Stage**: A job splits into stages at shuffle boundaries
- **Task**: One stage has one task per partition

## NB 8: Databricks File System (DBFS)

**Simple English:** DBFS is Databricks' virtual file system. It makes cloud storage (Azure Blob, ADLS) look like a regular folder on your computer.

```python
# List files
dbutils.fs.ls("/mnt/data/")  # Like 'ls' in terminal

# Modern approach: Use Unity Catalog Volumes
# /Volumes/catalog/schema/volume_name/file.csv
df = spark.read.csv("/Volumes/my_catalog/my_schema/landing/data.csv")
```

---

# MODULE 2: DataFrame Basics (NB 9-16)

## NB 9: Creating DataFrames

**Simple English:** A DataFrame is a table of data with rows and named columns. Like a spreadsheet in Excel, but it can hold billions of rows across many computers.

```python
# From a table (most common in Databricks)
df = spark.table("catalog.schema.employees")

# From SQL
df = spark.sql("SELECT * FROM employees WHERE dept = 'Engineering'")

# From Python lists (for testing)
df = spark.createDataFrame([
    (1, "Alice", 75000),
    (2, "Bob", 82000)
], ["id", "name", "salary"])

# From files
df = spark.read.csv("/path/to/file.csv", header=True, inferSchema=True)
df = spark.read.parquet("/path/to/data/")
df = spark.read.json("/path/to/data.json")
```

## NB 10: Selecting and Filtering

**Simple English:** Select = choose which columns. Filter = choose which rows.

```python
from pyspark.sql.functions import col

# Select columns (like choosing columns in Excel)
df.select("name", "salary")
df.select(col("name"), col("salary") * 1.1)    # With math

# Filter rows (like Excel filter feature)
df.filter(col("salary") > 70000)
df.filter(col("dept") == "Engineering")
df.filter((col("age") > 25) & (col("age") < 50))  # Multiple conditions
```

## NB 11: Adding and Modifying Columns

**Simple English:** withColumn adds a new column or replaces an existing one.

```python
from pyspark.sql.functions import col, lit, when, upper, concat

df.withColumn("bonus", col("salary") * 0.1)        # 10% bonus
df.withColumn("country", lit("Germany"))            # Same value for all rows

# Conditional (like IF in Excel)
df.withColumn("level",
    when(col("salary") >= 100000, "Senior")
    .when(col("salary") >= 70000, "Mid")
    .otherwise("Junior")
)
```

## NB 12: Data Types and Casting

```python
from pyspark.sql.functions import to_date, to_timestamp
from pyspark.sql.types import IntegerType, DoubleType

df.withColumn("age_int", col("age").cast(IntegerType()))      # String to Int
df.withColumn("date", to_date(col("date_str"), "yyyy-MM-dd")) # String to Date
```

## NB 13: Sorting and Limiting

```python
df.orderBy(col("salary").desc())    # Highest first
df.limit(10)                         # First 10 rows
display(df)                          # Rich table view in Databricks
```

## NB 14: Handling Nulls

**Simple English:** NULL means "unknown" or "missing". It is NOT zero or empty string.

```python
df.filter(col("email").isNull())           # Find nulls
df.na.fill(0, subset=["salary"])           # Replace nulls with 0
df.dropna(subset=["name", "email"])        # Remove rows with nulls
df.withColumn("phone", coalesce(col("mobile"), col("work"), lit("N/A")))  # First non-null
```

## NB 15: Column Expressions

```python
from pyspark.sql.functions import (
    round, length, trim, lower, year, datediff, current_date, regexp_replace
)

df.withColumn("tax", round(col("salary") * 0.3, 2))       # Math
df.withColumn("clean", trim(lower(col("email"))))          # String
df.withColumn("tenure", datediff(current_date(), col("hire_date")))  # Date
```

## NB 16: Schema (Structure)

```python
df.printSchema()   # See column names and types
df.columns         # List of column names
df.dtypes          # List of (name, type) pairs
```

---

# MODULE 3: Transformations (NB 17-22)

## NB 17: GroupBy and Aggregations

**Simple English:** GroupBy splits data into groups, then calculates something for each group.

**Analogy:** Teacher says "sit with your department." Then count people per group. That is groupBy + count.

```python
from pyspark.sql.functions import count, sum, avg, max, countDistinct

df.groupBy("department").agg(
    count("*").alias("headcount"),
    avg("salary").alias("avg_salary"),
    sum("salary").alias("total_payroll"),
    max("salary").alias("highest_paid")
)
```

## NB 18: Joins

**Simple English:** Joins combine two tables based on a matching column.

```python
# Inner join (only matching rows from BOTH tables)
result = employees.join(departments, "dept_id")

# Left join (ALL from left + matching from right)
result = employees.join(departments, "dept_id", "left")
```

| Join Type | What It Returns |
|-----------|-----------------|
| inner | Only matching rows from both tables |
| left | All from left + matching from right (nulls if no match) |
| right | All from right + matching from left |
| full | All from both (nulls where no match) |
| semi | Left rows that HAVE a match (no right columns) |
| anti | Left rows that have NO match (finding orphans) |

## NB 19: Union and Set Operations

```python
combined = df_jan.unionByName(df_feb)    # Stack vertically (same columns)
df.distinct()                             # Remove duplicates
df.dropDuplicates(["email"])              # Unique by email
```

## NB 20: Window Functions

**Simple English:** Calculate something across related rows WITHOUT collapsing the data.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, lag, lead, sum

window = Window.partitionBy("department").orderBy(col("salary").desc())

df.withColumn("rank", row_number().over(window))         # 1,2,3,4...
df.withColumn("prev_salary", lag("salary", 1).over(window))  # Previous row
df.withColumn("running_total", sum("amount").over(window))   # Cumulative
```

## NB 21: Pivot and Unpivot

```python
# Rows become columns
df.groupBy("product").pivot("quarter").sum("revenue")
# Result: product | Q1 | Q2 | Q3 | Q4
```

## NB 22: Complex Types (Arrays, Maps, Structs)

```python
from pyspark.sql.functions import explode, array_contains

df.select(explode(col("tags")))                   # One row per array element
df.filter(array_contains(col("tags"), "urgent"))  # Filter by element
df.select(col("address.city"))                    # Access nested struct
```

---

# MODULE 4: Data I/O (NB 23-28)

## NB 23: Reading Data

```python
df = spark.table("catalog.schema.table")                           # Table
df = spark.read.csv("/path/", header=True, inferSchema=True)       # CSV
df = spark.read.json("/path/", multiLine=True)                     # JSON
df = spark.read.parquet("/path/")                                  # Parquet
```

## NB 24: Writing Data

```python
df.write.format("delta").mode("overwrite").saveAsTable("catalog.schema.out")
df.write.format("delta").mode("append").saveAsTable("catalog.schema.out")

# Write modes: overwrite, append, ignore, error
```

## NB 25: File Formats Compared

| Format | Speed | Size | Best For |
|--------|-------|------|----------|
| Delta | Fast | Small | Production tables (default!) |
| Parquet | Fast | Small | Analytics |
| CSV | Slow | Large | Data exchange |
| JSON | Medium | Large | APIs, nested data |

## NB 26: Partitioning on Disk

**Simple English:** Splitting a big table into folders by a column value. Filter on that column = read only relevant folders.

```
/sales/ year=2024/ month=01/  <-- Only reads this for Jan 2024 query
                   month=02/
```

## NB 27: Repartitioning

```python
df.repartition(200)         # Increase partitions (with shuffle)
df.coalesce(10)             # Decrease partitions (no shuffle, fast)
```

## NB 28: External Data Sources

```python
# Azure Data Lake
df = spark.read.parquet("abfss://container@account.dfs.core.windows.net/path/")

# JDBC
df = spark.read.format("jdbc").options(url=url, dbtable=table, user=user, password=pw).load()
```

---

# MODULE 5: Functions Library (NB 29-34)

## NB 29: String Functions

```python
from pyspark.sql.functions import lower, upper, trim, length, substring, split, regexp_replace

df.withColumn("clean", trim(lower(col("email"))))           # Clean email
df.withColumn("digits", regexp_replace(col("phone"), "[^0-9]", ""))  # Only digits
```

## NB 30: Date and Time Functions

```python
from pyspark.sql.functions import year, month, datediff, date_add, to_date, current_date

df.withColumn("date", to_date(col("str"), "dd.MM.yyyy"))    # Parse date
df.withColumn("age_days", datediff(current_date(), col("birth")))  # Days between
```

## NB 31: Numeric Functions

```python
from pyspark.sql.functions import round, abs, floor, ceil, greatest
df.withColumn("rounded", round(col("price"), 2))
```

## NB 32: Conditional Logic

```python
from pyspark.sql.functions import when, coalesce

df.withColumn("grade",
    when(col("score") >= 90, "A")
    .when(col("score") >= 70, "B")
    .otherwise("C")
)
```

## NB 33: User Defined Functions (UDFs)

**Rule:** Built-in functions > pandas_udf > regular udf (fastest to slowest)

```python
@pandas_udf("double")
def normalize(s: pd.Series) -> pd.Series:
    return (s - s.mean()) / s.std()   # 10-100x faster than regular UDF
```

## NB 34: Higher-Order Functions

```python
df.withColumn("doubled", expr("transform(values, x -> x * 2)"))
df.withColumn("positives", expr("filter(values, x -> x > 0)"))
```

---

# MODULE 6: Spark SQL (NB 35-40)

## NB 35: SQL in Databricks

```python
result = spark.sql("SELECT dept, AVG(salary) FROM employees GROUP BY dept")
df.createOrReplaceTempView("my_view")  # Share DF with SQL cells
```

## NB 36: CTEs (Common Table Expressions)

```sql
WITH active AS (SELECT * FROM emp WHERE status = 'active')
SELECT department, COUNT(*) FROM active GROUP BY department
```

## NB 37: Subqueries

```sql
SELECT * FROM orders
WHERE customer_id IN (SELECT id FROM customers WHERE country = 'Germany')
```

## NB 38: Window Functions (SQL)

```sql
SELECT name, dept, salary,
  ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rank
FROM employees
```

## NB 39: PIVOT

```sql
SELECT * FROM sales PIVOT (SUM(revenue) FOR quarter IN ('Q1','Q2','Q3','Q4'))
```

## NB 40: Views and Scope

```python
df.createOrReplaceTempView("temp_data")           # This session only
spark.sql("CREATE VIEW catalog.schema.v AS ...")  # Permanent (UC)
```

---

# MODULE 7: Advanced Transformations (NB 41-47)

## NB 41: Multi-Table Joins

```python
result = orders.join(customers, "customer_id").join(products, "product_id")
```

## NB 42: Explode and Flatten

```python
df.select("id", explode("tags").alias("tag"))   # One row per array element
```

## NB 43: Deduplication

```python
# Keep latest per group
window = Window.partitionBy("id").orderBy(col("updated").desc())
df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1)
```

## NB 44: Sampling and Profiling

```python
df.sample(fraction=0.1, seed=42)    # 10% sample
df.describe().show()                 # Basic stats
```

## NB 45: Working with JSON

```python
from pyspark.sql.functions import from_json, get_json_object
df.withColumn("name", get_json_object(col("json_str"), "$.name"))
```

## NB 46: Error Handling

```python
df.withColumn("safe_int", try_cast(col("str"), "integer"))  # Returns null not error
```

## NB 47: Performance-Aware Transformations

**Rule:** Filter early, select only needed columns, join small tables with broadcast.

---

# MODULE 8: Data Quality and ETL (NB 48-54)

## NB 48: Data Quality Checks

```python
assert df.count() > 0, "Empty!"
assert df.filter(col("id").isNull()).count() == 0, "Null IDs found!"
assert df.count() == df.select("id").distinct().count(), "Duplicates!"
```

## NB 49: Medallion Architecture

**Simple English:**
- **Bronze** = Raw data, exactly as received (no changes)
- **Silver** = Cleaned (nulls handled, types fixed, deduped)
- **Gold** = Business-ready (aggregated, joined, dashboard-ready)

```
Files --> Bronze (raw) --> Silver (clean) --> Gold (business) --> Reports
```

## NB 50: Incremental Processing

```python
# Only process NEW data since last run
max_date = spark.sql("SELECT MAX(updated) FROM silver.orders").collect()[0][0]
new_data = spark.table("bronze.orders").filter(col("updated") > max_date)
```

## NB 51: MERGE Pattern

```sql
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

## NB 52: SCD Type 1 and 2

- **Type 1**: Overwrite old value (no history)
- **Type 2**: Keep old row + add new row (full history with date ranges)

## NB 53: Error Handling

```python
try:
    result = transform(df)
    result.write.saveAsTable("output")
except Exception as e:
    log_error(e)
    raise
```

## NB 54: ETL Best Practices

1. Always use Delta format
2. Filter and select early
3. Use MERGE for idempotent writes
4. Test with small data first
5. Add quality assertions at each layer
6. Log row counts for monitoring
7. Use explicit schemas (not inferSchema)

---

# MODULE 9: Delta Lake (NB 55-60)

## NB 55: Delta Lake Fundamentals

**Simple English:** Delta Lake adds reliability to data lakes. Without Delta, a failed write corrupts your data. Delta gives you: transactions (all-or-nothing), time travel (undo mistakes), schema enforcement (reject bad data).

**Analogy:** Regular files = writing on loose papers. Delta = bound notebook with numbered pages and index.

```python
# Time travel
df = spark.sql("SELECT * FROM my_table VERSION AS OF 5")
spark.sql("RESTORE TABLE my_table TO VERSION AS OF 3")
```

## NB 56: MERGE (Upsert)

```sql
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

## NB 57: Schema Evolution

```python
df.write.option("mergeSchema", "true").mode("append").saveAsTable("t")
```

## NB 58: OPTIMIZE and Z-ORDER

```sql
OPTIMIZE table ZORDER BY (date, region);      -- Compact + co-locate
ALTER TABLE t CLUSTER BY (date, region);      -- Liquid Clustering (modern)
```

## NB 59: VACUUM

```sql
VACUUM table RETAIN 168 HOURS;   -- Remove old files (minimum 7 days!)
```

## NB 60: Change Data Feed

```sql
ALTER TABLE t SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
SELECT * FROM table_changes('t', 5, 10);  -- Track row-level changes
```

---

# MODULE 10: Spark SQL Advanced (NB 61-63)

## NB 61: SQL in Databricks
- spark.sql() returns DataFrame
- %sql magic command
- _sqldf = last SQL cell result

## NB 62: Window Functions
- ROW_NUMBER, RANK, LAG, LEAD, running SUM

## NB 63: Complex Queries
- CTEs, recursive queries, PIVOT/UNPIVOT

---

# MODULE 11: Performance (NB 64-74)

## NB 64: Execution Plans
```python
df.explain(mode="formatted")  # See what Spark will do
```

## NB 65: Partitioning
- Partition by low-cardinality columns
- Each partition: 100MB-1GB

## NB 66: Broadcast Joins
```python
from pyspark.sql.functions import broadcast
big.join(broadcast(small), "key")  # No shuffle!
```

## NB 67: AQE (Adaptive Query Execution)
- Auto-optimizes at runtime
- Enabled by default, no code changes needed

## NB 68-74: Caching, Shuffle, Skew, Pushdown, Memory, Photon, Benchmarking

---

# MODULE 12: Streaming (NB 75-80)

## NB 75: Streaming Basics

**Simple English:** Process data as it arrives (like a conveyor belt) instead of waiting for a batch.

```python
stream = spark.readStream.format("delta").table("source")
stream.writeStream.format("delta") \
    .option("checkpointLocation", "/cp/") \
    .trigger(availableNow=True).toTable("target")
```

## NB 76: Auto Loader

```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/schema/") \
    .load("/landing/")
```

## NB 77-80: Triggers, Watermarks, Stream-Static Joins, Monitoring

---

# MODULE 13: Data Sources (NB 81-85)

## NB 81: ADLS Gen2
```python
df = spark.read.parquet("abfss://container@account.dfs.core.windows.net/path/")
```

## NB 82-85: S3/GCS, JDBC, File Formats, APIs

---

# MODULE 14: Machine Learning (NB 86-90)

## NB 86: Feature Engineering
```python
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
assembler = VectorAssembler(inputCols=["col1","col2"], outputCol="features")
```

## NB 87: Text/NLP
- Tokenizer, StopWordsRemover, TF-IDF, Word2Vec

## NB 88: Classification/Regression
```python
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
model = LogisticRegression(featuresCol="features", labelCol="label").fit(train)
```

## NB 89: Clustering
- K-Means, silhouette score, elbow method

## NB 90: ML Pipelines
```python
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
pipeline = Pipeline(stages=[assembler, scaler, lr])
cv = CrossValidator(estimator=pipeline, evaluator=evaluator, numFolds=3)
```

---

# MODULE 15: PySpark Pandas (NB 91-93)

## NB 91: PySpark Pandas API

**Simple English:** Use familiar pandas syntax on billions of rows. Spark distributes the work.

```python
import pyspark.pandas as ps
pdf = ps.read_csv("/data.csv")       # Distributed!
pdf.groupby("region").mean()          # Pandas syntax, Spark power
pdf.to_spark()                        # Convert back to Spark DF
```

## NB 92: Pandas UDFs
- 10-100x faster than regular UDFs
- Uses Apache Arrow

## NB 93: Python Libraries
```python
%pip install scikit-learn xgboost plotly  # Notebook-scoped install
```

---

# MODULE 16: dbutils and Multi-Language (NB 94-95)

## NB 94: dbutils

```python
dbutils.fs.ls("/path/")                          # List files
dbutils.secrets.get("scope", "key")              # Get secret
dbutils.widgets.text("date", "2024-01-01")       # Create parameter
dbutils.notebook.run("./child", 300, {})         # Run another notebook
```

## NB 95: Multi-Language
- %python, %sql, %scala, %r, %md, %sh
- Share data: createOrReplaceTempView()

---

# MODULE 17: Orchestration (NB 96-98)

## NB 96: Lakeflow Jobs

**Simple English:** Jobs are scheduled pipelines. Chain notebooks into a DAG where task B waits for task A.

```json
{
  "tasks": [
    {"task_key": "ingest", "notebook_task": {"notebook_path": "/ingest"}},
    {"task_key": "transform", "depends_on": [{"task_key": "ingest"}]}
  ],
  "schedule": {"quartz_cron_expression": "0 0 8 * * ?"}
}
```

## NB 97: Workflow Patterns
- MERGE for idempotent processing (safe to re-run)
- replaceWhere for partition overwrite
- Error handling and circuit breakers

## NB 98: CI/CD and DABs

```yaml
# databricks.yml
bundle:
  name: my_pipeline
resources:
  jobs:
    etl_daily:
      tasks:
        - task_key: run_etl
          notebook_task: {notebook_path: ./notebooks/etl.py}
targets:
  dev: {workspace: {host: "https://dev.azuredatabricks.net"}}
  prod: {workspace: {host: "https://prod.azuredatabricks.net"}}
```

```bash
databricks bundle deploy --target prod
```

---

# MODULE 18: Security (NB 99-100)

## NB 99: Unity Catalog

**Simple English:** Unity Catalog controls WHO can access WHAT data. Three levels: Catalog > Schema > Table.

```sql
-- Grant access
GRANT USE CATALOG ON CATALOG sales TO `analysts`;
GRANT USE SCHEMA ON SCHEMA sales.gold TO `analysts`;
GRANT SELECT ON TABLE sales.gold.revenue TO `analysts`;

-- Row-level security (users see only their region)
ALTER TABLE t SET ROW FILTER my_filter ON (region);

-- Column masking (hide PII)
ALTER TABLE t ALTER COLUMN email SET MASK mask_email;
```

## NB 100: Access Control and Secrets

```python
# Secrets (never shown in output)
password = dbutils.secrets.get(scope="my-scope", key="db-password")

# Cluster security modes:
# USER_ISOLATION = shared cluster, per-user permissions (recommended)
# SINGLE_USER = dedicated to one identity (for jobs)
```

---

# MODULE 19: Monitoring (NB 101-103)

## NB 101: Spark UI

**Simple English:** Dashboard showing what is happening in your job. Like a hospital monitor showing vital signs.

- **Jobs tab**: How many actions ran
- **Stages tab**: Where time was spent
- **SQL tab**: Query plan with actual metrics

```python
df.explain(mode="formatted")  # See execution plan
```

## NB 102: Logging

```python
import logging
logger = logging.getLogger("etl")
logger.info(f"Processed {count} rows")
```

## NB 103: Common Errors

| Error | Fix |
|-------|-----|
| Table not found | Use catalog.schema.table |
| OutOfMemoryError | Reduce collect(), increase memory |
| Null pointer | Filter nulls before operations |
| Skew | Salt keys or use AQE |
| Schema mismatch | Use mergeSchema option |

---

# MODULE 20: Advanced Topics (NB 104-111)

## NB 104: Databricks Connect

**Simple English:** Run Spark code from your local IDE (VS Code). Code runs on remote cluster. You get full debugging with breakpoints.

```python
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()  # Runs on remote cluster
```

## NB 105: Testing PySpark

**Simple English:** Write automated tests that verify your code works correctly.

```python
def clean_data(df):
    return df.filter(col("amount") > 0)  # Pure function

def test_clean_removes_negatives(spark):
    input = spark.createDataFrame([(-1,), (5,)], ["amount"])
    assert clean_data(input).count() == 1  # Only 5 survives
```

## NB 106: GraphFrames

**Simple English:** Process networks/graphs. Find most important person, shortest path, connected clusters.

```python
from graphframes import GraphFrame
g = GraphFrame(vertices_df, edges_df)
g.pageRank(maxIter=10)          # Most influential
g.shortestPaths(landmarks=["a"])  # Distance
```

## NB 107: Geospatial

**Simple English:** Process location data. Distances, regions, hexagonal grids.

```python
df.withColumn("h3", expr("h3_pointash3(lat, lon, 7)"))  # H3 hex index
```

## NB 108: Feature Store

**Simple English:** Shared library of pre-computed ML features. Compute once, use in every model.

```python
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()
fe.create_table(name="catalog.ml.features", primary_keys=["customer_id"], df=features)
```

## NB 109: MLflow

**Simple English:** Track ML experiments like a lab notebook. Records settings, results, and models.

```python
import mlflow
with mlflow.start_run():
    mlflow.log_param("n_estimators", 100)
    mlflow.log_metric("accuracy", 0.95)
    mlflow.sklearn.log_model(model, "model")
```

## NB 110: Cost Optimization

**Simple English:** Spend less money without losing performance.

| Rule | Savings |
|------|---------|
| Auto-terminate clusters (60 min) | 40% |
| Use job clusters (not interactive) | 30-50% |
| Spot instances for workers | 60-80% |
| Right-size (dont over-provision) | 20-40% |
| Photon engine (faster = less time) | 50-80% |

**Golden rule:** "If it is not running a job, it should not be running."

## NB 111: Certification Prep

**Exam weights (Data Engineer Associate):**
- Delta Lake: 25% (MERGE, time travel, OPTIMIZE, VACUUM)
- DataFrames: 20% (transformations vs actions, joins, windows)
- Unity Catalog: 15% (permissions, three-level namespace)
- Streaming: 15% (Auto Loader, triggers, checkpoints)
- Jobs: 15% (DAGs, idempotency, scheduling)
- Performance: 10% (AQE, broadcast, Photon)

---

# Quick Reference Cheat Sheet

```python
# === READ ===
df = spark.table("catalog.schema.table")
df = spark.read.csv("/path", header=True)

# === TRANSFORM ===
df.filter(col("x") > 10)
df.select("col1", "col2")
df.withColumn("new", col("x") * 2)
df.groupBy("key").agg(sum("val").alias("total"))
df.join(other, "key", "left")

# === WRITE ===
df.write.mode("overwrite").saveAsTable("catalog.schema.out")

# === DISPLAY ===
display(df)
```

```sql
-- MERGE
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- OPTIMIZE
OPTIMIZE table ZORDER BY (col);

-- GRANT
GRANT SELECT ON TABLE t TO `group_name`;
```

---

*111 Notebooks | 20 Modules | Estimated study time: 60-80 hours*
*Ready for: Databricks Data Engineer Associate Certification*