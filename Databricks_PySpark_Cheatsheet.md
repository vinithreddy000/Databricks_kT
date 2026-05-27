# Databricks & PySpark Cheat Sheet

> One-page quick reference. Print it. Pin it. Use it daily.

---

## 1. READ DATA

```python
df = spark.table("catalog.schema.table")
df = spark.read.csv("/path/", header=True, inferSchema=True)
df = spark.read.parquet("/path/")
df = spark.read.json("/path/", multiLine=True)
df = spark.sql("SELECT * FROM catalog.schema.table WHERE year = 2024")
df = spark.read.format("jdbc").options(url=url, dbtable=tbl, user=u, password=p).load()
df = spark.read.parquet("abfss://container@account.dfs.core.windows.net/path/")
```

---

## 2. WRITE DATA

```python
df.write.format("delta").mode("overwrite").saveAsTable("catalog.schema.output")
df.write.format("delta").mode("append").saveAsTable("catalog.schema.output")
df.write.partitionBy("year", "month").format("delta").saveAsTable("catalog.schema.out")
# Modes: overwrite | append | ignore | error
```

---

## 3. SELECT & FILTER

```python
from pyspark.sql.functions import col, lit

df.select("name", "salary")
df.select(col("salary") * 1.1)
df.filter(col("age") > 25)
df.filter((col("dept") == "Eng") & (col("age") > 30))
df.filter(col("name").like("%Smith%"))
df.filter(col("status").isin("active", "pending"))
```

---

## 4. ADD / MODIFY COLUMNS

```python
from pyspark.sql.functions import col, lit, when, upper, concat, coalesce

df.withColumn("bonus", col("salary") * 0.1)
df.withColumn("country", lit("Germany"))
df.withColumnRenamed("old", "new")
df.drop("temp_col")

df.withColumn("tier",
    when(col("amount") >= 1000, "Gold")
    .when(col("amount") >= 500, "Silver")
    .otherwise("Bronze")
)
```

---

## 5. AGGREGATIONS

```python
from pyspark.sql.functions import count, sum, avg, min, max, countDistinct

df.groupBy("department").agg(
    count("*").alias("total"),
    avg("salary").alias("avg_sal"),
    sum("salary").alias("payroll"),
    max("salary").alias("top_sal"),
    countDistinct("title").alias("roles")
)
```

---

## 6. JOINS

```python
from pyspark.sql.functions import broadcast

result = df1.join(df2, "key_col")                # Inner
result = df1.join(df2, "key_col", "left")        # Left outer
result = big.join(broadcast(small), "key")       # Broadcast (no shuffle)
```

| Type | Returns |
|------|---------|
| inner | Matching from both |
| left | All left + matching right |
| right | All right + matching left |
| full | All from both |
| semi | Left rows WITH match |
| anti | Left rows WITHOUT match |

---

## 7. WINDOW FUNCTIONS

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, lag, lead, sum

w = Window.partitionBy("dept").orderBy(col("salary").desc())

df.withColumn("rank", row_number().over(w))
df.withColumn("prev", lag("salary", 1).over(w))
df.withColumn("running_total", sum("amount").over(w))
```

---

## 8. STRING FUNCTIONS

```python
from pyspark.sql.functions import lower, upper, trim, substring, split, regexp_replace

df.withColumn("clean", trim(lower(col("email"))))
df.withColumn("digits", regexp_replace(col("phone"), "[^0-9]", ""))
df.withColumn("parts", split(col("name"), " "))
```

---

## 9. DATE FUNCTIONS

```python
from pyspark.sql.functions import (
    current_date, year, month, datediff, date_add, to_date, date_format
)

df.withColumn("dt", to_date(col("str"), "yyyy-MM-dd"))
df.withColumn("yr", year(col("date")))
df.withColumn("age_days", datediff(current_date(), col("hire_date")))
df.withColumn("next_wk", date_add(col("date"), 7))
```

---

## 10. NULL HANDLING

```python
df.filter(col("x").isNull())
df.filter(col("x").isNotNull())
df.na.fill(0, subset=["salary"])
df.na.fill({"name": "Unknown", "age": 0})
df.dropna(subset=["email"])
df.withColumn("val", coalesce(col("a"), col("b"), lit(0)))
```

---

## 11. DEDUPLICATION

```python
df.distinct()
df.dropDuplicates(["email"])

# Keep latest per key
w = Window.partitionBy("id").orderBy(col("updated").desc())
df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
```

---

## 12. UNION

```python
df1.unionByName(df2)       # Stack by column name (safe)
df1.intersect(df2)         # Rows in BOTH
df1.subtract(df2)          # Rows in df1 NOT in df2
```

---

## 13. DELTA LAKE

```sql
-- Time travel
SELECT * FROM t VERSION AS OF 5;
RESTORE TABLE t TO VERSION AS OF 3;

-- MERGE (upsert)
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Optimize
OPTIMIZE table ZORDER BY (date, region);
ALTER TABLE t CLUSTER BY (date, region);

-- Vacuum
VACUUM table RETAIN 168 HOURS;

-- Schema evolution
-- df.write.option("mergeSchema", "true").mode("append").saveAsTable("t")

-- Change Data Feed
ALTER TABLE t SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
SELECT * FROM table_changes('t', 5, 10);
```

---

## 14. STREAMING

```python
# Auto Loader
stream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/schema/") \
    .load("/landing/")

# Write
stream.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/cp/") \
    .trigger(availableNow=True) \
    .toTable("catalog.schema.target")
```

---

## 15. UNITY CATALOG

```sql
GRANT USE CATALOG ON CATALOG my_cat TO `team`;
GRANT USE SCHEMA ON SCHEMA my_cat.analytics TO `team`;
GRANT SELECT ON TABLE my_cat.analytics.sales TO `team`;
ALTER TABLE t SET ROW FILTER my_func ON (region);
ALTER TABLE t ALTER COLUMN email SET MASK mask_func;
```

---

## 16. DBUTILS

```python
dbutils.fs.ls("/path/")
dbutils.fs.cp("/src", "/dst", recurse=True)
dbutils.fs.rm("/path/", recurse=True)
dbutils.secrets.get("scope", "key")
dbutils.widgets.text("param", "default")
dbutils.widgets.get("param")
dbutils.notebook.run("./child", 300, {"k": "v"})
```

---

## 17. PERFORMANCE TIPS

```python
# Filter EARLY
df.filter(col("year") == 2024).join(other, "id")   # GOOD

# Broadcast small tables
big.join(broadcast(small), "key")

# Cache only if reused 2+ times
df.cache()
df.unpersist()

# Check plan
df.explain(mode="formatted")
```

---

## 18. COMMON PATTERNS

```python
# Explode array
df.select("id", explode("tags").alias("tag"))

# Pivot
df.groupBy("product").pivot("quarter").sum("revenue")

# Parse JSON
df.withColumn("name", get_json_object(col("json"), "$.name"))

# Temp view for SQL
df.createOrReplaceTempView("my_data")
```

---

## 19. MLFLOW

```python
import mlflow
with mlflow.start_run():
    mlflow.log_param("lr", 0.01)
    mlflow.log_metric("accuracy", 0.95)
    mlflow.sklearn.log_model(model, "model")
```

---

## 20. COST RULES

| Do This | Save |
|---------|------|
| Auto-terminate (60 min) | 40% |
| Job clusters | 30-50% |
| Spot instances | 60-80% |
| Right-size | 20-40% |
| Photon engine | 50-80% |

**Golden rule:** If it is not running a job, it should not be running.

---

## SHORTCUTS

| Action | Key |
|--------|-----|
| Run cell | Shift+Enter |
| Run cell stay | Ctrl+Enter |
| Add cell below | B |
| Delete cell | D, D |
| Comment toggle | Ctrl+/ |
| Command palette | Ctrl+Shift+P |

---

## EXAM RECALL

| Topic (Weight) | Must Know |
|----------------|-----------|
| Delta (25%) | MERGE, time travel, OPTIMIZE, VACUUM |
| DataFrames (20%) | Lazy vs eager, joins, windows |
| Unity Catalog (15%) | catalog.schema.table, GRANT |
| Streaming (15%) | Auto Loader, checkpoints |
| Jobs (15%) | Job clusters, DAGs, idempotent |
| Performance (10%) | AQE, broadcast, Photon |

---

*Print this. Keep it next to your keyboard.*
