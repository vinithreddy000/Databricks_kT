# Databricks notebook source
# DBTITLE 1,NB_50 Header
# MAGIC %md
# MAGIC # NB_50 — Pivot and Unpivot
# MAGIC
# MAGIC **Module 8: Transformations & Reshaping** | Notebook 50 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * pivot(): rows to columns
# MAGIC * Pivot with explicit value list (performance optimization)
# MAGIC * Multi-column aggregation in pivot
# MAGIC * Unpivot / stack(): columns to rows
# MAGIC * Melt pattern for wide-to-long
# MAGIC * Dynamic pivot (unknown values)
# MAGIC * Pivot + Window for cross-tab reports
# MAGIC * Real-world ETL reshape patterns
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Essential for reporting and analytics)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What is Pivot/Unpivot
# MAGIC %md
# MAGIC ## SECTION 1 — What is Pivot / Unpivot? (Real-World Analogy)
# MAGIC
# MAGIC ### 📊 The Spreadsheet Reshape
# MAGIC
# MAGIC Think of pivot/unpivot like rearranging a spreadsheet:
# MAGIC
# MAGIC **PIVOT** (long → wide): Like creating a cross-tab in Excel
# MAGIC ```
# MAGIC BEFORE (long):             AFTER (wide):
# MAGIC Product | Quarter | Sales   Product | Q1  | Q2  | Q3  | Q4
# MAGIC Widget  | Q1      | 100     Widget  | 100 | 150 | 200 | 250
# MAGIC Widget  | Q2      | 150     Gadget  | 80  | 120 | 160 | 200
# MAGIC Widget  | Q3      | 200
# MAGIC ```
# MAGIC
# MAGIC **UNPIVOT** (wide → long): The reverse — flatten columns to rows
# MAGIC ```
# MAGIC BEFORE (wide):              AFTER (long):
# MAGIC Product | Jan | Feb | Mar   Product | Month | Sales
# MAGIC Widget  | 100 | 150 | 200   Widget  | Jan   | 100
# MAGIC Gadget  | 80  | 120 | 160   Widget  | Feb   | 150
# MAGIC                             Widget  | Mar   | 200
# MAGIC ```
# MAGIC
# MAGIC ### When to Use Each
# MAGIC | Pattern | Use Case |
# MAGIC |---|---|
# MAGIC | Pivot | Dashboards, cross-tabs, feature engineering |
# MAGIC | Unpivot | Normalizing Excel exports, time-series prep, union-friendly shapes |
# MAGIC
# MAGIC ### Performance Warning
# MAGIC ```
# MAGIC pivot() triggers a SHUFFLE + potential data explosion.
# MAGIC Always pass explicit value list when possible!
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 2 — Pivot/Unpivot Mechanics
# MAGIC %md
# MAGIC ## SECTION 2 — Pivot / Unpivot Mechanics in Spark
# MAGIC
# MAGIC ### Pivot Syntax
# MAGIC ```python
# MAGIC # Basic pivot: groupBy → pivot → agg
# MAGIC df.groupBy("product").pivot("quarter").sum("sales")
# MAGIC
# MAGIC # With explicit values (MUCH faster — avoids extra job):
# MAGIC df.groupBy("product").pivot("quarter", ["Q1","Q2","Q3","Q4"]).sum("sales")
# MAGIC
# MAGIC # Multiple aggregations:
# MAGIC df.groupBy("product").pivot("quarter").agg(
# MAGIC     F.sum("sales").alias("total"),
# MAGIC     F.avg("price").alias("avg_price")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Unpivot / Stack Syntax
# MAGIC ```python
# MAGIC # Using stack() expression (built-in):
# MAGIC df.select(
# MAGIC     "product",
# MAGIC     expr("stack(3, 'Jan', Jan, 'Feb', Feb, 'Mar', Mar) as (month, sales)")
# MAGIC )
# MAGIC
# MAGIC # Using unpivot() (Spark 3.4+):
# MAGIC df.unpivot(
# MAGIC     ids=["product"],             # Keep these columns
# MAGIC     values=["Jan", "Feb", "Mar"], # Melt these to rows
# MAGIC     variableColumnName="month",
# MAGIC     valueColumnName="sales"
# MAGIC )
# MAGIC
# MAGIC # Manual melt with union:
# MAGIC df.select("product", lit("Jan").alias("month"), col("Jan").alias("sales"))
# MAGIC   .union(df.select("product", lit("Feb").alias("month"), col("Feb").alias("sales")))
# MAGIC ```
# MAGIC
# MAGIC ### Key Behaviors
# MAGIC * Pivot creates NULL for missing combinations
# MAGIC * Column names from pivot values become actual DataFrame columns
# MAGIC * Without explicit values, Spark runs an extra job to discover distinct values

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Basic pivot
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Basic Pivot
# ============================================================
# Real-world: Transform transaction log into quarterly sales report.

from pyspark.sql import SparkSession  # Import.
from pyspark.sql.functions import col, sum as spark_sum, avg, count  # Functions.

spark = SparkSession.builder.getOrCreate()  # Session.

# Sales transactions in long format.
sales = spark.createDataFrame([
    ("Widget", "Q1", 100, 10.00),
    ("Widget", "Q2", 150, 10.50),
    ("Widget", "Q3", 200, 11.00),
    ("Widget", "Q4", 250, 11.50),
    ("Gadget", "Q1", 80, 25.00),
    ("Gadget", "Q2", 120, 24.50),
    ("Gadget", "Q3", 160, 24.00),
    ("Gadget", "Q4", 200, 23.50),
    ("Doohickey", "Q1", 50, 5.00),
    ("Doohickey", "Q2", 75, 5.25),
    ("Doohickey", "Q3", 90, 5.50),
    ("Doohickey", "Q4", 110, 5.75),
], ["product", "quarter", "units_sold", "price"])  # Schema.

print("=== Original Long Format ===")  # Heading.
sales.show()  # Display.

# Basic pivot: products as rows, quarters as columns.
print("=== Pivoted: Units Sold by Quarter ===")  # Heading.
pivoted = sales.groupBy("product").pivot("quarter").sum("units_sold")  # Pivot.
pivoted.show()  # Display.

# Pivot with explicit values (faster — no extra scan).
print("=== Pivot with Explicit Values (optimized) ===")  # Heading.
pivoted_fast = (
    sales.groupBy("product")
    .pivot("quarter", ["Q1", "Q2", "Q3", "Q4"])  # Explicit list.
    .sum("units_sold")  # Aggregate.
)
pivoted_fast.show()  # Display.

# Multi-agg pivot: sum of units AND average price per quarter.
print("=== Multi-Aggregation Pivot ===")  # Heading.
multi_pivot = (
    sales.groupBy("product")
    .pivot("quarter", ["Q1", "Q2", "Q3", "Q4"])  # Pivot.
    .agg(
        spark_sum("units_sold").alias("units"),  # Total units.
        avg("price").alias("avg_price"),  # Average price.
    )
)
multi_pivot.show()  # Display (columns: Q1_units, Q1_avg_price, etc.).

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Basic unpivot with stack
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Basic Unpivot with stack()
# ============================================================
# Real-world: Wide Excel report needs to be normalized for DB storage.

from pyspark.sql.functions import col, expr, lit  # Imports.

# Wide format data (typical Excel export).
wide_sales = spark.createDataFrame([
    ("Widget", 100, 150, 200, 250),
    ("Gadget", 80, 120, 160, 200),
    ("Doohickey", 50, 75, 90, 110),
], ["product", "Q1", "Q2", "Q3", "Q4"])  # Wide columns.

print("=== Wide Format (from Excel) ===")  # Heading.
wide_sales.show()  # Display.

# Method 1: stack() — classic Spark approach.
print("=== Unpivot using stack() ===")  # Heading.
unpivoted_stack = wide_sales.select(
    col("product"),  # Keep product.
    expr("stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (quarter, units_sold)")  # Melt.
)
unpivoted_stack.show()  # Display.

# Method 2: unpivot() — Spark 3.4+ native method.
print("=== Unpivot using .unpivot() (Spark 3.4+) ===")  # Heading.
unpivoted_native = wide_sales.unpivot(
    ids=["product"],  # ID columns to keep.
    values=["Q1", "Q2", "Q3", "Q4"],  # Value columns to melt.
    variableColumnName="quarter",  # Name for the key column.
    valueColumnName="units_sold",  # Name for the value column.
)
unpivoted_native.show()  # Display.

# Method 3: Manual union approach (any Spark version).
print("=== Unpivot using union (manual) ===")  # Heading.
from functools import reduce  # For combining.
quarters = ["Q1", "Q2", "Q3", "Q4"]  # Columns to melt.
union_dfs = [  # Build list of DFs.
    wide_sales.select(
        col("product"),  # Keep.
        lit(q).alias("quarter"),  # Quarter name.
        col(q).alias("units_sold"),  # Value.
    )
    for q in quarters  # Each quarter.
]
unpivoted_union = reduce(lambda a, b: a.union(b), union_dfs)  # Combine.
unpivoted_union.orderBy("product", "quarter").show()  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Pivot for cross-tab reports
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Pivot for Cross-Tab Reports
# ============================================================
# Real-world: Create region × product matrix for executive dashboards.

from pyspark.sql.functions import (
    col, sum as spark_sum, count, round as spark_round
)  # Imports.

# Transaction data with region and product.
transactions = spark.createDataFrame([
    ("North", "Widget", 100), ("North", "Widget", 120),
    ("North", "Gadget", 80), ("North", "Gadget", 90),
    ("South", "Widget", 200), ("South", "Widget", 180),
    ("South", "Gadget", 150), ("South", "Gadget", 160),
    ("East", "Widget", 90), ("East", "Gadget", 70),
    ("West", "Widget", 110), ("West", "Gadget", 130),
    ("West", "Widget", 105), ("West", "Gadget", 125),
], ["region", "product", "revenue"])  # Schema.

print("=== Transaction Log ===")  # Heading.
transactions.show()  # Display.

# Cross-tab: region × product, sum of revenue.
print("=== Revenue Cross-Tab (Region × Product) ===")  # Heading.
crosstab = (
    transactions.groupBy("region")
    .pivot("product", ["Gadget", "Widget"])  # Explicit products.
    .sum("revenue")  # Sum per cell.
)
crosstab.show()  # Display.

# Count cross-tab: how many transactions per region/product.
print("=== Transaction Count Cross-Tab ===")  # Heading.
count_tab = (
    transactions.groupBy("region")
    .pivot("product", ["Gadget", "Widget"])  # Pivot.
    .count()  # Count.
)
count_tab.show()  # Display.

# Built-in crosstab shortcut (for quick exploration).
print("=== Built-in .crosstab() ===")  # Heading.
transactions.crosstab("region", "product").show()  # Quick cross-tab.

# Percentage cross-tab.
print("=== Revenue as % of Region Total ===")  # Heading.
from pyspark.sql.functions import expr  # Import.
crosstab.withColumn(
    "total", col("Gadget") + col("Widget")  # Row total.
).withColumn(
    "Gadget_pct", spark_round(col("Gadget") / col("total") * 100, 1)  # Percentage.
).withColumn(
    "Widget_pct", spark_round(col("Widget") / col("total") * 100, 1)  # Percentage.
).show()  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Dynamic pivot
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Dynamic Pivot
# ============================================================
# Real-world: Pivot when you don't know all possible values upfront.

from pyspark.sql.functions import (
    col, sum as spark_sum, collect_set, lit, coalesce
)  # Imports.

# IoT sensor data with dynamic sensor types.
sensor_data = spark.createDataFrame([
    ("device_1", "temperature", 22.5, "2024-01-01"),
    ("device_1", "humidity", 45.0, "2024-01-01"),
    ("device_1", "pressure", 1013.0, "2024-01-01"),
    ("device_1", "temperature", 23.0, "2024-01-02"),
    ("device_1", "humidity", 42.0, "2024-01-02"),
    ("device_2", "temperature", 19.5, "2024-01-01"),
    ("device_2", "humidity", 55.0, "2024-01-01"),
    ("device_2", "co2", 400.0, "2024-01-01"),  # Extra sensor!
    ("device_2", "temperature", 20.0, "2024-01-02"),
    ("device_2", "co2", 420.0, "2024-01-02"),
    ("device_3", "vibration", 0.5, "2024-01-01"),  # Another type!
], ["device_id", "metric", "value", "date"])  # Schema.

print("=== IoT Sensor Data (long format) ===")  # Heading.
sensor_data.show()  # Display.

# Step 1: Discover all unique metric types dynamically.
metric_types = (
    sensor_data.select("metric")
    .distinct()  # Unique values.
    .rdd.flatMap(lambda x: x)  # Extract.
    .collect()  # To list.
)
metric_types.sort()  # Sort for consistency.
print(f"Discovered metrics: {metric_types}")  # Show.

# Step 2: Pivot with discovered values.
print("=== Dynamic Pivot (all sensor types) ===")  # Heading.
pivoted_sensors = (
    sensor_data.groupBy("device_id", "date")
    .pivot("metric", metric_types)  # Use discovered list.
    .avg("value")  # Average per cell.
)
pivoted_sensors.show()  # Display.

# Step 3: Fill NULLs for missing sensors.
print("=== With Default Values for Missing Sensors ===")  # Heading.
filled = pivoted_sensors.na.fill(0.0)  # Default to 0.
filled.show()  # Display.

# Step 4: Dynamic column renaming (prefix with 'avg_').
print("=== With Prefixed Column Names ===")  # Heading.
renamed = pivoted_sensors  # Start.
for m in metric_types:  # Each metric.
    renamed = renamed.withColumnRenamed(m, f"avg_{m}")  # Prefix.
renamed.show()  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Multi-column unpivot
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Multi-Column Unpivot
# ============================================================
# Real-world: Financial report with paired columns (actual + budget).

from pyspark.sql.functions import col, expr, lit, array, struct  # Imports.

# Financial data with paired columns per month.
financial = spark.createDataFrame([
    ("Sales", 100, 110, 150, 140, 200, 190),
    ("Marketing", 50, 55, 60, 58, 70, 65),
    ("R&D", 80, 85, 90, 88, 95, 92),
    ("Support", 30, 32, 35, 34, 40, 38),
], ["department", "jan_actual", "jan_budget", "feb_actual", "feb_budget", "mar_actual", "mar_budget"])  # Wide.

print("=== Wide Financial Report ===")  # Heading.
financial.show()  # Display.

# Unpivot paired columns: each month becomes a row with actual + budget.
print("=== Unpivoted: month becomes a row ===")  # Heading.
unpivoted_financial = financial.select(
    col("department"),  # Keep.
    expr("""stack(3,
        'January', jan_actual, jan_budget,
        'February', feb_actual, feb_budget,
        'March', mar_actual, mar_budget
    ) as (month, actual, budget)""")  # Paired unpivot.
)
unpivoted_financial.show()  # Display.

# Add variance columns.
print("=== With Variance Analysis ===")  # Heading.
variance = unpivoted_financial.withColumn(
    "variance", col("actual") - col("budget")  # Over/under.
).withColumn(
    "variance_pct",
    expr("round((actual - budget) / budget * 100, 1)")  # Percentage.
)
variance.show()  # Display.

# Practical: survey data unpivot.
print("=== Survey Data Unpivot ===")  # Heading.
survey = spark.createDataFrame([
    ("resp_1", 4, 5, 3, 4, 5),
    ("resp_2", 3, 4, 2, 5, 4),
    ("resp_3", 5, 5, 4, 3, 3),
], ["respondent", "q1_score", "q2_score", "q3_score", "q4_score", "q5_score"])  # Wide.

survey_long = survey.select(
    col("respondent"),  # Keep.
    expr("""stack(5,
        'Q1', q1_score,
        'Q2', q2_score,
        'Q3', q3_score,
        'Q4', q4_score,
        'Q5', q5_score
    ) as (question, score)""")  # Unpivot.
)
survey_long.show()  # All responses in long format.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Pivot with window functions
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Pivot with Window Functions
# ============================================================
# Real-world: Running totals and rankings in pivoted format.

from pyspark.sql.functions import (
    col, sum as spark_sum, row_number, dense_rank, lag,
    round as spark_round, expr
)  # Imports.
from pyspark.sql.window import Window  # Window.

# Monthly sales by salesperson.
monthly_sales = spark.createDataFrame([
    ("Alice", "Jan", 5000), ("Alice", "Feb", 6000), ("Alice", "Mar", 5500),
    ("Alice", "Apr", 7000), ("Alice", "May", 6500), ("Alice", "Jun", 8000),
    ("Bob", "Jan", 4500), ("Bob", "Feb", 5500), ("Bob", "Mar", 6000),
    ("Bob", "Apr", 5000), ("Bob", "May", 7000), ("Bob", "Jun", 7500),
    ("Carol", "Jan", 6000), ("Carol", "Feb", 5000), ("Carol", "Mar", 7000),
    ("Carol", "Apr", 6500), ("Carol", "May", 5500), ("Carol", "Jun", 9000),
], ["salesperson", "month", "revenue"])  # Schema.

print("=== Monthly Sales Data ===")  # Heading.
monthly_sales.show()  # Display.

# Step 1: Add running total using window.
print("=== With Running Total ===")  # Heading.
month_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]  # Order.
w = Window.partitionBy("salesperson").orderBy("month")  # Window.

# Map month to sortable number.
from pyspark.sql.functions import when  # Import.
with_order = monthly_sales.withColumn(
    "month_num",
    when(col("month") == "Jan", 1).when(col("month") == "Feb", 2)
    .when(col("month") == "Mar", 3).when(col("month") == "Apr", 4)
    .when(col("month") == "May", 5).when(col("month") == "Jun", 6)
)  # Month number.

w_ordered = Window.partitionBy("salesperson").orderBy("month_num")  # Ordered window.
with_running = with_order.withColumn(
    "running_total", spark_sum("revenue").over(w_ordered)  # Cumulative.
).withColumn(
    "mom_change", col("revenue") - lag("revenue", 1).over(w_ordered)  # MoM delta.
)
with_running.orderBy("salesperson", "month_num").show()  # Display.

# Step 2: Pivot the running totals.
print("=== Pivoted Running Totals ===")  # Heading.
pivoted_running = (
    with_running.groupBy("salesperson")
    .pivot("month", month_order)  # Months as columns.
    .sum("running_total")  # Running total per month.
)
pivoted_running.show()  # Display.

# Step 3: Rank by total sales, then show.
print("=== Final Rankings ===")  # Heading.
totals = monthly_sales.groupBy("salesperson").agg(
    spark_sum("revenue").alias("total_revenue")  # Total.
)
w_rank = Window.orderBy(col("total_revenue").desc())  # Rank window.
ranked = totals.withColumn("rank", dense_rank().over(w_rank))  # Rank.
ranked.show()  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Production reshape pipeline
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Production Reshape Pipeline
# ============================================================
# Real-world: Configurable pivot/unpivot for ETL pipelines.

from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, min as spark_min, max as spark_max,
    expr, lit, coalesce, current_timestamp
)  # Imports.
from pyspark.sql import DataFrame  # Type.
from typing import List, Dict, Optional  # Typing.

class ReshapePipeline:
    """Production-grade pivot/unpivot with validation."""
    
    def __init__(self, df: DataFrame):
        """Initialize with source DataFrame."""
        self.df = df  # Store source.
        self.history = []  # Track operations.
    
    def smart_pivot(
        self,
        group_cols: List[str],
        pivot_col: str,
        agg_exprs: Dict[str, str],
        max_distinct: int = 100,
        fill_value=None
    ) -> DataFrame:
        """Pivot with safety checks and optimization."""
        # Safety: check cardinality before pivoting.
        distinct_count = self.df.select(pivot_col).distinct().count()  # Count.
        print(f"Pivot column '{pivot_col}' has {distinct_count} distinct values.")  # Info.
        
        if distinct_count > max_distinct:  # Too many?
            raise ValueError(
                f"Pivot would create {distinct_count} columns (max: {max_distinct}). "
                f"Filter data or increase max_distinct."
            )  # Abort.
        
        # Get explicit values for optimization.
        pivot_values = (
            self.df.select(pivot_col)
            .distinct().rdd.flatMap(lambda x: x).collect()  # Get values.
        )
        pivot_values.sort()  # Deterministic order.
        
        # Build pivot with aggregations.
        grouped = self.df.groupBy(*group_cols).pivot(pivot_col, pivot_values)  # Pivot.
        
        # Apply aggregations.
        agg_map = {"sum": spark_sum, "avg": avg, "count": count, "min": spark_min, "max": spark_max}
        agg_list = []  # Build list.
        for col_name, agg_type in agg_exprs.items():  # Each agg.
            agg_func = agg_map[agg_type]  # Get function.
            agg_list.append(agg_func(col_name).alias(f"{agg_type}_{col_name}"))  # Add.
        
        result = grouped.agg(*agg_list)  # Execute.
        
        if fill_value is not None:  # Fill NULLs?
            result = result.na.fill(fill_value)  # Fill.
        
        self.history.append(f"pivot({pivot_col}, {distinct_count} values)")  # Log.
        return result  # Return.
    
    def smart_unpivot(
        self,
        id_cols: List[str],
        value_cols: List[str],
        key_name: str = "variable",
        value_name: str = "value",
        drop_nulls: bool = True
    ) -> DataFrame:
        """Unpivot with validation and null handling."""
        # Validate columns exist.
        all_cols = set(self.df.columns)  # Available.
        missing = set(id_cols + value_cols) - all_cols  # Missing?
        if missing:  # Error?
            raise ValueError(f"Columns not found: {missing}")  # Abort.
        
        # Build stack expression.
        n = len(value_cols)  # Count.
        stack_items = ", ".join(
            [f"'{vc}', `{vc}`" for vc in value_cols]  # Name-value pairs.
        )
        stack_expr = f"stack({n}, {stack_items}) as ({key_name}, {value_name})"  # Expression.
        
        # Execute unpivot.
        result = self.df.select(
            *[col(c) for c in id_cols],  # Keep IDs.
            expr(stack_expr)  # Unpivot.
        )
        
        if drop_nulls:  # Remove null values?
            result = result.where(col(value_name).isNotNull())  # Filter.
        
        self.history.append(f"unpivot({n} cols -> {key_name}/{value_name})")  # Log.
        return result  # Return.

# Demo: IoT data reshape.
print("=== Production Reshape Pipeline Demo ===")  # Heading.

# Create test data.
iot_data = spark.createDataFrame([
    ("sensor_1", "temp", 22.5, "2024-01-01"), ("sensor_1", "temp", 23.0, "2024-01-02"),
    ("sensor_1", "humidity", 45.0, "2024-01-01"), ("sensor_1", "humidity", 43.0, "2024-01-02"),
    ("sensor_2", "temp", 19.0, "2024-01-01"), ("sensor_2", "temp", 20.5, "2024-01-02"),
    ("sensor_2", "humidity", 55.0, "2024-01-01"), ("sensor_2", "humidity", 52.0, "2024-01-02"),
], ["device", "metric", "reading", "date"])  # Schema.

pipeline = ReshapePipeline(iot_data)  # Init.

# Pivot: long to wide.
wide = pipeline.smart_pivot(
    group_cols=["device", "date"],  # Group.
    pivot_col="metric",  # Pivot on metric type.
    agg_exprs={"reading": "avg"},  # Average reading.
    fill_value=0.0  # Default.
)
print("\n=== Pivoted (wide) ===")  # Heading.
wide.show()  # Display.

# Unpivot back: wide to long.
pipeline2 = ReshapePipeline(wide)  # New pipeline on wide data.
long_again = pipeline2.smart_unpivot(
    id_cols=["device", "date"],  # Keep.
    value_cols=["humidity", "temp"],  # Melt.
    key_name="metric",  # Key column.
    value_name="avg_reading",  # Value column.
)
print("=== Unpivoted (back to long) ===")  # Heading.
long_again.show()  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Complex reshape patterns
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Complex Reshape Patterns
# ============================================================
# Real-world: Advanced transformations for analytics and ML.

from pyspark.sql.functions import (
    col, sum as spark_sum, avg, expr, lit, collect_list,
    struct, map_from_arrays, array, create_map, explode
)  # Imports.

# Pattern 1: Pivot with row/column totals (Grand Total).
print("=== Pattern 1: Pivot with Grand Totals ===")  # Heading.
region_sales = spark.createDataFrame([
    ("North", "Electronics", 5000), ("North", "Clothing", 3000),
    ("South", "Electronics", 4000), ("South", "Clothing", 3500),
    ("East", "Electronics", 4500), ("East", "Clothing", 2800),
    ("West", "Electronics", 5500), ("West", "Clothing", 4000),
], ["region", "category", "sales"])  # Schema.

# Pivot first.
pivoted = region_sales.groupBy("region").pivot(
    "category", ["Clothing", "Electronics"]  # Explicit.
).sum("sales")  # Sum.

# Add row totals.
with_row_total = pivoted.withColumn(
    "Total", col("Clothing") + col("Electronics")  # Row sum.
)

# Add grand total row using union.
from pyspark.sql.functions import sum as spark_sum  # Import.
grand_total = with_row_total.select(
    lit("TOTAL").alias("region"),  # Label.
    spark_sum("Clothing").alias("Clothing"),  # Column total.
    spark_sum("Electronics").alias("Electronics"),  # Column total.
    spark_sum("Total").alias("Total"),  # Grand total.
)
final_crosstab = with_row_total.union(grand_total)  # Append.
final_crosstab.show()  # Display.

# Pattern 2: Conditional pivot (only pivot specific values).
print("=== Pattern 2: Conditional Pivot ===")  # Heading.
orders = spark.createDataFrame([
    ("C1", "pending", 100), ("C1", "shipped", 200), ("C1", "delivered", 150),
    ("C2", "pending", 50), ("C2", "cancelled", 80),
    ("C3", "shipped", 300), ("C3", "delivered", 250), ("C3", "delivered", 100),
], ["customer", "status", "amount"])  # Schema.

# Pivot with count and sum side by side.
status_summary = orders.groupBy("customer").pivot(
    "status", ["pending", "shipped", "delivered", "cancelled"]  # All statuses.
).agg(
    spark_sum("amount").alias("total"),  # Total per status.
)
status_summary.na.fill(0).show()  # Display.

# Pattern 3: Explode + Pivot (array to columns).
print("=== Pattern 3: Explode to Pivot ===")  # Heading.
user_prefs = spark.createDataFrame([
    ("user_1", ["sports", "tech", "music"]),
    ("user_2", ["cooking", "sports"]),
    ("user_3", ["tech", "gaming", "music", "sports"]),
], ["user_id", "interests"])  # With arrays.

# Explode then pivot to one-hot encoding.
exploded = user_prefs.select("user_id", explode("interests").alias("interest"))  # Flatten.
one_hot = exploded.withColumn("flag", lit(1)).groupBy("user_id").pivot(
    "interest"  # Each interest becomes a column.
).sum("flag").na.fill(0)  # Binary flags.
one_hot.show()  # Display.
print("One-hot encoding via pivot — ready for ML!")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Time-series reshape
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Time-Series Reshape Patterns
# ============================================================
# Real-world: Reshape time-series for forecasting and analysis.

from pyspark.sql.functions import (
    col, sum as spark_sum, avg, lag, lead, expr, lit,
    date_format, month, year, dayofweek, round as spark_round,
    collect_list, struct, array
)  # Imports.
from pyspark.sql.window import Window  # Window.

# Daily metrics over time.
daily_metrics = spark.createDataFrame([
    ("2024-01-01", "revenue", 1000.0), ("2024-01-01", "orders", 50.0),
    ("2024-01-02", "revenue", 1200.0), ("2024-01-02", "orders", 55.0),
    ("2024-01-03", "revenue", 900.0), ("2024-01-03", "orders", 40.0),
    ("2024-01-04", "revenue", 1500.0), ("2024-01-04", "orders", 70.0),
    ("2024-01-05", "revenue", 1100.0), ("2024-01-05", "orders", 52.0),
    ("2024-01-06", "revenue", 800.0), ("2024-01-06", "orders", 35.0),
    ("2024-01-07", "revenue", 1300.0), ("2024-01-07", "orders", 60.0),
], ["date", "metric", "value"])  # Long format.

print("=== Daily Metrics (long) ===")  # Heading.
daily_metrics.show()  # Display.

# Pattern 1: Pivot metrics to columns for time-series analysis.
print("=== Pivoted: Metrics as Columns ===")  # Heading.
ts_wide = (
    daily_metrics.groupBy("date")
    .pivot("metric", ["orders", "revenue"])  # Metrics as columns.
    .sum("value")  # One value per date/metric.
)
ts_wide = ts_wide.orderBy("date")  # Sort.
ts_wide.show()  # Display.

# Pattern 2: Add derived features for ML.
print("=== With Time-Series Features ===")  # Heading.
w = Window.orderBy("date")  # Order by date.
ts_features = ts_wide.withColumn(
    "revenue_lag1", lag("revenue", 1).over(w)  # Yesterday's revenue.
).withColumn(
    "revenue_lag7", lag("revenue", 7).over(w)  # Week-ago revenue.
).withColumn(
    "orders_lag1", lag("orders", 1).over(w)  # Yesterday's orders.
).withColumn(
    "avg_order_value", spark_round(col("revenue") / col("orders"), 2)  # AOV.
).withColumn(
    "revenue_change",
    spark_round((col("revenue") - lag("revenue", 1).over(w)) / lag("revenue", 1).over(w) * 100, 1)
)  # % change.
ts_features.show()  # Display.

# Pattern 3: Reshape for seasonal comparison (month × year matrix).
print("=== Seasonal Comparison (Year × Month) ===")  # Heading.
monthly_data = spark.createDataFrame([
    (2022, "Jan", 10000), (2022, "Feb", 11000), (2022, "Mar", 12000),
    (2023, "Jan", 12000), (2023, "Feb", 13000), (2023, "Mar", 14500),
    (2024, "Jan", 13500), (2024, "Feb", 15000), (2024, "Mar", 16000),
], ["year", "month", "revenue"])  # Schema.

# Pivot: years as columns, months as rows.
year_comparison = (
    monthly_data.groupBy("month")
    .pivot("year", [2022, 2023, 2024])  # Years as columns.
    .sum("revenue")  # Revenue per year.
)
year_comparison.show()  # Display.

# Add YoY growth.
print("=== With Year-over-Year Growth ===")  # Heading.
year_comparison.withColumn(
    "yoy_22_23", spark_round((col("2023") - col("2022")) / col("2022") * 100, 1)  # Growth.
).withColumn(
    "yoy_23_24", spark_round((col("2024") - col("2023")) / col("2023") * 100, 1)  # Growth.
).show()  # Display.
print("Time-series reshaped for seasonal analysis!")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Key Takeaways
# MAGIC %md
# MAGIC ## SECTION 6 — Key Takeaways
# MAGIC
# MAGIC ### Pivot Rules
# MAGIC 1. **Always provide explicit value list** — avoids an extra job to scan distinct values
# MAGIC 2. **Check cardinality first** — pivoting 10,000 values creates 10,000 columns
# MAGIC 3. **NULLs appear for missing combinations** — use `.na.fill()` to handle
# MAGIC 4. **Column names come from data** — may need renaming for downstream use
# MAGIC
# MAGIC ### Unpivot Rules
# MAGIC 1. **stack() works in all Spark versions** — most portable approach
# MAGIC 2. **.unpivot() is cleaner** but requires Spark 3.4+
# MAGIC 3. **Union approach is most explicit** — good for complex transformations
# MAGIC 4. **Paired columns** (actual/budget) need multi-value stack expressions
# MAGIC
# MAGIC ### Performance
# MAGIC | Scenario | Recommendation |
# MAGIC |---|---|
# MAGIC | Known values | `pivot(col, [values])` — one stage |
# MAGIC | Unknown values | Collect distinct first, then pivot |
# MAGIC | High cardinality | Filter/bucket before pivoting |
# MAGIC | Very wide unpivot | Use stack() over union (fewer shuffles) |

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Practice Exercises
# MAGIC %md
# MAGIC ## SECTION 7 — Practice Exercises
# MAGIC
# MAGIC ### Exercise 1: Sales Pivot
# MAGIC Given daily sales with columns (store, product, day, amount), create a pivot showing stores as rows, days as columns, with total sales per cell.
# MAGIC
# MAGIC ### Exercise 2: Survey Unpivot
# MAGIC Given survey data with columns (respondent, q1, q2, q3, q4, q5), unpivot to long format (respondent, question, score).
# MAGIC
# MAGIC ### Exercise 3: Feature Engineering
# MAGIC Given user activity logs (user_id, action_type, count), pivot to create a one-hot/count feature matrix suitable for ML.
# MAGIC
# MAGIC ### Exercise 4: Seasonal Analysis
# MAGIC Given monthly revenue data over 3 years, create a year-over-year comparison matrix with growth percentages.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Solutions
# ============================================================
# SECTION 7 — EXERCISE SOLUTIONS
# ============================================================

# --- Exercise 1: Sales Pivot ---
print("=== Exercise 1: Sales Pivot ===")  # Heading.
daily_store = spark.createDataFrame([
    ("Store_A", "Widget", "Mon", 100), ("Store_A", "Widget", "Tue", 120),
    ("Store_A", "Widget", "Wed", 90), ("Store_B", "Widget", "Mon", 80),
    ("Store_B", "Widget", "Tue", 110), ("Store_B", "Widget", "Wed", 95),
], ["store", "product", "day", "amount"])  # Schema.

store_pivot = daily_store.groupBy("store").pivot(
    "day", ["Mon", "Tue", "Wed"]  # Explicit values.
).sum("amount")  # Sum.
store_pivot.show()  # Display.

# --- Exercise 2: Survey Unpivot ---
print("=== Exercise 2: Survey Unpivot ===")  # Heading.
survey_wide = spark.createDataFrame([
    ("R1", 4, 5, 3, 4, 5), ("R2", 3, 4, 2, 5, 4),
], ["respondent", "q1", "q2", "q3", "q4", "q5"])  # Wide.

survey_long = survey_wide.select(
    col("respondent"),  # Keep.
    expr("stack(5, 'Q1',q1, 'Q2',q2, 'Q3',q3, 'Q4',q4, 'Q5',q5) as (question, score)")  # Melt.
)
survey_long.show()  # Display.

# --- Exercise 3: One-Hot Feature Engineering ---
print("=== Exercise 3: One-Hot Features ===")  # Heading.
from pyspark.sql.functions import explode  # Import.
activity = spark.createDataFrame([
    ("u1", "click", 10), ("u1", "view", 50), ("u1", "purchase", 2),
    ("u2", "click", 5), ("u2", "view", 30),
], ["user_id", "action_type", "count"])  # Schema.

feature_matrix = activity.groupBy("user_id").pivot(
    "action_type", ["click", "purchase", "view"]  # Explicit.
).sum("count").na.fill(0)  # Fill missing.
feature_matrix.show()  # Display.

# --- Exercise 4: Seasonal YoY ---
print("=== Exercise 4: Seasonal YoY ===")  # Heading.
from pyspark.sql.functions import round as spark_round  # Import.
yearly_rev = spark.createDataFrame([
    (2022, "Q1", 50000), (2022, "Q2", 60000),
    (2023, "Q1", 58000), (2023, "Q2", 68000),
    (2024, "Q1", 65000), (2024, "Q2", 75000),
], ["year", "quarter", "revenue"])  # Schema.

yoy_matrix = yearly_rev.groupBy("quarter").pivot(
    "year", [2022, 2023, 2024]  # Years.
).sum("revenue")

yoy_matrix.withColumn(
    "growth_22_23", spark_round((col("2023") - col("2022")) / col("2022") * 100, 1)
).withColumn(
    "growth_23_24", spark_round((col("2024") - col("2023")) / col("2023") * 100, 1)
).show()  # Display.

print("All exercises completed! Practice with your own data next.")