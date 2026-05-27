# Databricks notebook source
# DBTITLE 1,NB_40 Header
# MAGIC %md
# MAGIC # NB_40 — Conditional Functions
# MAGIC
# MAGIC **Module 5: Built-in Functions** | Notebook 40 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * when/otherwise: PySpark's CASE WHEN equivalent
# MAGIC * Chaining multiple conditions
# MAGIC * SQL CASE WHEN via expr()
# MAGIC * Nested conditionals
# MAGIC * try_* functions: try_cast(), try_divide(), try_to_number()
# MAGIC * decode/encode patterns
# MAGIC * Performance: when() vs UDF for conditional logic
# MAGIC
# MAGIC **Difficulty:** ⭐⭐ (Essential pattern for every pipeline)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are Conditional Functions?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are Conditional Functions? (Real-World Analogy)
# MAGIC
# MAGIC ### 🚦 The Traffic Light
# MAGIC
# MAGIC Conditional functions are if-then-else logic applied to every row:
# MAGIC
# MAGIC | Traffic Light | PySpark | What It Does |
# MAGIC |---|---|---|
# MAGIC | If red → stop | `when(speed > 100, "speeding")` | Check condition |
# MAGIC | Else if yellow → caution | `.when(speed > 60, "fast")` | Chain conditions |
# MAGIC | Else green → go | `.otherwise("normal")` | Default fallback |
# MAGIC
# MAGIC ### Common Use Cases
# MAGIC * **Categorization:** Assign labels based on value ranges
# MAGIC * **Data cleaning:** Fix values conditionally
# MAGIC * **Feature engineering:** Create binary flags or buckets
# MAGIC * **Business rules:** Apply discounts, routing, scoring
# MAGIC * **Error handling:** try_* functions for safe type conversion

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Conditional Functions Work
# MAGIC %md
# MAGIC ## SECTION 2 — How Conditional Functions Work
# MAGIC
# MAGIC ### PySpark vs SQL Syntax
# MAGIC ```
# MAGIC PySpark Column API:                    SQL (via expr):
# MAGIC   when(cond1, val1)                     CASE
# MAGIC     .when(cond2, val2)                    WHEN cond1 THEN val1
# MAGIC     .when(cond3, val3)                    WHEN cond2 THEN val2
# MAGIC     .otherwise(default)                   ELSE default
# MAGIC                                         END
# MAGIC ```
# MAGIC
# MAGIC ### Evaluation Rules
# MAGIC ```
# MAGIC 1. Conditions are evaluated TOP-TO-BOTTOM (first match wins)
# MAGIC 2. If no condition matches and no otherwise() → NULL
# MAGIC 3. when() returns a Column — can be used in select, withColumn, filter
# MAGIC 4. Conditions can reference any column in the DataFrame
# MAGIC 5. try_* functions return NULL instead of raising errors
# MAGIC ```
# MAGIC
# MAGIC ### try_* Functions (Safe Alternatives)
# MAGIC ```
# MAGIC try_cast(expr AS type)    → NULL on failure (instead of error)
# MAGIC try_divide(a, b)          → NULL when b=0 (instead of error/infinity)
# MAGIC try_to_number(str, fmt)   → NULL on parse failure
# MAGIC try_to_timestamp(str,fmt) → NULL on parse failure
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: when/otherwise basics
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: when/otherwise Basics
# ============================================================
# Real-world: Categorizing data based on value ranges.

from pyspark.sql import SparkSession  # Import SparkSession.
from pyspark.sql.functions import col, when, lit, otherwise  # Import conditionals.

spark = SparkSession.builder.getOrCreate()  # Get active session.

# Student grades.
students = spark.createDataFrame([
    (1, "Alice", 95), (2, "Bob", 82), (3, "Charlie", 67),
    (4, "Diana", 45), (5, "Eve", 78), (6, "Frank", 90),
], ["id", "name", "score"])  # Score data.

# Simple when/otherwise.
print("=== Simple when/otherwise ===")  # Print heading.
students.select(
    col("name"), col("score"),  # Keep context.
    # Single condition.
    when(col("score") >= 60, "Pass").otherwise("Fail").alias("result"),
    # Boolean flag.
    when(col("score") >= 90, True).otherwise(False).alias("honors"),
).show(truncate=False)  # Display.

# Chained conditions (first match wins).
print("=== Chained when() — Letter Grades ===")  # Print heading.
students.select(
    col("name"), col("score"),  # Keep context.
    when(col("score") >= 90, "A")
        .when(col("score") >= 80, "B")
        .when(col("score") >= 70, "C")
        .when(col("score") >= 60, "D")
        .otherwise("F").alias("grade"),  # Letter grade.
    # Numeric GPA.
    when(col("score") >= 90, 4.0)
        .when(col("score") >= 80, 3.0)
        .when(col("score") >= 70, 2.0)
        .when(col("score") >= 60, 1.0)
        .otherwise(0.0).alias("gpa"),  # GPA.
).show(truncate=False)  # Display.

# Without otherwise → NULL.
print("=== Without otherwise → NULL ===")  # Print heading.
students.select(
    col("name"), col("score"),  # Keep.
    when(col("score") >= 90, "Excellent").alias("only_excellent"),  # NULL for non-90+.
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Multiple column conditions
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Multiple Column Conditions
# ============================================================
# Real-world: Business rules involving multiple fields.

from pyspark.sql.functions import col, when, lit  # Imports.

# Employee data for bonus calculation.
employees = spark.createDataFrame([
    (1, "Alice", "Engineering", 5, 95000),
    (2, "Bob", "Sales", 2, 55000),
    (3, "Charlie", "Engineering", 10, 120000),
    (4, "Diana", "Marketing", 3, 65000),
    (5, "Eve", "Sales", 8, 90000),
    (6, "Frank", "Engineering", 1, 70000),
], ["id", "name", "dept", "years", "salary"])  # Employee data.

# Multi-condition business rules.
print("=== Business Rule: Bonus Tier ===")  # Print heading.
employees.select(
    col("name"), col("dept"), col("years"), col("salary"),  # Keep context.
    # Bonus based on department AND tenure.
    when((col("dept") == "Engineering") & (col("years") >= 5), col("salary") * 0.20)
        .when((col("dept") == "Engineering") & (col("years") < 5), col("salary") * 0.10)
        .when((col("dept") == "Sales") & (col("years") >= 5), col("salary") * 0.25)
        .when(col("dept") == "Sales", col("salary") * 0.15)
        .otherwise(col("salary") * 0.05).alias("bonus"),  # Default 5%.
    # Risk level.
    when((col("years") <= 2) & (col("salary") < 60000), "High Flight Risk")
        .when(col("years") <= 2, "Moderate Risk")
        .otherwise("Low Risk").alias("retention_risk"),  # Risk assessment.
).show(truncate=False)  # Display.

# Combining conditions with AND, OR, NOT.
print("=== Complex Conditions (AND, OR, NOT) ===")  # Print heading.
employees.select(
    col("name"),  # Keep.
    # Senior engineer: Engineering + 5+ years + 100K+.
    when(
        (col("dept") == "Engineering") & (col("years") >= 5) & (col("salary") >= 100000),
        "Senior Engineer"
    ).when(
        (col("dept") == "Engineering") & (col("years") >= 5),
        "Staff Engineer"
    ).when(
        col("dept") == "Engineering",
        "Engineer"
    ).otherwise("Non-Engineering").alias("eng_level"),  # Engineering level.
    # Promotion eligible: NOT Sales AND years >= 3.
    when(
        (col("dept") != "Sales") & (col("years") >= 3), "Eligible"
    ).otherwise("Not Eligible").alias("promotion_eligible"),  # Eligibility.
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: SQL CASE WHEN via expr()
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: SQL CASE WHEN via expr()
# ============================================================
# Real-world: Using SQL syntax for complex conditional logic.

from pyspark.sql.functions import col, expr  # Imports.

# Product pricing data.
products = spark.createDataFrame([
    (1, "Laptop", 999.99, 50, "electronics"),
    (2, "Book", 19.99, 200, "education"),
    (3, "Headphones", 149.99, 10, "electronics"),
    (4, "Pen", 2.99, 1000, "stationery"),
    (5, "Monitor", 399.99, 25, "electronics"),
], ["id", "product", "price", "stock", "category"])  # Products.

# SQL CASE WHEN syntax.
print("=== SQL CASE WHEN via expr() ===")  # Print heading.
products.select(
    col("product"), col("price"), col("stock"), col("category"),  # Keep.
    # Simple CASE.
    expr("""
        CASE category
            WHEN 'electronics' THEN 'Tech'
            WHEN 'education' THEN 'Books'
            WHEN 'stationery' THEN 'Office'
            ELSE 'Other'
        END
    """).alias("dept"),  # Department mapping.
    # Searched CASE (with conditions).
    expr("""
        CASE
            WHEN price >= 500 THEN 'Premium'
            WHEN price >= 100 THEN 'Mid-Range'
            WHEN price >= 20 THEN 'Budget'
            ELSE 'Value'
        END
    """).alias("price_tier"),  # Price tier.
    # Stock status.
    expr("""
        CASE
            WHEN stock = 0 THEN 'Out of Stock'
            WHEN stock < 20 THEN 'Low Stock'
            WHEN stock < 100 THEN 'Normal'
            ELSE 'Overstocked'
        END
    """).alias("stock_status"),  # Stock level.
).show(truncate=False)  # Display.

# Computed CASE WHEN (calculations in THEN).
print("=== Computed Values in CASE ===")  # Print heading.
products.select(
    col("product"), col("price"), col("category"),  # Keep.
    # Dynamic discount based on category.
    expr("""
        CASE category
            WHEN 'electronics' THEN price * 0.90
            WHEN 'education' THEN price * 0.80
            ELSE price * 0.95
        END
    """).alias("sale_price"),  # Discounted price.
    # Restock priority score.
    expr("""
        CASE
            WHEN stock < 20 AND category = 'electronics' THEN 10
            WHEN stock < 20 THEN 7
            WHEN stock < 50 THEN 4
            ELSE 1
        END
    """).alias("restock_priority"),  # Priority.
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: try_* safe functions
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: try_* Safe Functions
# ============================================================
# Real-world: Safe type conversion and division without errors.

from pyspark.sql.functions import col, expr, when, lit  # Imports.

# Data with messy values.
messy = spark.createDataFrame([
    (1, "42", "2024-01-15", 100.0, 5.0),
    (2, "not_a_number", "2024-13-45", 50.0, 0.0),  # Bad date, div by zero!
    (3, "99.5", "2024-02-28", 75.0, 3.0),
    (4, "", "invalid", 200.0, 0.0),  # Empty string, bad date, div by zero!
    (5, "123", "2024-06-15", 0.0, 10.0),
], "id INT, num_str STRING, date_str STRING, numerator DOUBLE, denominator DOUBLE")  # Messy data.

# try_cast: returns NULL on failure instead of error.
print("=== try_cast() — Safe Type Conversion ===")  # Print heading.
messy.select(
    col("id"), col("num_str"), col("date_str"),  # Keep.
    # Regular cast would FAIL on "not_a_number".
    expr("try_cast(num_str AS DOUBLE)").alias("safe_double"),  # NULL on failure.
    expr("try_cast(num_str AS INT)").alias("safe_int"),  # NULL on failure.
    expr("try_cast(date_str AS DATE)").alias("safe_date"),  # NULL on bad date.
).show(truncate=False)  # Display safe casts.

# try_divide: returns NULL instead of infinity/error.
print("=== try_divide() — Safe Division ===")  # Print heading.
messy.select(
    col("id"), col("numerator"), col("denominator"),  # Keep.
    # Regular division: 100/0 = Infinity!
    (col("numerator") / col("denominator")).alias("unsafe_div"),  # May give Infinity.
    # try_divide: returns NULL when denominator is 0.
    expr("try_divide(numerator, denominator)").alias("safe_div"),  # NULL on div-by-zero.
).show(truncate=False)  # Display safe division.

# try_to_number: parse strings with format.
print("=== try_to_number() — Parse with Format ===")  # Print heading.
formatted = spark.createDataFrame([
    ("$1,234.56",), ("$99.99",), ("invalid",), ("$0.50",)
], ["price_str"])  # Formatted prices.

formatted.select(
    col("price_str"),  # Original.
    expr("try_to_number(price_str, '$9,999.99')").alias("parsed_price"),  # Safe parse.
).show(truncate=False)  # Display parsed.

print("""try_* functions ALWAYS return NULL on failure instead of raising errors.
Perfect for: ETL pipelines, data cleaning, user input processing.""")  # Summary.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Nested and complex conditionals
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Nested and Complex Conditionals
# ============================================================
# Real-world: Multi-level decision trees in data pipelines.

from pyspark.sql.functions import col, when, lit, expr, coalesce  # Imports.

# Insurance claim data.
claims = spark.createDataFrame([
    (1, "auto", 5000, 2, "urban", True),
    (2, "home", 50000, 0, "suburban", False),
    (3, "auto", 2000, 5, "rural", True),
    (4, "auto", 15000, 1, "urban", False),
    (5, "home", 100000, 0, "urban", True),
    (6, "health", 8000, 3, "suburban", True),
], ["id", "type", "amount", "prior_claims", "area", "is_premium"])  # Claims.

# Nested conditions: risk scoring.
print("=== Nested Conditional: Risk Score ===")  # Print heading.
claims.select(
    col("id"), col("type"), col("amount"), col("prior_claims"),  # Context.
    # Nested: type determines base, then modifiers.
    when(col("type") == "auto",
         when(col("prior_claims") >= 3, 9)  # High risk auto.
             .when(col("amount") > 10000, 7)  # Expensive auto.
             .otherwise(4)  # Normal auto.
    ).when(col("type") == "home",
         when(col("amount") > 75000, 8)  # Major home claim.
             .otherwise(3)  # Normal home.
    ).otherwise(5).alias("risk_score"),  # Default.
    # Premium discount.
    when(col("is_premium") & (col("prior_claims") == 0), col("amount") * 0.10)
        .when(col("is_premium"), col("amount") * 0.05)
        .otherwise(lit(0)).alias("discount"),  # Discount amount.
).show(truncate=False)  # Display.

# Multiple flags from same data.
print("=== Multiple Flags from Conditions ===")  # Print heading.
claims.select(
    col("id"), col("type"),  # Context.
    # Flag: needs manual review.
    when(
        (col("amount") > 20000) | (col("prior_claims") >= 3),
        True
    ).otherwise(False).alias("manual_review"),
    # Flag: fraud risk.
    when(
        (col("type") == "auto") & (col("amount") > 10000) & (col("prior_claims") >= 2),
        "HIGH"
    ).when(
        (col("prior_claims") >= 3), "MEDIUM"
    ).otherwise("LOW").alias("fraud_risk"),
    # Flag: priority.
    when(col("is_premium") & (col("amount") > 50000), "URGENT")
        .when(col("is_premium"), "HIGH")
        .when(col("amount") > 50000, "NORMAL")
        .otherwise("LOW").alias("priority"),
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Conditional aggregation
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Conditional Aggregation
# ============================================================
# Real-world: Counting/summing with conditions (pivot alternative).

from pyspark.sql.functions import (  # Import functions.
    col, when, sum as spark_sum, count, avg, expr, lit
)  # End imports.

# Sales data.
sales = spark.createDataFrame([
    ("Alice", "Q1", "online", 5000),
    ("Alice", "Q1", "store", 3000),
    ("Alice", "Q2", "online", 7000),
    ("Bob", "Q1", "online", 4000),
    ("Bob", "Q2", "store", 6000),
    ("Bob", "Q2", "online", 2000),
    ("Charlie", "Q1", "store", 8000),
    ("Charlie", "Q2", "online", 9000),
], ["rep", "quarter", "channel", "revenue"])  # Sales data.

# Conditional aggregation (like pivot but more flexible).
print("=== Conditional Aggregation (Pivot Alternative) ===")  # Print heading.
sales.groupBy("rep").agg(
    # Total revenue.
    spark_sum("revenue").alias("total_revenue"),
    # Revenue by channel.
    spark_sum(when(col("channel") == "online", col("revenue")).otherwise(0)).alias("online_rev"),
    spark_sum(when(col("channel") == "store", col("revenue")).otherwise(0)).alias("store_rev"),
    # Revenue by quarter.
    spark_sum(when(col("quarter") == "Q1", col("revenue")).otherwise(0)).alias("Q1_rev"),
    spark_sum(when(col("quarter") == "Q2", col("revenue")).otherwise(0)).alias("Q2_rev"),
    # Count transactions by channel.
    count(when(col("channel") == "online", True)).alias("online_count"),
    count(when(col("channel") == "store", True)).alias("store_count"),
).show(truncate=False)  # Display conditional aggregation.

# Conditional average.
print("=== Conditional Averages ===")  # Print heading.
sales.groupBy("rep").agg(
    avg("revenue").alias("avg_all"),  # Overall average.
    avg(when(col("channel") == "online", col("revenue"))).alias("avg_online"),  # Online only.
    avg(when(col("channel") == "store", col("revenue"))).alias("avg_store"),  # Store only.
    # Percentage online.
    (spark_sum(when(col("channel") == "online", col("revenue")).otherwise(0)) * 100.0 /
     spark_sum("revenue")).alias("online_pct"),
).show(truncate=False)  # Display conditional averages.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Dynamic rule engine
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Dynamic Rule Engine
# ============================================================
# Real-world: Configurable business rules applied via when/otherwise.

from pyspark.sql.functions import col, when, lit, expr, coalesce  # Imports.
from pyspark.sql import Column  # Type hint.
from functools import reduce  # For chaining.

# === Build rules dynamically ===
def apply_rules(df, rules, output_col, default="Unknown"):
    """Apply a list of (condition, value) rules as chained when/otherwise."""
    expr = None  # Accumulator.
    for condition, value in rules:  # Iterate rules.
        if expr is None:  # First rule.
            expr = when(condition, value)  # Start chain.
        else:
            expr = expr.when(condition, value)  # Add condition.
    expr = expr.otherwise(default)  # Add default.
    return df.withColumn(output_col, expr)  # Apply to DataFrame.

# Customer segmentation rules.
customers = spark.createDataFrame([
    (1, "Alice", 150000, 50, 5),
    (2, "Bob", 30000, 5, 1),
    (3, "Charlie", 75000, 25, 3),
    (4, "Diana", 200000, 100, 8),
    (5, "Eve", 10000, 2, 0),
], ["id", "name", "lifetime_value", "orders", "years"])  # Customer data.

# Define rules as (condition, label).
segment_rules = [
    ((col("lifetime_value") >= 100000) & (col("years") >= 5), "VIP"),
    ((col("lifetime_value") >= 50000) | (col("orders") >= 20), "Gold"),
    ((col("lifetime_value") >= 20000) | (col("orders") >= 5), "Silver"),
    (col("orders") >= 1, "Bronze"),
]

# Apply rules.
print("=== Dynamic Rule Engine ===")  # Print heading.
result = apply_rules(customers, segment_rules, "segment", default="Prospect")
result.show(truncate=False)  # Display segmented customers.

# Risk rules.
risk_rules = [
    ((col("years") <= 1) & (col("orders") <= 2), "High"),
    ((col("years") <= 2) & (col("lifetime_value") < 50000), "Medium"),
]

result2 = apply_rules(result, risk_rules, "churn_risk", default="Low")
print("=== With Churn Risk ===")  # Print heading.
result2.select("name", "segment", "churn_risk").show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Complex scoring systems
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Complex Scoring Systems
# ============================================================
# Real-world: Multi-factor scoring with weighted conditions.

from pyspark.sql.functions import (  # Import functions.
    col, when, lit, expr, round as spark_round, greatest, least
)  # End imports.

# Loan application data.
applications = spark.createDataFrame([
    (1, "Alice", 750, 85000, 250000, 5, 0),
    (2, "Bob", 620, 45000, 300000, 2, 3),
    (3, "Charlie", 800, 120000, 200000, 10, 0),
    (4, "Diana", 580, 35000, 150000, 1, 5),
    (5, "Eve", 700, 65000, 180000, 4, 1),
], ["id", "name", "credit_score", "income", "loan_amount", "years_employed", "delinquencies"])  # Applications.

# Multi-factor scoring.
print("=== Loan Scoring System ===")  # Print heading.
scored = applications.select(
    col("name"),  # Keep name.
    col("credit_score"), col("income"), col("loan_amount"),  # Keep inputs.
    # Credit score factor (0-40 points).
    when(col("credit_score") >= 750, 40)
        .when(col("credit_score") >= 700, 30)
        .when(col("credit_score") >= 650, 20)
        .when(col("credit_score") >= 600, 10)
        .otherwise(0).alias("credit_points"),
    # Income-to-loan ratio factor (0-30 points).
    when(col("income") * 4 >= col("loan_amount"), 30)  # Can afford 4x.
        .when(col("income") * 3 >= col("loan_amount"), 20)  # Can afford 3x.
        .when(col("income") * 2 >= col("loan_amount"), 10)  # Can afford 2x.
        .otherwise(0).alias("affordability_points"),
    # Employment stability (0-20 points).
    when(col("years_employed") >= 5, 20)
        .when(col("years_employed") >= 3, 15)
        .when(col("years_employed") >= 1, 10)
        .otherwise(0).alias("employment_points"),
    # Delinquency penalty (0 to -10).
    when(col("delinquencies") == 0, 10)
        .when(col("delinquencies") <= 2, 5)
        .otherwise(0).alias("history_points"),
)

# Calculate total score and decision.
final = scored.withColumn(
    "total_score",
    col("credit_points") + col("affordability_points") + col("employment_points") + col("history_points")
).withColumn(
    "decision",
    when(col("total_score") >= 80, "APPROVED")
        .when(col("total_score") >= 60, "REVIEW")
        .otherwise("DENIED")
).withColumn(
    "max_approved_amount",
    when(col("total_score") >= 80, col("loan_amount"))
        .when(col("total_score") >= 60, col("loan_amount") * 0.75)
        .otherwise(lit(0))
)

final.select("name", "total_score", "decision", "max_approved_amount").show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production conditional patterns
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Production Conditional Patterns
# ============================================================
# Real-world: Reusable conditional logic for pipelines.

from pyspark.sql.functions import (  # Import functions.
    col, when, lit, expr, coalesce, current_date, datediff,
    to_date, months_between, round as spark_round
)  # End imports.

# === Pattern: SLA classification ===
print("=== SLA Classification ===")  # Print heading.
tickets = spark.createDataFrame([
    (1, "critical", "2024-01-01", "2024-01-01"),  # Same day.
    (2, "high", "2024-01-01", "2024-01-03"),  # 2 days.
    (3, "medium", "2024-01-01", "2024-01-08"),  # 7 days.
    (4, "low", "2024-01-01", None),  # Not resolved.
    (5, "critical", "2024-01-01", "2024-01-05"),  # 4 days for critical.
], ["id", "priority", "created", "resolved"])  # Ticket data.

tickets.select(
    col("id"), col("priority"),  # Context.
    col("created"), col("resolved"),  # Dates.
    datediff(to_date(col("resolved")), to_date(col("created"))).alias("days_to_resolve"),  # Duration.
    # SLA target by priority.
    when(col("priority") == "critical", 1)
        .when(col("priority") == "high", 3)
        .when(col("priority") == "medium", 7)
        .otherwise(14).alias("sla_days"),  # Target.
    # SLA met?
    when(col("resolved").isNull(), "OPEN")
        .when(
            datediff(to_date(col("resolved")), to_date(col("created"))) <=
            when(col("priority") == "critical", 1)
                .when(col("priority") == "high", 3)
                .when(col("priority") == "medium", 7)
                .otherwise(14),
            "MET"
        ).otherwise("BREACHED").alias("sla_status"),  # Status.
).show(truncate=False)  # Display.

# === Pattern: Data quality flags ===
print("=== Data Quality Flag Pattern ===")  # Print heading.
data = spark.createDataFrame([
    (1, "alice@co.com", 25, 50000),
    (2, "invalid", -5, 999999999),
    (3, "", 150, None),
    (4, "bob@co.com", 30, 60000),
], "id INT, email STRING, age INT, salary INT")  # Quality test.

data.select(
    col("id"),  # Keep.
    # Validate each field.
    when(col("email").rlike("^[\\w.]+@[\\w.]+$"), "valid").otherwise("invalid").alias("email_valid"),
    when((col("age") >= 18) & (col("age") <= 120), "valid").otherwise("invalid").alias("age_valid"),
    when((col("salary").isNotNull()) & (col("salary") > 0) & (col("salary") < 10000000), "valid")
        .otherwise("invalid").alias("salary_valid"),
    # Overall row quality.
    when(
        col("email").rlike("^[\\w.]+@[\\w.]+$") &
        (col("age") >= 18) & (col("age") <= 120) &
        (col("salary").isNotNull()) & (col("salary") > 0),
        "CLEAN"
    ).otherwise("DIRTY").alias("row_quality"),
).show(truncate=False)  # Display quality flags.

print("✅ Conditional Functions mastery complete!")  # Done.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Conditional Functions
# MAGIC
# MAGIC ### Mistake 1: Forgetting otherwise() (silent NULLs)
# MAGIC ```python
# MAGIC # WRONG — no otherwise means unmatched rows get NULL!
# MAGIC df.withColumn("label", when(col("age") > 30, "senior"))
# MAGIC # Rows with age <= 30 get NULL!
# MAGIC
# MAGIC # CORRECT — always include otherwise.
# MAGIC df.withColumn("label", when(col("age") > 30, "senior").otherwise("junior"))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Wrong condition order (first match wins!)
# MAGIC ```python
# MAGIC # WRONG — broader condition first catches everything!
# MAGIC when(col("score") >= 60, "Pass")  # Everyone >= 60 matches here!
# MAGIC     .when(col("score") >= 90, "Excellent")  # Never reached!
# MAGIC
# MAGIC # CORRECT — most specific/restrictive first.
# MAGIC when(col("score") >= 90, "Excellent")  # Check highest first.
# MAGIC     .when(col("score") >= 60, "Pass")
# MAGIC     .otherwise("Fail")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Using when() in filter incorrectly
# MAGIC ```python
# MAGIC # WRONG — when() returns a Column, not a boolean for filter!
# MAGIC df.filter(when(col("age") > 30, True))  # Unpredictable!
# MAGIC
# MAGIC # CORRECT — use the condition directly in filter.
# MAGIC df.filter(col("age") > 30)  # Direct condition.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Type mismatch in when/otherwise branches
# MAGIC ```python
# MAGIC # WRONG — mixing types causes errors or unexpected casting!
# MAGIC when(col("x") > 0, "positive").otherwise(0)  # String vs Int!
# MAGIC
# MAGIC # CORRECT — all branches should return same type.
# MAGIC when(col("x") > 0, "positive").otherwise("zero_or_neg")  # All strings.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not using try_* for user input
# MAGIC ```python
# MAGIC # WRONG — cast() fails on bad data!
# MAGIC df.withColumn("num", col("str_col").cast("int"))  # NULL but might expect error handling.
# MAGIC
# MAGIC # BETTER — try_cast returns NULL explicitly.
# MAGIC df.select(expr("try_cast(str_col AS INT)"))  # Clear intent: NULL on failure.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Conditional Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Create letter grades using when/otherwise. Add a pass/fail flag.
# MAGIC 2. Use SQL CASE WHEN via expr() to categorize prices.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Add more grade tiers (A+, A, A-, B+, etc.).
# MAGIC 4. Use try_cast and try_divide for safe conversions.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Combine when() with aggregation: count by category.
# MAGIC 6. Use when() inside a window function for conditional ranking.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Build a shipping cost calculator: weight + distance + priority = cost.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build a complete loan approval system: multi-factor scoring with dynamic rules.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design a rule engine: rules as config (list of dicts), applied dynamically.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Compare: when/otherwise vs UDF for complex branching on 1M rows.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Test: NULL in conditions, type coercion, empty otherwise, nested when.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build SLA monitoring: classify tickets by response time vs. target.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create a guide: "when/otherwise vs CASE WHEN vs if/else UDF."

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all.

# --- Level 1: Letter grades ---
print("=== Level 1: Letter Grades ===")  # Print heading.
scores = spark.createDataFrame([
    ("Alice", 95), ("Bob", 82), ("Charlie", 67), ("Diana", 45)
], ["name", "score"])  # Scores.

scores.select(
    col("name"), col("score"),  # Keep.
    when(col("score") >= 90, "A")
        .when(col("score") >= 80, "B")
        .when(col("score") >= 70, "C")
        .when(col("score") >= 60, "D")
        .otherwise("F").alias("grade"),  # Grade.
    when(col("score") >= 60, "PASS").otherwise("FAIL").alias("status"),  # Pass/Fail.
).show()  # Display.

# --- Level 4: Shipping calculator ---
print("=== Level 4: Shipping Calculator ===")  # Print heading.
shipments = spark.createDataFrame([
    (1, 2.0, 100, "standard"), (2, 10.0, 500, "express"),
    (3, 0.5, 50, "overnight"), (4, 25.0, 1000, "standard"),
], ["id", "weight_kg", "distance_km", "priority"])  # Shipments.

shipments.select(
    col("id"), col("weight_kg"), col("distance_km"), col("priority"),  # Keep.
    # Base cost by weight.
    (when(col("weight_kg") <= 1, 5.0)
        .when(col("weight_kg") <= 5, 10.0)
        .when(col("weight_kg") <= 20, 20.0)
        .otherwise(50.0)
    # Distance multiplier.
    + when(col("distance_km") <= 100, 2.0)
        .when(col("distance_km") <= 500, 5.0)
        .otherwise(10.0)
    # Priority multiplier.
    ) * when(col("priority") == "overnight", 3.0)
        .when(col("priority") == "express", 2.0)
        .otherwise(1.0)
    .alias("shipping_cost"),  # Total cost.
).show(truncate=False)  # Display.

# --- Level 5: Conditional aggregation ---
print("=== Level 5: Conditional Aggregation ===")  # Print heading.
sales = spark.createDataFrame([
    ("Alice", "online", 100), ("Alice", "store", 200),
    ("Bob", "online", 150), ("Bob", "online", 300),
], ["rep", "channel", "amount"])  # Sales.

sales.groupBy("rep").agg(
    sum("amount").alias("total"),
    sum(when(col("channel") == "online", col("amount")).otherwise(0)).alias("online"),
    sum(when(col("channel") == "store", col("amount")).otherwise(0)).alias("store"),
    count(when(col("channel") == "online", True)).alias("online_txns"),
).show()  # Display.

print("✅ All homework solutions complete!")  # Completion message.