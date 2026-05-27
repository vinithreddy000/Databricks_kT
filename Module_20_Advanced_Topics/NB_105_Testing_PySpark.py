# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 105: Testing PySpark Code
# MAGIC ## Module 20: Advanced Topics
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Testing PySpark code** means writing automated checks that verify your transformations produce correct results. This includes unit tests (single functions), integration tests (full pipelines), and data quality tests (validate output data).
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC A car factory has quality checks at every station: the welder checks joints, the painter checks coverage, the final inspector checks everything together. Testing PySpark code is the same — check each transformation individually (unit) and the entire pipeline together (integration).
# MAGIC
# MAGIC ### Testing Pyramid:
# MAGIC | Level | What | How | Speed |
# MAGIC |-------|------|-----|-------|
# MAGIC | Unit tests | Single transform functions | pytest + local Spark | Fast (seconds) |
# MAGIC | Integration | Full pipeline end-to-end | Databricks Connect | Medium (minutes) |
# MAGIC | Data quality | Output table validation | Great Expectations / assertions | Scheduled |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Testing Pattern for PySpark:
# MAGIC
# MAGIC   1. EXTRACT transform logic into pure functions:
# MAGIC      def clean_data(df): return df.filter(col("amount") > 0).dropna()
# MAGIC
# MAGIC   2. WRITE tests that call the function with test data:
# MAGIC      def test_clean_removes_negatives(spark):
# MAGIC          input = spark.createDataFrame([(-1,), (5,), (0,)], ["amount"])
# MAGIC          result = clean_data(input)
# MAGIC          assert result.count() == 1  # Only amount=5 survives.
# MAGIC
# MAGIC   3. RUN tests:
# MAGIC      pytest tests/ -v  # Locally with DB Connect.
# MAGIC      OR: In a Databricks notebook with nutter/pytest.
# MAGIC
# MAGIC Project Structure:
# MAGIC   src/
# MAGIC     transforms.py      # Pure transform functions.
# MAGIC     loaders.py         # I/O functions (read/write tables).
# MAGIC   tests/
# MAGIC     conftest.py        # pytest fixtures (SparkSession).
# MAGIC     test_transforms.py # Unit tests for transforms.
# MAGIC     test_pipeline.py   # Integration tests.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-5: Examples
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-5 — TESTING PYSPARK CODE
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, when, sum as spark_sum  # Imports.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType  # Schema.

print("="*70)
print("SECTIONS 3-5: Testing PySpark Code")
print("="*70)

# ─── EXAMPLE 1: Testable transform functions ───
print("\n" + "-"*60)
print("EXAMPLE 1: Writing testable transform functions")
print("-"*60)

# RULE: Extract logic into PURE functions (input DF → output DF).
def clean_orders(df):
    """Remove invalid orders (negative amounts, nulls)."""
    return df.filter(col("amount") > 0).dropna(subset=["customer_id"])  # Pure function.

def categorize_orders(df):
    """Add tier column based on order amount."""
    return df.withColumn("tier",
        when(col("amount") >= 500, "premium")
        .when(col("amount") >= 100, "standard")
        .otherwise("basic")
    )  # Pure function.

def aggregate_by_customer(df):
    """Summarize orders by customer."""
    return df.groupBy("customer_id").agg(
        spark_sum("amount").alias("total_spent"),
        col("customer_id")  # Needed for count.
    ).drop("customer_id")  # Remove duplicate. Actually let's redo:

def aggregate_by_customer(df):
    """Summarize orders by customer."""
    return df.groupBy("customer_id").agg(
        spark_sum("amount").alias("total_spent")
    )  # Returns customer_id + total_spent.

print("✓ Each function: takes DataFrame in, returns DataFrame out.")
print("  No side effects (no writes, no reads). Easy to test.")

# ─── EXAMPLE 2: Unit tests (in-notebook) ───
print("\n" + "-"*60)
print("EXAMPLE 2: Unit tests (running in this notebook)")
print("-"*60)

# Test 1: clean_orders removes negatives and nulls.
test_data = spark.createDataFrame([
    (1, 100.0), (2, -50.0), (None, 200.0), (3, 0.0), (4, 300.0)
], ["customer_id", "amount"])

result = clean_orders(test_data)  # Apply function.
assert result.count() == 2, f"Expected 2 rows, got {result.count()}"  # Only (1,100) and (4,300).
print("  ✓ test_clean_orders_removes_invalid: PASSED")

# Test 2: categorize_orders assigns correct tiers.
test_data2 = spark.createDataFrame([
    (1, 50.0), (2, 150.0), (3, 600.0)
], ["customer_id", "amount"])

result2 = categorize_orders(test_data2)  # Apply function.
tiers = [row["tier"] for row in result2.orderBy("customer_id").collect()]  # Collect results.
assert tiers == ["basic", "standard", "premium"], f"Got {tiers}"  # Verify.
print("  ✓ test_categorize_orders_correct_tiers: PASSED")

# Test 3: aggregate_by_customer sums correctly.
test_data3 = spark.createDataFrame([
    (1, 100.0), (1, 200.0), (2, 50.0)
], ["customer_id", "amount"])

result3 = aggregate_by_customer(test_data3)  # Apply function.
result3_pd = result3.toPandas().set_index("customer_id")  # Collect for assertion.
assert result3_pd.loc[1, "total_spent"] == 300.0  # 100 + 200.
assert result3_pd.loc[2, "total_spent"] == 50.0   # Just 50.
print("  ✓ test_aggregate_by_customer_sums: PASSED")

print("\n✓ All 3 unit tests passed! Functions work correctly.")

# ─── EXAMPLE 3: conftest.py and pytest structure ───
print("\n" + "-"*60)
print("EXAMPLE 3: pytest project structure")
print("-"*60)

print("""
# tests/conftest.py (shared fixtures)
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    '''Create SparkSession for testing (local or DB Connect).'''
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()  # DB Connect.
    except ImportError:
        return SparkSession.builder.master("local[*]").getOrCreate()  # Local.

# tests/test_transforms.py
from src.transforms import clean_orders, categorize_orders

def test_clean_removes_negatives(spark):
    input_df = spark.createDataFrame([(-1,1), (5,2)], ["amount","customer_id"])
    result = clean_orders(input_df)
    assert result.count() == 1

def test_clean_removes_null_customer(spark):
    input_df = spark.createDataFrame([(100, None), (200, 1)], ["amount","customer_id"])
    result = clean_orders(input_df)
    assert result.count() == 1

# Run: pytest tests/ -v --tb=short
""")

print("✓ Structure: conftest.py (fixtures) + test_*.py (test functions).")
print("  Each test: Arrange (create data) → Act (call function) → Assert (check).")

# COMMAND ----------

# DBTITLE 1,Section 6: Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes (Top 5)
# MAGIC
# MAGIC ### Mistake 1: Testing with production data instead of controlled test data
# MAGIC ```python
# MAGIC # BAD: Test depends on production table (brittle, slow, non-deterministic).
# MAGIC def test_my_transform():
# MAGIC     df = spark.table("prod.big_table")  # What if data changes?
# MAGIC     result = my_transform(df)
# MAGIC     assert result.count() > 0  # Meaningless!
# MAGIC
# MAGIC # GOOD: Create deterministic test data.
# MAGIC def test_my_transform(spark):
# MAGIC     input_df = spark.createDataFrame([(1, 100), (2, -50)], ["id", "amount"])
# MAGIC     result = my_transform(input_df)
# MAGIC     assert result.count() == 1  # Exact expectation.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Not separating I/O from logic
# MAGIC ```python
# MAGIC # BAD: Transform function reads AND writes (untestable without real table).
# MAGIC def process_orders():
# MAGIC     df = spark.table("orders")      # Hard-coded read.
# MAGIC     result = df.filter(col("x") > 0)
# MAGIC     result.write.saveAsTable("out")  # Hard-coded write.
# MAGIC
# MAGIC # GOOD: Separate concerns.
# MAGIC def transform_orders(df):           # Pure logic (testable!).
# MAGIC     return df.filter(col("x") > 0)
# MAGIC
# MAGIC # In notebook/pipeline:
# MAGIC df = spark.table("orders")
# MAGIC result = transform_orders(df)
# MAGIC result.write.saveAsTable("out")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Using assert on floating-point equality
# MAGIC ```python
# MAGIC # BAD: Floating point comparison may fail due to precision.
# MAGIC assert result == 3.14159265358979  # May fail: 3.141592653589790001!
# MAGIC
# MAGIC # GOOD: Use approximate comparison.
# MAGIC assert abs(result - 3.14159) < 0.0001  # Tolerance.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Not testing edge cases
# MAGIC ```python
# MAGIC # BAD: Only testing the happy path.
# MAGIC def test_works():
# MAGIC     df = spark.createDataFrame([(1, 100)], ["id", "amt"])
# MAGIC     assert my_func(df).count() == 1  # Only tests normal case.
# MAGIC
# MAGIC # GOOD: Test edge cases too.
# MAGIC def test_empty_input(spark):       # What if input is empty?
# MAGIC def test_all_nulls(spark):         # What if all values are null?
# MAGIC def test_single_row(spark):        # Boundary condition.
# MAGIC def test_duplicate_keys(spark):    # What about duplicates?
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not running tests in CI/CD
# MAGIC ```yaml
# MAGIC # BAD: Tests exist but nobody runs them (manual only).
# MAGIC
# MAGIC # GOOD: Automate in CI pipeline.
# MAGIC # azure-pipelines.yml:
# MAGIC steps:
# MAGIC   - script: pytest tests/ -v --junitxml=results.xml
# MAGIC     displayName: 'Run PySpark unit tests'
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 7: Homework
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — HOMEWORK
# ═══════════════════════════════════════════════════════════════════
print("="*70)
print("HOMEWORK — Testing PySpark")
print("="*70)
print("\n--- Level 1-3: Basics ---")
print("  Write pure functions (DF in → DF out). No side effects.")
print("  Test with: spark.createDataFrame() for known inputs.")
print("  Assert exact outcomes (count, values, schema).")
print("\n--- Level 4-6: pytest ---")
print("  conftest.py with SparkSession fixture.")
print("  test_*.py with Arrange/Act/Assert pattern.")
print("  Edge cases: empty, nulls, duplicates, single row.")
print("\n--- Level 7-10: Production ---")
print("  Integration tests with DB Connect.")
print("  CI/CD: pytest in Azure DevOps/GitHub Actions.")
print("  Data quality: assert counts, null rates, schema after ETL.")
print("\n" + "="*70)
print("✓ HOMEWORK COMPLETED — Notebook 105")
print("="*70)