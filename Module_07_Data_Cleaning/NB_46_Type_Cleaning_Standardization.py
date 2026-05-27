# Databricks notebook source
# DBTITLE 1,NB_46 Header
# MAGIC %md
# MAGIC # NB_46 — Type Cleaning and Standardization
# MAGIC
# MAGIC **Module 7: Data Cleaning & Quality** | Notebook 46 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Type casting: cast(), astype()
# MAGIC * Safe casting with try_cast / when+cast patterns
# MAGIC * String-to-date/timestamp parsing
# MAGIC * Number formatting: currency, scientific, localized
# MAGIC * Boolean standardization (yes/no/true/false/1/0)
# MAGIC * Schema enforcement and validation
# MAGIC * Handling parse failures gracefully
# MAGIC * Data type normalization across sources
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Essential for pipeline reliability)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — Why Type Cleaning Matters
# MAGIC %md
# MAGIC ## SECTION 1 — Why Type Cleaning Matters (Real-World Analogy)
# MAGIC
# MAGIC ### 📦 The Unit Converter
# MAGIC
# MAGIC Type cleaning is like converting measurements between systems:
# MAGIC
# MAGIC | Source Format | Target Type | Challenge |
# MAGIC |---|---|---|
# MAGIC | "$1,234.56" | Double | Strip $, commas |
# MAGIC | "15-Mar-2024" | Date | Parse non-standard format |
# MAGIC | "yes"/"Y"/"1"/"true" | Boolean | Normalize variants |
# MAGIC | "1.23E+05" | Long | Scientific notation |
# MAGIC | "NULL"/"N/A"/"" | Null | Sentinel values |
# MAGIC
# MAGIC ### Why This Matters
# MAGIC 1. **Joining** fails silently on type mismatches (string "123" ≠ int 123)
# MAGIC 2. **Aggregations** on string columns give wrong results
# MAGIC 3. **Sorting** on string numbers: "9" > "10" (lexicographic!)
# MAGIC 4. **Storage** efficiency: string "true" = 4 bytes vs boolean = 1 bit
# MAGIC 5. **Downstream ML** pipelines expect numeric/boolean types
# MAGIC
# MAGIC ### The Golden Rule
# MAGIC ```
# MAGIC Source systems lie about types.
# MAGIC Always validate and cast explicitly.
# MAGIC Never trust schema inference on dirty data.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 2 — Casting Mechanics
# MAGIC %md
# MAGIC ## SECTION 2 — Casting Mechanics in Spark
# MAGIC
# MAGIC ### Basic Casting
# MAGIC ```python
# MAGIC col("x").cast("int")           # String/Float -> Integer
# MAGIC col("x").cast(IntegerType())   # Same with type object
# MAGIC col("x").cast("double")        # To double
# MAGIC col("x").cast("date")          # String -> Date (ISO format only)
# MAGIC col("x").cast("timestamp")     # String -> Timestamp
# MAGIC col("x").cast("boolean")       # To boolean
# MAGIC ```
# MAGIC
# MAGIC ### What Happens on Failure?
# MAGIC ```
# MAGIC "abc".cast("int")      -> NULL  (silent failure!)
# MAGIC "2024-13-45".cast("date") -> NULL  (invalid date)
# MAGIC "1.5".cast("int")      -> 1     (truncation, no error!)
# MAGIC "99999999999".cast("int") -> overflow/NULL
# MAGIC ```
# MAGIC
# MAGIC ### Safe Casting Pattern
# MAGIC ```python
# MAGIC # Detect failures: cast and check if NULL was introduced
# MAGIC df.withColumn("parsed", col("raw").cast("int"))
# MAGIC   .withColumn("failed", col("raw").isNotNull() & col("parsed").isNull())
# MAGIC ```
# MAGIC
# MAGIC ### Date/Timestamp Parsing
# MAGIC ```python
# MAGIC to_date(col("x"), "dd-MMM-yyyy")       # Custom format
# MAGIC to_timestamp(col("x"), "MM/dd/yyyy HH:mm")  # With time
# MAGIC date_format(col("x"), "yyyy-MM-dd")    # Reformat
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Basic type casting
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Basic Type Casting
# ============================================================
# Real-world: CSV import produces all-string columns.

from pyspark.sql import SparkSession  # Import.
from pyspark.sql.functions import col, when  # Functions.
from pyspark.sql.types import IntegerType, DoubleType, BooleanType  # Types.

spark = SparkSession.builder.getOrCreate()  # Session.

# Simulated CSV data: everything is strings.
raw = spark.createDataFrame([
    ("1", "Alice", "75000.50", "28", "true", "2020-03-15"),
    ("2", "Bob", "82000.00", "35", "false", "2019-06-01"),
    ("3", "Charlie", "65000.75", "42", "true", "2021-01-10"),
    ("4", "Diana", "90000.25", "31", "false", "2022-07-20"),
    ("5", "Eve", "55000.00", "26", "true", "2023-02-28"),
], ["id", "name", "salary", "age", "is_active", "hire_date"])  # All strings.

print("=== Before Casting (all strings) ===")  # Print heading.
raw.printSchema()  # Schema.
raw.show()  # Data.

# Cast each column to proper type.
typed = raw.select(
    col("id").cast("int").alias("id"),                    # String -> Int.
    col("name"),                                           # Keep as string.
    col("salary").cast("double").alias("salary"),          # String -> Double.
    col("age").cast("int").alias("age"),                   # String -> Int.
    col("is_active").cast("boolean").alias("is_active"),   # String -> Boolean.
    col("hire_date").cast("date").alias("hire_date"),      # String -> Date.
)

print("=== After Casting (proper types) ===")  # Print heading.
typed.printSchema()  # Schema.
typed.show()  # Data.

# Verify types allow proper operations.
print(f"Average salary: {typed.select(col('salary')).agg({'salary': 'avg'}).collect()[0][0]:.2f}")  # Works!
print(f"Max age: {typed.agg({'age': 'max'}).collect()[0][0]}")  # Works!

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Safe casting with failure detection
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Safe Casting with Failure Detection
# ============================================================
# Real-world: Dirty data with unparseable values.

from pyspark.sql.functions import col, when, lit, count, sum as spark_sum  # Imports.

# Dirty data: some values can't be cast.
dirty = spark.createDataFrame([
    ("1", "100.50", "2024-01-15"),
    ("2", "abc", "2024-02-01"),          # "abc" can't be a number.
    ("three", "300.00", "bad-date"),      # "three" not int, bad date.
    ("4", "$450.99", "15/03/2024"),       # $ symbol, non-ISO date.
    ("5", "500", "2024-04-01"),
    ("6", "N/A", "2024-05-15"),           # "N/A" not a number.
    ("7", "", "2024-06-01"),              # Empty string.
], ["id_str", "amount_str", "date_str"])  # Dirty strings.

print("=== Dirty Data ===")  # Heading.
dirty.show(truncate=False)  # Display.

# Safe cast: attempt cast and detect failures.
print("=== Safe Casting (detect failures) ===")  # Heading.
safe = dirty.select(
    col("id_str"),  # Original.
    col("id_str").cast("int").alias("id_parsed"),  # Attempt cast.
    col("amount_str"),  # Original.
    col("amount_str").cast("double").alias("amount_parsed"),  # Attempt.
    col("date_str"),  # Original.
    col("date_str").cast("date").alias("date_parsed"),  # Attempt.
)

# Add failure flags.
safe_flagged = safe.withColumn(
    "id_failed",
    (col("id_str").isNotNull()) & (col("id_parsed").isNull())  # Had value but cast failed.
).withColumn(
    "amount_failed",
    (col("amount_str").isNotNull()) & (col("amount_str") != "") & (col("amount_parsed").isNull())
).withColumn(
    "date_failed",
    (col("date_str").isNotNull()) & (col("date_str") != "") & (col("date_parsed").isNull())
)

safe_flagged.show(truncate=False)  # Display.

# Failure report.
print("=== Parse Failure Report ===")  # Heading.
safe_flagged.select(
    spark_sum(col("id_failed").cast("int")).alias("id_failures"),
    spark_sum(col("amount_failed").cast("int")).alias("amount_failures"),
    spark_sum(col("date_failed").cast("int")).alias("date_failures"),
).show()  # Report.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Date/timestamp parsing
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Date/Timestamp Parsing
# ============================================================
# Real-world: Dates in various formats from different systems.

from pyspark.sql.functions import (
    col, to_date, to_timestamp, date_format, coalesce
)  # Imports.

# Dates in multiple formats (common in real data).
dates = spark.createDataFrame([
    ("2024-01-15",),           # ISO format.
    ("01/15/2024",),           # US format MM/dd/yyyy.
    ("15-Jan-2024",),          # dd-MMM-yyyy.
    ("January 15, 2024",),     # Full month name.
    ("20240115",),             # Compact yyyyMMdd.
    ("2024.01.15",),           # Dot separator.
    ("15/01/2024",),           # EU format dd/MM/yyyy.
], ["raw_date"])  # Raw dates.

print("=== Multi-Format Date Parsing ===")  # Heading.

# Try multiple formats with coalesce (first successful parse wins).
parsed = dates.withColumn(
    "parsed_date",
    coalesce(
        to_date(col("raw_date"), "yyyy-MM-dd"),      # ISO.
        to_date(col("raw_date"), "MM/dd/yyyy"),      # US.
        to_date(col("raw_date"), "dd-MMM-yyyy"),     # Abbreviated month.
        to_date(col("raw_date"), "MMMM dd, yyyy"),   # Full month.
        to_date(col("raw_date"), "yyyyMMdd"),        # Compact.
        to_date(col("raw_date"), "yyyy.MM.dd"),      # Dot.
        to_date(col("raw_date"), "dd/MM/yyyy"),      # EU.
    )  # First non-null wins.
)

parsed.show(truncate=False)  # Display.

# Timestamp parsing.
print("=== Timestamp Parsing ===")  # Heading.
timestamps = spark.createDataFrame([
    ("2024-01-15 10:30:00",),
    ("2024-01-15T10:30:00.000Z",),  # ISO with T and Z.
    ("01/15/2024 2:30 PM",),        # US with AM/PM.
    ("15-Jan-2024 14:30:00",),      # Abbreviated.
], ["raw_ts"])  # Timestamps.

ts_parsed = timestamps.withColumn(
    "parsed_ts",
    coalesce(
        to_timestamp(col("raw_ts"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("raw_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        to_timestamp(col("raw_ts"), "MM/dd/yyyy h:mm a"),
        to_timestamp(col("raw_ts"), "dd-MMM-yyyy HH:mm:ss"),
    )  # First successful parse.
)
ts_parsed.show(truncate=False)  # Display.

# Standardize output format.
print("=== Standardized Output ===")  # Heading.
ts_parsed.withColumn(
    "standard", date_format(col("parsed_ts"), "yyyy-MM-dd HH:mm:ss")  # ISO output.
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Number and currency cleaning
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Number and Currency Cleaning
# ============================================================
# Real-world: Financial data with locale-specific formatting.

from pyspark.sql.functions import (
    col, regexp_replace, trim, when, lit, upper
)  # Imports.

# Messy numeric data.
numbers = spark.createDataFrame([
    ("$1,234.56",),        # US currency.
    ("€1.234,56",),        # EU currency (dot=thousands, comma=decimal).
    ("(500.00)",),         # Accounting negative.
    ("-1,000.50",),        # Standard negative.
    ("1.23E+04",),         # Scientific notation.
    ("  45.67  ",),        # Whitespace.
    ("100%",),             # Percentage.
    ("N/A",),              # Sentinel.
    ("",),                 # Empty.
    ("12,345",),           # US thousands separator.
], ["raw_number"])  # Raw numbers.

print("=== Raw Number Formats ===")  # Heading.
numbers.show(truncate=False)  # Display.

# Step 1: Clean US-format numbers.
print("=== US-Format Number Cleaning ===")  # Heading.
us_cleaned = numbers.withColumn(
    "cleaned",
    regexp_replace(col("raw_number"), "[\\$\\s%]", "")  # Remove $, spaces, %.
).withColumn(
    "cleaned",
    regexp_replace(col("cleaned"), ",", "")  # Remove commas.
).withColumn(
    "cleaned",
    # Handle accounting negatives: (500) -> -500.
    when(col("cleaned").startswith("(") & col("cleaned").endswith(")"),
         regexp_replace(regexp_replace(col("cleaned"), "\\(", "-"), "\\)", ""))
    .otherwise(col("cleaned"))  # Handle parens.
).withColumn(
    "cleaned",
    # Handle sentinels.
    when(col("cleaned").isin("N/A", "NA", "null", "NULL", ""), None)
    .otherwise(col("cleaned"))  # Sentinels to NULL.
).withColumn(
    "parsed_number", col("cleaned").cast("double")  # Final cast.
)

us_cleaned.show(truncate=False)  # Display.

# Boolean standardization.
print("=== Boolean Standardization ===")  # Heading.
booleans = spark.createDataFrame([
    ("yes",), ("YES",), ("Y",), ("y",), ("true",), ("True",), ("1",), ("on",),
    ("no",), ("NO",), ("N",), ("n",), ("false",), ("False",), ("0",), ("off",),
    ("maybe",), (None,), ("",),  # Edge cases.
], ["raw_bool"])  # Raw booleans.

std_bool = booleans.withColumn(
    "standardized",
    when(upper(trim(col("raw_bool"))).isin("YES", "Y", "TRUE", "1", "ON"), True)
    .when(upper(trim(col("raw_bool"))).isin("NO", "N", "FALSE", "0", "OFF"), False)
    .otherwise(None)  # Unknown -> NULL.
)
std_bool.show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Schema enforcement
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Schema Enforcement & Validation
# ============================================================
# Real-world: Enforce expected schema on incoming data.

from pyspark.sql.functions import col, when, lit, count, sum as spark_sum  # Imports.
from pyspark.sql.types import (  # Import types.
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, BooleanType, TimestampType
)  # End types.

# Define expected schema.
expected_schema = StructType([
    StructField("id", IntegerType(), False),        # Required int.
    StructField("name", StringType(), False),       # Required string.
    StructField("salary", DoubleType(), True),      # Optional double.
    StructField("hire_date", DateType(), True),     # Optional date.
    StructField("is_active", BooleanType(), True),  # Optional boolean.
])

# Incoming raw data (all strings from CSV).
raw_data = spark.createDataFrame([
    ("1", "Alice", "75000.50", "2024-01-15", "true"),
    ("2", "Bob", "invalid", "2024-02-01", "yes"),    # Invalid salary.
    ("x", "Charlie", "60000", "bad-date", "false"),  # Invalid id, date.
    ("4", "", "90000", "2024-04-01", "maybe"),       # Empty name, invalid bool.
    ("5", "Eve", "55000", "2024-05-15", "1"),
], ["id", "name", "salary", "hire_date", "is_active"])  # Raw.

print("=== Schema Enforcement ===")  # Heading.
print(f"Expected schema: {expected_schema.simpleString()}")  # Show expected.

# Apply schema with validation.
from pyspark.sql.functions import upper, trim, to_date  # More imports.

enforced = raw_data.select(
    col("id").cast("int").alias("id"),  # Cast id.
    when(trim(col("name")) == "", None).otherwise(col("name")).alias("name"),  # Empty->NULL.
    col("salary").cast("double").alias("salary"),  # Cast salary.
    to_date(col("hire_date"), "yyyy-MM-dd").alias("hire_date"),  # Parse date.
    when(upper(trim(col("is_active"))).isin("TRUE", "YES", "1"), True)
        .when(upper(trim(col("is_active"))).isin("FALSE", "NO", "0"), False)
        .otherwise(None).alias("is_active"),  # Boolean.
)

print("\n=== After Schema Enforcement ===")  # Heading.
enforced.printSchema()  # Check types.
enforced.show()  # Display.

# Validation report: which rows failed which columns.
print("=== Validation Report ===")  # Heading.
validation = raw_data.select(
    col("id"),
    (col("id").cast("int").isNull() & col("id").isNotNull()).alias("id_invalid"),
    (trim(col("name")) == "").alias("name_empty"),
    (col("salary").cast("double").isNull() & col("salary").isNotNull() & (col("salary") != "")).alias("salary_invalid"),
    (to_date(col("hire_date"), "yyyy-MM-dd").isNull() & col("hire_date").isNotNull()).alias("date_invalid"),
)
validation.show()  # Show failures.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Multi-source type normalization
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Multi-Source Type Normalization
# ============================================================
# Real-world: Same field represented differently across systems.

from pyspark.sql.functions import (
    col, when, upper, trim, regexp_replace, to_date, coalesce, lit
)  # Imports.

# Source A: structured format.
source_a = spark.createDataFrame([
    (101, "ACTIVE", "2024-01-15", 1500.00),
    (102, "INACTIVE", "2024-02-01", 2300.50),
], ["account_id", "status", "created_date", "balance"])  # Source A.

# Source B: different formats for same data.
source_b = spark.createDataFrame([
    ("A-201", "Y", "15/01/2024", "$1,800.00"),
    ("A-202", "N", "01/02/2024", "$950.25"),
], ["account_id", "status", "created_date", "balance"])  # Source B.

print("=== Source A (structured) ===")  # Heading.
source_a.printSchema()  # Schema.
source_a.show()  # Data.

print("=== Source B (different formats) ===")  # Heading.
source_b.printSchema()  # Schema.
source_b.show(truncate=False)  # Data.

# Normalize Source A.
print("=== Normalizing Source A ===")  # Heading.
norm_a = source_a.select(
    col("account_id").cast("string").alias("account_id"),  # String id.
    when(upper(col("status")) == "ACTIVE", True)
        .otherwise(False).alias("is_active"),  # Boolean.
    col("created_date").cast("date").alias("created_date"),  # Already good.
    col("balance").cast("double").alias("balance"),  # Already good.
    lit("SOURCE_A").alias("origin"),  # Track source.
)

# Normalize Source B.
print("=== Normalizing Source B ===")  # Heading.
norm_b = source_b.select(
    col("account_id").alias("account_id"),  # Keep as string.
    when(upper(trim(col("status"))).isin("Y", "YES", "ACTIVE"), True)
        .otherwise(False).alias("is_active"),  # Normalize boolean.
    to_date(col("created_date"), "dd/MM/yyyy").alias("created_date"),  # Parse EU date.
    regexp_replace(regexp_replace(col("balance"), "[\\$,]", ""), "\\s", "")
        .cast("double").alias("balance"),  # Clean currency.
    lit("SOURCE_B").alias("origin"),  # Track source.
)

# Union normalized sources.
print("=== Unified Dataset ===")  # Heading.
unified = norm_a.unionByName(norm_b)  # Combine.
unified.printSchema()  # Schema.
unified.show(truncate=False)  # Display.
print("Both sources now share identical schema and semantics!")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Production type cleaner
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Production Type Cleaner
# ============================================================
# Real-world: Reusable, configurable type cleaning framework.

from pyspark.sql.functions import (
    col, when, upper, trim, regexp_replace, to_date, to_timestamp,
    coalesce, lit, count, sum as spark_sum
)  # Imports.
from pyspark.sql import DataFrame  # Type.

class TypeCleaner:
    """Production type cleaning framework with audit."""
    
    BOOL_TRUE = ["TRUE", "YES", "Y", "1", "ON", "ACTIVE"]  # True values.
    BOOL_FALSE = ["FALSE", "NO", "N", "0", "OFF", "INACTIVE"]  # False values.
    NULL_SENTINELS = ["N/A", "NA", "NULL", "NONE", "", "-", "."]  # NULL values.
    
    def __init__(self, df):
        """Initialize with DataFrame."""
        self.df = df  # Store.
        self.audit = {}  # Track changes.
    
    def clean_numeric(self, col_name, target_type="double"):
        """Clean numeric: remove currency/commas, handle negatives."""
        self.df = self.df.withColumn(
            col_name,
            when(upper(trim(col(col_name))).isin(*self.NULL_SENTINELS), None)  # Sentinels.
            .otherwise(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(col(col_name), "[\\$\\s\\u20ac\\u00a3%]", ""),  # Currency.
                    ",", ""),  # Commas.
                "\\((.*?)\\)", "-$1")  # Accounting negatives.
            )
        ).withColumn(col_name, col(col_name).cast(target_type))  # Cast.
        self.audit[col_name] = f"numeric ({target_type})"  # Record.
        return self  # Chain.
    
    def clean_boolean(self, col_name):
        """Standardize boolean variants."""
        self.df = self.df.withColumn(
            col_name,
            when(upper(trim(col(col_name).cast("string"))).isin(*self.BOOL_TRUE), True)
            .when(upper(trim(col(col_name).cast("string"))).isin(*self.BOOL_FALSE), False)
            .otherwise(None)  # Unknown -> NULL.
        )
        self.audit[col_name] = "boolean"  # Record.
        return self  # Chain.
    
    def clean_date(self, col_name, formats=None):
        """Parse date from multiple formats."""
        if formats is None:  # Defaults.
            formats = ["yyyy-MM-dd", "MM/dd/yyyy", "dd/MM/yyyy", "dd-MMM-yyyy", "yyyyMMdd"]
        self.df = self.df.withColumn(
            col_name,
            coalesce(*[to_date(col(col_name), f) for f in formats])  # Try all.
        )
        self.audit[col_name] = f"date ({len(formats)} formats)"  # Record.
        return self  # Chain.
    
    def clean_string(self, col_name, case="upper"):
        """Normalize string: trim, case, sentinels."""
        cleaned = trim(col(col_name))  # Trim.
        if case == "upper":  # Upper.
            cleaned = upper(cleaned)
        self.df = self.df.withColumn(
            col_name,
            when(upper(cleaned).isin(*self.NULL_SENTINELS), None).otherwise(cleaned)
        )
        self.audit[col_name] = f"string ({case})"  # Record.
        return self  # Chain.
    
    def result(self):
        """Return cleaned DataFrame and print audit."""
        print(f"\n{'='*50}")
        print(f"  TYPE CLEANING AUDIT")
        print(f"{'='*50}")
        for c, action in self.audit.items():  # Report.
            print(f"  {c:20s} -> {action}")
        print(f"{'='*50}\n")
        return self.df  # Return.

# Apply production cleaner.
print("=== Production Type Cleaner ===")  # Heading.
messy = spark.createDataFrame([
    ("$12,500.00", "yes", "15-Jan-2024", "  alice  "),
    ("(500.00)", "N", "01/15/2024", "BOB"),
    ("N/A", "maybe", "2024-03-01", "n/a"),
    ("1.5E+03", "1", "20240401", "  Charlie  "),
], ["amount", "active", "date", "name"])  # Messy data.

clean = (
    TypeCleaner(messy)
    .clean_numeric("amount", "double")
    .clean_boolean("active")
    .clean_date("date")
    .clean_string("name", "upper")
    .result()
)
clean.show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Schema evolution handler
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Schema Evolution & Mismatch Handler
# ============================================================
# Real-world: Handle schema changes between data batches.

from pyspark.sql.functions import col, lit, when  # Imports.
from pyspark.sql.types import (  # Types.
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)  # End types.

def reconcile_schemas(df, target_schema):
    """
    Reconcile DataFrame to match target schema:
    - Add missing columns (with NULL)
    - Cast mismatched types
    - Remove extra columns
    - Report differences
    """
    actual_cols = set(df.columns)  # Actual columns.
    target_cols = {f.name for f in target_schema.fields}  # Expected columns.
    
    missing = target_cols - actual_cols  # Missing from data.
    extra = actual_cols - target_cols  # Extra in data.
    common = actual_cols & target_cols  # Shared columns.
    
    # Build report.
    print(f"\n{'='*50}")
    print(f"  SCHEMA RECONCILIATION REPORT")
    print(f"{'='*50}")
    if missing:  # Report missing.
        print(f"  ⚠️  Missing columns (added as NULL): {missing}")
    if extra:  # Report extra.
        print(f"  ⚠️  Extra columns (removed): {extra}")
    
    result = df  # Start.
    
    # Add missing columns.
    for field in target_schema.fields:  # Each expected field.
        if field.name in missing:  # If missing.
            result = result.withColumn(field.name, lit(None).cast(field.dataType))  # Add.
    
    # Cast common columns to target type.
    type_changes = []  # Track.
    for field in target_schema.fields:  # Each expected.
        if field.name in common:  # If exists.
            actual_type = dict(df.dtypes)[field.name]  # Current type.
            target_type = field.dataType.simpleString()  # Target type.
            if actual_type != target_type:  # Mismatch.
                result = result.withColumn(field.name, col(field.name).cast(field.dataType))
                type_changes.append(f"{field.name}: {actual_type} -> {target_type}")
    
    if type_changes:  # Report.
        print(f"  🔄 Type changes: {type_changes}")
    
    # Select only target columns in order.
    result = result.select([f.name for f in target_schema.fields])  # Reorder.
    print(f"  ✅ Final schema matches target.")
    print(f"{'='*50}\n")
    
    return result  # Return reconciled.

# Test: batch with missing/extra/mistyped columns.
target = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("amount", DoubleType()),
    StructField("date", StringType()),
    StructField("category", StringType()),  # Missing from batch.
])

# Incoming batch: missing 'category', extra 'notes', 'amount' as string.
batch = spark.createDataFrame([
    ("1", "Alice", "100.50", "2024-01-15", "some note"),
    ("2", "Bob", "200.00", "2024-02-01", "another note"),
], ["id", "name", "amount", "date", "notes"])  # Mismatch.

print("=== Before Reconciliation ===")  # Heading.
batch.printSchema()  # Show actual.

reconciled = reconcile_schemas(batch, target)  # Reconcile.
reconciled.printSchema()  # Show result.
reconciled.show()  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Data quality validation
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Type-Based Data Quality Validation
# ============================================================
# Real-world: Validate that data conforms to type constraints.

from pyspark.sql.functions import (
    col, when, length, regexp_extract, trim, upper,
    count, sum as spark_sum, lit
)  # Imports.

def validate_types(df, rules):
    """
    Validate data against type rules.
    Rules: dict of {col: {"type": ..., "pattern": ..., "range": ...}}
    """
    results = []  # Collect results.
    
    for col_name, rule in rules.items():  # Each rule.
        total = df.filter(col(col_name).isNotNull()).count()  # Non-null rows.
        failures = 0  # Count failures.
        
        if "type" in rule:  # Type check.
            cast_col = col(col_name).cast(rule["type"])  # Attempt cast.
            failures = df.filter(
                col(col_name).isNotNull() & cast_col.isNull()  # Cast failed.
            ).count()
        
        elif "pattern" in rule:  # Regex check.
            failures = df.filter(
                col(col_name).isNotNull() &
                (regexp_extract(col(col_name), rule["pattern"], 0) == "")  # No match.
            ).count()
        
        elif "values" in rule:  # Allowed values.
            failures = df.filter(
                col(col_name).isNotNull() &
                ~upper(trim(col(col_name))).isin(*[v.upper() for v in rule["values"]])
            ).count()
        
        pass_rate = round((total - failures) / max(total, 1) * 100, 1)  # Rate.
        status = "✅" if pass_rate >= 99 else "🟡" if pass_rate >= 95 else "🔴"  # Status.
        results.append((col_name, rule.get("type", rule.get("pattern", str(rule.get("values","")))),
                       total, failures, pass_rate, status))
    
    # Print report.
    print(f"\n{'='*70}")
    print(f"  DATA TYPE VALIDATION REPORT")
    print(f"{'='*70}")
    print(f"  {'Column':<12} {'Rule':<20} {'Total':<8} {'Fail':<6} {'Pass%':<8} {'Status'}")
    print(f"  {'-'*12} {'-'*20} {'-'*8} {'-'*6} {'-'*8} {'-'*6}")
    for r in results:  # Print each.
        print(f"  {r[0]:<12} {str(r[1]):<20} {r[2]:<8} {r[3]:<6} {r[4]:<8} {r[5]}")
    print(f"{'='*70}\n")

# Validation test.
test_data = spark.createDataFrame([
    ("1", "alice@co.com", "75000", "ACTIVE", "12345"),
    ("2", "bob@co.com", "abc", "INACTIVE", "67890"),
    ("x", "not-an-email", "50000", "MAYBE", "1234"),  # Bad id, email, status, zip.
    ("4", "diana@co.com", "90000", "ACTIVE", "abcde"),  # Bad zip.
    ("5", "eve@co.com", "60000", "INACTIVE", "54321"),
], ["id", "email", "salary", "status", "zip_code"])  # Test data.

# Define validation rules.
rules = {
    "id": {"type": "int"},  # Must be integer.
    "email": {"pattern": r"^[\w.+-]+@[\w-]+\.[\w.-]+$"},  # Email pattern.
    "salary": {"type": "double"},  # Must be numeric.
    "status": {"values": ["ACTIVE", "INACTIVE", "PENDING"]},  # Allowed values.
    "zip_code": {"pattern": r"^\d{5}$"},  # 5 digits.
}

validate_types(test_data, rules)  # Run validation.
print("✅ Type cleaning and standardization mastery complete!")  # Done.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Type Cleaning
# MAGIC
# MAGIC ### Mistake 1: Silent cast failures
# MAGIC ```python
# MAGIC # cast() returns NULL on failure — no error raised!
# MAGIC col("abc").cast("int")  # Returns NULL silently.
# MAGIC
# MAGIC # ALWAYS check for introduced NULLs after casting:
# MAGIC df.filter(col("raw").isNotNull() & col("parsed").isNull())  # These FAILED.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Integer truncation
# MAGIC ```python
# MAGIC # Casting float to int TRUNCATES, doesn't round!
# MAGIC col("1.9").cast("int")  # Returns 1, not 2!
# MAGIC
# MAGIC # Use round() first if you want rounding:
# MAGIC round(col("value"), 0).cast("int")
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Date format confusion (MM vs mm)
# MAGIC ```python
# MAGIC # WRONG: mm = minutes, not months!
# MAGIC to_date(col("x"), "yyyy-mm-dd")  # WRONG!
# MAGIC
# MAGIC # CORRECT: MM = months
# MAGIC to_date(col("x"), "yyyy-MM-dd")  # Correct.
# MAGIC # Java SimpleDateFormat: M=month, m=minute, d=day, D=dayOfYear
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Locale-dependent number formats
# MAGIC ```python
# MAGIC # European: 1.234,56 (dot=thousands, comma=decimal)
# MAGIC # US:       1,234.56 (comma=thousands, dot=decimal)
# MAGIC # Know your source locale BEFORE cleaning!
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: fill() only matches by type
# MAGIC ```python
# MAGIC # fill(0) only fills numeric columns!
# MAGIC # fill("") only fills string columns!
# MAGIC # ALWAYS use explicit column mapping:
# MAGIC df.na.fill({"num_col": 0, "str_col": "default"})
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Type Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Cast string columns to int, double, date, boolean.
# MAGIC 2. Detect cast failures (introduced NULLs).
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Parse dates in 3 different formats.
# MAGIC 4. Clean currency strings to doubles.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Build boolean normalizer handling yes/no/true/false/1/0/on/off.
# MAGIC 6. Combine number cleaning + safe casting + failure reporting.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Normalize data from 3 different sources with different schemas.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build a `TypeCleaner` class with chainable methods.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design schema evolution handler for batch ingestion.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Compare: regex cleanup + cast vs to_date with format vs try_cast.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Handle: overflow, NaN, Infinity, empty strings, whitespace-only.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build validation framework: rules config + audit report.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create cheat sheet: "Every Spark type and its gotchas."

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all.

# --- Level 1: Basic casting ---
print("=== Level 1: Basic Casting ===")  # Heading.
raw = spark.createDataFrame([("42", "3.14", "2024-01-15", "true")], ["i", "d", "dt", "b"])
raw.select(
    col("i").cast("int"),
    col("d").cast("double"),
    col("dt").cast("date"),
    col("b").cast("boolean"),
).show()  # Display.

# --- Level 3: Boolean normalizer ---
print("\n=== Level 3: Boolean Normalizer ===")  # Heading.
def normalize_bool(col_name):
    """Universal boolean normalizer."""
    return (
        when(upper(trim(col(col_name))).isin("TRUE","YES","Y","1","ON","ACTIVE"), True)
        .when(upper(trim(col(col_name))).isin("FALSE","NO","N","0","OFF","INACTIVE"), False)
        .otherwise(None)
    )

test = spark.createDataFrame([("yes",),("NO",),("1",),("off",),("maybe",)], ["val"])
test.withColumn("bool", normalize_bool("val")).show()

# --- Level 8: Edge cases ---
print("\n=== Level 8: Edge Cases ===")  # Heading.
edge = spark.createDataFrame([
    ("99999999999999",),  # Overflow for int.
    ("Infinity",),        # Special float.
    ("-Infinity",),       # Negative infinity.
    ("NaN",),             # Not a number.
    ("   ",),             # Whitespace only.
], ["val"])  # Edge cases.

edge.select(
    col("val"),
    col("val").cast("int").alias("as_int"),  # Overflow -> NULL.
    col("val").cast("double").alias("as_double"),  # Infinity/NaN -> special.
    (trim(col("val")) == "").alias("is_whitespace_only"),  # Detect.
).show()  # Display.

print("✅ All type cleaning solutions complete!")  # Done.