# Databricks notebook source
# DBTITLE 1,NB_41 Header
# MAGIC %md
# MAGIC # NB_41 — Python UDFs (Regular and Registered)
# MAGIC
# MAGIC **Module 6: User-Defined Functions** | Notebook 41 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * What UDFs are and when to use them
# MAGIC * Creating UDFs with @udf decorator and udf() function
# MAGIC * Return types: StringType, IntegerType, ArrayType, StructType, MapType
# MAGIC * Registering UDFs for SQL: spark.udf.register()
# MAGIC * Performance implications: serialization overhead, Photon bypass
# MAGIC * Best practices: avoid UDFs when built-in functions suffice
# MAGIC * Error handling inside UDFs
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Gateway to custom logic)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are UDFs?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are UDFs? (Real-World Analogy)
# MAGIC
# MAGIC ### 🔧 The Custom Tool in the Factory
# MAGIC
# MAGIC Spark has a toolbox of built-in functions (saw, hammer, drill). A UDF is like bringing your OWN custom tool to the assembly line:
# MAGIC
# MAGIC | Factory Concept | PySpark | Details |
# MAGIC |---|---|---|
# MAGIC | Standard tools | Built-in functions | `upper()`, `round()`, `when()` — optimized by Catalyst |
# MAGIC | Custom tool | UDF | Your Python function applied per row |
# MAGIC | Tool registration | `spark.udf.register()` | Make custom tool available to SQL workers |
# MAGIC | Cost of custom | Serialization overhead | Data moves Python ↔ JVM per row |
# MAGIC
# MAGIC ### When to Use UDFs
# MAGIC * ✅ Complex business logic not expressible with built-ins
# MAGIC * ✅ External library calls (regex, NLP, geo, custom parsers)
# MAGIC * ✅ Legacy Python code integration
# MAGIC * ❌ Simple string/math/date operations (use built-ins!)
# MAGIC * ❌ Performance-critical hot paths (10-100x slower than built-ins)
# MAGIC
# MAGIC ### Performance Hierarchy (Fastest → Slowest)
# MAGIC 1. **Built-in functions** — Catalyst-optimized, Photon-accelerated
# MAGIC 2. **SQL expressions via expr()** — Same as built-ins
# MAGIC 3. **Pandas UDFs (vectorized)** — Arrow-based batch processing
# MAGIC 4. **Python UDFs (row-by-row)** — Serialization per row ← THIS NOTEBOOK

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How UDFs Work
# MAGIC %md
# MAGIC ## SECTION 2 — How UDFs Work (Internal Mechanics)
# MAGIC
# MAGIC ### Execution Flow
# MAGIC ```
# MAGIC ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
# MAGIC │ JVM (Spark) │ ──► │ Serialize   │ ──► │ Python      │
# MAGIC │ DataFrame   │     │ Row → Pickle│     │ Worker      │
# MAGIC │ row data    │     │             │     │ runs f(x)   │
# MAGIC └─────────────┘     └─────────────┘     └─────────────┘
# MAGIC                                                │
# MAGIC ┌─────────────┐     ┌─────────────┐            │
# MAGIC │ JVM (Spark) │ ◄── │ Deserialize │ ◄──────────┘
# MAGIC │ result col  │     │ Pickle → Row│
# MAGIC └─────────────┘     └─────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Points
# MAGIC 1. **Per-row overhead:** Each row crosses the JVM↔Python boundary
# MAGIC 2. **No Catalyst optimization:** UDF is a black box to the optimizer
# MAGIC 3. **No Photon:** UDFs bypass Databricks Photon engine
# MAGIC 4. **Deterministic by default:** Same input → same output (cached)
# MAGIC 5. **NULL handling:** UDF receives None for NULL; return None to output NULL
# MAGIC
# MAGIC ### Two Ways to Create UDFs
# MAGIC ```python
# MAGIC # Method 1: @udf decorator
# MAGIC @udf(returnType=StringType())
# MAGIC def my_func(x):
# MAGIC     return x.upper()
# MAGIC
# MAGIC # Method 2: udf() function
# MAGIC my_func_udf = udf(lambda x: x.upper(), StringType())
# MAGIC
# MAGIC # Method 3: Register for SQL
# MAGIC spark.udf.register("my_sql_func", my_func, StringType())
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Creating basic UDFs
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Creating Basic UDFs
# ============================================================
# Real-world: Custom transformations not available as built-ins.

from pyspark.sql import SparkSession  # Import SparkSession.
from pyspark.sql.functions import col, udf, lit  # Import UDF utilities.
from pyspark.sql.types import (  # Import return types.
    StringType, IntegerType, DoubleType, BooleanType
)  # End type imports.

spark = SparkSession.builder.getOrCreate()  # Get active session.

# === Method 1: @udf decorator ===
@udf(returnType=StringType())  # Decorator with return type.
def title_case(name):  # Custom title case function.
    """Convert name to title case with special handling."""
    if name is None:  # Handle NULL.
        return None  # Return NULL for NULL input.
    # Custom logic: capitalize each word, handle "mcdonald" -> "McDonald".
    words = name.lower().split()  # Split into words.
    result = []  # Accumulator.
    for word in words:  # Process each word.
        if word.startswith("mc"):  # Special prefix.
            result.append("Mc" + word[2:].capitalize())  # McDonald.
        elif word in ("von", "van", "de", "la"):  # Particles.
            result.append(word)  # Keep lowercase.
        else:
            result.append(word.capitalize())  # Normal capitalize.
    return " ".join(result)  # Join back.

# === Method 2: udf() function ===
def classify_age(age):  # Regular Python function.
    """Classify age into generation."""
    if age is None:  # Handle NULL.
        return None  # Return NULL.
    if age < 13: return "Child"  # Child.
    if age < 20: return "Teen"  # Teenager.
    if age < 30: return "Young Adult"  # Young adult.
    if age < 50: return "Adult"  # Adult.
    return "Senior"  # Senior.

classify_age_udf = udf(classify_age, StringType())  # Wrap as UDF.

# Apply UDFs.
print("=== Applying UDFs ===")  # Print heading.
people = spark.createDataFrame([
    (1, "alice mcdonald", 28),
    (2, "bob van dyke", 45),
    (3, "charlie de silva", 12),
    (4, None, 65),
    (5, "eve johnson", None),
], ["id", "name", "age"])  # Sample data.

people.select(
    col("id"),  # Keep id.
    col("name"),  # Original name.
    title_case(col("name")).alias("formatted_name"),  # Apply decorator UDF.
    col("age"),  # Original age.
    classify_age_udf(col("age")).alias("generation"),  # Apply function UDF.
).show(truncate=False)  # Display results.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: UDFs with complex return types
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: UDFs with Complex Return Types
# ============================================================
# Real-world: UDFs that return arrays, structs, or maps.

from pyspark.sql.types import (  # Import complex types.
    ArrayType, StructType, StructField, StringType, IntegerType,
    MapType, DoubleType
)  # End imports.
from pyspark.sql.functions import col, udf  # Imports.

# UDF returning an Array.
@udf(returnType=ArrayType(StringType()))  # Returns list of strings.
def extract_hashtags(text):
    """Extract hashtags from text."""
    if text is None:  # Handle NULL.
        return None  # Return NULL.
    words = text.split()  # Split by space.
    return [w for w in words if w.startswith("#")]  # Keep hashtags.

# UDF returning a Struct.
name_parts_schema = StructType([  # Define struct schema.
    StructField("first_name", StringType()),  # First name field.
    StructField("last_name", StringType()),  # Last name field.
    StructField("name_length", IntegerType()),  # Length field.
])  # End schema.

@udf(returnType=name_parts_schema)  # Returns a struct.
def parse_full_name(full_name):
    """Parse full name into components."""
    if full_name is None:  # Handle NULL.
        return None  # Return NULL.
    parts = full_name.strip().split()  # Split by space.
    first = parts[0] if len(parts) > 0 else ""  # First name.
    last = parts[-1] if len(parts) > 1 else ""  # Last name.
    return (first, last, len(full_name))  # Return as tuple.

# UDF returning a Map.
@udf(returnType=MapType(StringType(), IntegerType()))  # Returns dict.
def char_frequency(text):
    """Count character frequency in text."""
    if text is None:  # Handle NULL.
        return None  # Return NULL.
    freq = {}  # Frequency dict.
    for char in text.lower():  # Iterate characters.
        if char.isalpha():  # Only letters.
            freq[char] = freq.get(char, 0) + 1  # Count.
    return freq  # Return frequency map.

# Apply complex UDFs.
print("=== Complex Return Types ===")  # Print heading.
tweets = spark.createDataFrame([
    (1, "Love #PySpark and #DataEngineering today!"),
    (2, "Just finished #ML course #AI #DeepLearning"),
    (3, "Regular text without hashtags"),
], ["id", "text"])  # Tweet data.

tweets.select(
    col("id"),  # Keep id.
    extract_hashtags(col("text")).alias("hashtags"),  # Array result.
).show(truncate=False)  # Display arrays.

print("=== Struct Return ===")  # Print heading.
names = spark.createDataFrame([
    ("Alice Smith",), ("Bob",), ("Charlie van Horn",)
], ["full_name"])  # Names.

result = names.select(
    col("full_name"),  # Original.
    parse_full_name(col("full_name")).alias("parsed"),  # Struct result.
)
result.show(truncate=False)  # Display struct.
result.select("full_name", "parsed.first_name", "parsed.last_name", "parsed.name_length").show()  # Access fields.

print("=== Map Return ===")  # Print heading.
spark.createDataFrame([("hello world",), ("PySpark",)], ["word"]).select(
    col("word"),  # Original.
    char_frequency(col("word")).alias("char_freq"),  # Map result.
).show(truncate=False)  # Display map.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Registering UDFs for SQL
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Registering UDFs for SQL
# ============================================================
# Real-world: Making UDFs available in Spark SQL queries.

from pyspark.sql.functions import col, expr  # Imports.
from pyspark.sql.types import StringType, DoubleType  # Types.

# Define Python functions.
def email_domain(email):  # Extract domain.
    """Extract domain from email address."""
    if email is None or "@" not in email:  # Validate.
        return None  # Return NULL for invalid.
    return email.split("@")[1].lower()  # Return domain.

def fahrenheit_to_celsius(f):  # Temperature conversion.
    """Convert Fahrenheit to Celsius."""
    if f is None:  # Handle NULL.
        return None  # Return NULL.
    return round((f - 32) * 5 / 9, 2)  # Formula.

# Register for SQL.
spark.udf.register("email_domain", email_domain, StringType())  # Register domain UDF.
spark.udf.register("f_to_c", fahrenheit_to_celsius, DoubleType())  # Register temp UDF.

# Create temp view for SQL.
users = spark.createDataFrame([
    (1, "Alice", "alice@gmail.com", 98.6),
    (2, "Bob", "bob@yahoo.com", 101.3),
    (3, "Charlie", "charlie@company.org", 97.1),
    (4, "Diana", "invalid_email", 99.5),
], ["id", "name", "email", "temp_f"])  # User data.

users.createOrReplaceTempView("users")  # Register view.

# Use UDFs in SQL.
print("=== UDFs in SQL Queries ===")  # Print heading.
spark.sql("""
    SELECT
        name,
        email,
        email_domain(email) AS domain,
        temp_f,
        f_to_c(temp_f) AS temp_c
    FROM users
""").show(truncate=False)  # Display SQL results.

# Can also use registered UDFs via expr().
print("=== Registered UDFs via expr() ===")  # Print heading.
users.select(
    col("name"),  # Keep name.
    expr("email_domain(email)").alias("domain"),  # Use via expr.
    expr("f_to_c(temp_f)").alias("temp_celsius"),  # Use via expr.
).show(truncate=False)  # Display.

print("""NOTE: spark.udf.register() makes UDFs available in:
- spark.sql() queries
- expr() expressions
- SQL notebooks/cells
The @udf decorator only works in DataFrame API (select, withColumn).""")  # Note.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Error handling in UDFs
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Error Handling in UDFs
# ============================================================
# Real-world: Robust UDFs that don't crash on bad data.

from pyspark.sql.functions import col, udf, when  # Imports.
from pyspark.sql.types import StringType, DoubleType, StructType, StructField  # Types.
import json  # For JSON parsing.

# UDF with try/except error handling.
@udf(returnType=StringType())  # String return.
def safe_json_extract(json_str, key):
    """Safely extract a key from JSON string."""
    try:  # Attempt parsing.
        if json_str is None:  # Handle NULL.
            return None  # Return NULL.
        data = json.loads(json_str)  # Parse JSON.
        return str(data.get(key))  # Get key value.
    except (json.JSONDecodeError, TypeError, KeyError):  # Catch errors.
        return None  # Return NULL on error.

# UDF returning result + error info.
error_result_schema = StructType([  # Schema for result + error.
    StructField("value", DoubleType()),  # Parsed value.
    StructField("error", StringType()),  # Error message (NULL if success).
])  # End schema.

@udf(returnType=error_result_schema)  # Struct return.
def safe_parse_number(text):
    """Parse number with error reporting."""
    if text is None:  # Handle NULL.
        return (None, "NULL_INPUT")  # Report NULL.
    try:  # Attempt parsing.
        value = float(text.strip().replace(",", ""))  # Parse (handle commas).
        return (value, None)  # Success: value + no error.
    except ValueError as e:  # Parse failed.
        return (None, f"PARSE_ERROR: {str(e)[:50]}")  # NULL value + error msg.

# Apply error-safe UDFs.
print("=== Error-Safe JSON Extraction ===")  # Print heading.
json_data = spark.createDataFrame([
    (1, '{"name": "Alice", "age": 30}'),
    (2, '{"name": "Bob"}'),  # Missing age.
    (3, 'not valid json'),  # Invalid JSON.
    (4, None),  # NULL.
], ["id", "json_str"])  # JSON data.

json_data.select(
    col("id"),  # Keep id.
    col("json_str"),  # Original.
    safe_json_extract(col("json_str"), lit("name")).alias("name"),  # Extract name.
    safe_json_extract(col("json_str"), lit("age")).alias("age"),  # Extract age.
).show(truncate=False)  # Display.

# Apply number parser with error reporting.
print("=== Parse Numbers with Error Reporting ===")  # Print heading.
number_data = spark.createDataFrame([
    ("42.5",), ("1,234.56",), ("not a number",), ("",), (None,)
], ["raw_value"])  # Raw numbers.

result = number_data.select(
    col("raw_value"),  # Original.
    safe_parse_number(col("raw_value")).alias("parsed"),  # Parse.
)
result.select(
    col("raw_value"),  # Original.
    col("parsed.value").alias("numeric_value"),  # Extracted value.
    col("parsed.error").alias("error_msg"),  # Error if any.
).show(truncate=False)  # Display with errors.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: UDFs with external libraries
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: UDFs with External Libraries
# ============================================================
# Real-world: Using Python libraries (re, datetime, hashlib) in UDFs.

from pyspark.sql.functions import col, udf  # Imports.
from pyspark.sql.types import (  # Types.
    StringType, BooleanType, ArrayType, MapType, IntegerType
)  # End types.

# UDF using regex library.
import re  # Import regex.

@udf(returnType=ArrayType(StringType()))  # Returns list of strings.
def extract_emails(text):
    """Extract all email addresses from text using regex."""
    if text is None:  # Handle NULL.
        return None  # Return NULL.
    pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'  # Email regex.
    return re.findall(pattern, text)  # Find all matches.

@udf(returnType=BooleanType())  # Returns True/False.
def is_valid_phone(phone):
    """Validate phone number format."""
    if phone is None:  # Handle NULL.
        return None  # Return NULL.
    # Accept: +1-234-567-8900, (234) 567-8900, 234-567-8900, 2345678900.
    pattern = r'^(\+\d{1,3}[-.\s]?)?(\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}$'  # Phone regex.
    return bool(re.match(pattern, phone.strip()))  # Return match result.

# UDF using hashlib for anonymization.
import hashlib  # Import hashlib.

@udf(returnType=StringType())  # Returns hashed string.
def anonymize_pii(value, salt="default_salt"):
    """One-way hash for PII anonymization."""
    if value is None:  # Handle NULL.
        return None  # Return NULL.
    # SHA-256 hash with salt.
    salted = f"{salt}:{value}".encode('utf-8')  # Add salt.
    return hashlib.sha256(salted).hexdigest()[:16]  # Return first 16 chars.

# Apply library-based UDFs.
print("=== Regex-Based Email Extraction ===")  # Print heading.
messages = spark.createDataFrame([
    (1, "Contact alice@gmail.com or bob@yahoo.com for details"),
    (2, "No emails here, just text"),
    (3, "Send to support@company.org and info@company.org"),
], ["id", "message"])  # Messages.

messages.select(
    col("id"),  # Keep id.
    extract_emails(col("message")).alias("emails_found"),  # Extract.
).show(truncate=False)  # Display.

print("=== Phone Validation ===")  # Print heading.
phones = spark.createDataFrame([
    ("+1-234-567-8900",), ("(234) 567-8900",), ("234-567-8900",),
    ("2345678900",), ("123",), ("not a phone",),
], ["phone"])  # Phone numbers.

phones.select(
    col("phone"),  # Original.
    is_valid_phone(col("phone")).alias("is_valid"),  # Validate.
).show(truncate=False)  # Display.

print("=== PII Anonymization ===")  # Print heading.
pii = spark.createDataFrame([
    ("alice@co.com",), ("bob@co.com",), ("alice@co.com",)  # Same value = same hash.
], ["email"])  # PII data.

pii.select(
    col("email"),  # Original.
    anonymize_pii(col("email")).alias("anonymized"),  # Hashed.
).show(truncate=False)  # Display (same input = same hash).

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: UDF performance comparison
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: UDF Performance Comparison
# ============================================================
# Real-world: Demonstrating why built-ins are preferred.

from pyspark.sql.functions import col, udf, upper, length, when, lit, concat  # Imports.
from pyspark.sql.types import StringType, IntegerType  # Types.
import time  # For timing.

# Create test data.
test_df = spark.range(100000).select(  # 100K rows.
    col("id"),  # Keep id.
    concat(lit("user_"), col("id").cast("string")).alias("name"),  # Generate names.
)
test_df.cache()  # Cache to avoid re-read.
test_df.count()  # Force cache materialization.

# UDF version of upper().
@udf(returnType=StringType())  # UDF version.
def python_upper(s):
    """Python's upper() wrapped as UDF."""
    return s.upper() if s else None  # Python upper.

# Compare performance.
print("=== Performance: Built-in vs UDF ===")  # Print heading.

# Built-in function.
start = time.time()  # Start timer.
result1 = test_df.select(upper(col("name")).alias("upper_name"))  # Built-in.
result1.collect()  # Force execution.
builtin_time = time.time() - start  # Elapsed.
print(f"Built-in upper():     {builtin_time:.3f} seconds")  # Display time.

# UDF function.
start = time.time()  # Start timer.
result2 = test_df.select(python_upper(col("name")).alias("upper_name"))  # UDF.
result2.collect()  # Force execution.
udf_time = time.time() - start  # Elapsed.
print(f"Python UDF upper():   {udf_time:.3f} seconds")  # Display time.

# Comparison.
print(f"\nUDF is {udf_time/builtin_time:.1f}x SLOWER than built-in!")  # Ratio.
print("\nRule: ALWAYS prefer built-in functions over UDFs when possible.")
print("UDFs should only be used for logic that CANNOT be expressed with built-ins.")

# Cleanup.
test_df.unpersist()  # Remove from cache.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Deterministic and nondeterministic UDFs
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Deterministic vs Nondeterministic UDFs
# ============================================================
# Real-world: UDFs that use external state or random values.

from pyspark.sql.functions import col, udf, lit  # Imports.
from pyspark.sql.types import StringType, DoubleType  # Types.
import random  # Random module.
import uuid  # UUID module.

# Deterministic UDF (default): same input → same output.
@udf(returnType=StringType())  # Deterministic by default.
def format_currency(amount):
    """Format number as USD currency."""
    if amount is None:  # Handle NULL.
        return None  # Return NULL.
    return f"${amount:,.2f}"  # Format with commas.

# Nondeterministic UDF: output may vary for same input.
@udf(returnType=StringType())  # Default: deterministic.
def generate_uuid(dummy):
    """Generate a random UUID (nondeterministic)."""
    return str(uuid.uuid4())  # Random each time.

# Mark as nondeterministic (important for correctness!).
generate_uuid = generate_uuid.asNondeterministic()  # Mark nondeterministic.

print("=== Deterministic UDF ===")  # Print heading.
prices = spark.createDataFrame([
    (1, 1234.5), (2, 99.9), (3, 1000000.0)
], ["id", "price"])  # Prices.

prices.select(
    col("id"),  # Keep.
    col("price"),  # Original.
    format_currency(col("price")).alias("formatted"),  # Formatted.
).show(truncate=False)  # Display.

print("=== Nondeterministic UDF ===")  # Print heading.
prices.select(
    col("id"),  # Keep.
    generate_uuid(col("id")).alias("unique_id"),  # Random UUID.
).show(truncate=False)  # Display (different each run!).

print("""IMPORTANT:
- Deterministic UDFs may be called multiple times for same row (caching optimized).
- Nondeterministic UDFs: Spark won't cache/optimize, evaluates fresh each time.
- Mark UDF as .asNondeterministic() if it uses: random, timestamp, external API, UUID.
- This prevents Spark from incorrectly caching/reusing stale results.""")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Multi-column UDFs and closure patterns
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Multi-Column UDFs and Closures
# ============================================================
# Real-world: UDFs that accept multiple columns or use config.

from pyspark.sql.functions import col, udf, struct, lit  # Imports.
from pyspark.sql.types import (  # Types.
    StringType, DoubleType, StructType, StructField, BooleanType
)  # End types.

# Multi-column UDF: pass struct or multiple columns.
@udf(returnType=StringType())  # String return.
def full_address(street, city, state, zip_code):
    """Combine address components with validation."""
    parts = [street, city, state, zip_code]  # All parts.
    # Filter out NULLs and empty strings.
    valid_parts = [p for p in parts if p and p.strip()]  # Non-empty.
    if not valid_parts:  # All empty.
        return None  # Return NULL.
    return ", ".join(valid_parts)  # Join with commas.

# Closure pattern: UDF factory with configuration.
def make_classifier(thresholds, labels):
    """Create a UDF that classifies values based on thresholds."""
    def classify(value):  # Inner function captures thresholds.
        if value is None:  # Handle NULL.
            return None  # Return NULL.
        for threshold, label in zip(thresholds, labels):  # Check each.
            if value < threshold:  # Below threshold.
                return label  # Return label.
        return labels[-1] if labels else "Unknown"  # Default.
    return udf(classify, StringType())  # Return as UDF.

# Create configured classifiers.
temp_classifier = make_classifier(  # Temperature classifier.
    thresholds=[0, 15, 25, 35],  # Boundaries.
    labels=["Freezing", "Cold", "Comfortable", "Hot", "Extreme"]  # Labels.
)

risk_classifier = make_classifier(  # Risk score classifier.
    thresholds=[20, 50, 80],  # Boundaries.
    labels=["Low", "Medium", "High", "Critical"]  # Labels.
)

# Apply multi-column UDF.
print("=== Multi-Column UDF ===")  # Print heading.
addresses = spark.createDataFrame([
    ("123 Main St", "NYC", "NY", "10001"),
    ("456 Oak Ave", None, "CA", "90210"),
    (None, None, None, None),
], ["street", "city", "state", "zip"])  # Address data.

addresses.select(
    full_address(col("street"), col("city"), col("state"), col("zip")).alias("full_addr"),
).show(truncate=False)  # Display.

# Apply closure-based classifiers.
print("=== Closure-Based Classifiers ===")  # Print heading.
readings = spark.createDataFrame([
    (1, -5.0, 15), (2, 10.0, 45), (3, 22.0, 75), (4, 38.0, 95)
], ["id", "temperature", "risk_score"])  # Readings.

readings.select(
    col("id"),  # Keep.
    col("temperature"),  # Original.
    temp_classifier(col("temperature")).alias("temp_class"),  # Classify temp.
    col("risk_score"),  # Original.
    risk_classifier(col("risk_score")).alias("risk_level"),  # Classify risk.
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production UDF patterns
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Production UDF Patterns
# ============================================================
# Real-world: Best practices for UDFs in production pipelines.

from pyspark.sql.functions import col, udf, when, coalesce, lit, expr  # Imports.
from pyspark.sql.types import StringType, DoubleType, BooleanType  # Types.
import re  # Regex.

# === Pattern 1: Validate-then-apply (avoid UDF when possible) ===
print("=== Pattern 1: Minimize UDF Usage ===")  # Print heading.

# BAD: Using UDF for simple logic.
@udf(returnType=StringType())
def bad_classify(score):  # UDF for simple when/otherwise.
    if score is None: return None
    if score >= 90: return "A"
    if score >= 80: return "B"
    return "C"

# GOOD: Use built-in when/otherwise instead!
print("GOOD approach: Use when/otherwise (10-100x faster):")
scores = spark.createDataFrame([(95,), (82,), (70,)], ["score"])  # Scores.
scores.select(
    col("score"),  # Keep.
    when(col("score") >= 90, "A")  # Built-in conditional.
        .when(col("score") >= 80, "B")
        .otherwise("C").alias("grade_builtin"),
    bad_classify(col("score")).alias("grade_udf"),  # UDF (slower!).
).show()  # Same result, different speed.

# === Pattern 2: UDF with logging and metrics ===
from pyspark.sql.functions import size, array  # More imports.

# Complex validation that truly needs a UDF.
@udf(returnType=StringType())  # Returns cleaned string or NULL.
def clean_product_code(code):
    """Validate and clean product codes. Format: XX-NNNN-NN."""
    if code is None:  # Handle NULL.
        return None  # Return NULL.
    # Remove whitespace and convert.
    cleaned = code.strip().upper()  # Clean.
    # Validate format.
    pattern = r'^[A-Z]{2}-\d{4}-\d{2}$'  # Expected format.
    if re.match(pattern, cleaned):  # Valid.
        return cleaned  # Return as-is.
    # Try to fix common issues.
    cleaned = re.sub(r'[^A-Z0-9]', '-', cleaned)  # Replace non-alphanumeric.
    parts = [p for p in cleaned.split('-') if p]  # Split by dash.
    if len(parts) >= 3:  # Enough parts.
        return f"{parts[0][:2]}-{parts[1][:4].zfill(4)}-{parts[2][:2].zfill(2)}"  # Reconstruct.
    return None  # Can't fix.

print("\n=== Pattern 2: Complex Validation UDF ===")  # Print heading.
products = spark.createDataFrame([
    ("AB-1234-56",), ("  cd-5678-09  ",), ("XY 9999 01",),
    ("invalid",), (None,),
], ["raw_code"])  # Product codes.

products.select(
    col("raw_code"),  # Original.
    clean_product_code(col("raw_code")).alias("cleaned_code"),  # Cleaned.
).show(truncate=False)  # Display.

# === Pattern 3: UDF Registry for team sharing ===
print("\n=== Pattern 3: UDF Registry ===")  # Print heading.

# Register all utility UDFs.
spark.udf.register("clean_product_code", clean_product_code.func, StringType())

# Now usable in SQL across the team.
spark.sql("SELECT clean_product_code('ab-1234-56') AS cleaned").show()  # SQL access.

print("""
✅ UDF Best Practices Summary:
1. ALWAYS try built-in functions first
2. Handle NULLs explicitly (first line of every UDF)
3. Use try/except for error resilience
4. Mark nondeterministic UDFs with .asNondeterministic()
5. Register UDFs for SQL access when needed
6. Use closures for configurable UDFs
7. Return complex types (Struct/Array/Map) for multi-value results
""")  # Best practices.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Python UDFs
# MAGIC
# MAGIC ### Mistake 1: Not handling NULL
# MAGIC ```python
# MAGIC # WRONG — crashes on NULL input!
# MAGIC @udf(returnType=StringType())
# MAGIC def bad_udf(x):
# MAGIC     return x.upper()  # AttributeError: NoneType has no attribute 'upper'
# MAGIC
# MAGIC # CORRECT — always check for None.
# MAGIC @udf(returnType=StringType())
# MAGIC def good_udf(x):
# MAGIC     if x is None:
# MAGIC         return None
# MAGIC     return x.upper()
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Wrong return type
# MAGIC ```python
# MAGIC # WRONG — function returns int but declared as StringType!
# MAGIC @udf(returnType=StringType())
# MAGIC def count_words(text):
# MAGIC     return len(text.split())  # Returns INT, not STRING!
# MAGIC
# MAGIC # CORRECT — match return type to actual output.
# MAGIC @udf(returnType=IntegerType())
# MAGIC def count_words(text):
# MAGIC     return len(text.split())  # Returns INT, declared as INT.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Using UDFs for simple operations
# MAGIC ```python
# MAGIC # WRONG — UDF for something built-in can do:
# MAGIC @udf(returnType=StringType())
# MAGIC def to_upper(s): return s.upper() if s else None
# MAGIC
# MAGIC # CORRECT — use built-in (100x faster!):
# MAGIC from pyspark.sql.functions import upper
# MAGIC df.select(upper(col("name")))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Trying to access DataFrame inside a UDF
# MAGIC ```python
# MAGIC # WRONG — can't use Spark objects inside UDF!
# MAGIC @udf(returnType=StringType())
# MAGIC def lookup(key):
# MAGIC     return other_df.filter(col("id") == key).first()["name"]  # ERROR!
# MAGIC
# MAGIC # CORRECT — use a broadcast join or collect lookup table first.
# MAGIC lookup_dict = dict(other_df.collect())  # Collect to driver.
# MAGIC @udf(returnType=StringType())
# MAGIC def lookup(key):
# MAGIC     return lookup_dict.get(key)  # Use Python dict.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Not registering for SQL
# MAGIC ```python
# MAGIC # The @udf decorator only works in Python DataFrame API.
# MAGIC # To use in spark.sql(), you MUST register:
# MAGIC spark.udf.register("my_func", my_python_func, StringType())
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of UDF Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Create a UDF that reverses a string. Apply to a name column.
# MAGIC 2. Register a UDF for SQL that converts Celsius to Fahrenheit.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Modify the title_case UDF to handle "o'brien" → "O'Brien".
# MAGIC 4. Change the classify_age UDF boundaries and add a new category.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Create a UDF that returns a struct with: word_count, char_count, has_numbers.
# MAGIC 6. Combine a validation UDF with when/otherwise for flagging.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Build a UDF that validates and formats international phone numbers.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build a text classification UDF that categorizes support tickets by keywords.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design a UDF factory: given a config dict of {pattern: category}, return a classifier UDF.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Refactor 3 existing UDFs to use built-in functions instead. Benchmark the difference.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Test UDF behavior with: NULL, empty string, very long strings, unicode, special chars.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build a production data quality UDF suite: email validator, phone validator, address parser, date normalizer.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create a decision tree: "When to use UDF vs built-in vs Pandas UDF."

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all.
from pyspark.sql.types import *  # All types.

# --- Level 1: Reverse string UDF ---
print("=== Level 1: Reverse String ===")  # Print heading.
@udf(returnType=StringType())  # String return.
def reverse_string(s):
    """Reverse a string."""
    if s is None: return None  # Handle NULL.
    return s[::-1]  # Python slice reversal.

spark.createDataFrame([("Hello",), ("PySpark",), (None,)], ["word"]).select(
    col("word"),  # Original.
    reverse_string(col("word")).alias("reversed"),  # Reversed.
).show()  # Display.

# --- Level 2: Celsius to Fahrenheit (registered) ---
print("=== Level 2: C to F (SQL) ===")  # Print heading.
def c_to_f(c):  # Conversion function.
    if c is None: return None  # Handle NULL.
    return round(c * 9 / 5 + 32, 2)  # Formula.

spark.udf.register("celsius_to_fahrenheit", c_to_f, DoubleType())  # Register.
spark.sql("SELECT celsius_to_fahrenheit(0) AS freezing, celsius_to_fahrenheit(100) AS boiling").show()  # Test.

# --- Level 5: Text classifier ---
print("=== Level 5: Ticket Classifier ===")  # Print heading.
@udf(returnType=StringType())  # String return.
def classify_ticket(text):
    """Classify support ticket by keywords."""
    if text is None: return "Unknown"  # Handle NULL.
    text_lower = text.lower()  # Lowercase for matching.
    if any(w in text_lower for w in ["crash", "error", "broken", "down"]): return "Bug"
    if any(w in text_lower for w in ["slow", "performance", "timeout"]): return "Performance"
    if any(w in text_lower for w in ["feature", "request", "would like"]): return "Feature Request"
    if any(w in text_lower for w in ["how", "help", "question"]): return "Question"
    return "General"  # Default.

tickets = spark.createDataFrame([
    ("App crashes on login",), ("Performance is slow lately",),
    ("How do I export data?",), ("Would like dark mode feature",),
], ["description"])  # Tickets.

tickets.select(
    col("description"),  # Original.
    classify_ticket(col("description")).alias("category"),  # Classified.
).show(truncate=False)  # Display.

# --- Level 6: UDF Factory ---
print("=== Level 6: UDF Factory ===")  # Print heading.
def make_keyword_classifier(rules_dict, default="Other"):
    """Factory: create classifier from {category: [keywords]} config."""
    def classify(text):  # Inner function.
        if text is None: return default  # Handle NULL.
        text_lower = text.lower()  # Lowercase.
        for category, keywords in rules_dict.items():  # Check rules.
            if any(kw in text_lower for kw in keywords):  # Match.
                return category  # Return category.
        return default  # No match.
    return udf(classify, StringType())  # Return as UDF.

# Configure and use.
email_classifier = make_keyword_classifier({
    "Spam": ["buy now", "limited offer", "click here"],
    "Work": ["meeting", "deadline", "project"],
    "Personal": ["dinner", "weekend", "birthday"],
})

emails = spark.createDataFrame([
    ("Buy now! Limited offer!",), ("Meeting at 3pm about the project",),
    ("Happy birthday!",), ("Regular email content",),
], ["subject"])  # Emails.

emails.select(
    col("subject"),  # Original.
    email_classifier(col("subject")).alias("category"),  # Classified.
).show(truncate=False)  # Display.

print("✅ All homework solutions complete!")  # Completion.