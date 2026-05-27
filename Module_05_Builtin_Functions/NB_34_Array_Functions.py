# Databricks notebook source
# DBTITLE 1,NB_34 Header
# MAGIC %md
# MAGIC # NB_34 — Array Functions (Every One)
# MAGIC
# MAGIC **Module 5: Built-in Functions** | Notebook 34 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Creating arrays: array(), split(), sequence()
# MAGIC * Inspection: size(), array_contains(), array_position(), element_at()
# MAGIC * Transformation: array_distinct(), array_sort(), array_remove(), array_repeat()
# MAGIC * Set operations: array_union(), array_intersect(), array_except()
# MAGIC * Flattening: explode(), posexplode(), explode_outer(), flatten()
# MAGIC * Joining: array_join(), concat() for arrays
# MAGIC * Slicing: slice(), array_min(), array_max()
# MAGIC * Aggregation: aggregate(), shuffle(), arrays_zip(), arrays_overlap()
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Arrays are everywhere in real data)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are Array Functions?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are Array Functions? (Real-World Analogy)
# MAGIC
# MAGIC ### 📦 The Box of Items
# MAGIC
# MAGIC Imagine each row has a "box" (array) containing multiple items:
# MAGIC
# MAGIC | Real World | PySpark Array Function | What It Does |
# MAGIC |---|---|---|
# MAGIC | Open the box and count | `size()` | Count elements |
# MAGIC | Check if item is inside | `array_contains()` | Boolean check |
# MAGIC | Remove duplicates | `array_distinct()` | Unique elements only |
# MAGIC | Sort items | `array_sort()` | Alphabetical/numeric order |
# MAGIC | Unpack everything onto table | `explode()` | One row per element |
# MAGIC | Merge two boxes | `array_union()` | Combined unique items |
# MAGIC | Items in both boxes | `array_intersect()` | Common elements |
# MAGIC | Join items into string | `array_join()` | "a, b, c" |
# MAGIC | Take first 3 items | `slice()` | Subset of array |
# MAGIC
# MAGIC ### Where Arrays Appear in Real Data
# MAGIC * **E-commerce:** Product tags, category hierarchies
# MAGIC * **IoT:** Sensor readings per time window
# MAGIC * **Text:** Tokenized words from split()
# MAGIC * **Logs:** Multiple events per session
# MAGIC * **Social:** Friend lists, interests, likes

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Array Functions Work
# MAGIC %md
# MAGIC ## SECTION 2 — How Array Functions Work (Internal Mechanics)
# MAGIC
# MAGIC ### Array Memory Layout
# MAGIC ```
# MAGIC Row: ["tags" = ["spark", "python", "data"]]
# MAGIC │
# MAGIC ├─ Stored as: ArrayType(StringType)
# MAGIC ├─ Index: 0-based internally, but element_at() is 1-based!
# MAGIC └─ NULL handling: Arrays can contain NULLs, and can BE null
# MAGIC ```
# MAGIC
# MAGIC ### Function Categories
# MAGIC ```
# MAGIC ┌─────────────────┬─────────────────┬─────────────────┐
# MAGIC │ CREATE / INSPECT │ TRANSFORM        │ EXPLODE / JOIN   │
# MAGIC │                 │                  │                  │
# MAGIC │ array()         │ array_distinct() │ explode()        │
# MAGIC │ split()         │ array_sort()     │ posexplode()     │
# MAGIC │ sequence()      │ array_remove()   │ explode_outer()  │
# MAGIC │ size()          │ array_repeat()   │ flatten()        │
# MAGIC │ array_contains()│ array_union()    │ array_join()     │
# MAGIC │ array_position()│ array_intersect()│ concat()         │
# MAGIC │ element_at()    │ array_except()   │ arrays_zip()     │
# MAGIC │ arrays_overlap()│ slice()          │                  │
# MAGIC └─────────────────┴─────────────────┴─────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Critical Rules
# MAGIC 1. `element_at()` is **1-based** (first element = 1, not 0)
# MAGIC 2. `explode()` removes rows with empty/NULL arrays; use `explode_outer()` to keep them
# MAGIC 3. `array_union()` returns distinct elements; `concat()` keeps duplicates
# MAGIC 4. Most array functions return NULL if the input array is NULL
# MAGIC 5. `flatten()` only removes one level of nesting (array of arrays → array)

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Creating and Inspecting Arrays
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Creating and Inspecting Arrays
# ============================================================
# Real-world: Product tags, user skills, multi-value attributes.

from pyspark.sql import SparkSession  # Import SparkSession.
from pyspark.sql.functions import (  # Import array functions.
    col, array, split, size, array_contains, array_position,
    element_at, lit, sequence
)  # End imports.
from pyspark.sql.types import ArrayType, StringType, IntegerType  # Types.

spark = SparkSession.builder.getOrCreate()  # Get active session.

# Create data with arrays.
data = [
    (1, "Alice", ["python", "spark", "sql"], [85, 92, 78]),
    (2, "Bob", ["java", "scala", "spark", "kafka"], [90, 88, 95, 70]),
    (3, "Charlie", ["python", "ml"], [75, 80]),
    (4, "Diana", [], []),  # Empty arrays.
]

df = spark.createDataFrame(data, "id INT, name STRING, skills ARRAY<STRING>, scores ARRAY<INT>")  # Create DataFrame.

# Inspect arrays.
print("=== Array Inspection Functions ===")  # Print heading.
df.select(
    col("name"),  # Keep name.
    col("skills"),  # Original array.
    size(col("skills")).alias("num_skills"),  # Count elements: [3, 4, 2, 0].
    array_contains(col("skills"), "spark").alias("knows_spark"),  # Check membership.
    array_contains(col("skills"), "python").alias("knows_python"),  # Check membership.
    array_position(col("skills"), "spark").alias("spark_pos"),  # 1-based position (0=not found).
    element_at(col("skills"), 1).alias("first_skill"),  # 1-based: get first element.
    element_at(col("skills"), -1).alias("last_skill"),  # Negative: get last element.
).show(truncate=False)  # Display inspection results.

# Create arrays from columns.
print("=== Creating Arrays ===")  # Print heading.
people = spark.createDataFrame([
    ("Alice", 85, 92, 78),
    ("Bob", 90, 70, 95),
], ["name", "test1", "test2", "test3"])  # Individual columns.

people.select(
    col("name"),  # Keep name.
    array(col("test1"), col("test2"), col("test3")).alias("all_scores"),  # Combine into array.
    # split() creates array from delimited string.
).show(truncate=False)  # Display array creation.

# split() to create arrays from strings.
print("=== split() — String to Array ===")  # Print heading.
tags_df = spark.createDataFrame([
    ("spark,python,sql",), ("java,kafka",), ("",)
], ["tags_str"])  # Comma-separated strings.

tags_df.select(
    col("tags_str"),  # Original string.
    split(col("tags_str"), ",").alias("tags_array"),  # Split into array.
    size(split(col("tags_str"), ",")).alias("num_tags"),  # Count tags.
).show(truncate=False)  # Display split results.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Array Transformations
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Array Transformations
# ============================================================
# Real-world: Cleaning, sorting, and deduplicating multi-value fields.

from pyspark.sql.functions import (  # Import transform functions.
    col, array_distinct, array_sort, array_remove, array_repeat,
    array_union, array_intersect, array_except, concat, shuffle
)  # End imports.

# Create data with duplicates and unsorted arrays.
transform_df = spark.createDataFrame([
    (1, ["b", "a", "c", "a", "b"], ["x", "y", "z"]),
    (2, ["spark", "python", "spark", "sql"], ["python", "java", "sql"]),
    (3, ["one", "two", "three"], ["three", "four", "five"]),
], "id INT, arr1 ARRAY<STRING>, arr2 ARRAY<STRING>")  # Create DataFrame.

# Distinct, sort, remove.
print("=== array_distinct, array_sort, array_remove ===")  # Print heading.
transform_df.select(
    col("id"),  # Keep id.
    col("arr1"),  # Original array.
    array_distinct(col("arr1")).alias("distinct"),  # Remove duplicates.
    array_sort(col("arr1")).alias("sorted"),  # Sort alphabetically.
    array_sort(array_distinct(col("arr1"))).alias("sorted_distinct"),  # Both.
    array_remove(col("arr1"), "a").alias("remove_a"),  # Remove all "a" elements.
).show(truncate=False)  # Display results.

# Set operations.
print("=== Set Operations: union, intersect, except ===")  # Print heading.
transform_df.select(
    col("id"),  # Keep id.
    col("arr1"), col("arr2"),  # Both arrays.
    array_union(col("arr1"), col("arr2")).alias("union"),  # All unique from both.
    array_intersect(col("arr1"), col("arr2")).alias("intersect"),  # Common elements.
    array_except(col("arr1"), col("arr2")).alias("except_1_minus_2"),  # In arr1 but not arr2.
).show(truncate=False)  # Display set operations.

# Concat (keeps duplicates) vs Union (removes duplicates).
print("=== concat() vs array_union() ===")  # Print heading.
transform_df.filter(col("id") == 2).select(
    concat(col("arr1"), col("arr2")).alias("concat_keeps_dupes"),  # All elements.
    array_union(col("arr1"), col("arr2")).alias("union_removes_dupes"),  # Unique only.
).show(truncate=False)  # Display comparison.

# array_repeat.
print("=== array_repeat ===")  # Print heading.
spark.createDataFrame([("hello",), ("world",)], ["word"]).select(
    col("word"),  # Original.
    array_repeat(col("word"), 3).alias("repeated_3x"),  # Repeat 3 times.
).show(truncate=False)  # Display repeat results.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Explode and Flatten
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Explode and Flatten
# ============================================================
# Real-world: Converting array columns into individual rows for analysis.

from pyspark.sql.functions import (  # Import explode functions.
    col, explode, posexplode, explode_outer, posexplode_outer,
    flatten, array_join, slice as array_slice, arrays_zip
)  # End imports.

# Create e-commerce data with product tags.
orders = spark.createDataFrame([
    (1, "Alice", ["electronics", "laptop", "premium"]),
    (2, "Bob", ["books", "fiction"]),
    (3, "Charlie", []),  # Empty array.
    (4, "Diana", None),  # NULL array.
], "id INT, customer STRING, tags ARRAY<STRING>")  # Order data.

# explode — one row per element (drops empty/NULL).
print("=== explode() — Drops empty/NULL arrays ===")  # Print heading.
orders.select(
    col("id"), col("customer"),  # Keep context.
    explode(col("tags")).alias("tag"),  # One row per tag.
).show(truncate=False)  # Charlie and Diana are GONE!

# explode_outer — keeps rows with empty/NULL arrays.
print("=== explode_outer() — Keeps empty/NULL as NULL row ===")  # Print heading.
orders.select(
    col("id"), col("customer"),  # Keep context.
    explode_outer(col("tags")).alias("tag"),  # One row per tag, NULLs preserved.
).show(truncate=False)  # Charlie and Diana preserved with NULL tag.

# posexplode — with position index.
print("=== posexplode() — With Position Index ===")  # Print heading.
orders.filter(col("id") == 1).select(
    col("customer"),  # Keep customer.
    posexplode(col("tags")).alias("position", "tag"),  # Position + value.
).show(truncate=False)  # Display with positions.

# flatten — flatten array of arrays.
print("=== flatten() — Array of Arrays → Single Array ===")  # Print heading.
nested = spark.createDataFrame([
    (1, [["a", "b"], ["c", "d"], ["e"]]),
    (2, [["spark"], ["python", "sql"]]),
], "id INT, nested_arr ARRAY<ARRAY<STRING>>")  # Nested arrays.

nested.select(
    col("id"),  # Keep id.
    col("nested_arr"),  # Original nested.
    flatten(col("nested_arr")).alias("flattened"),  # Single-level array.
).show(truncate=False)  # Display flattened results.

# array_join — array to string.
print("=== array_join() — Array to Delimited String ===")  # Print heading.
orders.filter(col("id").isin(1, 2)).select(
    col("customer"),  # Keep customer.
    col("tags"),  # Original array.
    array_join(col("tags"), ", ").alias("tags_csv"),  # Join with comma.
    array_join(col("tags"), " | ").alias("tags_pipe"),  # Join with pipe.
).show(truncate=False)  # Display joined strings.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Sequence, Slice, and Zip
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Sequence, Slice, and Zip
# ============================================================
# Real-world: Generating ranges, subsetting, and pairing arrays.

from pyspark.sql.functions import (  # Import functions.
    col, sequence, slice as array_slice, arrays_zip, arrays_overlap,
    array_min, array_max, lit, size, element_at
)  # End imports.

# sequence() — generate a range array.
print("=== sequence() — Generate Ranges ===")  # Print heading.
range_df = spark.createDataFrame([
    (1, 1, 5),   # 1 to 5.
    (2, 0, 10),  # 0 to 10.
    (3, 5, 1),   # 5 down to 1 (step = -1 auto).
], ["id", "start", "end"])  # Range specs.

range_df.select(
    col("id"),  # Keep id.
    sequence(col("start"), col("end")).alias("range_arr"),  # Auto step.
    sequence(col("start"), col("end"), lit(2)).alias("step_2"),  # Step of 2.
).show(truncate=False)  # Display sequences.

# slice() — subset of array.
print("=== slice() — Take a Subset ===")  # Print heading.
slice_df = spark.createDataFrame([
    (1, ["a", "b", "c", "d", "e", "f"]),
    (2, ["spark", "python", "java", "scala"]),
], "id INT, items ARRAY<STRING>")  # Arrays to slice.

slice_df.select(
    col("id"),  # Keep id.
    col("items"),  # Original.
    array_slice(col("items"), 1, 3).alias("first_3"),  # Start=1 (1-based!), length=3.
    array_slice(col("items"), 2, 2).alias("middle_2"),  # Start at position 2, take 2.
    array_slice(col("items"), -2, 2).alias("last_2"),  # Negative: from end.
).show(truncate=False)  # Display sliced arrays.

# arrays_zip() — pair elements from multiple arrays.
print("=== arrays_zip() — Pair Arrays Element-by-Element ===")  # Print heading.
zip_df = spark.createDataFrame([
    (1, ["Math", "Science", "English"], [85, 92, 78]),
    (2, ["Physics", "Chemistry"], [90, 88]),
], "id INT, subjects ARRAY<STRING>, scores ARRAY<INT>")  # Paired data.

zip_df.select(
    col("id"),  # Keep id.
    arrays_zip(col("subjects"), col("scores")).alias("subject_score_pairs"),  # Zipped.
).show(truncate=False)  # Display zipped arrays.

# arrays_overlap() and array_min/max.
print("=== arrays_overlap(), array_min(), array_max() ===")  # Print heading.
overlap_df = spark.createDataFrame([
    ([1, 2, 3], [3, 4, 5]),  # Overlap: 3.
    ([10, 20], [30, 40]),  # No overlap.
    (["a", "b"], ["b", "c"]),  # String overlap: "b".
], "arr1 ARRAY<STRING>, arr2 ARRAY<STRING>")  # Note: using string for mixed demo.

num_df = spark.createDataFrame([
    ([10, 3, 7, 1, 9],),
    ([100, -5, 42, 0],),
], "numbers ARRAY<INT>")  # Numeric arrays.

num_df.select(
    col("numbers"),  # Original.
    array_min(col("numbers")).alias("min_val"),  # Minimum element.
    array_max(col("numbers")).alias("max_val"),  # Maximum element.
    size(col("numbers")).alias("count"),  # Number of elements.
).show(truncate=False)  # Display min/max.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Explode patterns
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Explode Patterns for Analysis
# ============================================================
# Real-world: Tag analysis, skill frequency, co-occurrence.

from pyspark.sql.functions import (  # Import analysis functions.
    col, explode, explode_outer, posexplode, count, collect_list,
    collect_set, array_distinct, size, desc, array_join
)  # End imports.

# E-commerce product data.
products = spark.createDataFrame([
    (101, "Laptop", ["electronics", "computing", "premium", "portable"]),
    (102, "Mouse", ["electronics", "accessories", "ergonomic"]),
    (103, "Book", ["education", "fiction", "premium"]),
    (104, "Headphones", ["electronics", "audio", "premium", "wireless"]),
    (105, "Pen", ["stationery", "writing"]),
], "id INT, product STRING, tags ARRAY<STRING>")  # Product tags.

# Pattern 1: Tag frequency analysis (which tags are most common?).
print("=== Pattern 1: Tag Frequency Analysis ===")  # Print heading.
tag_counts = products.select(
    explode(col("tags")).alias("tag"),  # One row per tag.
).groupBy("tag").agg(
    count("*").alias("frequency"),  # Count occurrences.
).orderBy(desc("frequency"))  # Sort by frequency.

tag_counts.show(truncate=False)  # Display tag frequencies.

# Pattern 2: Find products with a specific tag.
print("=== Pattern 2: Products with 'premium' tag ===")  # Print heading.
from pyspark.sql.functions import array_contains  # Import.
products.filter(
    array_contains(col("tags"), "premium")  # Filter for premium.
).select("id", "product", "tags").show(truncate=False)  # Display matches.

# Pattern 3: Reconstruct arrays after filtering (collect_list).
print("=== Pattern 3: Explode, Filter, Re-aggregate ===")  # Print heading.
# Goal: Remove "electronics" tag from all products.
filtered = products.select(
    col("id"), col("product"),  # Keep context.
    explode(col("tags")).alias("tag"),  # Explode.
).filter(
    col("tag") != "electronics"  # Remove specific tag.
).groupBy("id", "product").agg(
    collect_list("tag").alias("tags_cleaned"),  # Re-aggregate.
)

filtered.show(truncate=False)  # Display cleaned tags.

# Pattern 4: Explode + join for enrichment.
print("=== Pattern 4: Tag-based Recommendations ===")  # Print heading.
# Find products that share tags with product 101.
target_tags = ["electronics", "premium"]  # Tags to match.

products.select(
    col("id"), col("product"),  # Keep context.
    explode(col("tags")).alias("tag"),  # Explode.
).filter(
    (col("tag").isin(target_tags)) & (col("id") != 101)  # Match tags, exclude self.
).groupBy("id", "product").agg(
    count("*").alias("shared_tags"),  # Count matching tags.
    collect_list("tag").alias("matching_tags"),  # Which tags matched.
).orderBy(desc("shared_tags")).show(truncate=False)  # Show recommendations.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Array aggregation patterns
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Array Aggregation Patterns
# ============================================================
# Real-world: Building arrays from groups, combining multi-value results.

from pyspark.sql.functions import (  # Import aggregation functions.
    col, collect_list, collect_set, array_sort, array_distinct,
    size, array_join, count, desc, struct, sort_array
)  # End imports.

# Sales data — multiple purchases per customer.
sales = spark.createDataFrame([
    ("Alice", "Laptop", "2024-01-15"),
    ("Alice", "Mouse", "2024-01-15"),
    ("Alice", "Keyboard", "2024-02-01"),
    ("Bob", "Book", "2024-01-20"),
    ("Bob", "Book", "2024-02-10"),  # Duplicate product!
    ("Bob", "Pen", "2024-02-15"),
    ("Charlie", "Laptop", "2024-03-01"),
], ["customer", "product", "date"])  # Sales data.

# collect_list vs collect_set.
print("=== collect_list() vs collect_set() ===")  # Print heading.
sales.groupBy("customer").agg(
    collect_list("product").alias("all_products"),  # Keeps duplicates and order.
    collect_set("product").alias("unique_products"),  # Removes duplicates.
    count("*").alias("total_purchases"),  # Count all.
    size(collect_set("product")).alias("unique_count"),  # Distinct count.
).show(truncate=False)  # Display aggregation comparison.

# sorted collect_list.
print("=== Sorted Arrays ===")  # Print heading.
sales.groupBy("customer").agg(
    sort_array(collect_list("product")).alias("products_sorted"),  # Sort alphabetically.
    sort_array(collect_list("date")).alias("dates_sorted"),  # Chronological.
    sort_array(collect_list("date"), asc=False).alias("dates_desc"),  # Reverse chronological.
).show(truncate=False)  # Display sorted arrays.

# Array as comma-separated string.
print("=== Array → CSV String ===")  # Print heading.
sales.groupBy("customer").agg(
    array_join(sort_array(collect_set("product")), ", ").alias("products_csv"),  # Comma-separated.
    array_join(sort_array(collect_list("date")), " | ").alias("purchase_dates"),  # Pipe-separated.
).show(truncate=False)  # Display CSV strings.

# Count how many unique products each customer bought.
print("=== Customer Summary ===")  # Print heading.
sales.groupBy("customer").agg(
    size(collect_set("product")).alias("unique_products"),  # Unique products.
    sort_array(collect_set("product")).alias("product_list"),  # Which products.
).orderBy(desc("unique_products")).show(truncate=False)  # Display summary.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Complex array transformations
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Complex Array Transformations
# ============================================================
# Real-world: Multi-step array processing pipelines.

from pyspark.sql.functions import (  # Import advanced functions.
    col, expr, transform, filter as array_filter, aggregate,
    exists, forall, array_sort, array_distinct, size,
    explode, collect_list, lit, when, arrays_zip
)  # End imports.

# Student course data.
students = spark.createDataFrame([
    (1, "Alice", ["PySpark", "SQL", "Python", "ML"], [90, 85, 95, 70]),
    (2, "Bob", ["Java", "Scala", "PySpark"], [80, 75, 88]),
    (3, "Charlie", ["Python", "ML", "DL", "NLP", "CV"], [60, 55, 70, 65, 50]),
], "id INT, name STRING, courses ARRAY<STRING>, scores ARRAY<INT>")  # Student data.

# transform() — apply function to each element.
print("=== transform() — Apply to Each Element ===")  # Print heading.
students.select(
    col("name"),  # Keep name.
    col("scores"),  # Original scores.
    # Add 5 bonus points to each score.
    expr("transform(scores, x -> x + 5)").alias("with_bonus"),
    # Convert scores to letter grades.
    expr("transform(scores, x -> CASE WHEN x >= 90 THEN 'A' WHEN x >= 80 THEN 'B' WHEN x >= 70 THEN 'C' ELSE 'D' END)").alias("grades"),
    # Uppercase all course names.
    expr("transform(courses, x -> upper(x))").alias("courses_upper"),
).show(truncate=False)  # Display transformed arrays.

# filter() — keep only elements matching condition.
print("=== filter() — Keep Matching Elements ===")  # Print heading.
students.select(
    col("name"),  # Keep name.
    col("scores"),  # Original.
    # Keep only scores >= 80.
    expr("filter(scores, x -> x >= 80)").alias("high_scores"),
    # Keep only courses with 'P' in name.
    expr("filter(courses, x -> x LIKE '%P%' OR x LIKE '%p%')").alias("p_courses"),
).show(truncate=False)  # Display filtered arrays.

# aggregate() — reduce array to single value.
print("=== aggregate() — Reduce to Single Value ===")  # Print heading.
students.select(
    col("name"),  # Keep name.
    col("scores"),  # Original.
    # Sum all scores.
    expr("aggregate(scores, 0, (acc, x) -> acc + x)").alias("total_score"),
    # Average (sum / size).
    expr("aggregate(scores, 0, (acc, x) -> acc + x, acc -> acc / size(scores))").alias("avg_score"),
    # Find max using aggregate.
    expr("aggregate(scores, 0, (acc, x) -> CASE WHEN x > acc THEN x ELSE acc END)").alias("max_score"),
).show(truncate=False)  # Display aggregated results.

# exists() and forall() — boolean checks.
print("=== exists() and forall() ===")  # Print heading.
students.select(
    col("name"),  # Keep name.
    # Does any score exceed 90?
    expr("exists(scores, x -> x > 90)").alias("has_A_grade"),
    # Are ALL scores above 60?
    expr("forall(scores, x -> x > 60)").alias("all_passing"),
    # Any course with 'Spark' in name?
    expr("exists(courses, x -> x LIKE '%Spark%')").alias("has_spark"),
).show(truncate=False)  # Display boolean results.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Arrays with window functions
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Arrays with Window Functions
# ============================================================
# Real-world: Rolling arrays, session history, clickstream analysis.

from pyspark.sql.functions import (  # Import window + array functions.
    col, collect_list, array_sort, slice as array_slice, size,
    row_number, array_join, sort_array, struct, desc
)  # End imports.
from pyspark.sql.window import Window  # Import Window.

# Clickstream data.
clicks = spark.createDataFrame([
    ("user1", "home", "2024-01-01 10:00:00"),
    ("user1", "products", "2024-01-01 10:05:00"),
    ("user1", "product_detail", "2024-01-01 10:08:00"),
    ("user1", "cart", "2024-01-01 10:12:00"),
    ("user1", "checkout", "2024-01-01 10:15:00"),
    ("user2", "home", "2024-01-01 11:00:00"),
    ("user2", "search", "2024-01-01 11:02:00"),
    ("user2", "products", "2024-01-01 11:05:00"),
    ("user2", "home", "2024-01-01 11:10:00"),
], ["user_id", "page", "timestamp"])  # Click data.

# Build page journey as array per user.
print("=== User Page Journey ===")  # Print heading.
journey = clicks.groupBy("user_id").agg(
    sort_array(collect_list(struct("timestamp", "page"))).alias("journey_struct"),  # Ordered journey.
)

# Extract just the pages in order.
journey.select(
    col("user_id"),  # Keep user.
    expr("transform(journey_struct, x -> x.page)").alias("page_sequence"),  # Extract pages.
    size(col("journey_struct")).alias("num_pages"),  # Page count.
).show(truncate=False)  # Display journeys.

# Rolling window: collect last N pages at each click.
print("=== Rolling History (Last 3 Pages) ===")  # Print heading.
from pyspark.sql.functions import collect_list  # Reimport.

# Window: all preceding rows for this user.
w = Window.partitionBy("user_id").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)

clicks_with_history = clicks.withColumn(
    "pages_so_far",
    collect_list(col("page")).over(w)  # Accumulate pages.
).withColumn(
    "last_3_pages",
    array_slice(col("pages_so_far"), -3, 3)  # Last 3 pages only.
)

clicks_with_history.select(
    "user_id", "page", "pages_so_far", "last_3_pages"
).show(truncate=False)  # Display rolling history.

# Build conversion funnel check.
print("=== Funnel Analysis ===")  # Print heading.
from pyspark.sql.functions import array_contains  # Import.

funnel = clicks.groupBy("user_id").agg(
    collect_list(col("page")).alias("all_pages"),  # All visited pages.
)

funnel.select(
    col("user_id"),  # Keep user.
    array_contains(col("all_pages"), "home").alias("visited_home"),  # Step 1.
    array_contains(col("all_pages"), "products").alias("visited_products"),  # Step 2.
    array_contains(col("all_pages"), "cart").alias("visited_cart"),  # Step 3.
    array_contains(col("all_pages"), "checkout").alias("completed_purchase"),  # Step 4.
).show(truncate=False)  # Display funnel results.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production array utilities
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Production Array Utilities
# ============================================================
# Real-world: Reusable array operations for data pipelines.

from pyspark.sql.functions import (  # Import production helpers.
    col, expr, array, array_distinct, array_sort, array_union,
    array_except, size, explode, collect_set, array_join,
    when, lit, flatten, arrays_zip, element_at, array_contains
)  # End imports.

# === Utility: Jaccard Similarity between two arrays ===
print("=== Jaccard Similarity Between Arrays ===")  # Print heading.
from pyspark.sql.functions import array_intersect, array_union  # Set ops.

pairs = spark.createDataFrame([
    ("user1", "user2", ["spark", "python", "sql", "ml"], ["python", "sql", "java", "kafka"]),
    ("user1", "user3", ["spark", "python", "sql", "ml"], ["spark", "python", "sql", "ml", "dl"]),
    ("user2", "user3", ["python", "sql", "java", "kafka"], ["spark", "python", "sql", "ml", "dl"]),
], ["userA", "userB", "skills_A", "skills_B"])  # Skill comparison.

pairs.select(
    col("userA"), col("userB"),  # Keep users.
    size(array_intersect(col("skills_A"), col("skills_B"))).alias("common"),  # |A∩B|.
    size(array_union(col("skills_A"), col("skills_B"))).alias("total"),  # |A∪B|.
    # Jaccard = |intersection| / |union|.
    (size(array_intersect(col("skills_A"), col("skills_B"))).cast("double") /
     size(array_union(col("skills_A"), col("skills_B"))).cast("double")
    ).alias("jaccard_similarity"),  # Similarity score [0,1].
).show(truncate=False)  # Display similarity.

# === Multi-valued attribute matching ===
print("=== Product Recommendation by Tag Overlap ===")  # Print heading.
catalog = spark.createDataFrame([
    (1, "Gaming Laptop", ["electronics", "gaming", "portable", "high-perf"]),
    (2, "Office Laptop", ["electronics", "business", "portable", "lightweight"]),
    (3, "Gaming Mouse", ["electronics", "gaming", "accessories", "ergonomic"]),
    (4, "Desk Lamp", ["furniture", "lighting", "ergonomic"]),
    (5, "Headphones", ["electronics", "audio", "gaming", "wireless"]),
], "id INT, product STRING, tags ARRAY<STRING>")  # Product catalog.

# Find products most similar to product 1.
target = ["electronics", "gaming", "portable", "high-perf"]  # Target product tags.

catalog.filter(col("id") != 1).select(
    col("product"),  # Keep product.
    col("tags"),  # Keep tags.
    size(array_intersect(col("tags"), array([lit(t) for t in target]))).alias("matching_tags"),  # Overlap.
    array_intersect(col("tags"), array([lit(t) for t in target])).alias("which_match"),  # Which tags.
).orderBy(desc("matching_tags")).show(truncate=False)  # Ranked recommendations.

# === Array difference for change detection ===
print("=== Change Detection: What was added/removed? ===")  # Print heading.
changes = spark.createDataFrame([
    (1, ["admin", "user", "editor"], ["admin", "user", "editor", "reviewer"]),  # Added reviewer.
    (2, ["admin", "user", "billing"], ["admin", "user"]),  # Removed billing.
    (3, ["read", "write"], ["read", "write", "delete"]),  # Added delete.
], "id INT, old_roles ARRAY<STRING>, new_roles ARRAY<STRING>")  # Role changes.

changes.select(
    col("id"),  # Keep id.
    col("old_roles"), col("new_roles"),  # Before and after.
    array_except(col("new_roles"), col("old_roles")).alias("added"),  # New items.
    array_except(col("old_roles"), col("new_roles")).alias("removed"),  # Removed items.
).show(truncate=False)  # Display changes.

print("✅ Array functions mastery complete!")  # Done.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Array Functions
# MAGIC
# MAGIC ### Mistake 1: Using 0-based indexing with element_at()
# MAGIC ```python
# MAGIC # WRONG — element_at() is 1-based! Index 0 is an error!
# MAGIC df.select(element_at(col("arr"), 0))  # Error!
# MAGIC
# MAGIC # CORRECT — First element is index 1.
# MAGIC df.select(element_at(col("arr"), 1))  # First element.
# MAGIC df.select(element_at(col("arr"), -1))  # Last element.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Using explode() and losing rows with empty arrays
# MAGIC ```python
# MAGIC # WRONG — explode drops rows where array is empty or NULL.
# MAGIC df.select(explode(col("tags")))  # Rows with [] or NULL vanish!
# MAGIC
# MAGIC # CORRECT — Use explode_outer() to keep all rows.
# MAGIC df.select(explode_outer(col("tags")))  # Empty/NULL → NULL row preserved.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Confusing array_union with concat
# MAGIC ```python
# MAGIC # array_union([1,2,3], [2,3,4]) → [1,2,3,4]  (unique elements)
# MAGIC # concat([1,2,3], [2,3,4])      → [1,2,3,2,3,4]  (all elements, keeps dupes)
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Assuming array_sort works on mixed types
# MAGIC ```python
# MAGIC # array_sort only works within a single data type.
# MAGIC # Mixing strings and integers in array will cause errors.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Forgetting that size() returns -1 for NULL arrays
# MAGIC ```python
# MAGIC # size(NULL) = -1, NOT 0!
# MAGIC # size([])   = 0
# MAGIC # Always check for NULL first:
# MAGIC when(col("arr").isNull(), 0).otherwise(size(col("arr")))
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Array Function Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Create an array from 3 columns. Check `size()`, `array_contains()`, `element_at()`.
# MAGIC 2. Explode an array and count element frequencies.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Change `explode()` to `posexplode()` and use the position for ranking.
# MAGIC 4. Use `array_except()` to find skills that user A has but user B doesn't.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Split a comma-separated string, sort the resulting array, then join back with pipes.
# MAGIC 6. Use `arrays_zip()` + `explode()` to create a normalized table from parallel arrays.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Build a tag-based recommendation engine: given a user's interests (array), find the 3 most similar products by tag overlap.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Process clickstream data: collect user page visits into ordered arrays, detect if user followed a specific funnel path (home→products→cart→checkout).
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design a reusable function that computes Jaccard similarity between any two array columns.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Compare: explode + groupBy + collect_list vs. transform() for array manipulation on 1M rows.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Test: NULL arrays vs empty arrays in all functions. What does `size(NULL)` return?
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build an audit system: compare old and new arrays to detect added/removed elements.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create a decision tree: "When to explode vs when to use higher-order functions."

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all for solutions.

# --- Level 1: Basic array operations ---
print("=== Level 1: Basic Array Operations ===")  # Print heading.
basic = spark.createDataFrame([
    ("Alice", 85, 92, 78), ("Bob", 90, 70, 95)
], ["name", "t1", "t2", "t3"])  # Test data.

basic.select(
    col("name"),  # Keep name.
    array(col("t1"), col("t2"), col("t3")).alias("scores"),  # Create array.
    size(array(col("t1"), col("t2"), col("t3"))).alias("count"),  # Size.
    array_contains(array(col("t1"), col("t2"), col("t3")), 92).alias("has_92"),  # Contains.
    element_at(array(col("t1"), col("t2"), col("t3")), 2).alias("second"),  # 2nd element.
).show(truncate=False)  # Display results.

# --- Level 5: Funnel detection ---
print("=== Level 5: Funnel Path Detection ===")  # Print heading.
funnel_data = spark.createDataFrame([
    ("u1", ["home", "products", "cart", "checkout"]),  # Complete funnel.
    ("u2", ["home", "products", "home"]),  # Incomplete.
    ("u3", ["home", "search", "products", "cart", "checkout"]),  # Extra steps but complete.
], "user_id STRING, pages ARRAY<STRING>")  # Page sequences.

# Check if ALL funnel steps exist in the array.
funnel_steps = ["home", "products", "cart", "checkout"]  # Required steps.
funnel_data.select(
    col("user_id"),  # Keep user.
    col("pages"),  # Original journey.
    # Check each funnel step is present.
    array_contains(col("pages"), "home").alias("step1_home"),
    array_contains(col("pages"), "products").alias("step2_products"),
    array_contains(col("pages"), "cart").alias("step3_cart"),
    array_contains(col("pages"), "checkout").alias("step4_checkout"),
    # All steps present = complete funnel.
    (array_contains(col("pages"), "home") &
     array_contains(col("pages"), "products") &
     array_contains(col("pages"), "cart") &
     array_contains(col("pages"), "checkout")).alias("funnel_complete"),
).show(truncate=False)  # Display funnel results.

# --- Level 8: Edge cases ---
print("=== Level 8: NULL vs Empty Array ===")  # Print heading.
edge_df = spark.createDataFrame([
    (1, ["a", "b"], "has_elements"),
    (2, [], "empty_array"),
    (3, None, "null_array"),
], "id INT, arr ARRAY<STRING>, description STRING")  # Edge cases.

edge_df.select(
    col("id"), col("description"),  # Context.
    col("arr"),  # Original.
    size(col("arr")).alias("size"),  # -1 for NULL, 0 for empty.
    col("arr").isNull().alias("is_null"),  # True only for NULL.
    (size(col("arr")) == 0).alias("is_empty_arr"),  # True for empty (False for NULL!).
).show(truncate=False)  # Display edge case results.

print("✅ All homework solutions complete!")  # Completion message.