# Databricks notebook source
# DBTITLE 1,NB_31 Header
# MAGIC %md
# MAGIC # NB_31 — String Functions (Every One)
# MAGIC
# MAGIC **Module 5: Built-in Functions** | Notebook 31 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC - concat, concat_ws, upper, lower, initcap
# MAGIC - trim, ltrim, rtrim, lpad, rpad
# MAGIC - substring, substr, left, right, overlay
# MAGIC - split, regexp_replace, regexp_extract, regexp_extract_all
# MAGIC - translate, reverse, repeat, space
# MAGIC - length, char_length, octet_length, bit_length
# MAGIC - instr, locate, position
# MAGIC - soundex, levenshtein, sentences
# MAGIC - format_string, format_number, printf
# MAGIC - encode, decode, base64, unbase64
# MAGIC - ascii, chr, char, hex, unhex
# MAGIC - url_encode, url_decode
# MAGIC - to_json, from_json, schema_of_json
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Comprehensive Reference)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are String Functions?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are String Functions? (Real-World Analogy)
# MAGIC
# MAGIC ### 🏭 The Text Processing Factory
# MAGIC
# MAGIC Imagine a **mail sorting center** that handles millions of letters daily:
# MAGIC
# MAGIC | Factory Station | String Function | What It Does |
# MAGIC |---|---|---|
# MAGIC | **Stamp machine** | `upper()` / `lower()` | Standardizes all text to one case |
# MAGIC | **Envelope cutter** | `substring()` / `split()` | Extracts parts from a string |
# MAGIC | **Label printer** | `concat()` / `format_string()` | Combines multiple pieces together |
# MAGIC | **Error correction** | `regexp_replace()` | Fixes patterns in addresses |
# MAGIC | **Measuring tape** | `length()` | Checks how long a string is |
# MAGIC | **Phonetic scanner** | `soundex()` | Groups similar-sounding names |
# MAGIC | **Glue machine** | `lpad()` / `rpad()` | Pads strings to fixed width |
# MAGIC | **Scanner / decoder** | `base64()` / `encode()` | Converts text to/from encoded formats |
# MAGIC
# MAGIC ### Why String Functions Matter
# MAGIC - **80% of real data** has string columns that need cleaning
# MAGIC - Names come as "JOHN", "john", "John" — you need standardization
# MAGIC - Phone numbers arrive as "(555) 123-4567" or "5551234567" — you need extraction
# MAGIC - Addresses have typos — you need pattern matching
# MAGIC - IDs need padding — "42" must become "000042"
# MAGIC
# MAGIC ### PySpark String Functions vs Python Strings
# MAGIC - Python `str` methods work on **one string at a time**
# MAGIC - PySpark string functions work on **millions of rows in parallel**
# MAGIC - Never use Python UDFs for string work when a built-in function exists!

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How String Functions Work
# MAGIC %md
# MAGIC ## SECTION 2 — How String Functions Work (Internal Mechanics)
# MAGIC
# MAGIC ### The Function Categories
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                   PYSPARK STRING FUNCTIONS                       │
# MAGIC ├──────────────────┬──────────────────┬───────────────────────────┤
# MAGIC │   CASE / TRIM    │   EXTRACT/SPLIT  │    SEARCH / MATCH         │
# MAGIC │   upper()        │   substring()    │    instr()                │
# MAGIC │   lower()        │   split()        │    locate()               │
# MAGIC │   initcap()      │   regexp_extract │    regexp_extract()       │
# MAGIC │   trim()         │   left() / right │    like / rlike           │
# MAGIC │   ltrim/rtrim    │   overlay()      │    contains()             │
# MAGIC │   lpad/rpad      │   sentences()    │    startswith/endswith    │
# MAGIC ├──────────────────┼──────────────────┼───────────────────────────┤
# MAGIC │   COMBINE        │   TRANSFORM      │    ENCODE / DECODE        │
# MAGIC │   concat()       │   translate()    │    base64()               │
# MAGIC │   concat_ws()    │   reverse()      │    unbase64()             │
# MAGIC │   format_string  │   repeat()       │    encode() / decode()    │
# MAGIC │   printf()       │   replace()      │    hex() / unhex()        │
# MAGIC │   format_number  │   regexp_replace │    ascii() / chr()        │
# MAGIC │                  │   overlay()      │    url_encode/decode      │
# MAGIC ├──────────────────┼──────────────────┼───────────────────────────┤
# MAGIC │   MEASURE        │   SIMILARITY     │    JSON                   │
# MAGIC │   length()       │   soundex()      │    to_json()              │
# MAGIC │   char_length()  │   levenshtein()  │    from_json()            │
# MAGIC │   octet_length() │                  │    schema_of_json()       │
# MAGIC │   bit_length()   │                  │    json_tuple()           │
# MAGIC └──────────────────┴──────────────────┴───────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Execution Flow
# MAGIC
# MAGIC ```
# MAGIC DataFrame Column (StringType)
# MAGIC         │
# MAGIC         ▼
# MAGIC ┌─────────────────┐
# MAGIC │ Catalyst checks │──→ Is there a built-in Tungsten impl?
# MAGIC │ function type   │         │
# MAGIC └─────────────────┘    YES ─┤──→ Runs in JVM (fast, no Python)
# MAGIC                        NO ──┤──→ Falls back to codegen
# MAGIC                             ▼
# MAGIC                     Result Column (StringType or other)
# MAGIC ```
# MAGIC
# MAGIC ### Key Rules
# MAGIC 1. **All string functions return NULL if input is NULL** (no exceptions)
# MAGIC 2. **Indexing is 1-based** in PySpark (not 0-based like Python)
# MAGIC 3. **Functions are case-insensitive** in SQL but case-sensitive in Python API
# MAGIC 4. **Regex uses Java regex syntax** (not Python re syntax)

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Case and Trim Functions
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Case Conversion & Trimming
# ============================================================
# Real-world: Standardizing customer names from a messy CRM export

from pyspark.sql import SparkSession  # Import SparkSession
from pyspark.sql.functions import (  # Import all string functions we need
    col, upper, lower, initcap, trim, ltrim, rtrim, lpad, rpad
)

# Create SparkSession (already available in Databricks, but explicit for clarity)
spark = SparkSession.builder.getOrCreate()  # Get existing session

# Create sample data — messy customer names from a CRM
data = [
    ("  john DOE  ",),       # Extra spaces, mixed case
    ("JANE smith",),         # All upper first name, lower last
    ("  bob WILLIAMS  ",),   # Leading/trailing spaces
    ("alice JOHNSON",),      # Mixed case
    ("  CHARLIE brown ",),   # Spaces and mixed case
]

# Create DataFrame with one column
df = spark.createDataFrame(data, ["raw_name"])  # Schema: raw_name STRING

# Apply case and trim functions
result = df.select(
    col("raw_name"),                          # Original messy value
    upper(col("raw_name")).alias("upper"),     # ALL UPPERCASE
    lower(col("raw_name")).alias("lower"),     # all lowercase
    initcap(col("raw_name")).alias("initcap"), # Title Case (first letter each word)
    trim(col("raw_name")).alias("trimmed"),    # Remove leading/trailing spaces
    ltrim(col("raw_name")).alias("ltrimmed"),  # Remove leading spaces only
    rtrim(col("raw_name")).alias("rtrimmed"),  # Remove trailing spaces only
)

result.show(truncate=False)  # Show all results without truncation

# Padding examples — useful for ID formatting
df2 = spark.createDataFrame([("42",), ("7",), ("123",)], ["id_raw"])  # Raw IDs

padded = df2.select(
    col("id_raw"),                              # Original
    lpad(col("id_raw"), 6, "0").alias("lpad"),  # Left-pad with zeros to width 6: "000042"
    rpad(col("id_raw"), 6, "*").alias("rpad"),  # Right-pad with stars to width 6: "42****"
)

padded.show()  # Display padded results

# Expected Output:
# +----------------+----------------+----------------+----------------+-------------+-------------+-------------+
# |raw_name        |upper           |lower           |initcap         |trimmed      |ltrimmed     |rtrimmed     |
# +----------------+----------------+----------------+----------------+-------------+-------------+-------------+
# |  john DOE      |  JOHN DOE      |  john doe      |  John Doe      |john DOE     |john DOE     |  john DOE   |
# |JANE smith      |JANE SMITH      |jane smith      |Jane Smith      |JANE smith   |JANE smith   |JANE smith   |
# |  bob WILLIAMS  |  BOB WILLIAMS  |  bob williams  |  Bob Williams  |bob WILLIAMS |bob WILLIAMS |  bob WILLIAMS|
# +----------------+----------------+----------------+----------------+-------------+-------------+-------------+
# Note: initcap capitalizes first letter of EACH word, lowercases the rest
# Note: trim only removes spaces, not other whitespace by default

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Concat and Substring
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Concatenation & Substring
# ============================================================
# Real-world: Building full names and extracting parts from codes

from pyspark.sql.functions import (
    concat, concat_ws, substring, length, lit, left, right
)

# Sample employee data
emp_data = [
    ("John", "Doe", "EMP-2024-00142"),
    ("Jane", "Smith", "EMP-2023-00078"),
    ("Bob", "Williams", "EMP-2024-00256"),
    ("Alice", "Johnson", "EMP-2022-00001"),
]

# Create DataFrame
df = spark.createDataFrame(emp_data, ["first_name", "last_name", "emp_code"])

# Concatenation examples
concat_result = df.select(
    # concat() — joins strings without separator
    concat(col("first_name"), lit(" "), col("last_name")).alias("full_name_concat"),
    
    # concat_ws() — joins with separator (ws = "with separator")
    concat_ws(" ", col("first_name"), col("last_name")).alias("full_name_ws"),
    
    # concat_ws handles NULLs better — skips them instead of making result NULL
    concat_ws("-", col("first_name"), col("last_name"), col("emp_code")).alias("all_joined"),
)

concat_result.show(truncate=False)  # Show concatenation results

# Substring examples — extract parts of the employee code
# Format: EMP-YYYY-NNNNN
substr_result = df.select(
    col("emp_code"),  # Original code
    
    # substring(col, start_pos, length) — 1-based indexing!
    substring(col("emp_code"), 1, 3).alias("prefix"),      # "EMP" (pos 1, len 3)
    substring(col("emp_code"), 5, 4).alias("year"),         # "2024" (pos 5, len 4)
    substring(col("emp_code"), 10, 5).alias("number"),      # "00142" (pos 10, len 5)
    
    # length() — get string length
    length(col("emp_code")).alias("code_length"),           # 14
    
    # right() — get last N characters (available in newer Spark versions)
    # Use substring with negative position as alternative
    substring(col("emp_code"), -5, 5).alias("last_5"),     # "00142"
)

substr_result.show(truncate=False)  # Show extraction results

# Expected Output:
# +---------------+------------+----------------------------+
# |full_name_concat|full_name_ws|all_joined                 |
# +---------------+------------+----------------------------+
# |John Doe       |John Doe    |John-Doe-EMP-2024-00142    |
# |Jane Smith     |Jane Smith  |Jane-Smith-EMP-2023-00078  |
# +---------------+------------+----------------------------+
# Key: concat() returns NULL if ANY input is NULL
#      concat_ws() skips NULLs gracefully

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Split and Basic Search
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Split, Instr, and Contains
# ============================================================
# Real-world: Parsing email addresses and searching in text

from pyspark.sql.functions import (
    split, instr, locate, size, element_at,
    col, lit
)

# Sample data — emails and descriptions
email_data = [
    ("john.doe@company.com", "Senior Software Engineer at NYC office"),
    ("jane_smith@gmail.com", "Data Scientist working remotely"),
    ("bob.williams@company.co.uk", "Manager in London office"),
    ("alice@startup.io", "CTO and co-founder"),
]

df = spark.createDataFrame(email_data, ["email", "description"])

# Split examples
split_result = df.select(
    col("email"),  # Original email
    
    # split(col, pattern) — returns an ARRAY
    split(col("email"), "@").alias("split_at"),              # ["john.doe", "company.com"]
    
    # Access specific elements (1-based with element_at)
    element_at(split(col("email"), "@"), 1).alias("username"),  # "john.doe"
    element_at(split(col("email"), "@"), 2).alias("domain"),    # "company.com"
    
    # Split username by dot
    split(element_at(split(col("email"), "@"), 1), "\\.").alias("name_parts"),  # ["john", "doe"]
    
    # Count parts after split using size()
    size(split(col("email"), "\\.")).alias("dot_count_plus_1"),  # Number of parts
)

split_result.show(truncate=False)  # Show split results

# Search functions
search_result = df.select(
    col("email"),
    col("description"),
    
    # instr(col, substring) — returns position (1-based), 0 if not found
    instr(col("email"), "@").alias("at_position"),           # Position of @
    
    # locate(substring, col, start_pos) — like instr but can specify start
    locate(".", col("email"), 1).alias("first_dot_pos"),     # First dot position
    
    # contains — returns boolean (available via SQL expr or column method)
    col("description").contains("office").alias("has_office"),  # True/False
    
    # startswith and endswith
    col("email").startswith("john").alias("starts_john"),       # True/False
    col("email").endswith(".com").alias("ends_com"),            # True/False
)

search_result.show(truncate=False)  # Show search results

# Expected Output (split_result):
# +-------------------------+---------------------------+----------+----------------+
# |email                    |split_at                   |username  |domain          |
# +-------------------------+---------------------------+----------+----------------+
# |john.doe@company.com     |[john.doe, company.com]    |john.doe  |company.com     |
# |jane_smith@gmail.com     |[jane_smith, gmail.com]    |jane_smith|gmail.com       |
# +-------------------------+---------------------------+----------+----------------+

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Regex Functions
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Regular Expressions
# ============================================================
# Real-world: Extracting phone numbers, cleaning addresses, validating formats

from pyspark.sql.functions import (
    regexp_replace, regexp_extract, col, lit
)

# Sample data with messy phone numbers and addresses
contact_data = [
    ("John Doe", "(555) 123-4567", "123 Main St, Apt 4B, New York, NY 10001"),
    ("Jane Smith", "555.987.6543", "456 Oak Ave, Suite 200, Chicago, IL 60601"),
    ("Bob W.", "+1-555-246-8135", "789 Pine Rd, Los Angeles, CA 90001"),
    ("Alice J.", "5553698521", "321 Elm Blvd, Houston, TX 77001"),
    ("Charlie B.", "(555)111-2222 ext.5", "654 Maple Dr, #12, Miami, FL 33101"),
]

df = spark.createDataFrame(contact_data, ["name", "phone", "address"])

# regexp_replace(col, pattern, replacement) — replace all matches
clean_phones = df.select(
    col("phone"),  # Original messy phone
    
    # Remove all non-digit characters
    regexp_replace(col("phone"), r"[^0-9]", "").alias("digits_only"),
    
    # Standardize to (XXX) XXX-XXXX format
    regexp_replace(
        regexp_replace(col("phone"), r"[^0-9]", ""),  # First strip to digits
        r"^1?(\d{3})(\d{3})(\d{4}).*$",              # Match 10 digits (skip leading 1)
        r"($1) $2-$3"                                  # Format as (XXX) XXX-XXXX
    ).alias("formatted_phone"),
)

clean_phones.show(truncate=False)  # Show cleaned phones

# regexp_extract(col, pattern, group_index) — extract specific group
extracted = df.select(
    col("address"),  # Original address
    
    # Extract state abbreviation (2 uppercase letters before zip)
    regexp_extract(col("address"), r", ([A-Z]{2}) \d{5}", 1).alias("state"),
    
    # Extract zip code (5 digits at end)
    regexp_extract(col("address"), r"(\d{5})$", 1).alias("zip_code"),
    
    # Extract city (word(s) before state)
    regexp_extract(col("address"), r", ([A-Za-z ]+), [A-Z]{2}", 1).alias("city"),
    
    # Extract street number
    regexp_extract(col("address"), r"^(\d+)", 1).alias("street_num"),
)

extracted.show(truncate=False)  # Show extracted parts

# Expected Output (clean_phones):
# +-------------------+------------+----------------+
# |phone              |digits_only |formatted_phone |
# +-------------------+------------+----------------+
# |(555) 123-4567     |5551234567  |(555) 123-4567  |
# |555.987.6543       |5559876543  |(555) 987-6543  |
# |+1-555-246-8135    |15552468135 |(555) 246-8135  |
# |5553698521         |5553698521  |(555) 369-8521  |
# |(555)111-2222 ext.5|555111222250|(555) 111-2222  |
# +-------------------+------------+----------------+

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Translate, Reverse, Repeat
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Translate, Reverse, Repeat, Overlay
# ============================================================
# Real-world: Data masking, character replacement, string manipulation

from pyspark.sql.functions import (
    translate, reverse, repeat, overlay, col, lit,
    format_string, format_number, space
)

# Sample data
data = [
    ("Hello World", "1234567890", 1234567.891, "ABCDEFGHIJ"),
    ("PySpark Fun", "0987654321", 9876.5, "KLMNOPQRST"),
    ("Data Lake", "5555551234", 42.0, "UVWXYZ1234"),
]

df = spark.createDataFrame(data, ["text", "phone", "amount", "code"])

# translate(col, from_chars, to_chars) — character-by-character replacement
# Like Python's str.maketrans() but distributed
translate_result = df.select(
    col("text"),
    
    # Replace vowels with stars (character mapping: a→*, e→*, i→*, o→*, u→*)
    translate(col("text"), "aeiouAEIOU", "**********").alias("vowels_masked"),
    
    # ROT13-like cipher (simplified: just swap a few letters)
    translate(col("text"), "abcdefghij", "ZYXWVUTSRQ").alias("cipher"),
    
    # Mask phone number — replace digits with X
    translate(col("phone"), "0123456789", "XXXXXXXXXX").alias("phone_masked"),
)

translate_result.show(truncate=False)  # Show translated results

# reverse(), repeat(), space()
manip_result = df.select(
    col("text"),
    
    # reverse() — reverse string characters
    reverse(col("text")).alias("reversed"),              # "dlroW olleH"
    
    # repeat(col, n) — repeat string n times
    repeat(lit("=-"), 5).alias("separator"),             # "=-=-=-=-=-=-=-"
    
    # space(n) — create n spaces (useful for formatting)
    concat(lit("["), space(lit(5)), lit("]")).alias("spaces"),  # "[     ]"
    
    # overlay(col, replacement, position, length) — replace part of string
    overlay(col("code"), lit("****"), 4, 4).alias("code_partial_mask"),  # "ABC****HIJ"
)

manip_result.show(truncate=False)  # Show manipulation results

# format_string and format_number — printf-style formatting
format_result = df.select(
    col("text"),
    col("amount"),
    
    # format_string(format, args...) — C-style printf
    format_string("Name: %-15s | Amount: $%,.2f", col("text"), col("amount")).alias("formatted"),
    
    # format_number(col, decimal_places) — adds commas and rounds
    format_number(col("amount"), 2).alias("amount_formatted"),  # "1,234,567.89"
)

format_result.show(truncate=False)  # Show formatted results

# Expected Output (translate_result):
# +-----------+------------+------------+------------+
# |text       |vowels_masked|cipher     |phone_masked|
# +-----------+------------+------------+------------+
# |Hello World|H*ll* W*rld |HvllT WTrlw|XXXXXXXXXX  |
# |PySpark Fun|PySpZrk F*n |PySuZrk Fun|XXXXXXXXXX  |
# +-----------+------------+------------+------------+

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Soundex, Levenshtein, Similarity
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Soundex, Levenshtein, Similarity
# ============================================================
# Real-world: Fuzzy name matching for deduplication or search

from pyspark.sql.functions import (
    soundex, levenshtein, col, lit, length, expr
)

# Sample data — names with typos and variations
names_data = [
    ("Robert", "Rupert"),      # Similar sounding
    ("Smith", "Smyth"),        # Spelling variation
    ("Catherine", "Katherine"), # Different spelling, same name
    ("John", "Jon"),           # Missing letter
    ("William", "Williams"),   # Extra letter
    ("Steven", "Stephen"),     # Different spelling
    ("Michael", "Michele"),    # Gender variation
    ("Thompson", "Thomson"),   # With/without p
]

df = spark.createDataFrame(names_data, ["name1", "name2"])

# Soundex — phonetic algorithm (same code = similar pronunciation)
# Returns 4-character code: first letter + 3 digits
soundex_result = df.select(
    col("name1"),
    col("name2"),
    soundex(col("name1")).alias("soundex1"),        # Phonetic code for name1
    soundex(col("name2")).alias("soundex2"),        # Phonetic code for name2
    (soundex(col("name1")) == soundex(col("name2"))).alias("sounds_similar"),  # Match?
)

soundex_result.show(truncate=False)  # Show soundex comparison

# Levenshtein distance — minimum edits (insert/delete/replace) to transform one string to another
lev_result = df.select(
    col("name1"),
    col("name2"),
    
    # levenshtein(col1, col2) — returns integer edit distance
    levenshtein(col("name1"), col("name2")).alias("edit_distance"),
    
    # Calculate similarity ratio: 1 - (distance / max_length)
    # Higher = more similar (1.0 = identical, 0.0 = completely different)
    (1 - levenshtein(col("name1"), col("name2")) / 
     expr("greatest(length(name1), length(name2))")).alias("similarity_ratio"),
)

lev_result.show(truncate=False)  # Show levenshtein comparison

# Practical application: Find potential duplicate customers
customers = spark.createDataFrame([
    (1, "Robert Smith"),
    (2, "Rupert Smyth"),
    (3, "Bob Smith"),
    (4, "John Williams"),
    (5, "Jon William"),
], ["id", "customer_name"])

# Self-join to find similar names (edit distance <= 3)
from pyspark.sql.functions import broadcast

duplicates = customers.alias("a").crossJoin(broadcast(customers.alias("b"))) \
    .filter(col("a.id") < col("b.id")) \
    .withColumn("distance", levenshtein(col("a.customer_name"), col("b.customer_name"))) \
    .filter(col("distance") <= 3) \
    .select(
        col("a.id").alias("id1"),
        col("a.customer_name").alias("name1"),
        col("b.id").alias("id2"),
        col("b.customer_name").alias("name2"),
        col("distance")
    )

duplicates.show(truncate=False)  # Show potential duplicates

# Expected Output (lev_result):
# +---------+---------+-------------+----------------+
# |name1    |name2    |edit_distance |similarity_ratio|
# +---------+---------+-------------+----------------+
# |Robert   |Rupert   |2            |0.666...        |
# |Smith    |Smyth    |1            |0.8             |
# |Catherine|Katherine|1            |0.888...        |
# |John     |Jon      |1            |0.75            |
# +---------+---------+-------------+----------------+

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Regex Mastery and Multi-Pattern Extraction
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Regex Mastery & Multi-Pattern Extraction
# ============================================================
# Real-world: Parsing semi-structured log files and extracting all entities

from pyspark.sql.functions import (
    regexp_extract, regexp_replace, regexp_extract_all,
    col, lit, explode, expr, when, length, trim
)

# Sample log data — semi-structured server logs
log_data = [
    ("2024-01-15 08:23:45.123 [INFO] User john.doe@company.com logged in from 192.168.1.100 (Chrome/120.0)",),
    ("2024-01-15 08:24:01.456 [ERROR] Failed login for admin@system.org from 10.0.0.55 - Invalid password (attempt 3/5)",),
    ("2024-01-15 08:25:12.789 [WARN] High memory usage: 87.5% on node worker-03.cluster.local (PID: 12345)",),
    ("2024-01-15 08:26:33.012 [INFO] API call to https://api.service.com/v2/users?limit=100 returned 200 in 234ms",),
    ("2024-01-15 08:27:44.345 [ERROR] Connection timeout to db-replica-02:5432 after 30000ms (retries: 3)",),
]

df = spark.createDataFrame(log_data, ["log_line"])

# Extract multiple fields from each log line using regex groups
parsed_logs = df.select(
    # Extract timestamp
    regexp_extract(col("log_line"), r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})", 1).alias("timestamp"),
    
    # Extract log level (INFO, ERROR, WARN, DEBUG)
    regexp_extract(col("log_line"), r"\[(\w+)\]", 1).alias("level"),
    
    # Extract email addresses (if present)
    regexp_extract(col("log_line"), r"([\w.]+@[\w.]+\.[a-z]{2,})", 1).alias("email"),
    
    # Extract IP addresses
    regexp_extract(col("log_line"), r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", 1).alias("ip_address"),
    
    # Extract URLs
    regexp_extract(col("log_line"), r"(https?://[^\s)]+)", 1).alias("url"),
    
    # Extract numeric values with context
    regexp_extract(col("log_line"), r"(\d+)ms", 1).alias("duration_ms"),
    
    # Extract percentage values
    regexp_extract(col("log_line"), r"(\d+\.?\d*)%", 1).alias("percentage"),
    
    # Everything after the [LEVEL] tag is the message
    regexp_extract(col("log_line"), r"\[\w+\]\s+(.*)", 1).alias("message"),
)

parsed_logs.show(truncate=False)  # Show parsed log fields

# Advanced: Use regexp_extract_all to find ALL matches (Spark 3.1+)
# Extract all numbers from each line
numbers_extracted = df.select(
    col("log_line"),
    expr("regexp_extract_all(log_line, '(\\d+)', 1)").alias("all_numbers"),  # All number sequences
    expr("regexp_extract_all(log_line, '([\\w.]+@[\\w.]+)', 1)").alias("all_emails"),  # All emails
)

numbers_extracted.show(truncate=False)  # Show all extracted patterns

# Complex replacement: Redact sensitive information
redacted = df.select(
    # Mask email addresses
    regexp_replace(
        # Mask IP addresses
        regexp_replace(
            col("log_line"),
            r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}",  # IP pattern
            "[REDACTED_IP]"                              # Replacement
        ),
        r"[\w.]+@[\w.]+\.[a-z]{2,}",                   # Email pattern
        "[REDACTED_EMAIL]"                               # Replacement
    ).alias("redacted_log")
)

redacted.show(truncate=False)  # Show redacted logs

# Expected Output (parsed_logs first 2 rows):
# +-------------------------+-----+----------------------+-------------+---+
# |timestamp                |level|email                 |ip_address   |url|
# +-------------------------+-----+----------------------+-------------+---+
# |2024-01-15 08:23:45.123  |INFO |john.doe@company.com  |192.168.1.100|   |
# |2024-01-15 08:24:01.456  |ERROR|admin@system.org      |10.0.0.55    |   |
# +-------------------------+-----+----------------------+-------------+---+

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: JSON String Functions
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: JSON String Functions
# ============================================================
# Real-world: Processing JSON stored as strings (API responses, event payloads)

from pyspark.sql.functions import (
    to_json, from_json, schema_of_json, get_json_object, json_tuple,
    col, lit, struct, array, create_map, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, MapType
)

# Sample data — JSON strings from an API
json_data = [
    ('{"name": "John", "age": 30, "city": "NYC", "skills": ["Python", "Spark"]}',),
    ('{"name": "Jane", "age": 25, "city": "LA", "skills": ["SQL", "Scala", "Java"]}',),
    ('{"name": "Bob", "age": 35, "city": "Chicago", "skills": ["Python"]}',),
]

df = spark.createDataFrame(json_data, ["json_str"])

# get_json_object — extract single value from JSON string using path
json_extract = df.select(
    col("json_str"),
    get_json_object(col("json_str"), "$.name").alias("name"),       # Extract name
    get_json_object(col("json_str"), "$.age").alias("age"),         # Extract age
    get_json_object(col("json_str"), "$.skills[0]").alias("skill1"),  # First skill
    get_json_object(col("json_str"), "$.skills[1]").alias("skill2"),  # Second skill
)

json_extract.show(truncate=False)  # Show extracted JSON fields

# json_tuple — extract multiple fields at once (more efficient than multiple get_json_object)
json_multi = df.select(
    json_tuple(col("json_str"), "name", "age", "city").alias("name", "age", "city")
)

json_multi.show(truncate=False)  # Show json_tuple results

# from_json — parse JSON string into a proper struct column
# First, define the schema
json_schema = StructType([
    StructField("name", StringType(), True),       # Name field
    StructField("age", IntegerType(), True),        # Age field
    StructField("city", StringType(), True),        # City field
    StructField("skills", ArrayType(StringType()), True),  # Skills array
])

# Parse JSON string into struct
parsed = df.select(
    from_json(col("json_str"), json_schema).alias("data")  # Parse to struct
).select(
    col("data.name"),      # Access struct fields with dot notation
    col("data.age"),
    col("data.city"),
    col("data.skills"),
    col("data.skills")[0].alias("primary_skill"),  # Array indexing
)

parsed.show(truncate=False)  # Show parsed struct results

# to_json — convert struct/map/array back to JSON string
struct_df = spark.createDataFrame([
    ("John", 30, ["Python", "Spark"]),
    ("Jane", 25, ["SQL", "Java"]),
], ["name", "age", "skills"])

jsonified = struct_df.select(
    # Convert entire row to JSON
    to_json(struct("name", "age", "skills")).alias("full_json"),
    
    # Convert just some columns
    to_json(struct(col("name"), col("age"))).alias("partial_json"),
    
    # Convert array to JSON
    to_json(col("skills")).alias("skills_json"),
)

jsonified.show(truncate=False)  # Show JSON strings

# schema_of_json — infer schema from a JSON string (useful for dynamic schemas)
schema_str = schema_of_json(lit('{"name": "x", "age": 1, "skills": ["a"]}'))
print(f"Inferred schema: {schema_str}")  # Print inferred schema expression

# Expected Output (json_extract):
# +----+---+------+------+
# |name|age|skill1|skill2|
# +----+---+------+------+
# |John|30 |Python|Spark |
# |Jane|25 |SQL   |Scala |
# |Bob |35 |Python|null  |
# +----+---+------+------+

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Encoding, URL, Base64, Full Pipeline
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Encoding, URL Functions & Full Pipeline
# ============================================================
# Real-world: Processing web data, encoding/decoding, building a complete
# string cleaning pipeline for production use

from pyspark.sql.functions import (
    base64, unbase64, encode, decode, col, lit,
    ascii, chr, hex, unhex, md5, sha2,
    regexp_replace, trim, lower, initcap, when,
    concat_ws, split, size, length, expr, translate
)

# ---- PART 1: Encoding Functions ----
encoding_data = [
    ("Hello World", "https://example.com/path?q=hello world&lang=en"),
    ("PySpark™", "https://api.com/search?term=für München"),
    ("Data & Analytics", "https://site.com/page#section 1"),
]

df = spark.createDataFrame(encoding_data, ["text", "url"])

# base64 / unbase64 — encode binary data as text
encoded = df.select(
    col("text"),
    
    # base64(col) — encode string to base64
    base64(encode(col("text"), "UTF-8")).alias("base64_encoded"),
    
    # Decode back: unbase64 returns binary, decode converts to string
    decode(unbase64(base64(encode(col("text"), "UTF-8"))), "UTF-8").alias("decoded_back"),
    
    # hex / unhex — hexadecimal encoding
    hex(col("text")).alias("hex_encoded"),
    
    # ascii — get ASCII value of first character
    ascii(col("text")).alias("first_char_ascii"),  # 72 for 'H'
    
    # chr — convert ASCII code to character
    chr(lit(65)).alias("chr_65"),  # 'A'
)

encoded.show(truncate=False)  # Show encoding results

# URL encoding (using expr for url_encode/url_decode in Spark 3.4+)
# For older versions, use regexp_replace as workaround
url_result = df.select(
    col("url"),
    # Manual URL-safe replacement for common characters
    regexp_replace(
        regexp_replace(
            regexp_replace(col("url"), " ", "%20"),  # Space → %20
            "#", "%23"                                 # Hash → %23
        ),
        "ü", "%C3%BC"                                  # ü → UTF-8 encoded
    ).alias("url_encoded_manual"),
)

url_result.show(truncate=False)  # Show URL encoding

# ---- PART 2: Production String Cleaning Pipeline ----
# Simulate messy data from multiple sources
messy_data = [
    ("  JOHN   DOE  ", "(555) 123-4567", "john.doe@COMPANY.COM", "$1,234.56"),
    ("jane    smith", "555.987.6543", "JANE_SMITH@gmail.COM", "$987.00"),
    (" Bob  Williams III ", "+1-555-246-8135", "bob.w@Company.co.uk", "$12,345.67"),
    ("  alice   johnson  ", "5553698521", "Alice.J@STARTUP.IO", "$0.99"),
    ("CHARLIE  BROWN Jr.", "(555)111-2222", "charlie@old-domain.net", "$100,000.00"),
]

raw_df = spark.createDataFrame(messy_data, ["name", "phone", "email", "revenue"])

# Build a complete cleaning pipeline using chained string functions
cleaned_df = raw_df.select(
    # Clean name: trim → collapse multiple spaces → proper case
    initcap(
        regexp_replace(trim(col("name")), r"\s+", " ")  # Collapse spaces
    ).alias("clean_name"),
    
    # Clean phone: extract digits → format
    regexp_replace(
        regexp_replace(col("phone"), r"[^0-9]", ""),  # Digits only
        r"^1?(\d{3})(\d{3})(\d{4})$",                 # Parse
        r"+1-$1-$2-$3"                                  # Format
    ).alias("clean_phone"),
    
    # Clean email: lowercase entire thing
    lower(trim(col("email"))).alias("clean_email"),
    
    # Clean revenue: remove $ and commas, cast to double
    regexp_replace(
        regexp_replace(col("revenue"), r"[$,]", ""),  # Remove $ and ,
        r"^(\d+\.?\d*)$", r"$1"                       # Keep number
    ).cast("double").alias("clean_revenue"),
    
    # Derived: extract domain from email
    lower(
        element_at(split(trim(col("email")), "@"), 2)
    ).alias("email_domain"),
    
    # Derived: name parts count
    size(split(regexp_replace(trim(col("name")), r"\s+", " "), " ")).alias("name_parts"),
)

cleaned_df.show(truncate=False)  # Show final cleaned data

# Verify cleaning quality
print("=== Data Quality Check ===")
cleaned_df.select(
    expr("count(*) as total_rows"),
    expr("sum(case when clean_name is null then 1 else 0 end) as null_names"),
    expr("sum(case when clean_phone rlike '^\\+1-\\d{3}-\\d{3}-\\d{4}$' then 1 else 0 end) as valid_phones"),
    expr("sum(case when clean_email rlike '^[a-z0-9_.]+@[a-z0-9.-]+$' then 1 else 0 end) as valid_emails"),
    expr("avg(clean_revenue) as avg_revenue"),
).show(truncate=False)

# Expected Output (cleaned_df):
# +--------------------+--------------+------------------------+-------------+----------------+-----------+
# |clean_name          |clean_phone   |clean_email             |clean_revenue|email_domain    |name_parts |
# +--------------------+--------------+------------------------+-------------+----------------+-----------+
# |John Doe            |+1-555-123-4567|john.doe@company.com   |1234.56      |company.com     |2          |
# |Jane Smith          |+1-555-987-6543|jane_smith@gmail.com   |987.0        |gmail.com       |2          |
# |Bob Williams Iii    |+1-555-246-8135|bob.w@company.co.uk    |12345.67     |company.co.uk   |3          |
# +--------------------+--------------+------------------------+-------------+----------------+-----------+

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with String Functions
# MAGIC
# MAGIC ### ❌ Mistake 1: Using 0-based indexing (Python habit)
# MAGIC ```python
# MAGIC # WRONG — PySpark uses 1-based indexing!
# MAGIC df.select(substring(col("text"), 0, 3))  # Returns unexpected results
# MAGIC
# MAGIC # CORRECT — Start at position 1
# MAGIC df.select(substring(col("text"), 1, 3))  # First 3 characters
# MAGIC ```
# MAGIC **Why:** PySpark follows SQL conventions (1-based), not Python (0-based). Position 0 returns empty string in most cases.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 2: Using concat() with NULL values
# MAGIC ```python
# MAGIC # WRONG — concat returns NULL if ANY argument is NULL
# MAGIC df.select(concat(col("first"), lit(" "), col("last")))  # NULL if first OR last is NULL
# MAGIC
# MAGIC # CORRECT — Use concat_ws which skips NULLs
# MAGIC df.select(concat_ws(" ", col("first"), col("last")))  # "John" if last is NULL
# MAGIC
# MAGIC # OR — Use coalesce to provide defaults
# MAGIC df.select(concat(coalesce(col("first"), lit("")), lit(" "), coalesce(col("last"), lit(""))))
# MAGIC ```
# MAGIC **Why:** `concat()` propagates NULLs (SQL standard). `concat_ws()` was designed to handle NULLs gracefully.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 3: Forgetting to escape regex special characters
# MAGIC ```python
# MAGIC # WRONG — dot means "any character" in regex
# MAGIC df.select(split(col("email"), "."))  # Splits on EVERY character!
# MAGIC
# MAGIC # CORRECT — Escape the dot
# MAGIC df.select(split(col("email"), "\\."))  # Splits on literal dots
# MAGIC
# MAGIC # Also common: forgetting to escape in regexp_extract
# MAGIC # WRONG: regexp_extract(col("text"), "price: $100", 0)  # $ means end-of-string
# MAGIC # CORRECT: regexp_extract(col("text"), "price: \\$100", 0)
# MAGIC ```
# MAGIC **Why:** PySpark regex uses Java regex engine. Special chars: `.` `*` `+` `?` `^` `$` `{` `}` `[` `]` `(` `)` `|` `\\`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 4: Using Python string methods on DataFrame columns
# MAGIC ```python
# MAGIC # WRONG — This is Python string method, not PySpark!
# MAGIC df.select(col("name").upper())  # AttributeError! Columns don't have .upper()
# MAGIC
# MAGIC # ALSO WRONG — Using a UDF when built-in exists
# MAGIC from pyspark.sql.functions import udf
# MAGIC upper_udf = udf(lambda x: x.upper() if x else None)  # SLOW!
# MAGIC
# MAGIC # CORRECT — Use PySpark built-in function
# MAGIC from pyspark.sql.functions import upper
# MAGIC df.select(upper(col("name")))  # Fast, distributed, optimized
# MAGIC ```
# MAGIC **Why:** Python methods execute in Python (slow, one-at-a-time). Built-in functions execute in JVM (fast, parallel, Tungsten-optimized).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 5: Using regexp for simple operations
# MAGIC ```python
# MAGIC # WRONG — Overkill: using regex to check if string contains a word
# MAGIC df.filter(regexp_extract(col("text"), "(error)", 1) != "")  # Works but slow
# MAGIC
# MAGIC # CORRECT — Use contains() or like() for simple patterns
# MAGIC df.filter(col("text").contains("error"))  # Simple, fast
# MAGIC df.filter(col("text").like("%error%"))    # SQL-style, also fast
# MAGIC
# MAGIC # WRONG — Using regex to replace fixed strings
# MAGIC df.select(regexp_replace(col("text"), "old_value", "new_value"))  # Compiles regex engine
# MAGIC
# MAGIC # BETTER for fixed strings — Use translate() for character replacement
# MAGIC # Or overlay() for position-based replacement
# MAGIC ```
# MAGIC **Why:** Regex compilation has overhead. For simple contains/starts/ends checks, dedicated functions are faster and more readable.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of String Function Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste (Run these exactly)
# MAGIC 1. Create a DataFrame with 5 names in mixed case. Apply `upper()`, `lower()`, and `initcap()` to all.
# MAGIC 2. Use `trim()`, `lpad()`, and `rpad()` on strings with spaces.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Modify the phone cleaning example to format as `XXX-XXX-XXXX` instead of `(XXX) XXX-XXXX`.
# MAGIC 4. Change the email parser to also extract the TLD (`.com`, `.co.uk`).
# MAGIC
# MAGIC ### Level 3 — Combine Two Concepts
# MAGIC 5. Parse a CSV-like string column using `split()`, then clean each element with `trim()` and `initcap()`.
# MAGIC 6. Use `soundex()` + `levenshtein()` together to build a name matching score.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Parse URLs like `https://www.example.com/path/page?param=value#anchor` into components: protocol, domain, path, query params, fragment.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build a complete address standardization pipeline:
# MAGIC    - Input: "123 n. main st, apt 4b, new york, ny 10001"
# MAGIC    - Output: Standardize abbreviations (st→Street, n.→North, apt→Apartment), proper case, validate zip.
# MAGIC
# MAGIC ### Level 6 — Design Your Own
# MAGIC 9. Design a credit card masking function that:
# MAGIC    - Accepts any format (spaces, dashes, no separator)
# MAGIC    - Returns `****-****-****-1234` (last 4 visible)
# MAGIC    - Validates it's 16 digits
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. You have 100M rows of log data. Compare performance of:
# MAGIC     - Multiple `regexp_extract()` calls vs one complex pattern with groups
# MAGIC     - `contains()` vs `like()` vs `rlike()` for simple search
# MAGIC     - Measure with `spark.time()` or `%%timeit`
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Handle these edge cases in your string pipeline:
# MAGIC     - Unicode characters (emojis, accented letters, CJK)
# MAGIC     - NULL values at every stage
# MAGIC     - Empty strings vs NULL (they're different!)
# MAGIC     - Strings with only whitespace
# MAGIC     - Very long strings (>10KB per cell)
# MAGIC
# MAGIC ### Level 9 — Production Ready
# MAGIC 12. Build a reusable `clean_text_column()` function that:
# MAGIC     - Accepts a DataFrame and column name
# MAGIC     - Trims, collapses whitespace, handles NULL
# MAGIC     - Optionally: lowercases, removes special chars, truncates
# MAGIC     - Returns the DataFrame with cleaned column
# MAGIC     - Includes assertion checks and logging
# MAGIC
# MAGIC ### Level 10 — Teach Someone
# MAGIC 13. Create a "String Functions Cheat Sheet" notebook that:
# MAGIC     - Shows every function with a one-line example
# MAGIC     - Groups by use case (cleaning, extracting, comparing, encoding)
# MAGIC     - Includes performance notes (which are fast vs slow)
# MAGIC     - Has a decision tree: "Given this problem, use this function"

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

# --- Level 1: Copy-Paste ---
from pyspark.sql.functions import *  # Import all functions for solutions

# Solution 1: Case functions
names_df = spark.createDataFrame([
    ("jOHN dOE",), ("JANE SMITH",), ("bob williams",), 
    ("Alice Johnson",), ("  CHARLIE brown  ",)
], ["raw_name"])

names_df.select(
    col("raw_name"),                          # Original
    upper(col("raw_name")).alias("upper"),     # ALL CAPS
    lower(col("raw_name")).alias("lower"),     # all lower
    initcap(trim(col("raw_name"))).alias("proper"),  # Trim + Proper Case
).show(truncate=False)

# Solution 2: Padding
pad_df = spark.createDataFrame([("42",), ("7",), ("1234",)], ["id"])
pad_df.select(
    lpad(col("id"), 8, "0").alias("left_padded"),   # "00000042"
    rpad(col("id"), 8, ".").alias("right_padded"),  # "42......"
).show()

# --- Level 3: Combine (Parse CSV string + clean) ---
csv_df = spark.createDataFrame([
    ("  john doe , new york , engineer  ",),
    ("JANE SMITH,  chicago  , analyst",),
], ["csv_line"])

csv_df.select(
    initcap(trim(element_at(split(col("csv_line"), ","), 1))).alias("name"),
    initcap(trim(element_at(split(col("csv_line"), ","), 2))).alias("city"),
    lower(trim(element_at(split(col("csv_line"), ","), 3))).alias("role"),
).show(truncate=False)

# --- Level 4: URL Parser ---
url_df = spark.createDataFrame([
    ("https://www.example.com/path/page?param=value&x=1#section2",),
    ("http://api.service.io/v2/users?limit=100",),
    ("https://data.company.com/reports/2024/Q1",),
], ["url"])

url_df.select(
    # Protocol: everything before ://
    regexp_extract(col("url"), r"^(https?)://", 1).alias("protocol"),
    # Domain: between :// and first /
    regexp_extract(col("url"), r"://([^/]+)", 1).alias("domain"),
    # Path: between domain and ? or #
    regexp_extract(col("url"), r"://[^/]+((/[^?#]*)?)", 1).alias("path"),
    # Query params: between ? and # (or end)
    regexp_extract(col("url"), r"\?([^#]*)", 1).alias("query"),
    # Fragment: after #
    regexp_extract(col("url"), r"#(.*)", 1).alias("fragment"),
).show(truncate=False)

# --- Level 6: Credit Card Masking ---
def mask_credit_card(df, cc_col):
    """Mask credit card: show only last 4 digits as ****-****-****-1234"""
    # Step 1: Remove all non-digit characters
    digits_only = regexp_replace(col(cc_col), r"[^0-9]", "")  # Strip formatting
    # Step 2: Validate 16 digits
    is_valid = length(digits_only) == 16  # Must be exactly 16 digits
    # Step 3: Extract last 4 and format
    masked = concat(
        lit("****-****-****-"),           # Fixed mask prefix
        substring(digits_only, 13, 4)     # Last 4 digits
    )
    # Step 4: Return masked if valid, else "INVALID"
    return df.withColumn(
        f"{cc_col}_masked",
        when(is_valid, masked).otherwise(lit("INVALID"))  # Conditional masking
    )

# Test credit card masking
cc_df = spark.createDataFrame([
    ("4532-1234-5678-9012",),   # Dashes
    ("4532 1234 5678 9012",),   # Spaces
    ("4532123456789012",),      # No separator
    ("1234-5678",),             # Too short — invalid
], ["card_number"])

mask_credit_card(cc_df, "card_number").show(truncate=False)

# --- Level 9: Production-Ready clean_text_column ---
def clean_text_column(df, col_name, lowercase=False, remove_special=False, max_length=None):
    """
    Production-ready text cleaning function.
    
    Args:
        df: Input DataFrame
        col_name: Column to clean
        lowercase: If True, convert to lowercase
        remove_special: If True, remove non-alphanumeric chars (keep spaces)
        max_length: If set, truncate to this length
    
    Returns:
        DataFrame with cleaned column (original preserved as {col}_raw)
    """
    # Preserve original
    result = df.withColumn(f"{col_name}_raw", col(col_name))  # Keep original
    
    # Step 1: Handle NULL — replace with empty string for processing
    cleaned = coalesce(col(col_name), lit(""))  # NULL → empty string
    
    # Step 2: Trim whitespace
    cleaned = trim(cleaned)  # Remove leading/trailing spaces
    
    # Step 3: Collapse multiple spaces to single space
    cleaned = regexp_replace(cleaned, r"\s+", " ")  # "  a   b  " → "a b"
    
    # Step 4: Optional lowercase
    if lowercase:  # Only if requested
        cleaned = lower(cleaned)  # Convert to lowercase
    
    # Step 5: Optional special character removal
    if remove_special:  # Only if requested
        cleaned = regexp_replace(cleaned, r"[^a-zA-Z0-9 ]", "")  # Keep alphanum + space
    
    # Step 6: Optional truncation
    if max_length:  # Only if max_length specified
        cleaned = substring(cleaned, 1, max_length)  # Truncate to max_length
    
    # Step 7: Convert empty strings back to NULL
    cleaned = when(length(cleaned) == 0, lit(None)).otherwise(cleaned)  # Empty → NULL
    
    # Apply the cleaned expression
    result = result.withColumn(col_name, cleaned)  # Replace with cleaned version
    
    return result  # Return cleaned DataFrame

# Test the production function
test_df = spark.createDataFrame([
    ("  Hello   World!  ",),
    (None,),
    ("   ",),
    ("Special: @#$% chars!!!",),
    ("A" * 100,),  # Very long string
], ["text"])

# Apply with all options
cleaned = clean_text_column(test_df, "text", lowercase=True, remove_special=True, max_length=20)
cleaned.show(truncate=False)  # Show cleaned results

print("✅ All homework solutions complete!")  # Confirmation message