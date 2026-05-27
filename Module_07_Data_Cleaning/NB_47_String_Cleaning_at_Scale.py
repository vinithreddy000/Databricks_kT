# Databricks notebook source
# DBTITLE 1,NB_47 Header
# MAGIC %md
# MAGIC # NB_47 — String Cleaning at Scale
# MAGIC
# MAGIC **Module 7: Data Cleaning & Quality** | Notebook 47 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Trimming and whitespace normalization
# MAGIC * Case standardization (upper, lower, initcap)
# MAGIC * Regex-based pattern extraction and replacement
# MAGIC * Special character removal and Unicode handling
# MAGIC * Name parsing and standardization
# MAGIC * Address/phone/email normalization
# MAGIC * Tokenization and text splitting
# MAGIC * Production string cleaning pipeline
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (High-volume text processing)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — String Cleaning Challenges
# MAGIC %md
# MAGIC ## SECTION 1 — String Cleaning Challenges (Real-World Analogy)
# MAGIC
# MAGIC ### 📦 The Messy Filing Cabinet
# MAGIC
# MAGIC Strings in real data are like handwritten labels — everyone writes differently:
# MAGIC
# MAGIC | Dirty Input | Problem | Cleaned Output |
# MAGIC |---|---|---|
# MAGIC | "  Alice  Smith  " | Extra spaces | "Alice Smith" |
# MAGIC | "aLiCe SMITH" | Inconsistent case | "Alice Smith" |
# MAGIC | "O'Brien-Smith" | Special chars | Handle carefully! |
# MAGIC | "Élèna Müller" | Unicode accents | Normalize or preserve? |
# MAGIC | "123 Main St." vs "123 Main Street" | Abbreviations | Standardize |
# MAGIC | "(555) 123-4567" vs "5551234567" | Format variations | Single format |
# MAGIC
# MAGIC ### Common String Issues at Scale
# MAGIC 1. **Leading/trailing whitespace** (invisible but breaks joins!)
# MAGIC 2. **Internal multiple spaces** ("John  Smith" ≠ "John Smith")
# MAGIC 3. **Case inconsistency** ("new york" vs "New York" vs "NEW YORK")
# MAGIC 4. **Non-printable characters** (\t, \n, \r, zero-width spaces)
# MAGIC 5. **Mixed encoding** (Latin-1 vs UTF-8 artifacts)
# MAGIC 6. **Abbreviation variants** (St/Street, Ave/Avenue, Dr/Doctor)
# MAGIC
# MAGIC ### Performance Tip
# MAGIC String operations are CPU-intensive. Order operations from cheapest to most expensive:
# MAGIC `trim > upper/lower > replace > regexp_replace > UDF`

# COMMAND ----------

# DBTITLE 1,SECTION 2 — String Functions Reference
# MAGIC %md
# MAGIC ## SECTION 2 — PySpark String Functions Quick Reference
# MAGIC
# MAGIC ### Trimming & Whitespace
# MAGIC ```python
# MAGIC trim(col)                    # Remove leading/trailing spaces
# MAGIC ltrim(col) / rtrim(col)      # One side only
# MAGIC regexp_replace(col, "\\s+", " ")  # Collapse internal spaces
# MAGIC ```
# MAGIC
# MAGIC ### Case
# MAGIC ```python
# MAGIC upper(col) / lower(col)      # Full case change
# MAGIC initcap(col)                 # First Letter Of Each Word
# MAGIC ```
# MAGIC
# MAGIC ### Extraction & Replacement
# MAGIC ```python
# MAGIC regexp_extract(col, pattern, group)   # Extract regex match
# MAGIC regexp_replace(col, pattern, replacement)  # Replace matches
# MAGIC translate(col, "from", "to")          # Character-by-character swap
# MAGIC overlay(col, replacement, pos, len)   # Overwrite substring
# MAGIC ```
# MAGIC
# MAGIC ### Splitting & Combining
# MAGIC ```python
# MAGIC split(col, pattern)           # Split into array
# MAGIC concat(col1, col2)           # Concatenate
# MAGIC concat_ws(sep, col1, col2)   # Concat with separator
# MAGIC substring(col, pos, len)     # Extract substring
# MAGIC ```
# MAGIC
# MAGIC ### Pattern Matching
# MAGIC ```python
# MAGIC col.like("%pattern%")        # SQL LIKE
# MAGIC col.rlike("regex")           # Regex match (boolean)
# MAGIC regexp_extract(col, r, 0)   # Extract first match
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Whitespace and case
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Whitespace and Case Normalization
# ============================================================
# Real-world: Customer names from web forms with random spacing/case.

from pyspark.sql import SparkSession  # Import.
from pyspark.sql.functions import (  # Import functions.
    col, trim, ltrim, rtrim, upper, lower, initcap,
    regexp_replace, length, lit
)  # End imports.

spark = SparkSession.builder.getOrCreate()  # Session.

# Messy customer names.
names = spark.createDataFrame([
    ("  Alice  Smith  ",),      # Extra spaces everywhere.
    ("BOB JONES",),             # All caps.
    ("charlie brown",),         # All lower.
    ("  dIaNa   pRiNcE  ",),   # Random case + spaces.
    ("\tEve\nWilson\r",),      # Tab, newline, carriage return.
    ("  frank   lee   jr  ",),  # Multiple internal spaces.
    (" ",),                     # Only whitespace.
    (None,),                    # NULL.
], ["raw_name"])  # Raw names.

print("=== Raw Names (with issues) ===")  # Heading.
names.select(
    col("raw_name"),  # Original.
    length(col("raw_name")).alias("len"),  # Length reveals hidden chars.
).show(truncate=False)  # Display.

# Step 1: Remove non-printable chars (\t, \n, \r).
print("=== Step-by-Step Cleaning ===")  # Heading.
cleaned = names.withColumn(
    "step1_printable",
    regexp_replace(col("raw_name"), "[\\t\\n\\r]", " ")  # Replace control chars with space.
).withColumn(
    "step2_trim",
    trim(col("step1_printable"))  # Remove leading/trailing spaces.
).withColumn(
    "step3_collapse",
    regexp_replace(col("step2_trim"), "\\s+", " ")  # Collapse multiple spaces to one.
).withColumn(
    "step4_initcap",
    initcap(col("step3_collapse"))  # Proper case.
).withColumn(
    "final_name",
    # Handle whitespace-only and empty strings.
    regexp_replace(initcap(regexp_replace(trim(
        regexp_replace(col("raw_name"), "[\\t\\n\\r]", " ")
    ), "\\s+", " ")), "^\\s*$", "")  # All-in-one.
)

cleaned.select("raw_name", "step1_printable", "step2_trim", "step3_collapse", "step4_initcap").show(truncate=False)
print("\n=== Final Clean Names ===")  # Heading.
cleaned.select("raw_name", "final_name", length("final_name").alias("len")).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Pattern extraction with regex
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Pattern Extraction with Regex
# ============================================================
# Real-world: Extract structured data from freeform text.

from pyspark.sql.functions import (  # Import functions.
    col, regexp_extract, regexp_replace, split, size, trim
)  # End imports.

# Freeform text with embedded structured data.
text_data = spark.createDataFrame([
    ("Contact: alice@company.com, Phone: (555) 123-4567",),
    ("Email bob.jones@gmail.com or call 555.987.6543",),
    ("Reach me at charlie_b@yahoo.co.uk | 1-800-555-0199",),
    ("No contact info here, just text.",),
    ("Multiple: test@a.com, other@b.com, phone 555-111-2222",),
], ["raw_text"])  # Raw text.

print("=== Extract Emails ===")  # Heading.
# Email pattern: word chars + @ + domain.
email_pattern = r"([\w.+-]+@[\w-]+\.[\w.-]+)"  # Email regex.

extracted = text_data.withColumn(
    "email",
    regexp_extract(col("raw_text"), email_pattern, 1)  # First email.
)
extracted.select("raw_text", "email").show(truncate=False)  # Display.

# Extract phone numbers.
print("=== Extract Phone Numbers ===")  # Heading.
phone_pattern = r"(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})"  # Phone regex.

with_phone = extracted.withColumn(
    "phone_raw",
    regexp_extract(col("raw_text"), phone_pattern, 1)  # Extract phone.
).withColumn(
    "phone_clean",
    regexp_replace(col("phone_raw"), "[^\\d]", "")  # Digits only.
)
with_phone.select("email", "phone_raw", "phone_clean").show(truncate=False)  # Display.

# Extract all numbers from text.
print("=== Extract All Numbers ===")  # Heading.
number_data = spark.createDataFrame([
    ("Order #12345 for $67.89 (qty: 3)",),
    ("Invoice 2024-001: amount 1,234.56 USD",),
    ("Temp: 98.6F, BP: 120/80",),
], ["text"])  # Texts with numbers.

number_data.withColumn(
    "numbers_found",
    split(regexp_replace(
        regexp_replace(col("text"), "[^\\d.]+", " "),  # Keep digits and dots.
    "^\\s+|\\s+$", ""), "\\s+")  # Split into array.
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Special character handling
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Special Character Handling
# ============================================================
# Real-world: Clean special characters while preserving meaning.

from pyspark.sql.functions import (
    col, regexp_replace, translate, trim, upper, lower
)  # Imports.

# Data with various special characters.
special = spark.createDataFrame([
    ("O'Brien-Smith",),       # Apostrophe, hyphen (valid in names!).
    ("M\u00fcller & S\u00f6hne GmbH",),  # Umlauts, ampersand.
    ("caf\u00e9 \u00e9l\u00e8gant",),        # Accented chars.
    ("Price: $100 (50% off!)",),  # Punctuation.
    ("Hello\u00a0World",),        # Non-breaking space (U+00A0).
    ("data\u200b\u200bcleaning",),     # Zero-width spaces.
    ("\u2018Smart Quotes\u2019",),    # Curly quotes.
    ("\u2014em dash\u2014here",),    # Em dash.
], ["raw_text"])  # Special chars.

print("=== Original Special Characters ===")  # Heading.
special.show(truncate=False)  # Display.

# Strategy 1: Remove ALL non-alphanumeric (aggressive).
print("=== Strategy 1: Keep Only Alphanumeric + Space ===")  # Heading.
special.withColumn(
    "alpha_only",
    regexp_replace(col("raw_text"), "[^a-zA-Z0-9\\s]", "")  # Strip special.
).show(truncate=False)  # Display.

# Strategy 2: Normalize Unicode (accents -> base chars).
print("=== Strategy 2: Normalize Common Unicode ===")  # Heading.
special.withColumn(
    "normalized",
    translate(
        col("raw_text"),
        "\u00e9\u00e8\u00ea\u00f6\u00fc\u00e4\u00e1\u00e0\u00f1\u00c9\u00d6\u00dc\u2018\u2019\u201c\u201d\u2014\u00a0",  # From chars.
        "eeeouaaanEOU''\"\" -"  # To ASCII equivalents.
    )
).show(truncate=False)  # Display.

# Strategy 3: Remove zero-width and invisible characters.
print("=== Strategy 3: Remove Invisible Characters ===")  # Heading.
special.withColumn(
    "visible_only",
    regexp_replace(
        regexp_replace(col("raw_text"), "[\u200b\u200c\u200d\ufeff]", ""),  # Zero-width.
        "\u00a0", " "  # Non-breaking space -> regular space.
    )
).show(truncate=False)  # Display.

print("\nTIP: Choose strategy based on use case:")
print("- Search/matching: aggressive cleaning (Strategy 1)")
print("- Display/storage: preserve meaning (Strategy 3)")
print("- Analytics: normalize accents (Strategy 2)")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Address and phone normalization
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Address & Phone Normalization
# ============================================================
# Real-world: Standardize addresses and phone numbers.

from pyspark.sql.functions import (
    col, regexp_replace, upper, trim, when, lit, split, concat_ws
)  # Imports.

# Messy addresses.
addresses = spark.createDataFrame([
    ("123 Main St., Apt 4B",),
    ("456 BROADWAY AVENUE",),
    ("789 elm street, suite 100",),
    ("  1010 N. First Dr.  ",),
    ("2020 South Oak Blvd, #201",),
], ["raw_address"])  # Addresses.

print("=== Address Standardization ===")  # Heading.

# Common abbreviation mappings.
std_address = addresses.withColumn(
    "clean",
    upper(trim(col("raw_address")))  # Uppercase + trim.
).withColumn("clean",
    regexp_replace(col("clean"), "\\bST\\.?",  "STREET")  # St -> Street.
).withColumn("clean",
    regexp_replace(col("clean"), "\\bAVE\\.?", "AVENUE")  # Ave -> Avenue.
).withColumn("clean",
    regexp_replace(col("clean"), "\\bBLVD\\.?", "BOULEVARD")  # Blvd -> Boulevard.
).withColumn("clean",
    regexp_replace(col("clean"), "\\bDR\\.?",  "DRIVE")  # Dr -> Drive.
).withColumn("clean",
    regexp_replace(col("clean"), "\\bAPT\\.?\\s*", "UNIT ")  # Apt -> Unit.
).withColumn("clean",
    regexp_replace(col("clean"), "#", "UNIT ")  # # -> Unit.
).withColumn("clean",
    regexp_replace(col("clean"), "\\bN\\.?\\s", "NORTH ")  # N -> North.
).withColumn("clean",
    regexp_replace(col("clean"), "\\bS\\.?\\s", "SOUTH ")  # S -> South.
).withColumn("clean",
    regexp_replace(col("clean"), "\\s+", " ")  # Collapse spaces.
)

std_address.select("raw_address", "clean").show(truncate=False)  # Display.

# Phone number standardization.
print("=== Phone Number Standardization ===")  # Heading.
phones = spark.createDataFrame([
    ("(555) 123-4567",),
    ("555.123.4567",),
    ("555 123 4567",),
    ("+1-555-123-4567",),
    ("5551234567",),
    ("1-800-FLOWERS",),  # Vanity number.
], ["raw_phone"])  # Phones.

# Normalize to digits, then format.
std_phone = phones.withColumn(
    "digits",
    regexp_replace(col("raw_phone"), "[^\\d]", "")  # Keep only digits.
).withColumn(
    "formatted",
    when(length(col("digits")) == 10,
         concat_ws("-",
             col("digits").substr(1, 3),  # Area code.
             col("digits").substr(4, 3),  # Exchange.
             col("digits").substr(7, 4),  # Line.
         )
    ).when(length(col("digits")) == 11,
         concat_ws("-",
             col("digits").substr(2, 3),  # Area code (skip country).
             col("digits").substr(5, 3),  # Exchange.
             col("digits").substr(8, 4),  # Line.
         )
    ).otherwise(lit("INVALID"))  # Can't parse.
)
std_phone.show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Name parsing and tokenization
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Name Parsing & Tokenization
# ============================================================
# Real-world: Parse full names into components.

from pyspark.sql.functions import (
    col, split, size, element_at, trim, initcap, when,
    regexp_replace, concat_ws, array_remove, array
)  # Imports.

# Full names in various formats.
names = spark.createDataFrame([
    ("Alice Smith",),              # Simple: first last.
    ("Bob Michael Jones",),        # First middle last.
    ("Charlie",),                  # Single name.
    ("Diana Prince-Wayne",),       # Hyphenated last name.
    ("Eve van der Berg",),         # Multi-word last name.
    ("Dr. Frank Lee Jr.",),        # Title + suffix.
    ("GRACE HOPPER III",),         # Suffix.
    ("Smith, John",),              # Last, First format.
], ["full_name"])  # Full names.

print("=== Name Parsing ===")  # Heading.

# Step 1: Normalize.
cleaned = names.withColumn(
    "normalized",
    initcap(trim(regexp_replace(col("full_name"), "\\s+", " ")))  # Clean + case.
)

# Step 2: Handle "Last, First" format.
cleaned = cleaned.withColumn(
    "normalized",
    when(col("normalized").contains(","),
         concat_ws(" ",
             trim(element_at(split(col("normalized"), ","), 2)),  # First name.
             trim(element_at(split(col("normalized"), ","), 1)),  # Last name.
         )
    ).otherwise(col("normalized"))  # Keep as-is.
)

# Step 3: Parse into components.
parsed = cleaned.withColumn(
    "parts", split(col("normalized"), " ")  # Split into array.
).withColumn(
    "num_parts", size(col("parts"))  # Count parts.
).withColumn(
    "first_name",
    when(col("num_parts") >= 2, element_at(col("parts"), 1))
    .otherwise(col("normalized"))  # Single name.
).withColumn(
    "last_name",
    when(col("num_parts") >= 2, element_at(col("parts"), col("num_parts")))
    .otherwise(None)  # No last name.
).withColumn(
    "middle",
    when(col("num_parts") >= 3,
         concat_ws(" ", *[element_at(col("parts"), lit(i)) for i in range(2, 5)])  # Middle parts.
    ).otherwise(None)
)

parsed.select("full_name", "first_name", "last_name", "num_parts").show(truncate=False)

# Text tokenization.
print("=== Text Tokenization ===")  # Heading.
texts = spark.createDataFrame([
    ("The quick brown fox jumps over the lazy dog",),
    ("PySpark is great for big data processing!",),
], ["text"])  # Texts.

texts.withColumn(
    "tokens", split(lower(regexp_replace(col("text"), "[^a-zA-Z\\s]", "")), "\\s+")
).withColumn(
    "token_count", size(col("tokens"))  # Count tokens.
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Email and ID cleaning
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Email and ID Cleaning
# ============================================================
# Real-world: Standardize emails, IDs, and codes.

from pyspark.sql.functions import (
    col, lower, trim, regexp_replace, regexp_extract, when,
    split, element_at, concat, lit, length, lpad
)  # Imports.

# Messy emails.
emails = spark.createDataFrame([
    (" Alice.Smith@Company.COM ",),   # Case + spaces.
    ("bob+spam@gmail.com",),          # Plus addressing.
    ("CHARLIE@YAHOO.CO.UK",),         # All caps.
    ("not-an-email",),                 # Invalid.
    ("diana @company.com",),           # Space in middle.
    ("eve@company..com",),             # Double dot.
], ["raw_email"])  # Emails.

print("=== Email Standardization ===")  # Heading.
email_pattern = r"^[\w.+-]+@[\w-]+\.[\w.-]+$"  # Valid email pattern.

std_emails = emails.withColumn(
    "cleaned",
    lower(trim(regexp_replace(col("raw_email"), "\\s", "")))  # Lower + remove spaces.
).withColumn(
    "is_valid",
    col("cleaned").rlike(email_pattern)  # Validate.
).withColumn(
    "local_part",
    when(col("is_valid"), element_at(split(col("cleaned"), "@"), 1))  # Before @.
).withColumn(
    "domain",
    when(col("is_valid"), element_at(split(col("cleaned"), "@"), 2))  # After @.
)
std_emails.show(truncate=False)  # Display.

# ID/Code standardization.
print("=== ID/Code Standardization ===")  # Heading.
ids = spark.createDataFrame([
    ("ABC-123",),
    ("abc123",),
    ("  ABC 123  ",),
    ("abc-00123",),
    ("ABC_123",),
    ("12345",),      # Numeric only.
], ["raw_id"])  # Raw IDs.

# Standardize: uppercase, no separators, zero-padded.
std_ids = ids.withColumn(
    "clean_id",
    upper(regexp_replace(trim(col("raw_id")), "[^A-Za-z0-9]", ""))  # Remove separators.
).withColumn(
    "prefix",
    regexp_extract(col("clean_id"), "^([A-Z]+)", 1)  # Alpha prefix.
).withColumn(
    "number",
    regexp_extract(col("clean_id"), "(\\d+)$", 1)  # Numeric suffix.
).withColumn(
    "formatted_id",
    when(col("prefix") != "",
         concat(col("prefix"), lit("-"), lpad(col("number"), 5, "0"))  # ABC-00123.
    ).otherwise(lpad(col("number"), 8, "0"))  # 00012345 for numeric.
)
std_ids.show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Production string cleaning pipeline
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Production String Cleaning Pipeline
# ============================================================
# Real-world: Reusable, configurable string cleaning framework.

from pyspark.sql.functions import (
    col, trim, upper, lower, initcap, regexp_replace,
    translate, when, length, lit
)  # Imports.
from pyspark.sql import DataFrame  # Type.

class StringCleaner:
    """Chainable string cleaning pipeline."""
    
    def __init__(self, df, column):
        """Initialize cleaner for a specific column."""
        self.df = df  # DataFrame.
        self.col = column  # Target column.
        self.steps = []  # Track steps.
    
    def strip(self):
        """Remove leading/trailing whitespace."""
        self.df = self.df.withColumn(self.col, trim(col(self.col)))  # Trim.
        self.steps.append("strip")  # Track.
        return self  # Chain.
    
    def collapse_spaces(self):
        """Replace multiple spaces with single space."""
        self.df = self.df.withColumn(
            self.col, regexp_replace(col(self.col), "\\s+", " ")  # Collapse.
        )
        self.steps.append("collapse_spaces")  # Track.
        return self  # Chain.
    
    def remove_control_chars(self):
        """Remove tabs, newlines, carriage returns."""
        self.df = self.df.withColumn(
            self.col, regexp_replace(col(self.col), "[\\t\\n\\r\\x00-\\x1f]", "")  # Control chars.
        )
        self.steps.append("remove_control")  # Track.
        return self  # Chain.
    
    def normalize_case(self, case="initcap"):
        """Standardize case."""
        if case == "upper":  # Upper.
            self.df = self.df.withColumn(self.col, upper(col(self.col)))
        elif case == "lower":  # Lower.
            self.df = self.df.withColumn(self.col, lower(col(self.col)))
        else:  # Initcap.
            self.df = self.df.withColumn(self.col, initcap(col(self.col)))
        self.steps.append(f"case:{case}")  # Track.
        return self  # Chain.
    
    def remove_special(self, keep_pattern="a-zA-Z0-9\\s"):
        """Remove characters NOT matching pattern."""
        self.df = self.df.withColumn(
            self.col, regexp_replace(col(self.col), f"[^{keep_pattern}]", "")  # Remove.
        )
        self.steps.append("remove_special")  # Track.
        return self  # Chain.
    
    def null_if_empty(self):
        """Convert empty/whitespace-only strings to NULL."""
        self.df = self.df.withColumn(
            self.col,
            when(trim(col(self.col)) == "", None).otherwise(col(self.col))  # Empty->NULL.
        )
        self.steps.append("null_if_empty")  # Track.
        return self  # Chain.
    
    def normalize_unicode(self):
        """Replace common Unicode with ASCII."""
        self.df = self.df.withColumn(
            self.col,
            translate(col(self.col),
                "\u00e9\u00e8\u00ea\u00eb\u00f6\u00fc\u00e4\u00e1\u00e0\u00f1\u00c9\u00d6\u00dc",
                "eeeeouaaanEOU")
        )
        self.steps.append("normalize_unicode")  # Track.
        return self  # Chain.
    
    def result(self):
        """Return cleaned DataFrame."""
        print(f"  Column '{self.col}' cleaned: {' -> '.join(self.steps)}")  # Report.
        return self.df  # Return.

# Apply pipeline.
print("=== Production String Cleaner ===")  # Heading.
messy = spark.createDataFrame([
    ("  \tALICE   Smith  \n",),
    ("  bob   JONES  ",),
    ("   ",),                  # Empty.
    ("caf\u00e9 \u00e9l\u00e8gant",),
    ("Charlie\u00a0Brown",),  # Non-breaking space.
], ["name"])  # Messy.

clean = (
    StringCleaner(messy, "name")
    .remove_control_chars()
    .strip()
    .collapse_spaces()
    .normalize_unicode()
    .normalize_case("initcap")
    .null_if_empty()
    .result()
)
clean.show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Batch column cleaning
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Batch Column Cleaning
# ============================================================
# Real-world: Apply cleaning rules to all string columns at once.

from pyspark.sql.functions import (
    col, trim, regexp_replace, upper, when, length
)  # Imports.
from pyspark.sql import DataFrame  # Type.

def clean_all_strings(df, operations=None):
    """
    Apply cleaning operations to ALL string columns.
    Operations: list of (operation_name, params) tuples.
    """
    if operations is None:  # Defaults.
        operations = [
            ("strip", {}),
            ("collapse_spaces", {}),
            ("null_if_empty", {}),
        ]
    
    # Identify string columns.
    string_cols = [c for c, t in df.dtypes if t == "string"]  # String cols.
    print(f"Cleaning {len(string_cols)} string columns: {string_cols}")  # Report.
    
    result = df  # Start.
    for col_name in string_cols:  # Each string column.
        for op, params in operations:  # Each operation.
            if op == "strip":  # Trim.
                result = result.withColumn(col_name, trim(col(col_name)))
            elif op == "collapse_spaces":  # Collapse.
                result = result.withColumn(col_name, regexp_replace(col(col_name), "\\s+", " "))
            elif op == "null_if_empty":  # Empty -> NULL.
                result = result.withColumn(
                    col_name, when(trim(col(col_name)) == "", None).otherwise(col(col_name))
                )
            elif op == "upper":  # Uppercase.
                result = result.withColumn(col_name, upper(col(col_name)))
            elif op == "remove_special":  # Remove specials.
                keep = params.get("keep", "a-zA-Z0-9\\s.-")  # Default keep.
                result = result.withColumn(
                    col_name, regexp_replace(col(col_name), f"[^{keep}]", "")
                )
    
    return result  # Return cleaned.

# Test on a multi-column DataFrame.
print("=== Batch String Cleaning ===")  # Heading.
multi = spark.createDataFrame([
    (1, "  Alice  ", "  alice@co.com  ", "  NYC  ", 100.0),
    (2, "  Bob  ", "", "  Chicago  ", 200.0),
    (3, "  Charlie  ", "  charlie@co.com  ", "   ", 300.0),
], ["id", "name", "email", "city", "amount"])  # Multi-type.

print("Before:")
multi.show(truncate=False)  # Show before.

clean = clean_all_strings(multi, [
    ("strip", {}),             # Trim all.
    ("collapse_spaces", {}),   # Collapse.
    ("null_if_empty", {}),     # Empty -> NULL.
])

print("\nAfter:")
clean.show(truncate=False)  # Show after.

# Verify: non-string columns untouched.
print(f"\nAmount unchanged: {clean.select('amount').collect() == multi.select('amount').collect()}")  # Check.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Fuzzy string standardization
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Fuzzy String Standardization
# ============================================================
# Real-world: Standardize free-text values to canonical forms.

from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, levenshtein, lit,
    soundex, length, least
)  # Imports.

# Free-text "country" field with variations.
countries = spark.createDataFrame([
    ("USA",), ("U.S.A.",), ("United States",), ("US",), ("america",),
    ("United Kingdom",), ("UK",), ("U.K.",), ("Great Britain",), ("England",),
    ("Deutschland",), ("Germany",), ("DE",),
    ("France",), ("FR",), ("FRANCE",),
], ["raw_country"])  # Raw countries.

print("=== Country Standardization ===")  # Heading.

# Rule-based mapping for known variants.
std_countries = countries.withColumn(
    "clean", upper(trim(regexp_replace(col("raw_country"), "[.]", "")))  # Normalize.
).withColumn(
    "standard_country",
    when(col("clean").isin("USA", "US", "UNITED STATES", "AMERICA", "UNITED STATES OF AMERICA"), "United States")
    .when(col("clean").isin("UK", "UNITED KINGDOM", "GREAT BRITAIN", "ENGLAND", "GB"), "United Kingdom")
    .when(col("clean").isin("GERMANY", "DEUTSCHLAND", "DE"), "Germany")
    .when(col("clean").isin("FRANCE", "FR"), "France")
    .otherwise(col("raw_country"))  # Unmatched: keep original.
)

std_countries.show(truncate=False)  # Display.

# Fuzzy matching using Levenshtein distance.
print("=== Fuzzy Matching with Levenshtein ===")  # Heading.
fuzzy = spark.createDataFrame([
    ("Unted States",),    # Typo.
    ("Unitd Kingdm",),    # Typo.
    ("Grmany",),          # Typo.
    ("Frnace",),          # Typo.
    ("Australa",),        # Typo.
], ["misspelled"])  # Misspelled.

# Reference values.
reference = ["United States", "United Kingdom", "Germany", "France", "Australia", "Canada"]

# Calculate Levenshtein distance to each reference.
result = fuzzy  # Start.
for ref in reference:  # Each reference.
    result = result.withColumn(
        f"dist_{ref.replace(' ', '_')}",
        levenshtein(upper(col("misspelled")), lit(ref.upper()))  # Distance.
    )

# Find closest match.
from pyspark.sql.functions import array, sort_array, struct  # Imports.
dist_cols = [c for c in result.columns if c.startswith("dist_")]  # Distance cols.

result.show(truncate=False)  # Display distances.

# Soundex matching.
print("=== Soundex Matching ===")  # Heading.
fuzzy.withColumn(
    "soundex_code", soundex(col("misspelled"))  # Phonetic code.
).show(truncate=False)  # Display.

print("✅ String cleaning at scale mastery complete!")  # Done.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with String Cleaning
# MAGIC
# MAGIC ### Mistake 1: Not trimming before comparisons
# MAGIC ```python
# MAGIC # "Alice" != "Alice " (trailing space!)
# MAGIC # ALWAYS trim before joins, filters, or groupBy:
# MAGIC df.withColumn("name", trim(col("name")))
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Case-sensitive matching
# MAGIC ```python
# MAGIC # "alice" != "Alice" != "ALICE"
# MAGIC # Normalize case before comparisons:
# MAGIC df.filter(upper(col("city")) == "NEW YORK")  # Case-insensitive.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Regex without escaping
# MAGIC ```python
# MAGIC # WRONG: . matches ANY character in regex!
# MAGIC regexp_replace(col("x"), "Mr.", "Mr")  # Matches "Mrx" too!
# MAGIC
# MAGIC # CORRECT: escape the dot.
# MAGIC regexp_replace(col("x"), "Mr\\.", "Mr")  # Only literal dot.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Destroying valid special characters
# MAGIC ```python
# MAGIC # Don't blindly remove all special chars!
# MAGIC # "O'Brien" -> "OBrien" loses meaning.
# MAGIC # Keep apostrophes and hyphens in names:
# MAGIC regexp_replace(col("name"), "[^a-zA-Z\\s'-]", "")  # Keep ' and -.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Unicode invisible characters
# MAGIC ```python
# MAGIC # Zero-width spaces, non-breaking spaces look identical to nothing/space!
# MAGIC # But they break equality checks.
# MAGIC # Always remove invisible Unicode before matching:
# MAGIC regexp_replace(col("x"), "[\\u200b\\u200c\\u200d\\ufeff\\u00a0]", "")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of String Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Trim, collapse spaces, and normalize case on a name column.
# MAGIC 2. Extract emails from text using regex.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Add phone number extraction alongside email.
# MAGIC 4. Change initcap to upper for a different business rule.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Build address standardizer with abbreviation expansion.
# MAGIC 6. Chain: trim + control chars + collapse + initcap.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Parse "Last, First Middle" names into components.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build `StringCleaner` class with configurable pipeline.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design country/state normalizer with lookup table.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Benchmark: regexp_replace vs translate vs UDF on 1M rows.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Handle: Unicode accents, zero-width spaces, RTL text, emojis.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build batch string cleaner that handles all string columns.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create guide: "Regex patterns every data engineer needs."

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all.

# --- Level 1: Basic trim + extract ---
print("=== Level 1: Trim + Extract ===")  # Heading.
test = spark.createDataFrame([("  Hello  World  ",), ("  test@email.com here  ",)], ["val"])
test.withColumn("cleaned", initcap(regexp_replace(trim(col("val")), "\\s+", " "))).show(truncate=False)
test.withColumn("email", regexp_extract(col("val"), r"([\w.+-]+@[\w-]+\.[\w.-]+)", 1)).show(truncate=False)

# --- Level 5: Configurable pipeline ---
print("\n=== Level 5: Config Pipeline ===")  # Heading.
def apply_string_pipeline(df, col_name, steps):
    """Apply ordered cleaning steps."""
    result = df
    for step in steps:
        if step == "trim":
            result = result.withColumn(col_name, trim(col(col_name)))
        elif step == "upper":
            result = result.withColumn(col_name, upper(col(col_name)))
        elif step == "collapse":
            result = result.withColumn(col_name, regexp_replace(col(col_name), "\\s+", " "))
        elif step == "alpha_only":
            result = result.withColumn(col_name, regexp_replace(col(col_name), "[^a-zA-Z\\s]", ""))
    return result

test2 = spark.createDataFrame([("  Hello!!  World  123  ",)], ["val"])
apply_string_pipeline(test2, "val", ["trim", "collapse", "alpha_only", "upper"]).show(truncate=False)

# --- Level 8: Emoji handling ---
print("\n=== Level 8: Emoji Handling ===")  # Heading.
emoji_data = spark.createDataFrame([
    ("Hello \ud83d\udc4b World \ud83c\udf0d!",), ("Data \ud83d\udcca Science \ud83e\uddea",)
], ["text"])

emoji_data.withColumn(
    "no_emoji", regexp_replace(col("text"), "[^\\x00-\\x7F]", "")  # ASCII only.
).withColumn(
    "no_emoji_clean", regexp_replace(trim(regexp_replace(col("text"), "[^\\x00-\\x7F]", "")), "\\s+", " ")
).show(truncate=False)

print("✅ All string cleaning solutions complete!")  # Done.