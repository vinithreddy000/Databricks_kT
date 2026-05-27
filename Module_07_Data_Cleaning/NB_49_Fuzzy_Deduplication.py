# Databricks notebook source
# DBTITLE 1,NB_49 Header
# MAGIC %md
# MAGIC # NB_49 — Deduplication: Exact and Fuzzy Matching
# MAGIC
# MAGIC **Module 7: Data Cleaning & Quality** | Notebook 49 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * Levenshtein distance for string similarity
# MAGIC * Soundex and phonetic matching
# MAGIC * Jaro-Winkler similarity
# MAGIC * N-gram and token-based matching
# MAGIC * Blocking strategies for performance
# MAGIC * Record linkage workflow
# MAGIC * Composite similarity scoring
# MAGIC * Production fuzzy dedup pipeline
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐⭐ (Advanced data matching)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — Why Fuzzy Matching?
# MAGIC %md
# MAGIC ## SECTION 1 — Why Fuzzy Matching? (Real-World Analogy)
# MAGIC
# MAGIC ### 📦 The Detective Board
# MAGIC
# MAGIC Fuzzy dedup is like connecting suspects — same person, different disguises:
# MAGIC
# MAGIC | Record A | Record B | Same Person? | Method |
# MAGIC |---|---|---|---|
# MAGIC | "John Smith" | "Jon Smith" | Yes (typo) | Levenshtein |
# MAGIC | "Catherine" | "Katherine" | Yes (variant) | Soundex/Phonetic |
# MAGIC | "123 Main St" | "123 Main Street" | Yes (abbreviation) | Token matching |
# MAGIC | "Robert" | "Bob" | Yes (nickname) | Lookup table |
# MAGIC | "alice@old.com" | "alice@new.com" | Maybe | Composite score |
# MAGIC
# MAGIC ### Exact vs Fuzzy Matching
# MAGIC ```
# MAGIC Exact:  "John Smith" == "John Smith"  → TRUE
# MAGIC         "John Smith" == "Jon Smith"   → FALSE (misses real match!)
# MAGIC
# MAGIC Fuzzy:  similarity("John Smith", "Jon Smith") = 0.9  → MATCH!
# MAGIC         similarity("John Smith", "Jane Doe")   = 0.2  → NO MATCH
# MAGIC ```
# MAGIC
# MAGIC ### The Performance Challenge
# MAGIC * Comparing every pair = O(n²) — 1M rows = 500 BILLION comparisons!
# MAGIC * Solution: **Blocking** — only compare records that share a "block key"
# MAGIC * Block keys: first letter, zip code, soundex, etc.
# MAGIC * Reduces O(n²) to O(n·b) where b = average block size

# COMMAND ----------

# DBTITLE 1,SECTION 2 — Similarity Methods Reference
# MAGIC %md
# MAGIC ## SECTION 2 — Similarity Methods in PySpark
# MAGIC
# MAGIC ### Built-in Functions
# MAGIC ```python
# MAGIC levenshtein(col1, col2)          # Edit distance (int)
# MAGIC soundex(col)                     # Phonetic code (4 chars)
# MAGIC ```
# MAGIC
# MAGIC ### Levenshtein Distance
# MAGIC * Counts minimum edits (insert/delete/replace) to transform one string into another
# MAGIC * "kitten" → "sitting" = 3 edits
# MAGIC * Lower distance = more similar
# MAGIC * Normalize: 1 - (distance / max(len1, len2))
# MAGIC
# MAGIC ### Soundex
# MAGIC * Encodes by sound: "Smith" and "Smyth" both = S530
# MAGIC * Good for names with phonetic variants
# MAGIC * Limited: only first 4 chars of code
# MAGIC
# MAGIC ### Jaro-Winkler (via UDF or library)
# MAGIC * Score 0-1 (1 = identical)
# MAGIC * Emphasizes matching prefix (good for names)
# MAGIC * "Martha" vs "Marhta" = 0.961
# MAGIC
# MAGIC ### Token-Based (Jaccard)
# MAGIC * Split into tokens, measure overlap
# MAGIC * {"john", "smith"} vs {"smith", "john"} = 1.0 (order doesn't matter)
# MAGIC * Good for addresses, multi-word fields
# MAGIC
# MAGIC ### Composite Scoring
# MAGIC ```python
# MAGIC score = (0.4 * name_sim + 0.3 * address_sim + 0.2 * phone_sim + 0.1 * email_sim)
# MAGIC if score > 0.8: MATCH
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Levenshtein distance
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: Levenshtein Distance
# ============================================================
# Real-world: Find near-duplicate names using edit distance.

from pyspark.sql import SparkSession  # Import.
from pyspark.sql.functions import (  # Import functions.
    col, levenshtein, length, greatest, round as spark_round,
    lower, trim, regexp_replace, lit
)  # End imports.

spark = SparkSession.builder.getOrCreate()  # Session.

# Customer records with potential duplicates.
customers = spark.createDataFrame([
    (1, "John Smith", "john.smith@email.com", "NYC"),
    (2, "Jon Smith", "jon.smith@email.com", "NYC"),       # Typo in first name.
    (3, "John Smyth", "johnsmyth@email.com", "NYC"),     # Typo in last name.
    (4, "Jane Doe", "jane.doe@email.com", "Boston"),
    (5, "Alice Johnson", "alice.j@email.com", "Chicago"),
    (6, "Alise Johnson", "alise.j@email.com", "Chicago"),  # Typo.
    (7, "Bob Williams", "bob.w@email.com", "Seattle"),
    (8, "Robert Williams", "bob.w@email.com", "Seattle"),  # Nickname vs full.
], ["id", "name", "email", "city"])  # Customers.

print("=== Customer Records ===")  # Heading.
customers.show(truncate=False)  # Display.

# Pairwise Levenshtein comparison (self-join for small datasets).
print("=== Levenshtein Distance (All Pairs) ===")  # Heading.
left = customers.alias("a")  # Left.
right = customers.alias("b")  # Right.

# Cross-join (only compare a.id < b.id to avoid duplicates).
pairs = left.crossJoin(right).filter(
    col("a.id") < col("b.id")  # Only upper triangle.
)

# Compute similarity.
similar = pairs.withColumn(
    "name_distance",
    levenshtein(lower(col("a.name")), lower(col("b.name")))  # Edit distance.
).withColumn(
    "max_len",
    greatest(length(col("a.name")), length(col("b.name")))  # Max length.
).withColumn(
    "name_similarity",
    spark_round(1 - col("name_distance") / col("max_len"), 3)  # Normalized.
).filter(
    col("name_similarity") >= 0.7  # Only high similarity.
).select(
    col("a.id").alias("id_a"),
    col("a.name").alias("name_a"),
    col("b.id").alias("id_b"),
    col("b.name").alias("name_b"),
    col("name_distance"),
    col("name_similarity"),
).orderBy(col("name_similarity").desc())  # Best matches first.

similar.show(truncate=False)  # Display.
print("Pairs with similarity >= 0.7 are potential duplicates.")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Soundex phonetic matching
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Soundex Phonetic Matching
# ============================================================
# Real-world: Match names that SOUND alike but are spelled differently.

from pyspark.sql.functions import (  # Import functions.
    col, soundex, upper, trim, when, lit
)  # End imports.

# Names with phonetic variants.
names = spark.createDataFrame([
    (1, "Smith"),
    (2, "Smyth"),        # Sounds like Smith.
    (3, "Schmidt"),      # Different sound.
    (4, "Catherine"),
    (5, "Katherine"),    # Sounds alike.
    (6, "Kathryn"),      # Sounds alike.
    (7, "Johnson"),
    (8, "Jonson"),       # Sounds alike.
    (9, "Johnston"),     # Close.
    (10, "Williams"),
    (11, "Williamson"),  # Close but different.
], ["id", "name"])  # Names.

print("=== Soundex Codes ===")  # Heading.
with_soundex = names.withColumn(
    "soundex_code", soundex(col("name"))  # Generate soundex.
)
with_soundex.show(truncate=False)  # Display.

# Group by soundex to find phonetic duplicates.
print("=== Phonetic Duplicate Groups ===")  # Heading.
from pyspark.sql.functions import collect_list, count, size  # Imports.

phonetic_groups = with_soundex.groupBy("soundex_code").agg(
    collect_list("name").alias("matching_names"),  # Collect.
    count("*").alias("group_size"),  # Count.
).filter(col("group_size") > 1)  # Only groups with >1 member.

phonetic_groups.show(truncate=False)  # Display.

# Self-join on soundex for candidate pairs.
print("=== Soundex-Based Candidate Pairs ===")  # Heading.
left = with_soundex.alias("a")  # Left.
right = with_soundex.alias("b")  # Right.

candidates = left.join(
    right,
    (col("a.soundex_code") == col("b.soundex_code")) & (col("a.id") < col("b.id")),  # Same sound.
    "inner"  # Inner join.
).select(
    col("a.name").alias("name_a"),
    col("b.name").alias("name_b"),
    col("a.soundex_code").alias("soundex"),
)
candidates.show(truncate=False)  # Display.
print("Soundex groups names by sound — use as BLOCKING strategy for large datasets.")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Token-based similarity
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Token-Based Similarity
# ============================================================
# Real-world: Compare multi-word strings regardless of word order.

from pyspark.sql.functions import (  # Import functions.
    col, split, lower, trim, regexp_replace, array_intersect,
    array_union, size, round as spark_round, lit
)  # End imports.

# Address comparison (order/abbreviation varies).
addresses = spark.createDataFrame([
    (1, "123 Main Street Apt 4", 2, "123 Main St Apartment 4"),
    (3, "456 Oak Avenue Suite 100", 4, "456 Oak Ave Ste 100"),
    (5, "789 Elm Drive", 6, "789 Elm Dr North"),
    (7, "100 Broadway New York", 8, "100 Broadway NY"),
    (9, "555 Pine Road", 10, "999 Cedar Lane"),  # Not a match.
], ["id_a", "address_a", "id_b", "address_b"])  # Addresses.

print("=== Token-Based (Jaccard) Similarity ===")  # Heading.

# Tokenize: split into word arrays.
tokenized = addresses.withColumn(
    "tokens_a",
    split(lower(trim(regexp_replace(col("address_a"), "[^a-zA-Z0-9\\s]", ""))), "\\s+")
).withColumn(
    "tokens_b",
    split(lower(trim(regexp_replace(col("address_b"), "[^a-zA-Z0-9\\s]", ""))), "\\s+")
)

# Jaccard similarity: |intersection| / |union|.
jaccard = tokenized.withColumn(
    "intersection",
    size(array_intersect(col("tokens_a"), col("tokens_b")))  # Shared tokens.
).withColumn(
    "union",
    size(array_union(col("tokens_a"), col("tokens_b")))  # All unique tokens.
).withColumn(
    "jaccard_similarity",
    spark_round(col("intersection") / col("union"), 3)  # Jaccard score.
)

jaccard.select(
    "address_a", "address_b", "intersection", "union", "jaccard_similarity"
).show(truncate=False)  # Display.

# Overlap coefficient: |intersection| / min(|A|, |B|).
print("=== Overlap Coefficient ===")  # Heading.
from pyspark.sql.functions import least as spark_least  # Import.

overlap = tokenized.withColumn(
    "intersect_count", size(array_intersect(col("tokens_a"), col("tokens_b")))
).withColumn(
    "min_size", spark_least(size(col("tokens_a")), size(col("tokens_b")))
).withColumn(
    "overlap_coeff",
    spark_round(col("intersect_count") / col("min_size"), 3)  # Overlap.
)

overlap.select("address_a", "address_b", "overlap_coeff").show(truncate=False)
print("Jaccard penalizes length differences; Overlap doesn't.")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Blocking strategies
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Blocking Strategies
# ============================================================
# Real-world: Reduce comparison space from O(n²) to manageable.

from pyspark.sql.functions import (  # Import functions.
    col, soundex, substring, lower, trim, upper,
    levenshtein, greatest, length, round as spark_round
)  # End imports.

# Larger dataset (simulated).
records = spark.createDataFrame([
    (1, "John Smith", "NYC", "10001"),
    (2, "Jon Smith", "NYC", "10001"),       # Near-duplicate.
    (3, "John Smyth", "NYC", "10002"),      # Near-duplicate.
    (4, "Jane Doe", "Boston", "02101"),
    (5, "Alice Johnson", "Chicago", "60601"),
    (6, "Alise Johnson", "Chicago", "60601"),  # Near-duplicate.
    (7, "Bob Williams", "Seattle", "98101"),
    (8, "Robert Williams", "Seattle", "98101"),  # Same person?
    (9, "Tom Brown", "NYC", "10001"),
    (10, "Thomas Brown", "NYC", "10001"),    # Same person?
], ["id", "name", "city", "zip"])  # Records.

print("=== Blocking Strategy Comparison ===")  # Heading.
print(f"Without blocking: {records.count() * (records.count()-1) // 2} pairs to compare")

# Block 1: Same city.
print("\n--- Block by City ---")
blocked_city = records.alias("a").join(
    records.alias("b"),
    (col("a.city") == col("b.city")) & (col("a.id") < col("b.id")),  # Same city.
    "inner"
)
print(f"Pairs to compare: {blocked_city.count()}")

# Block 2: Same zip code.
print("\n--- Block by Zip Code ---")
blocked_zip = records.alias("a").join(
    records.alias("b"),
    (col("a.zip") == col("b.zip")) & (col("a.id") < col("b.id")),  # Same zip.
    "inner"
)
print(f"Pairs to compare: {blocked_zip.count()}")

# Block 3: Same first letter of name + same city (compound block).
print("\n--- Block by First Letter + City ---")
with_block = records.withColumn("block_key",
    concat(substring(upper(col("name")), 1, 1), lit("_"), col("city"))  # Compound key.
)

blocked_compound = with_block.alias("a").join(
    with_block.alias("b"),
    (col("a.block_key") == col("b.block_key")) & (col("a.id") < col("b.id")),
    "inner"
)
print(f"Pairs to compare: {blocked_compound.count()}")

# Block 4: Soundex blocking (best for names).
print("\n--- Block by Soundex ---")
from pyspark.sql.functions import concat  # Import.
with_soundex = records.withColumn("name_soundex", soundex(col("name")))  # Soundex.

blocked_soundex = with_soundex.alias("a").join(
    with_soundex.alias("b"),
    (col("a.name_soundex") == col("b.name_soundex")) & (col("a.id") < col("b.id")),
    "inner"
)
print(f"Pairs to compare: {blocked_soundex.count()}")

# Compare within soundex blocks.
matches = blocked_soundex.withColumn(
    "similarity",
    spark_round(1 - levenshtein(lower(col("a.name")), lower(col("b.name"))) /
    greatest(length(col("a.name")), length(col("b.name"))), 3)
).select("a.name", "b.name", "similarity").filter(col("similarity") >= 0.6)

print("\n=== Matches Found (soundex block + Levenshtein) ===")
matches.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Composite similarity scoring
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: Composite Similarity Scoring
# ============================================================
# Real-world: Combine multiple fields for stronger matching.

from pyspark.sql.functions import (  # Import functions.
    col, levenshtein, greatest, length, lower, trim,
    when, lit, round as spark_round, soundex
)  # End imports.

# Records with multiple fields to compare.
records = spark.createDataFrame([
    (1, "John Smith", "john.smith@gmail.com", "555-1234", "123 Main St"),
    (2, "Jon Smith", "jon.smith@gmail.com", "555-1234", "123 Main Street"),   # Same person.
    (3, "John Smith", "jsmith@yahoo.com", "555-9999", "456 Oak Ave"),         # Different person!
    (4, "Alice Johnson", "alice.j@email.com", "555-5678", "789 Elm Dr"),
    (5, "Alise Jonson", "alice.j@email.com", "555-5678", "789 Elm Drive"),    # Same person.
], ["id", "name", "email", "phone", "address"])  # Records.

print("=== Multi-Field Similarity ===")  # Heading.
records.show(truncate=False)  # Display.

# Compare all pairs in same soundex block.
with_block = records.withColumn("block", soundex(col("name")))  # Block.

pairs = with_block.alias("a").join(
    with_block.alias("b"),
    (col("a.block") == col("b.block")) & (col("a.id") < col("b.id")),
    "inner"
)

# Compute field-level similarities.
def normalized_lev(c1, c2):
    """Normalized Levenshtein similarity."""
    return 1 - levenshtein(lower(c1), lower(c2)) / greatest(length(c1), length(c2), lit(1))

scored = pairs.withColumn(
    "name_sim", spark_round(normalized_lev(col("a.name"), col("b.name")), 3)  # Name similarity.
).withColumn(
    "email_sim", spark_round(normalized_lev(col("a.email"), col("b.email")), 3)  # Email.
).withColumn(
    "phone_sim", when(col("a.phone") == col("b.phone"), lit(1.0)).otherwise(lit(0.0))  # Exact phone.
).withColumn(
    "address_sim", spark_round(normalized_lev(col("a.address"), col("b.address")), 3)  # Address.
)

# Weighted composite score.
scored = scored.withColumn(
    "composite_score",
    spark_round(
        col("name_sim") * 0.30 +      # Name weight: 30%.
        col("email_sim") * 0.25 +     # Email weight: 25%.
        col("phone_sim") * 0.25 +     # Phone weight: 25%.
        col("address_sim") * 0.20,    # Address weight: 20%.
    3)
).withColumn(
    "match_decision",
    when(col("composite_score") >= 0.85, "MATCH")
    .when(col("composite_score") >= 0.65, "REVIEW")
    .otherwise("NO_MATCH")  # Decision.
)

scored.select(
    col("a.name").alias("name_a"), col("b.name").alias("name_b"),
    "name_sim", "email_sim", "phone_sim", "address_sim",
    "composite_score", "match_decision"
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Record linkage workflow
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Record Linkage Workflow
# ============================================================
# Real-world: Link records across two different data sources.

from pyspark.sql.functions import (  # Import functions.
    col, levenshtein, greatest, length, lower, trim,
    when, lit, round as spark_round, soundex, regexp_replace,
    monotonically_increasing_id
)  # End imports.

# Source 1: CRM system.
crm = spark.createDataFrame([
    ("C001", "Alice M. Johnson", "alice@company.com", "Chicago, IL"),
    ("C002", "Robert Williams", "bob.w@email.com", "Seattle, WA"),
    ("C003", "Diana Prince", "diana.p@email.com", "New York, NY"),
], ["crm_id", "name", "email", "location"])  # CRM.

# Source 2: Billing system.
billing = spark.createDataFrame([
    ("B101", "Alice Johnson", "a.johnson@company.com", "Chicago"),
    ("B102", "Bob Williams", "bob.w@email.com", "Seattle"),
    ("B103", "Frank Castle", "frank@email.com", "Miami"),  # No CRM match.
], ["bill_id", "name", "email", "city"])  # Billing.

print("=== Record Linkage: CRM <-> Billing ===")  # Heading.
print("CRM records:")
crm.show(truncate=False)  # Display.
print("Billing records:")
billing.show(truncate=False)  # Display.

# Step 1: Normalize for comparison.
crm_norm = crm.withColumn("name_clean", lower(trim(regexp_replace(col("name"), "\\s+", " "))))
bill_norm = billing.withColumn("name_clean", lower(trim(regexp_replace(col("name"), "\\s+", " "))))

# Step 2: Cross-join (small datasets) or block-join (large).
linked = crm_norm.alias("c").crossJoin(bill_norm.alias("b"))  # All pairs.

# Step 3: Score each pair.
def norm_lev(c1, c2):
    return 1 - levenshtein(c1, c2) / greatest(length(c1), length(c2), lit(1))

scored = linked.withColumn(
    "name_sim", spark_round(norm_lev(col("c.name_clean"), col("b.name_clean")), 3)
).withColumn(
    "email_sim", spark_round(norm_lev(lower(col("c.email")), lower(col("b.email"))), 3)
).withColumn(
    "overall_score", spark_round(col("name_sim") * 0.5 + col("email_sim") * 0.5, 3)
).withColumn(
    "link_decision",
    when(col("overall_score") >= 0.75, "LINKED")
    .when(col("overall_score") >= 0.55, "POSSIBLE")
    .otherwise("NO_LINK")
)

# Step 4: Best matches.
print("=== Linkage Results ===")  # Heading.
from pyspark.sql.window import Window  # Import.
from pyspark.sql.functions import row_number, desc  # Imports.

w = Window.partitionBy("c.crm_id").orderBy(col("overall_score").desc())  # Best per CRM.
best = scored.withColumn("rank", row_number().over(w)).filter(col("rank") == 1).drop("rank")

best.select(
    col("c.crm_id"), col("c.name").alias("crm_name"),
    col("b.bill_id"), col("b.name").alias("bill_name"),
    "name_sim", "email_sim", "overall_score", "link_decision"
).show(truncate=False)  # Display.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Scalable fuzzy dedup pipeline
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Scalable Fuzzy Dedup Pipeline
# ============================================================
# Real-world: Production pipeline with blocking + scoring + clustering.

from pyspark.sql.functions import (  # Import functions.
    col, lower, trim, regexp_replace, soundex, levenshtein,
    greatest, length, lit, when, round as spark_round,
    row_number, desc, collect_list, first, count, min as spark_min
)  # End imports.
from pyspark.sql.window import Window  # Window.

def fuzzy_dedup_pipeline(df, name_col, blocking_cols=None, threshold=0.80):
    """
    Production fuzzy dedup:
    1. Normalize text
    2. Generate block keys
    3. Compare within blocks
    4. Cluster matches
    5. Pick master record
    """
    # Step 1: Normalize.
    normalized = df.withColumn(
        "_name_norm",
        lower(trim(regexp_replace(col(name_col), "\\s+", " ")))  # Normalize.
    ).withColumn(
        "_block_key", soundex(col(name_col))  # Soundex block.
    )
    
    # Step 2: Self-join within blocks.
    left = normalized.alias("a")  # Left.
    right = normalized.alias("b")  # Right.
    
    # Join condition: same block, different rows.
    join_cond = (col("a._block_key") == col("b._block_key"))  # Same block.
    if blocking_cols:  # Additional blocking.
        for bc in blocking_cols:
            join_cond = join_cond & (col(f"a.{bc}") == col(f"b.{bc}"))
    
    pairs = left.join(right, join_cond & (col("a.id") < col("b.id")), "inner")
    
    # Step 3: Score pairs.
    scored = pairs.withColumn(
        "similarity",
        spark_round(
            1 - levenshtein(col("a._name_norm"), col("b._name_norm")) /
            greatest(length(col("a._name_norm")), length(col("b._name_norm")), lit(1)),
        3)
    ).filter(col("similarity") >= threshold)  # Keep matches.
    
    # Step 4: Build clusters (simple: pick lowest id as master).
    # For each matched pair, assign the lower id as the "master".
    matches = scored.select(
        col("a.id").alias("id_a"),
        col("b.id").alias("id_b"),
        col("a._name_norm").alias("name_a"),
        col("b._name_norm").alias("name_b"),
        col("similarity"),
    )
    
    # Step 5: Report.
    match_count = matches.count()  # Matched pairs.
    total = df.count()  # Total.
    
    print(f"\n{'='*55}")
    print(f"  FUZZY DEDUP REPORT")
    print(f"{'='*55}")
    print(f"  Total records: {total}")
    print(f"  Matching pairs found: {match_count}")
    print(f"  Threshold: {threshold}")
    print(f"  Blocking: soundex + {blocking_cols or 'none'}")
    print(f"{'='*55}\n")
    
    return matches  # Return match pairs.

# Test pipeline.
test_data = spark.createDataFrame([
    (1, "John Smith", "NYC"),
    (2, "Jon Smith", "NYC"),
    (3, "John Smyth", "NYC"),
    (4, "Jane Doe", "Boston"),
    (5, "Alice Johnson", "Chicago"),
    (6, "Alise Johnson", "Chicago"),
    (7, "Bob Williams", "Seattle"),
    (8, "Tom Anderson", "NYC"),
], ["id", "name", "city"])  # Test data.

matches = fuzzy_dedup_pipeline(test_data, "name", blocking_cols=["city"], threshold=0.75)
matches.show(truncate=False)  # Display matches.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Connected components clustering
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: Connected Components for Clustering
# ============================================================
# Real-world: Group all records that belong to the same entity.

from pyspark.sql.functions import (  # Import functions.
    col, least as spark_least, greatest as spark_greatest,
    collect_set, size, explode, min as spark_min,
    first, count
)  # End imports.

# Match pairs (from fuzzy matching).
match_pairs = spark.createDataFrame([
    (1, 2, 0.92),   # John Smith ~ Jon Smith.
    (1, 3, 0.85),   # John Smith ~ John Smyth.
    (5, 6, 0.88),   # Alice Johnson ~ Alise Johnson.
    (10, 11, 0.90), # Tom Anderson ~ Thomas Anderson.
    (10, 12, 0.82), # Tom Anderson ~ T. Anderson.
    (11, 12, 0.87), # Thomas Anderson ~ T. Anderson (transitive!).
], ["id_a", "id_b", "similarity"])  # Match pairs.

print("=== Match Pairs ===")  # Heading.
match_pairs.show()  # Display.

# Simple connected components via iterative union-find.
def find_clusters(pairs_df):
    """
    Simple iterative connected components.
    Assign each record to its lowest-ID cluster member.
    """
    # Initialize: each record maps to itself.
    all_ids = pairs_df.select(col("id_a").alias("id")).union(
        pairs_df.select(col("id_b").alias("id"))  # All IDs.
    ).distinct()
    
    # Start with each id pointing to itself.
    mapping = all_ids.withColumn("cluster_id", col("id"))  # Initial.
    
    # Iterate: propagate minimum cluster_id through edges.
    for iteration in range(5):  # Usually converges in 2-3 iterations.
        # Join edges with current mapping.
        updated = pairs_df.join(
            mapping.alias("m1"), col("id_a") == col("m1.id"), "inner"
        ).select(
            col("id_b").alias("id"),
            col("m1.cluster_id").alias("new_cluster"),
        ).union(
            pairs_df.join(
                mapping.alias("m2"), col("id_b") == col("m2.id"), "inner"
            ).select(
                col("id_a").alias("id"),
                col("m2.cluster_id").alias("new_cluster"),
            )
        )
        
        # Take minimum cluster_id per id.
        new_mapping = updated.groupBy("id").agg(
            spark_min("new_cluster").alias("cluster_id")  # Min.
        )
        
        # Merge with existing (keep minimum).
        mapping = mapping.join(new_mapping, "id", "left").select(
            col("id"),
            spark_least(
                mapping["cluster_id"], new_mapping["cluster_id"]
            ).alias("cluster_id")
        )
    
    return mapping  # Return id -> cluster_id.

# Apply clustering.
print("=== Cluster Assignment ===")  # Heading.
clusters = find_clusters(match_pairs)  # Cluster.
clusters.orderBy("cluster_id", "id").show()  # Display.

# Show cluster groups.
print("=== Cluster Groups ===")  # Heading.
clusters.groupBy("cluster_id").agg(
    collect_set("id").alias("member_ids"),  # Members.
    count("*").alias("size"),  # Size.
).show(truncate=False)  # Display.

print("Records 1,2,3 form one cluster (John/Jon/Smyth).")
print("Records 10,11,12 form one cluster (Tom/Thomas/T. Anderson).")
print("Transitive closure: if A~B and B~C, then A,B,C are same entity.")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Master record selection
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Master Record Selection
# ============================================================
# Real-world: Pick the best record from each cluster as "golden."

from pyspark.sql.functions import (  # Import functions.
    col, when, length, count, row_number, desc, first,
    coalesce, lit, round as spark_round, greatest
)  # End imports.
from pyspark.sql.window import Window  # Window.

# Full records with cluster assignments.
full_records = spark.createDataFrame([
    (1, 1, "John Smith", "john.smith@email.com", "555-1234", "2024-01-15"),
    (2, 1, "Jon Smith", None, "555-1234", "2024-02-01"),              # Missing email.
    (3, 1, "John Smyth", "john@other.com", None, "2024-01-20"),       # Missing phone.
    (5, 5, "Alice Johnson", "alice@email.com", "555-5678", "2024-03-01"),
    (6, 5, "Alise Johnson", "alice.j@email.com", "555-5678", "2024-03-10"),  # More recent.
    (10, 10, "Tom Anderson", None, "555-9999", "2024-01-01"),
    (11, 10, "Thomas Anderson", "tom@email.com", "555-9999", "2024-02-15"),  # Fullest.
    (12, 10, "T. Anderson", "tom@email.com", None, "2024-03-01"),  # Most recent.
], ["id", "cluster_id", "name", "email", "phone", "last_updated"])  # Records.

print("=== Clustered Records ===")  # Heading.
full_records.show(truncate=False)  # Display.

# Strategy 1: Most complete record (fewest NULLs).
print("=== Strategy 1: Most Complete Record ===")  # Heading.

# Count non-null fields as "completeness score".
field_cols = ["name", "email", "phone"]  # Fields to check.
with_score = full_records.withColumn(
    "completeness",
    sum([when(col(c).isNotNull(), 1).otherwise(0) for c in field_cols])  # Count non-nulls.
)

w = Window.partitionBy("cluster_id").orderBy(
    col("completeness").desc(),  # Most complete first.
    col("last_updated").desc(),  # Then most recent.
)

master_complete = with_score.withColumn(
    "rank", row_number().over(w)  # Rank.
).filter(col("rank") == 1).drop("rank", "completeness")  # Keep best.

master_complete.show(truncate=False)  # Display.

# Strategy 2: Golden record (merge best fields from cluster).
print("=== Strategy 2: Golden Record (Merged Best) ===")  # Heading.

# For each cluster: take longest name, first non-null email/phone, latest update.
from pyspark.sql.functions import max as spark_max, collect_list  # Imports.

golden = full_records.groupBy("cluster_id").agg(
    # Longest name (most complete).
    first("name", ignorenulls=True).alias("name"),  # First non-null.
    first("email", ignorenulls=True).alias("email"),  # First non-null email.
    first("phone", ignorenulls=True).alias("phone"),  # First non-null phone.
    spark_max("last_updated").alias("last_updated"),  # Latest date.
    count("*").alias("records_merged"),  # How many merged.
    collect_list("id").alias("source_ids"),  # Track sources.
)

golden.show(truncate=False)  # Display.
print("✅ Fuzzy deduplication mastery complete!")  # Done.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Fuzzy Dedup
# MAGIC
# MAGIC ### Mistake 1: No blocking (O(n²) explosion)
# MAGIC ```python
# MAGIC # WRONG: Cross-join all records.
# MAGIC df.crossJoin(df)  # 1M rows = 500 BILLION comparisons!
# MAGIC
# MAGIC # CORRECT: Block first, compare within blocks.
# MAGIC blocked = df.withColumn("block", soundex("name"))
# MAGIC left.join(right, "block")  # Only compare within blocks.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 2: Single-field matching
# MAGIC ```python
# MAGIC # WRONG: Match on name alone.
# MAGIC # "John Smith" in NYC != "John Smith" in London!
# MAGIC
# MAGIC # CORRECT: Use composite scoring (name + email + phone + location).
# MAGIC score = 0.3*name_sim + 0.3*email_sim + 0.2*phone_sim + 0.2*city_sim
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 3: Not normalizing before comparison
# MAGIC ```python
# MAGIC # "JOHN SMITH" vs "john smith" gives Levenshtein = 10!
# MAGIC # ALWAYS normalize: lower, trim, remove special chars FIRST.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 4: Ignoring transitive relationships
# MAGIC ```python
# MAGIC # If A matches B, and B matches C, then A matches C!
# MAGIC # Use connected components to find full clusters.
# MAGIC # Simple pairwise dedup misses these transitive links.
# MAGIC ```
# MAGIC
# MAGIC ### Mistake 5: Fixed threshold for all fields
# MAGIC ```python
# MAGIC # Name similarity 0.8 = good match.
# MAGIC # Email similarity 0.8 = could be completely different person!
# MAGIC # Phone exact match is much stronger signal.
# MAGIC # Use field-specific thresholds and weights.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Fuzzy Dedup Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC 1. Compute Levenshtein distance between name pairs.
# MAGIC 2. Use Soundex to group phonetically similar names.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Change threshold from 0.8 to 0.7 and observe more matches.
# MAGIC 4. Add city as a blocking column.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC 5. Build: soundex blocking + Levenshtein scoring.
# MAGIC 6. Add token-based (Jaccard) similarity for addresses.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Link records between CRM and billing systems.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build complete fuzzy dedup pipeline with audit.
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC 9. Design composite scoring with field-specific weights.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Compare blocking strategies: first-letter vs soundex vs zip code.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Handle: nicknames (Bob/Robert), prefixes (Dr./Mr.), suffixes (Jr./III).
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC 12. Build: blocking + scoring + clustering + golden record selection.
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC 13. Create guide: "Choosing similarity metrics for different field types."

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all.
from pyspark.sql.window import Window  # Window.

# --- Level 1: Basic Levenshtein ---
print("=== Level 1: Levenshtein Distance ===")  # Heading.
pairs = spark.createDataFrame([
    ("John Smith", "Jon Smith"),
    ("Catherine", "Katherine"),
    ("Robert", "Bob"),
    ("Alice", "Alice"),
], ["name1", "name2"])  # Pairs.

pairs.withColumn("distance", levenshtein(col("name1"), col("name2"))).withColumn(
    "similarity", round(1 - col("distance") / greatest(length("name1"), length("name2")), 3)
).show(truncate=False)  # Display.

# --- Level 5: Pipeline function ---
print("\n=== Level 5: Quick Fuzzy Dedup ===")  # Heading.
def quick_fuzzy_dedup(df, name_col, id_col, threshold=0.80):
    """Quick fuzzy dedup with soundex blocking."""
    blocked = df.withColumn("_sx", soundex(col(name_col)))
    pairs = blocked.alias("a").join(
        blocked.alias("b"),
        (col("a._sx") == col("b._sx")) & (col(f"a.{id_col}") < col(f"b.{id_col}")),
        "inner"
    )
    scored = pairs.withColumn("sim",
        round(1 - levenshtein(lower(col(f"a.{name_col}")), lower(col(f"b.{name_col}"))) /
        greatest(length(col(f"a.{name_col}")), length(col(f"b.{name_col}")), lit(1)), 3)
    ).filter(col("sim") >= threshold)
    return scored.select(col(f"a.{id_col}").alias("id_a"), col(f"b.{id_col}").alias("id_b"), "sim")

test = spark.createDataFrame([
    (1, "Alice Johnson"), (2, "Alise Jonson"), (3, "Bob Smith"), (4, "Bobby Smith")
], ["id", "name"])

quick_fuzzy_dedup(test, "name", "id", 0.7).show()

# --- Level 8: Nickname handling ---
print("\n=== Level 8: Nickname Lookup ===")  # Heading.
nicknames = {"bob": "robert", "rob": "robert", "bobby": "robert",
             "bill": "william", "will": "william", "tom": "thomas"}

from pyspark.sql.functions import create_map  # Import.
# In production, use a broadcast join with nickname table.
print("Strategy: Before matching, normalize common nicknames to canonical form.")
print(f"Example mappings: {dict(list(nicknames.items())[:4])}")

print("\n✅ All fuzzy dedup homework solutions complete!")  # Done.