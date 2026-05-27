# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 87: Text Processing & NLP with PySpark
# MAGIC ## Module 14: Machine Learning
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **NLP (Natural Language Processing)** in PySpark involves converting text data into numerical features that ML models can process. MLlib provides tokenizers, stop-word removers, and vectorizers (TF-IDF, Word2Vec, CountVectorizer) to turn raw text into feature vectors.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine teaching a computer to classify emails as spam or not:
# MAGIC 1. **Tokenize**: Split "Buy cheap watches now!" → ["buy", "cheap", "watches", "now"]
# MAGIC 2. **Remove stop words**: Remove "now" (common, not useful) → ["buy", "cheap", "watches"]
# MAGIC 3. **Vectorize**: Count word frequencies or compute TF-IDF scores → [0.3, 0.8, 0.2, ...]
# MAGIC 4. **Model**: Feed the numeric vector to a classifier.
# MAGIC
# MAGIC ### MLlib Text Transformers:
# MAGIC | Transformer | Purpose |
# MAGIC |------------|--------|
# MAGIC | `Tokenizer` | Split text into words |
# MAGIC | `RegexTokenizer` | Split with custom regex pattern |
# MAGIC | `StopWordsRemover` | Remove common words (the, is, a) |
# MAGIC | `CountVectorizer` | Bag-of-words (word counts) |
# MAGIC | `HashingTF` | Hash-based term frequencies (fixed size) |
# MAGIC | `IDF` | Inverse Document Frequency (weight rare words higher) |
# MAGIC | `Word2Vec` | Word embeddings (semantic meaning) |
# MAGIC | `NGram` | Create N-gram features (word pairs/triples) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Text Processing Pipeline:
# MAGIC
# MAGIC   Raw Text         Tokenize         Remove Stops      Vectorize         Model
# MAGIC   ──────────         ────────         ────────────      ─────────         ─────
# MAGIC   "I love Spark"   ["i","love",     ["love",           [0.0, 0.8,       → predict
# MAGIC                     "spark"]         "spark"]            0.5, ...]         label
# MAGIC
# MAGIC TF-IDF (Term Frequency – Inverse Document Frequency):
# MAGIC   TF(word, doc)  = count(word in doc) / total_words_in_doc
# MAGIC   IDF(word)      = log(total_docs / docs_containing_word)
# MAGIC   TF-IDF         = TF × IDF
# MAGIC
# MAGIC   High TF-IDF = word is frequent in THIS doc but rare across ALL docs.
# MAGIC   (Important, distinctive words score high; common words score low.)
# MAGIC
# MAGIC CountVectorizer vs HashingTF:
# MAGIC   CountVectorizer: Learns vocabulary, creates exact word counts.
# MAGIC     + Interpretable (you know which word is which index).
# MAGIC     - Must fit on data first (vocabulary can be large).
# MAGIC   HashingTF: Hashes words to fixed-size vector (no fitting needed).
# MAGIC     + Fast, fixed memory, no fitting step.
# MAGIC     - Hash collisions (different words may share an index).
# MAGIC     - Not interpretable (can't reverse hash to word).
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: NLP Examples and Homework
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7 — NLP EXAMPLES & HOMEWORK
# ═══════════════════════════════════════════════════════════════════

from pyspark.ml.feature import (
    Tokenizer, RegexTokenizer, StopWordsRemover,
    CountVectorizer, HashingTF, IDF, Word2Vec, NGram
)  # NLP imports.
from pyspark.ml import Pipeline  # Pipeline.
from pyspark.sql.functions import col, size  # Functions.

print("="*70)
print("SECTIONS 3-5: Text Processing with PySpark MLlib")
print("="*70)

# ─── EXAMPLE 1: Tokenizer + StopWordsRemover ───
print("\n" + "-"*60)
print("EXAMPLE 1: Tokenize text and remove stop words")
print("-"*60)

# Sample text data.
texts = spark.createDataFrame([
    (0, "Spark is an amazing distributed computing framework", 1),
    (1, "I love working with big data and machine learning", 1),
    (2, "The weather is nice today in the park", 0),
    (3, "Python and Spark make data engineering easy", 1),
    (4, "I went to the store to buy some food", 0)
], ["id", "text", "label"])

print("\nRaw text data:")
texts.select("id", "text", "label").show(truncate=False)

# Step 1: Tokenize (split into words).
tokenizer = Tokenizer(inputCol="text", outputCol="words")
tokenized = tokenizer.transform(texts)

# Step 2: Remove stop words.
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
filtered = remover.transform(tokenized)

print("After tokenize + remove stop words:")
filtered.select("text", "filtered_words").show(truncate=False)
print("✓ Common words ('is','the','to','an','I') removed. Only meaningful words remain.")

# ─── EXAMPLE 2: TF-IDF Vectorization ───
print("\n" + "-"*60)
print("EXAMPLE 2: TF-IDF (Term Frequency – Inverse Document Frequency)")
print("-"*60)

# CountVectorizer: creates bag-of-words.
cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features", vocabSize=50)
cv_model = cv.fit(filtered)
counted = cv_model.transform(filtered)

# IDF: weight rare words higher.
idf = IDF(inputCol="raw_features", outputCol="features")
idf_model = idf.fit(counted)
tfidf = idf_model.transform(counted)

print("TF-IDF features:")
tfidf.select("id", "filtered_words", "features").show(truncate=50)
print(f"Vocabulary (top 10): {cv_model.vocabulary[:10]}")
print("✓ TF-IDF: common words get low weight, rare important words get high weight.")

# ─── EXAMPLE 3: Complete NLP Pipeline ───
print("\n" + "-"*60)
print("EXAMPLE 3: Complete NLP Pipeline (tokenize → TF-IDF → ready)")
print("-"*60)

nlp_pipeline = Pipeline(stages=[
    Tokenizer(inputCol="text", outputCol="words"),
    StopWordsRemover(inputCol="words", outputCol="clean_words"),
    CountVectorizer(inputCol="clean_words", outputCol="tf", vocabSize=100),
    IDF(inputCol="tf", outputCol="features")
])

nlp_model = nlp_pipeline.fit(texts)  # Fit pipeline.
nlp_features = nlp_model.transform(texts)  # Transform.

print("NLP Pipeline output (ready for classification):")
nlp_features.select("id", "text", "features", "label").show(truncate=50)
print("✓ Text is now a numeric vector. Ready for LogisticRegression, NaiveBayes, etc.")

# ─── EXAMPLE 4: Word2Vec (semantic embeddings) ───
print("\n" + "-"*60)
print("EXAMPLE 4: Word2Vec — capture semantic meaning")
print("-"*60)

# Word2Vec learns dense vector representations.
w2v = Word2Vec(
    vectorSize=10,         # Embedding dimension.
    inputCol="filtered_words",
    outputCol="w2v_features",
    minCount=1             # Min word frequency.
)
w2v_model = w2v.fit(filtered)
w2v_result = w2v_model.transform(filtered)

print("Word2Vec embeddings (10-dimensional):")
w2v_result.select("id", "w2v_features").show(truncate=60)
print("✓ Word2Vec captures semantic similarity (king-queen ≈ man-woman).")
print("  Better than TF-IDF for understanding meaning, but needs more data.")

# ─── SECTION 6: Common Mistakes ───
print("\n" + "="*70)
print("SECTION 6 — COMMON MISTAKES")
print("="*70)
print("""
1. Not lowercasing text before tokenizing (Tokenizer does this, RegexTokenizer may not).
2. Not removing stop words (model learns noise instead of signal).
3. vocabSize too small in CountVectorizer (loses important words).
4. Using HashingTF without enough features (collisions reduce accuracy).
5. Applying Word2Vec on very small datasets (needs 1000s+ of documents).
""")

# ─── SECTION 7: Homework ───
print("="*70)
print("SECTION 7 — HOMEWORK")
print("="*70)

print("""
Level 1: Tokenize a sentence.
  Tokenizer(inputCol="text", outputCol="words").transform(df)

Level 2: Remove stop words.
  StopWordsRemover(inputCol="words", outputCol="filtered").transform(df)

Level 3: CountVectorizer (bag of words).
  cv = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=100)
  cv.fit(df).transform(df)

Level 4: Full TF-IDF pipeline.
  Tokenizer → StopWordsRemover → CountVectorizer → IDF

Level 5: Word2Vec for embeddings.
  Word2Vec(vectorSize=50, inputCol="words", outputCol="embedding")

Level 6: NGram features (word pairs).
  NGram(n=2, inputCol="words", outputCol="bigrams")
  Creates: ["spark", "ml"] → ["spark ml"]

Level 7: When TF-IDF vs Word2Vec?
  TF-IDF: bag-of-words, good for classification, interpretable.
  Word2Vec: dense embeddings, captures meaning, needs more data.

Level 10: Teach NLP in PySpark:
  "Turn text into numbers: Tokenize → Clean → Vectorize → Model.
   TF-IDF for classification (spam, sentiment).
   Word2Vec for semantic similarity.
   Always: Pipeline for reproducibility."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 87")
print("="*70)