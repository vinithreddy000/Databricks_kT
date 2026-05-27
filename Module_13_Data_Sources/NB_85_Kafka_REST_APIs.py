# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 85: Kafka & REST APIs
# MAGIC ## Module 13: Data Sources & Connectors
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 50 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Apache Kafka** is a distributed event streaming platform for real-time data pipelines. **REST APIs** are HTTP endpoints that expose data from web services. Both are crucial data sources for modern architectures.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC **Kafka**: A newspaper printing press with infinite editions. Publishers (producers) send articles to topics. Subscribers (consumers) read at their own pace — even articles from yesterday. The press never throws away old editions (retention).
# MAGIC
# MAGIC **REST APIs**: A library reference desk. You ask (HTTP GET) for specific information, the librarian finds it and gives you a response (JSON). You can ask one question at a time, and each response is independent.
# MAGIC
# MAGIC ### When to Use Each:
# MAGIC | Source | Volume | Latency | Use Case |
# MAGIC |--------|--------|---------|----------|
# MAGIC | Kafka | Very high (M events/sec) | Milliseconds | IoT, clickstream, logs, CDC |
# MAGIC | REST API | Low-medium (1000s req/sec) | Seconds | Reference data, enrichment, external services |
# MAGIC | Event Hubs (Azure) | High | Milliseconds | Same as Kafka (Kafka-compatible) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Kafka Streaming Architecture:
# MAGIC
# MAGIC   [Producers]       [Kafka Cluster]        [Databricks Consumer]
# MAGIC   App, IoT,    →    Topic: "orders"   →    spark.readStream
# MAGIC   CDC, Logs         Partition 0: [....]         .format("kafka")
# MAGIC                     Partition 1: [....]         .load()
# MAGIC                     Partition 2: [....]         
# MAGIC                                                Processes in parallel
# MAGIC                                                (1 Spark task per partition)
# MAGIC
# MAGIC Kafka Message Schema (always the same):
# MAGIC   key:       binary (message key, used for partitioning)
# MAGIC   value:     binary (the actual message payload — your data!)
# MAGIC   topic:     string (which topic it came from)
# MAGIC   partition: int    (which partition)
# MAGIC   offset:    long   (position in partition)
# MAGIC   timestamp: long   (when produced)
# MAGIC
# MAGIC   Your job: CAST value from binary to string, then parse (JSON/Avro/etc).
# MAGIC
# MAGIC REST API Pattern:
# MAGIC
# MAGIC   [Databricks]  ─── HTTP GET/POST ───▶  [REST API]
# MAGIC        │                                     │
# MAGIC   requests.get(url)                      Returns JSON
# MAGIC        │                                     │
# MAGIC   Convert to DataFrame          [{"id":1, "name":"Alice"}, ...]
# MAGIC   spark.createDataFrame(data)
# MAGIC
# MAGIC   For large-scale API calls: use Spark UDFs or foreachPartition.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: Kafka and API Examples
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7 — KAFKA & REST API PATTERNS
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, from_json, to_json, struct, expr  # Imports.
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType  # Types.

print("="*70)
print("SECTIONS 3-7: Kafka & REST API Patterns")
print("="*70)

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 1: Reading from Kafka (streaming)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 1: Streaming read from Kafka")
print("-"*60)

print("""
# Read streaming data from Kafka topic.
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "orders")           # Topic to subscribe to.
    .option("startingOffsets", "earliest")    # Start from beginning.
    .option("maxOffsetsPerTrigger", 10000)    # Rate limit per batch.
    .load()
)

# Kafka always returns: key, value, topic, partition, offset, timestamp.
# The 'value' column is BINARY — you must cast and parse it!

# Step 1: Cast binary value to string.
kafka_parsed = kafka_stream.select(
    col("key").cast("string").alias("msg_key"),
    col("value").cast("string").alias("json_str"),  # Usually JSON.
    col("timestamp").alias("kafka_timestamp")
)

# Step 2: Parse JSON string into structured columns.
order_schema = StructType([
    StructField("order_id", LongType()),
    StructField("customer_id", LongType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType())
])

orders = kafka_parsed.select(
    from_json(col("json_str"), order_schema).alias("data"),
    col("kafka_timestamp")
).select("data.*", "kafka_timestamp")  # Flatten struct.

# Step 3: Write to Delta.
orders.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/kafka_orders")
    .trigger(processingTime="10 seconds")
    .toTable("catalog.bronze.orders_raw")
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 2: Azure Event Hubs (Kafka-compatible)
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 2: Azure Event Hubs (Kafka protocol)")
print("-"*60)

print("""
# Azure Event Hubs supports Kafka protocol natively!
conn_str = dbutils.secrets.get("eventhubs", "connection-string")
eh_sasl = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{conn_str}";'

eh_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "mynamespace.servicebus.windows.net:9093")
    .option("subscribe", "my-event-hub")      # Event Hub name = Kafka topic.
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", eh_sasl)
    .option("startingOffsets", "earliest")
    .load()
)

# Same parsing as regular Kafka:
eh_parsed = eh_stream.select(
    col("value").cast("string").alias("json_body"),
    col("timestamp")
)

Note: Event Hubs on Azure is the most common real-time source.
      Use the Kafka protocol (port 9093) for best Spark compatibility.
""")

# ─────────────────────────────────────────────────────────────────
# EXAMPLE 3: REST API data ingestion
# ─────────────────────────────────────────────────────────────────
print("\n" + "-"*60)
print("EXAMPLE 3: REST API ingestion with requests")
print("-"*60)

import requests  # HTTP library (pre-installed in Databricks).
import json  # JSON parsing.

# Simple API call (small data — fits in driver memory).
print("\nDemo: Fetching data from a public REST API")
try:
    response = requests.get("https://jsonplaceholder.typicode.com/posts", timeout=10)
    if response.status_code == 200:
        posts = response.json()  # List of dicts.
        print(f"  API returned: {len(posts)} records")
        
        # Convert to Spark DataFrame.
        df_api = spark.createDataFrame(posts)  # List of dicts → DataFrame.
        print(f"  DataFrame: {df_api.count()} rows, columns: {df_api.columns}")
        display(df_api.select("id", "title").limit(3))
    else:
        print(f"  API returned status: {response.status_code}")
except Exception as e:
    print(f"  API call failed (expected in restricted networks): {type(e).__name__}")
    # Create sample data instead.
    sample_data = [{"id": i, "title": f"Post {i}", "userId": i % 5} for i in range(10)]
    df_api = spark.createDataFrame(sample_data)
    print(f"  Using sample data: {df_api.count()} rows")

print("""
\nPatterns for REST API at scale:

  # Small API response (< 100K records): Use driver-side requests.
  data = requests.get(url).json()
  df = spark.createDataFrame(data)

  # Large API (paginated): Loop through pages on driver.
  all_data = []
  page = 1
  while True:
      resp = requests.get(f"{url}?page={page}").json()
      if not resp: break
      all_data.extend(resp)
      page += 1
  df = spark.createDataFrame(all_data)

  # Parallel API calls (many endpoints): Use Spark UDF.
  @udf("string")
  def call_api(id):
      import requests
      return requests.get(f"https://api.example.com/item/{id}").text
  
  df_with_api = df.withColumn("api_response", call_api(col("id")))
""")

# ─── SECTION 6 & 7 ───
print("\n" + "="*70)
print("SECTION 6 — COMMON MISTAKES")
print("="*70)
print("""
1. Forgetting to cast Kafka value from binary: col("value").cast("string").
2. Not setting maxOffsetsPerTrigger (Kafka floods Spark with backlog).
3. Calling REST APIs from executor without error handling/retries.
4. Using requests on driver for millions of API calls (use Spark UDF instead).
5. Not using Event Hubs Kafka protocol (using the old native connector instead).
""")

print("="*70)
print("SECTION 7 — HOMEWORK")
print("="*70)
print("""
Level 1: Kafka format name? Answer: "kafka"
Level 2: Kafka value column type? Answer: binary (must cast to string!).
Level 3: How to parse JSON from Kafka? from_json(col("value").cast("string"), schema).
Level 4: Azure Event Hubs protocol? Kafka on port 9093 (SASL_SSL).
Level 5: How to read from REST API? requests.get(url).json() → createDataFrame().
Level 6: How to limit Kafka read rate? .option("maxOffsetsPerTrigger", 10000).
Level 7: Parallel API calls? UDF or foreachPartition with requests.
Level 8: Writing to Kafka?
  df.select(to_json(struct("*")).alias("value"))
    .write.format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("topic", "output-topic").save()
Level 10: Teach Kafka+APIs:
  "Kafka: format('kafka'), cast value from binary, parse JSON.
   Event Hubs: same as Kafka (port 9093, SASL_SSL).
   REST APIs: requests library for small data, UDFs for parallel.
   Always: set maxOffsetsPerTrigger to prevent backlog floods."
""")

print("\n" + "="*70)
print("✓ ALL HOMEWORK COMPLETED — Notebook 85")
print("✓ MODULE 13 (DATA SOURCES) COMPLETE! All 5 notebooks (81-85) done.")
print("="*70)