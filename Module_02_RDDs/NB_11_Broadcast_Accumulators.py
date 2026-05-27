# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 11: Broadcast Variables & Accumulators
# MAGIC # Module: RDDs (Resilient Distributed Datasets)
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 40 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: Company Handbook + Time Card
# MAGIC
# MAGIC **Broadcast Variable** = The Company Handbook:
# MAGIC - Everyone in the company needs the same handbook
# MAGIC - Instead of printing one copy per employee per task, you put ONE copy in each office (node)
# MAGIC - Read-only — employees can read it, nobody changes it
# MAGIC - Shared data that goes FROM driver TO all workers
# MAGIC
# MAGIC **Accumulator** = The Time Card / Tally Counter:
# MAGIC - Each employee writes their hours on a time card
# MAGIC - At the end of the week, the manager collects ALL cards and totals them
# MAGIC - Write-only from workers — only the manager (driver) reads the total
# MAGIC - Shared counter that goes FROM workers TO driver
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Summary
# MAGIC
# MAGIC | Feature | Broadcast Variable | Accumulator |
# MAGIC |---------|-------------------|-------------|
# MAGIC | Direction | Driver → Workers | Workers → Driver |
# MAGIC | Access | Read-only on workers | Write-only on workers (add only) |
# MAGIC | Use case | Share lookup tables, configs | Count events, sum metrics |
# MAGIC | Created by | `sc.broadcast(data)` | `sc.accumulator(0)` |
# MAGIC | Access value | `broadcast_var.value` | `accumulator.value` (on driver only) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use Broadcast Variables
# MAGIC
# MAGIC 1. You have a **lookup table** (country codes, config, mappings)
# MAGIC 2. The data is **small enough to fit in memory** on each executor (< few hundred MB)
# MAGIC 3. The data is **used in every task** (otherwise just use a closure variable)
# MAGIC 4. Without broadcast: data is serialized with EACH task (sent N times)
# MAGIC 5. With broadcast: data is sent ONCE per executor, shared across all tasks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use Accumulators
# MAGIC
# MAGIC 1. You need to **count something** across all tasks (errors, invalid records)
# MAGIC 2. You need to **sum metrics** across distributed computation
# MAGIC 3. **Warning:** Accumulators in transformations can be unreliable (due to re-execution)
# MAGIC 4. **Best practice:** Only use accumulators inside ACTIONS (foreach, foreachPartition)

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Broadcast Variable Internals
# MAGIC
# MAGIC ```
# MAGIC   Without Broadcast:                With Broadcast:
# MAGIC   ────────────────────                ──────────────────
# MAGIC   
# MAGIC   Driver has lookup_dict              Driver has lookup_dict
# MAGIC       │                                   │
# MAGIC       ├─ Task 1: copy of dict (10MB)       ├─ Executor 1: ONE copy (10MB)
# MAGIC       ├─ Task 2: copy of dict (10MB)       │    ├─ Task 1: reference
# MAGIC       ├─ Task 3: copy of dict (10MB)       │    └─ Task 2: reference
# MAGIC       ├─ Task 4: copy of dict (10MB)       ├─ Executor 2: ONE copy (10MB)
# MAGIC       └─ Task 5: copy of dict (10MB)       │    ├─ Task 3: reference
# MAGIC                                            │    └─ Task 4: reference
# MAGIC   Total network: 50MB                      └─ Executor 3: ...
# MAGIC                                     Total network: 30MB (one per executor)
# MAGIC ```
# MAGIC
# MAGIC ### Accumulator Internals
# MAGIC
# MAGIC ```
# MAGIC   Driver: accumulator = 0
# MAGIC       │
# MAGIC       ├─ Executor 1: local counter = 0
# MAGIC       │      Task 1: counter += 5
# MAGIC       │      Task 2: counter += 3
# MAGIC       │      → sends 8 back to driver
# MAGIC       ├─ Executor 2: local counter = 0
# MAGIC       │      Task 3: counter += 4
# MAGIC       │      Task 4: counter += 2
# MAGIC       │      → sends 6 back to driver
# MAGIC       │
# MAGIC   Driver: accumulator = 8 + 6 = 14 ✅
# MAGIC ```
# MAGIC
# MAGIC ### Important Rules
# MAGIC
# MAGIC 1. **Broadcast values are IMMUTABLE** — once broadcast, you cannot change them
# MAGIC 2. **Accumulators are ONLY guaranteed accurate inside actions** (foreach, count, etc.)
# MAGIC 3. In transformations (map, filter), Spark may re-execute tasks → double-counting!
# MAGIC 4. Call `broadcast_var.unpersist()` to free memory when done
# MAGIC 5. Call `broadcast_var.destroy()` to completely remove it

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Broadcast Variables
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner: Broadcast Variables
# ═══════════════════════════════════════════════════════

sc = spark.sparkContext  # Get SparkContext

print("=== Broadcast Variables ===")
print()

# Use case: Lookup table for country codes
country_codes = {  # Small lookup table (would be on driver)
    "US": "United States",
    "UK": "United Kingdom",
    "DE": "Germany",
    "FR": "France",
    "JP": "Japan",
    "IN": "India"
}

# Broadcast the lookup table to all executors
broadcast_countries = sc.broadcast(country_codes)  # Send once to all nodes
print(f"Broadcast type: {type(broadcast_countries)}")  # Broadcast object
print(f"Value on driver: {broadcast_countries.value}")  # Access via .value

# Create an RDD of orders with country codes
orders = sc.parallelize([
    ("order_001", "US", 250.00),
    ("order_002", "UK", 180.50),
    ("order_003", "DE", 320.00),
    ("order_004", "JP", 450.75),
    ("order_005", "FR", 125.00),
    ("order_006", "IN", 95.50),
    ("order_007", "US", 310.00),
])

# Use broadcast variable inside a transformation
def enrich_order(order):
    order_id, code, amount = order  # Unpack the tuple
    country_name = broadcast_countries.value.get(code, "Unknown")  # Lookup using .value
    return (order_id, country_name, amount)  # Return enriched order

enriched = orders.map(enrich_order)  # Apply to all orders
print("\nEnriched orders:")
for order in enriched.collect():  # Show results
    print(f"  {order[0]}: {order[1]}, ${order[2]:.2f}")

# Clean up: free the broadcast variable
broadcast_countries.unpersist()  # Remove from executor memory
print("\n--- Broadcast unpersisted (memory freed) ---")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Accumulators
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner: Accumulators
# ═══════════════════════════════════════════════════════

print("=== Accumulators ===")
print()

# 1. Simple counter
print("--- 1. Simple Counter ---")
total_records = sc.accumulator(0)  # Initialize to 0
error_count = sc.accumulator(0)  # Separate counter for errors

# Sample data with some invalid records
records = sc.parallelize([
    "Alice,85", "Bob,92", "INVALID", "Charlie,78",
    "Diana,95", "ERROR", "Eve,88", "Frank,91"
])

# Process records and count errors using foreach (ACTION — reliable!)
def process_record(record):
    total_records.add(1)  # Count every record
    if "INVALID" in record or "ERROR" in record:
        error_count.add(1)  # Count bad records

records.foreach(process_record)  # foreach is an ACTION (safe!)
print(f"Total records: {total_records.value}")  # 8
print(f"Error records: {error_count.value}")  # 2
print(f"Valid records: {total_records.value - error_count.value}")  # 6

# 2. Float accumulator (for sums)
print("\n--- 2. Float Accumulator ---")
total_amount = sc.accumulator(0.0)  # Float accumulator
orders = sc.parallelize([10.50, 25.00, 8.75, 42.30, 15.95])
orders.foreach(lambda x: total_amount.add(x))  # Sum all amounts
print(f"Total amount: ${total_amount.value:.2f}")  # $102.50

# 3. Named accumulators (visible in Spark UI)
print("\n--- 3. Named Accumulator ---")
named_acc = sc.accumulator(0, "My Named Counter")  # Shows in Spark UI!
sc.parallelize(range(100)).foreach(lambda x: named_acc.add(1))
print(f"Named accumulator: {named_acc.value}")  # 100
print("(Named accumulators appear in the Spark UI under Accumulators tab)")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Broadcast + Accumulator Together
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner: Combining Broadcast + Accumulator
# ═══════════════════════════════════════════════════════

print("=== Real-World: Broadcast + Accumulator Together ===")
print()

# Scenario: Process log entries, look up severity, count unknowns

# Broadcast: severity lookup table (shared read-only data)
severity_map = {"INFO": 1, "WARN": 2, "ERROR": 3, "CRITICAL": 4}
broadcast_severity = sc.broadcast(severity_map)

# Accumulators: count metrics (shared write-only counters)
high_severity_count = sc.accumulator(0)  # Count severe events
unknown_severity_count = sc.accumulator(0)  # Count unknown types

# Simulated log entries
logs = sc.parallelize([
    "2024-01-15 INFO User login",
    "2024-01-15 ERROR Database timeout",
    "2024-01-15 WARN Low memory",
    "2024-01-15 CRITICAL System crash",
    "2024-01-15 DEBUG Trace info",  # Not in our lookup!
    "2024-01-15 ERROR Disk full",
    "2024-01-15 INFO App started",
    "2024-01-15 UNKNOWN Mystery event"  # Not in our lookup!
])

# Process logs using broadcast (lookup) and accumulators (counting)
def process_log(log_line):
    parts = log_line.split(" ", 2)  # Split: date, level, message
    level = parts[1]  # Extract log level
    severity = broadcast_severity.value.get(level, -1)  # Lookup severity (-1 if unknown)
    if severity == -1:
        unknown_severity_count.add(1)  # Count unknown levels
    elif severity >= 3:
        high_severity_count.add(1)  # Count ERROR and CRITICAL

logs.foreach(process_log)  # Process all logs (ACTION — accumulators are safe here)

print(f"High severity events (ERROR/CRITICAL): {high_severity_count.value}")  # 3
print(f"Unknown severity levels: {unknown_severity_count.value}")  # 2
print(f"Total logs processed: {logs.count()}")  # 8

# Cleanup
broadcast_severity.unpersist()  # Free broadcast memory
print("\n--- Pattern: Broadcast for lookups + Accumulators for metrics ---")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate: Advanced Patterns
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Advanced Broadcast & Accumulator Patterns
# ═══════════════════════════════════════════════════════

from pyspark import AccumulatorParam  # For custom accumulators

print("=== Advanced Patterns ===")
print()

# Pattern 1: Large broadcast for map-side joins
print("--- Pattern 1: Map-Side Join (Broadcast Join) ---")
# When one dataset is small, broadcast it to avoid shuffle join
small_lookup = {i: f"product_{i}" for i in range(1000)}  # 1000 products
broadcast_products = sc.broadcast(small_lookup)

# Large RDD of sales
sales = sc.parallelize([(i % 1000, i * 10.0) for i in range(10000)])  # 10K sales

# Map-side join: enrich sales with product names (no shuffle!)
enriched_sales = sales.map(
    lambda x: (x[0], broadcast_products.value.get(x[0], "Unknown"), x[1])
)
print(f"  First 3 enriched: {enriched_sales.take(3)}")
print(f"  No shuffle needed! All done locally on each executor.")
broadcast_products.unpersist()

# Pattern 2: Custom CollectionAccumulator (collect items, not just sum)
print("\n--- Pattern 2: Custom Accumulator (List Collector) ---")

class ListAccumulatorParam(AccumulatorParam):
    """Custom accumulator that collects items into a list."""
    def zero(self, initialValue):  # Starting value
        return []  # Empty list
    def addInPlace(self, acc, value):  # How to add
        acc.append(value)  # Append item
        return acc

# Create list accumulator to collect error messages
error_messages = sc.accumulator([], ListAccumulatorParam())  # Custom accumulator

data = sc.parallelize(["ok", "fail:timeout", "ok", "fail:disk", "ok", "fail:memory"])
data.foreach(lambda x: error_messages.add(x) if x.startswith("fail") else None)
print(f"  Collected errors: {error_messages.value}")  # ['fail:timeout', 'fail:disk', 'fail:memory']

print("\n--- WARNING about Accumulators in Transformations ---")
print("Accumulators in map/filter are UNRELIABLE (tasks may re-execute).")
print("ONLY trust accumulator values after an ACTION completes.")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Broadcast for Config-Driven Processing
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 2: Config-Driven Processing
# ═══════════════════════════════════════════════════════

print("=== Broadcast for Configuration-Driven Processing ===")
print()

# Scenario: Different processing rules per region
# Instead of hardcoding, broadcast the config!

# Configuration: tax rates and currency by region
region_config = {  # This could come from a database or API
    "US": {"tax_rate": 0.08, "currency": "USD", "free_shipping_min": 50},
    "UK": {"tax_rate": 0.20, "currency": "GBP", "free_shipping_min": 30},
    "DE": {"tax_rate": 0.19, "currency": "EUR", "free_shipping_min": 40},
    "JP": {"tax_rate": 0.10, "currency": "JPY", "free_shipping_min": 5000},
    "IN": {"tax_rate": 0.18, "currency": "INR", "free_shipping_min": 500},
}

# Broadcast the config to all executors
bc_config = sc.broadcast(region_config)  # Sent once per executor

# Orders from different regions
orders = sc.parallelize([
    ("ord_1", "US", 45.00),   # Below free shipping
    ("ord_2", "US", 120.00),  # Above free shipping
    ("ord_3", "UK", 25.00),   # Below free shipping
    ("ord_4", "DE", 80.00),   # Above free shipping
    ("ord_5", "JP", 3000.00), # Below free shipping
    ("ord_6", "IN", 750.00),  # Above free shipping
])

# Apply region-specific logic using broadcast config
def calculate_total(order):
    order_id, region, amount = order  # Unpack
    config = bc_config.value.get(region, {})  # Get region config
    tax = amount * config.get("tax_rate", 0)  # Calculate tax
    shipping = 0 if amount >= config.get("free_shipping_min", 0) else 9.99  # Free shipping?
    total = amount + tax + shipping  # Final total
    currency = config.get("currency", "???")
    return (order_id, region, f"{currency} {total:.2f}", "Free Ship" if shipping == 0 else "+ Shipping")

results = orders.map(calculate_total).collect()  # Apply config to all orders

print("Order Results (config-driven):")
for r in results:  # Print each result
    print(f"  {r[0]} | {r[1]} | Total: {r[2]} | {r[3]}")

# Expected Output:
# ord_1 | US | USD 58.59 | + Shipping (45 + 3.60 tax + 9.99 ship)
# ord_2 | US | USD 129.60 | Free Ship (120 + 9.60 tax)
# ord_3 | UK | GBP 39.99 | + Shipping (25 + 5.00 tax + 9.99 ship)
# ord_4 | DE | EUR 95.20 | Free Ship (80 + 15.20 tax)
# ord_5 | JP | JPY 3309.99 | + Shipping (3000 + 300 tax + 9.99 ship)
# ord_6 | IN | INR 885.00 | Free Ship (750 + 135 tax)

bc_config.unpersist()  # Free memory
print("\n--- Key: Broadcast avoids sending config with every single task ---")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Multiple Accumulators for Data Profiling
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate Example 3: Data Profiling with Accumulators
# ═══════════════════════════════════════════════════════

print("=== Multiple Accumulators for Data Profiling ===")
print()

# Scenario: Profile a dataset to understand quality BEFORE expensive processing
# Use accumulators to gather stats in a SINGLE pass

# Create multiple accumulators for different metrics
total_rows = sc.accumulator(0)          # Total record count
null_name = sc.accumulator(0)           # Missing names
null_email = sc.accumulator(0)          # Missing emails
invalid_age = sc.accumulator(0)         # Age out of range
duplicate_flag = sc.accumulator(0)      # Potential duplicates (same name)
min_age_acc = sc.accumulator(999)       # Track min age (will use min trick)
max_age_acc = sc.accumulator(0)         # Track max age

# Sample dataset (simulating messy real-world data)
user_data = sc.parallelize([
    {"name": "Alice", "email": "alice@test.com", "age": 30},
    {"name": None, "email": "bob@test.com", "age": 25},       # Missing name
    {"name": "Charlie", "email": None, "age": 45},             # Missing email
    {"name": "Diana", "email": "diana@test.com", "age": -5},   # Invalid age
    {"name": "Eve", "email": "eve@test.com", "age": 200},      # Invalid age
    {"name": "Frank", "email": "frank@test.com", "age": 35},
    {"name": None, "email": None, "age": 28},                  # Both missing
    {"name": "Grace", "email": "grace@test.com", "age": 55},
    {"name": "Alice", "email": "alice2@test.com", "age": 31},  # Duplicate name
    {"name": "Henry", "email": "henry@test.com", "age": 42},
])

# Collect all names to detect duplicates (small dataset only!)
all_names = user_data.map(lambda r: r.get("name")).filter(lambda n: n is not None).collect()
name_counts = {}  # Simple counter
for n in all_names:
    name_counts[n] = name_counts.get(n, 0) + 1
duplicates = {n for n, c in name_counts.items() if c > 1}  # Names appearing > 1 time
bc_duplicates = sc.broadcast(duplicates)  # Broadcast known duplicates

# Profile in a single pass using foreach (ACTION = reliable accumulators)
def profile_record(record):
    total_rows.add(1)  # Count every record
    if record.get("name") is None:
        null_name.add(1)  # Missing name
    elif record["name"] in bc_duplicates.value:
        duplicate_flag.add(1)  # Duplicate name
    if record.get("email") is None:
        null_email.add(1)  # Missing email
    age = record.get("age", -1)
    if age < 0 or age > 120:
        invalid_age.add(1)  # Invalid age
    else:
        # Track min/max using accumulators (trick: compare in add)
        if age > max_age_acc.value:
            max_age_acc.add(age - max_age_acc.value)  # Increment to reach new max
        # Note: min tracking with accumulators is tricky; we'll compute separately

user_data.foreach(profile_record)  # Single pass through data

# Print the data quality report
print("╔══════════════════════════════════════╗")
print("║     DATA QUALITY PROFILE REPORT      ║")
print("╠══════════════════════════════════════╣")
print(f"║ Total Records:     {total_rows.value:>15} ║")
print(f"║ Null Names:        {null_name.value:>15} ║")
print(f"║ Null Emails:       {null_email.value:>15} ║")
print(f"║ Invalid Ages:      {invalid_age.value:>15} ║")
print(f"║ Duplicate Names:   {duplicate_flag.value:>15} ║")
print(f"║ Max Age Seen:      {max_age_acc.value:>15} ║")
print("╚══════════════════════════════════════╝")

valid_pct = ((total_rows.value - null_name.value - null_email.value - invalid_age.value) / total_rows.value) * 100
print(f"\nOverall data quality: ~{valid_pct:.1f}% of records have no issues")

bc_duplicates.unpersist()  # Cleanup
print("\n--- Key: Single pass profiling avoids multiple scans ---")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Broadcast for Multi-Level Lookups
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 1: Multi-Level Broadcast Lookups
# ═══════════════════════════════════════════════════════

print("=== Advanced: Multi-Level Hierarchical Lookups ===")
print()

# Scenario: Employee data needs enrichment from multiple lookup tables
# In real world, these would come from dimension tables

# Lookup 1: Department hierarchy
dept_hierarchy = {
    "ENG": {"name": "Engineering", "division": "Technology", "cost_center": "CC100"},
    "MKT": {"name": "Marketing", "division": "Business", "cost_center": "CC200"},
    "FIN": {"name": "Finance", "division": "Business", "cost_center": "CC300"},
    "OPS": {"name": "Operations", "division": "Technology", "cost_center": "CC400"},
    "HR":  {"name": "Human Resources", "division": "Admin", "cost_center": "CC500"},
}

# Lookup 2: Location details
location_info = {
    "SFO": {"city": "San Francisco", "country": "US", "timezone": "PST"},
    "NYC": {"city": "New York", "country": "US", "timezone": "EST"},
    "LDN": {"city": "London", "country": "UK", "timezone": "GMT"},
    "BLR": {"city": "Bangalore", "country": "IN", "timezone": "IST"},
    "TKY": {"city": "Tokyo", "country": "JP", "timezone": "JST"},
}

# Lookup 3: Salary bands
salary_bands = {
    "L1": {"min": 50000, "max": 70000, "title": "Junior"},
    "L2": {"min": 70000, "max": 100000, "title": "Mid-Level"},
    "L3": {"min": 100000, "max": 140000, "title": "Senior"},
    "L4": {"min": 140000, "max": 200000, "title": "Staff"},
    "L5": {"min": 200000, "max": 300000, "title": "Principal"},
}

# Broadcast ALL lookups (sent once per executor, shared across tasks)
bc_dept = sc.broadcast(dept_hierarchy)       # ~small KB
bc_location = sc.broadcast(location_info)    # ~small KB
bc_salary = sc.broadcast(salary_bands)       # ~small KB

# Employee records (imagine millions of these)
employees = sc.parallelize([
    ("EMP001", "Alice", "ENG", "SFO", "L3", 125000),
    ("EMP002", "Bob", "MKT", "NYC", "L2", 85000),
    ("EMP003", "Charlie", "FIN", "LDN", "L4", 160000),
    ("EMP004", "Diana", "OPS", "BLR", "L1", 55000),
    ("EMP005", "Eve", "ENG", "TKY", "L5", 250000),
    ("EMP006", "Frank", "HR", "SFO", "L2", 78000),
])

# Multi-level enrichment using all three broadcast lookups
def enrich_employee(emp):
    emp_id, name, dept_code, loc_code, level, salary = emp  # Unpack
    dept = bc_dept.value.get(dept_code, {})        # Lookup department
    loc = bc_location.value.get(loc_code, {})      # Lookup location
    band = bc_salary.value.get(level, {})          # Lookup salary band
    
    # Compute derived fields
    division = dept.get("division", "Unknown")     # Get division from dept
    city = loc.get("city", "Unknown")              # Get city from location
    title = band.get("title", "Unknown")           # Get title from band
    band_min = band.get("min", 0)                  # Band minimum
    band_max = band.get("max", 0)                  # Band maximum
    
    # Calculate position in salary band (0-100%)
    if band_max > band_min:
        band_position = ((salary - band_min) / (band_max - band_min)) * 100
    else:
        band_position = 0
    
    return {
        "id": emp_id, "name": name,
        "title": f"{title} ({level})",
        "dept": dept.get("name", "Unknown"),
        "division": division,
        "location": f"{city} ({loc.get('timezone', '?')})",
        "salary": f"${salary:,}",
        "band_position": f"{band_position:.0f}%"
    }

# Apply enrichment (no shuffle, all done locally!)
enriched = employees.map(enrich_employee).collect()  # Trigger execution

print("Enriched Employee Records:")
print("-" * 80)
for emp in enriched:  # Print each enriched record
    print(f"  {emp['id']} | {emp['name']:<8} | {emp['title']:<16} | "
          f"{emp['dept']:<16} | {emp['location']:<22} | "
          f"{emp['salary']:<10} | Band: {emp['band_position']}")

# Cleanup all broadcasts
bc_dept.unpersist()      # Free department lookup
bc_location.unpersist()  # Free location lookup
bc_salary.unpersist()    # Free salary lookup
print("\n--- Key: Multiple broadcasts = multiple dimensions enriched without shuffle ---")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Accumulator Reliability Patterns
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 2: Accumulator Reliability & Workarounds
# ═══════════════════════════════════════════════════════

print("=== Advanced: Accumulator Reliability Deep Dive ===")
print()

# PROBLEM: Accumulators in transformations are UNRELIABLE
# WHY: Spark may re-execute tasks (speculation, failure recovery)
# RESULT: Accumulator gets incremented MULTIPLE times for same data

# --- Demo 1: Reliable (inside action) ---
print("--- Demo 1: RELIABLE — Accumulator in foreach (action) ---")
reliable_counter = sc.accumulator(0)  # Reset counter
data = sc.parallelize(range(1000), 4)  # 1000 items, 4 partitions
data.foreach(lambda x: reliable_counter.add(1))  # Action = guaranteed once
print(f"  Expected: 1000, Got: {reliable_counter.value}")  # Always 1000

# --- Demo 2: Potentially unreliable (inside transformation) ---
print("\n--- Demo 2: RISKY — Accumulator in map (transformation) ---")
risky_counter = sc.accumulator(0)  # Reset counter
mapped = data.map(lambda x: (risky_counter.add(1), x * 2)[1])  # Transformation
mapped.count()  # Trigger execution
print(f"  Expected: 1000, Got: {risky_counter.value}")  # Usually 1000, but NOT guaranteed!
print("  ⚠️  If tasks are re-executed, this number can be > 1000")

# --- Demo 3: SAFE pattern for transformations ---
print("\n--- Demo 3: SAFE pattern using foreachPartition ---")
partition_counter = sc.accumulator(0)  # Reset
error_collector = sc.accumulator(0)  # Error counter

# Process data with partition-level control
def process_partition(iterator):
    """Process entire partition — more efficient than per-element."""
    local_count = 0  # Local counter (fast, no network)
    local_errors = 0  # Local error counter
    for item in iterator:
        local_count += 1
        if item % 7 == 0:  # Simulate: every 7th item is an "error"
            local_errors += 1
    # Add to accumulator ONCE per partition (fewer updates, still inside action)
    partition_counter.add(local_count)
    error_collector.add(local_errors)

data.foreachPartition(process_partition)  # Action = reliable!
print(f"  Total processed: {partition_counter.value}")  # 1000
print(f"  Error items (divisible by 7): {error_collector.value}")  # 143

# --- Demo 4: Best Practice — Use filter + count instead of accumulator ---
print("\n--- Demo 4: ALTERNATIVE — filter().count() instead of accumulator ---")
error_count_safe = data.filter(lambda x: x % 7 == 0).count()  # 100% reliable!
print(f"  Errors via filter+count: {error_count_safe}")  # 143 (guaranteed accurate)
print("  → When you only need a count, filter().count() is simpler AND safer")

print("\n--- Summary ---")
print("  ✅ RELIABLE: accumulator in foreach / foreachPartition")
print("  ✅ RELIABLE: filter().count() for counting")
print("  ⚠️  RISKY: accumulator in map / filter (may double-count)")
print("  💡 TIP: foreachPartition is MORE EFFICIENT (fewer accumulator updates)")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Broadcast Join vs Shuffle Join Benchmark
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced Example 3: Broadcast vs Shuffle Join Performance
# ═══════════════════════════════════════════════════════

import time  # For timing comparisons

print("=== Advanced: Broadcast Join vs Shuffle Join Performance ===")
print()

# Scenario: Join a LARGE fact table with a SMALL dimension table
# Compare two approaches:
#   1. Shuffle join (groupByKey/join) — moves data across network
#   2. Broadcast join (map + broadcast lookup) — no shuffle!

# Small dimension: 100 products (fits in memory easily)
product_dim = {i: f"Product_{i:03d}" for i in range(100)}  # 100 products

# Large fact: 500K sales transactions
large_sales = sc.parallelize(
    [(i % 100, i * 1.5) for i in range(500000)],  # 500K rows
    numSlices=8  # 8 partitions
)

print(f"Fact table size: {large_sales.count():,} rows")
print(f"Dimension table size: {len(product_dim)} entries")
print()

# --- Approach 1: Shuffle Join (PairRDD join) ---
print("--- Approach 1: Shuffle Join ---")
start_time = time.time()  # Start timer

# Convert dimension to PairRDD for join
dim_rdd = sc.parallelize(list(product_dim.items()))  # (id, name) pairs

# Perform shuffle join (moves data across network!)
shuffle_result = large_sales.join(dim_rdd)  # (key, (amount, name))
shuffle_count = shuffle_result.count()  # Trigger execution

shuffle_time = time.time() - start_time  # End timer
print(f"  Result count: {shuffle_count:,}")
print(f"  Time: {shuffle_time:.3f} seconds")
print(f"  Method: data shuffled across network (expensive!)")

# --- Approach 2: Broadcast Join (map-side, no shuffle) ---
print("\n--- Approach 2: Broadcast Join ---")
start_time = time.time()  # Start timer

# Broadcast the small dimension table
bc_products = sc.broadcast(product_dim)  # Sent once per executor

# Map-side join: lookup locally, no shuffle needed!
broadcast_result = large_sales.map(
    lambda x: (x[0], x[1], bc_products.value.get(x[0], "Unknown"))  # Local lookup
)
broadcast_count = broadcast_result.count()  # Trigger execution

broadcast_time = time.time() - start_time  # End timer
print(f"  Result count: {broadcast_count:,}")
print(f"  Time: {broadcast_time:.3f} seconds")
print(f"  Method: local lookup on each executor (fast!)")

# --- Comparison ---
print("\n" + "=" * 50)
print("PERFORMANCE COMPARISON:")
print("=" * 50)
speedup = shuffle_time / broadcast_time if broadcast_time > 0 else float('inf')
print(f"  Shuffle join: {shuffle_time:.3f}s")
print(f"  Broadcast join: {broadcast_time:.3f}s")
print(f"  Speedup: {speedup:.1f}x faster with broadcast")
print()
print("WHY broadcast is faster:")
print("  1. No shuffle = no network transfer of the large dataset")
print("  2. No shuffle = no disk spill during sort/merge")
print("  3. Lookup is O(1) hash-map access on each executor")
print()
print("WHEN to use each:")
print("  Broadcast: small dim (< few hundred MB) + large fact")
print("  Shuffle: both tables are large (no choice but to shuffle)")

bc_products.unpersist()  # Cleanup
print("\n--- Key: Broadcast joins eliminate the #1 performance killer (shuffle) ---")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Using Accumulators Inside Transformations (map/filter)
# MAGIC **Issue:** If a task is re-executed (due to failure or speculation), the accumulator is incremented AGAIN.  
# MAGIC **Fix:** Only use accumulators inside ACTIONS (`foreach`, `foreachPartition`). Or treat the value as approximate.
# MAGIC
# MAGIC ### Mistake #2: Reading Accumulator Value on Executors
# MAGIC **Issue:** `accumulator.value` on executors doesn't show the global total — only the local value.  
# MAGIC **Fix:** Only read `.value` on the driver (after an action completes).
# MAGIC
# MAGIC ### Mistake #3: Broadcasting Large Data (> 1GB)
# MAGIC **Issue:** Broadcasting a 2GB table sends 2GB to EVERY executor → OOM.  
# MAGIC **Fix:** Only broadcast small data (< few hundred MB). For large joins, use a regular shuffle join.
# MAGIC
# MAGIC ### Mistake #4: Modifying Broadcast Value
# MAGIC **Issue:** `broadcast_var.value["new_key"] = 123` may work locally but WON'T propagate to executors.  
# MAGIC **Fix:** Broadcast variables are IMMUTABLE. Create a new broadcast if you need updated data.
# MAGIC
# MAGIC ### Mistake #5: Forgetting to unpersist() Broadcast
# MAGIC **Issue:** Broadcast variables stay in executor memory until the SparkContext ends.  
# MAGIC **Fix:** Always call `broadcast_var.unpersist()` when you're done using it.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC ### Level 1: Broadcast a dict of {code: name}. Map an RDD to replace codes.
# MAGIC ### Level 2: Create an accumulator, use foreach to count items > 50.
# MAGIC ### Level 3: Combine broadcast lookup + accumulator to enrich + count errors.
# MAGIC ### Level 4: Broadcast a stop-words list. Filter out stop words from text RDD.
# MAGIC ### Level 5: Use a named accumulator. Find it in the Spark UI.
# MAGIC ### Level 6: Implement a map-side join using broadcast.
# MAGIC ### Level 7: Demonstrate accumulator unreliability in transformations vs actions.
# MAGIC ### Level 8: Build a custom ListAccumulator to collect invalid records.
# MAGIC ### Level 9: Benchmark: broadcast join vs shuffle join on different data sizes.
# MAGIC ### Level 10: Design a data quality pipeline using broadcast configs + accumulators.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════
import time
from pyspark import AccumulatorParam

# Level 1: Broadcast lookup
print("=== Level 1 ===")
status_map = {1: "Active", 2: "Pending", 3: "Closed", 4: "Cancelled"}
bc_status = sc.broadcast(status_map)
items = sc.parallelize([("order_a", 1), ("order_b", 3), ("order_c", 2)])
result = items.map(lambda x: (x[0], bc_status.value.get(x[1], "Unknown")))
print(f"Enriched: {result.collect()}")  # [('order_a','Active'), ('order_b','Closed'), ('order_c','Pending')]
bc_status.unpersist()

# Level 2: Accumulator count
print("\n=== Level 2 ===")
gt50_count = sc.accumulator(0)
nums = sc.parallelize([10, 55, 30, 80, 45, 92, 15, 67])
nums.foreach(lambda x: gt50_count.add(1) if x > 50 else None)
print(f"Items > 50: {gt50_count.value}")  # 4 (55, 80, 92, 67)

# Level 4: Stop words filter
print("\n=== Level 4 ===")
stop_words = {"the", "a", "an", "is", "in", "on", "at", "to", "and", "of"}
bc_stops = sc.broadcast(stop_words)
text = sc.parallelize(["the cat is on the mat", "a dog in the park"])
filtered = text.flatMap(lambda s: s.split(" ")).filter(lambda w: w not in bc_stops.value)
print(f"Without stop words: {filtered.collect()}")  # ['cat','mat','dog','park']
bc_stops.unpersist()

# Level 7: Accumulator unreliability demo
print("\n=== Level 7 ===")
reliable_acc = sc.accumulator(0)
unreliable_acc = sc.accumulator(0)
data = sc.parallelize(range(100))
# In action (reliable)
data.foreach(lambda x: reliable_acc.add(1))
print(f"In foreach (reliable): {reliable_acc.value}")  # 100
# In transformation (potentially unreliable due to re-execution)
transformed = data.map(lambda x: (unreliable_acc.add(1), x)[1])
transformed.count()  # Trigger execution
print(f"In map (may vary): {unreliable_acc.value}")  # Usually 100, but not guaranteed!
print("  → In production, only trust accumulators inside actions.")

# Level 10: Data quality pipeline
print("\n=== Level 10 ===")
# Config broadcast + quality metrics accumulators
config = {"min_age": 0, "max_age": 120, "required_fields": ["name", "age"]}
bc_config = sc.broadcast(config)
null_count = sc.accumulator(0)
range_errors = sc.accumulator(0)
valid_count = sc.accumulator(0)

people = sc.parallelize([
    {"name": "Alice", "age": 30}, {"name": "Bob", "age": -5},
    {"name": None, "age": 25}, {"name": "Eve", "age": 150},
    {"name": "Frank", "age": 45}
])

def quality_check(record):
    cfg = bc_config.value
    if any(record.get(f) is None for f in cfg["required_fields"]):
        null_count.add(1)
    elif not (cfg["min_age"] <= record.get("age", -1) <= cfg["max_age"]):
        range_errors.add(1)
    else:
        valid_count.add(1)

people.foreach(quality_check)
print(f"Valid: {valid_count.value}, Nulls: {null_count.value}, Range errors: {range_errors.value}")
bc_config.unpersist()

print("\n\u2705 All homework complete!")