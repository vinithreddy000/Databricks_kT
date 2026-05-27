# Databricks notebook source
# DBTITLE 1,NB_32 Header
# MAGIC %md
# MAGIC # NB_32 — Date Time Functions
# MAGIC
# MAGIC **Module 5: Built-in Functions** | Notebook 32 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC * current_date, current_timestamp, now
# MAGIC * to_date, to_timestamp, cast to date and timestamp
# MAGIC * date_format, year, quarter, month, day, hour, minute, second
# MAGIC * date_add, date_sub, add_months, datediff, months_between
# MAGIC * last_day, next_day, trunc, date_trunc
# MAGIC * make_date, make_timestamp
# MAGIC * unix_timestamp, from_unixtime, to_unix_timestamp patterns
# MAGIC * timezone conversion with to_utc_timestamp and from_utc_timestamp
# MAGIC * window() for time-based grouping
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Comprehensive Reference)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are Date Time Functions?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are Date Time Functions? (Real-World Analogy)
# MAGIC
# MAGIC ### The Railway Timetable Control Room
# MAGIC
# MAGIC Imagine running a large railway network:
# MAGIC
# MAGIC | Railway Task | PySpark Date/Time Function | What It Does |
# MAGIC |---|---|---|
# MAGIC | Check today's operational date | `current_date()` | Gets today's date |
# MAGIC | Check exact control-room clock time | `current_timestamp()` | Gets current timestamp |
# MAGIC | Read a printed ticket date | `to_date()` | Converts text into a date |
# MAGIC | Read a ticket timestamp | `to_timestamp()` | Converts text into a timestamp |
# MAGIC | Add 7 days to a booking | `date_add()` | Shifts a date forward |
# MAGIC | Find delay between two events | `datediff()` | Computes date difference |
# MAGIC | Round to start of month | `trunc()` | Buckets to month or year |
# MAGIC | Convert local station time to UTC | `to_utc_timestamp()` | Timezone conversion |
# MAGIC | Group trains by 15-minute slot | `window()` | Time-based grouping |
# MAGIC
# MAGIC ### Why Date/Time Functions Matter
# MAGIC
# MAGIC Almost every business dataset contains time:
# MAGIC
# MAGIC * Orders have `order_date`
# MAGIC * Sensors have `event_timestamp`
# MAGIC * Employees have `hire_date`
# MAGIC * Billing has `invoice_month`
# MAGIC * Streaming data arrives with event time
# MAGIC
# MAGIC ### Core Idea
# MAGIC
# MAGIC Date/time functions help you answer:
# MAGIC
# MAGIC * When did something happen?
# MAGIC * How long between two events?
# MAGIC * Which month, quarter, or year does this belong to?
# MAGIC * How do we standardize timestamps across time zones?
# MAGIC * How do we aggregate time-based events correctly?

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Date Time Functions Work
# MAGIC %md
# MAGIC ## SECTION 2 — How Date Time Functions Work (Internal Mechanics)
# MAGIC
# MAGIC ### Date vs Timestamp
# MAGIC
# MAGIC ```text
# MAGIC DATE
# MAGIC   Example: 2026-05-26
# MAGIC   Stores: year, month, day
# MAGIC   No hour/minute/second
# MAGIC
# MAGIC TIMESTAMP
# MAGIC   Example: 2026-05-26 14:35:42.123
# MAGIC   Stores: date + time
# MAGIC   May be affected by timezone conversion logic
# MAGIC ```
# MAGIC
# MAGIC ### Common Workflow
# MAGIC
# MAGIC ```text
# MAGIC Raw string column
# MAGIC     │
# MAGIC     ├──> to_date("dd.MM.yyyy")
# MAGIC     │       └──> DATE column
# MAGIC     │               └──> year(), month(), quarter(), last_day()
# MAGIC     │
# MAGIC     └──> to_timestamp("dd.MM.yyyy HH:mm:ss")
# MAGIC             └──> TIMESTAMP column
# MAGIC                     └──> hour(), minute(), second(), window(), timezone conversion
# MAGIC ```
# MAGIC
# MAGIC ### Function Families
# MAGIC
# MAGIC ```text
# MAGIC PARSE
# MAGIC   to_date, to_timestamp, unix_timestamp, make_date, make_timestamp
# MAGIC
# MAGIC EXTRACT
# MAGIC   year, quarter, month, dayofmonth, dayofweek, weekofyear,
# MAGIC   hour, minute, second
# MAGIC
# MAGIC SHIFT / DIFFERENCE
# MAGIC   date_add, date_sub, add_months, datediff, months_between
# MAGIC
# MAGIC ROUND / BUCKET
# MAGIC   trunc, date_trunc, last_day, next_day, window
# MAGIC
# MAGIC FORMAT / CONVERT
# MAGIC   date_format, from_unixtime, to_utc_timestamp, from_utc_timestamp
# MAGIC ```
# MAGIC
# MAGIC ### Important Rules
# MAGIC
# MAGIC 1. `to_date()` returns NULL when parsing fails
# MAGIC 2. `to_timestamp()` also returns NULL when parsing fails
# MAGIC 3. `datediff(end, start)` returns days as integer
# MAGIC 4. `months_between()` returns fractional months
# MAGIC 5. `window()` is for timestamp bucketing, especially useful in streaming
# MAGIC 6. Timezone conversion changes clock time while representing the same instant

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: current_date and current_timestamp
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: current_date() and current_timestamp()
# ============================================================
# Real-world: Stamping records with today's date and current processing time.

from pyspark.sql import SparkSession  # Import SparkSession for completeness.
from pyspark.sql.functions import current_date, current_timestamp, now, lit, col  # Import date and timestamp functions.

spark = SparkSession.builder.getOrCreate()  # Get the active Spark session.

# Create a tiny DataFrame to attach processing metadata.
orders_df = spark.createDataFrame(  # Build example order records.
    [(101, "Laptop"), (102, "Phone"), (103, "Tablet")],  # Sample rows.
    ["order_id", "product"]  # Column names.
)  # End DataFrame creation.

# Add today's date and current timestamp to each row.
stamped_df = orders_df.withColumn("process_date", current_date())  # Add current date.
stamped_df = stamped_df.withColumn("process_ts", current_timestamp())  # Add current timestamp.
stamped_df = stamped_df.withColumn("now_ts", now())  # Add now() which is equivalent to current_timestamp().

# Show the stamped results.
print("=== current_date() and current_timestamp() ===")  # Print section heading.
stamped_df.show(truncate=False)  # Display the DataFrame.

# Show schema so learners can see DATE vs TIMESTAMP types.
print("=== Schema ===")  # Print schema heading.
stamped_df.printSchema()  # Display schema.

# Expected Output:
# +--------+-------+------------+-----------------------+-----------------------+
# |order_id|product|process_date|process_ts             |now_ts                 |
# +--------+-------+------------+-----------------------+-----------------------+
# |101     |Laptop |2026-05-26  |2026-05-26 14:35:42.xxx|2026-05-26 14:35:42.xxx|
# |102     |Phone  |2026-05-26  |2026-05-26 14:35:42.xxx|2026-05-26 14:35:42.xxx|
# +--------+-------+------------+-----------------------+-----------------------+
# Note: The exact timestamp value depends on notebook run time.
# Note: All rows in one query get the same current_timestamp value for that evaluation.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: to_date and to_timestamp
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: to_date() and to_timestamp()
# ============================================================
# Real-world: Parsing CSV or API string fields into real date/time types.

from pyspark.sql.functions import to_date, to_timestamp  # Import parsing functions.

# Create raw string data in different formats.
raw_df = spark.createDataFrame(  # Build sample raw input.
    [
        ("26.05.2026", "26.05.2026 14:45:30"),  # European date and timestamp.
        ("01.01.2025", "01.01.2025 00:00:00"),  # New year example.
        ("15.08.2024", "15.08.2024 09:10:11"),  # Another valid example.
        ("bad_date", "not_a_timestamp"),  # Invalid strings for NULL demo.
    ],  # End rows.
    ["date_str", "ts_str"]  # Column names.
)  # End DataFrame creation.

# Parse strings using explicit format patterns.
parsed_df = raw_df.withColumn("parsed_date", to_date(col("date_str"), "dd.MM.yyyy"))  # Parse date string.
parsed_df = parsed_df.withColumn("parsed_ts", to_timestamp(col("ts_str"), "dd.MM.yyyy HH:mm:ss"))  # Parse timestamp string.

# Show parsed results.
print("=== Parsing with to_date() and to_timestamp() ===")  # Print heading.
parsed_df.show(truncate=False)  # Display parsed values.

# Show schema after parsing.
print("=== Schema After Parsing ===")  # Print schema heading.
parsed_df.printSchema()  # Display schema.

# Expected Output:
# +----------+-------------------+-----------+-------------------+
# |date_str  |ts_str             |parsed_date|parsed_ts          |
# +----------+-------------------+-----------+-------------------+
# |26.05.2026|26.05.2026 14:45:30|2026-05-26 |2026-05-26 14:45:30|
# |01.01.2025|01.01.2025 00:00:00|2025-01-01 |2025-01-01 00:00:00|
# |bad_date  |not_a_timestamp    |null       |null               |
# +----------+-------------------+-----------+-------------------+
# Note: Invalid parse results become NULL instead of throwing an error in normal usage.

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Extract parts and format dates
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: Extract Parts and Format Dates
# ============================================================
# Real-world: Building year/month/day reporting columns from an order date.

from pyspark.sql.functions import (  # Import extraction and formatting helpers.
    year, quarter, month, dayofmonth, dayofweek, weekofyear,  # Date parts.
    hour, minute, second, date_format  # Time parts and formatting.
)  # End import list.

# Create timestamp data for extraction.
time_df = spark.createDataFrame(  # Build sample event timestamps.
    [
        (1, "2026-05-26 14:45:30"),  # Afternoon event.
        (2, "2025-01-01 00:00:00"),  # Midnight event.
        (3, "2024-12-31 23:59:59"),  # End-of-year event.
    ],  # End rows.
    ["event_id", "event_ts_str"]  # Column names.
)  # End DataFrame creation.

# Parse the timestamp strings first.
time_df = time_df.withColumn("event_ts", to_timestamp(col("event_ts_str"), "yyyy-MM-dd HH:mm:ss"))  # Parse timestamp.

# Extract many useful date and time parts.
extracted_df = time_df.select(  # Select original and derived columns.
    col("event_id"),  # Keep id.
    col("event_ts"),  # Keep parsed timestamp.
    year(col("event_ts")).alias("year"),  # Extract year.
    quarter(col("event_ts")).alias("quarter"),  # Extract quarter.
    month(col("event_ts")).alias("month"),  # Extract month.
    dayofmonth(col("event_ts")).alias("day"),  # Extract day of month.
    dayofweek(col("event_ts")).alias("day_of_week"),  # Extract weekday number.
    weekofyear(col("event_ts")).alias("week_of_year"),  # Extract ISO-like week number.
    hour(col("event_ts")).alias("hour"),  # Extract hour.
    minute(col("event_ts")).alias("minute"),  # Extract minute.
    second(col("event_ts")).alias("second"),  # Extract second.
    date_format(col("event_ts"), "yyyy/MM/dd HH:mm").alias("formatted_ts"),  # Format nicely.
    date_format(col("event_ts"), "EEEE").alias("weekday_name")  # Full weekday name.
)  # End select.

# Display extracted features.
print("=== Date Part Extraction and Formatting ===")  # Print heading.
extracted_df.show(truncate=False)  # Show full output.

# Expected Output:
# +--------+-------------------+----+-------+-----+---+-----------+------------+----+------+------+
# |event_id|event_ts           |year|quarter|month|day|day_of_week|week_of_year|hour|minute|second|
# +--------+-------------------+----+-------+-----+---+-----------+------------+----+------+------+
# |1       |2026-05-26 14:45:30|2026|2      |5    |26 |3          |22          |14  |45    |30    |
# +--------+-------------------+----+-------+-----+---+-----------+------------+----+------+------+

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: Date arithmetic and differences
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: Date Arithmetic and Differences
# ============================================================
# Real-world: Delivery lead time, subscription tenure, due-date calculations.

from pyspark.sql.functions import (  # Import date arithmetic helpers.
    date_add, date_sub, add_months, datediff, months_between, last_day, next_day
)  # End import list.

# Create start and end date examples.
calc_df = spark.createDataFrame(  # Build sample lifecycle data.
    [
        ("Alice", "2026-05-01", "2026-05-26"),  # 25-day span.
        ("Bob", "2025-01-15", "2025-03-20"),  # Multi-month span.
        ("Charlie", "2024-02-29", "2024-03-31"),  # Leap year example.
    ],  # End rows.
    ["customer", "start_str", "end_str"]  # Column names.
)  # End creation.

# Parse string dates.
calc_df = calc_df.withColumn("start_date", to_date(col("start_str"), "yyyy-MM-dd"))  # Parse start date.
calc_df = calc_df.withColumn("end_date", to_date(col("end_str"), "yyyy-MM-dd"))  # Parse end date.

# Compute shifted dates and differences.
result_df = calc_df.select(  # Select useful derived columns.
    col("customer"),  # Keep customer.
    col("start_date"),  # Keep start.
    col("end_date"),  # Keep end.
    date_add(col("start_date"), 7).alias("plus_7_days"),  # Add 7 days.
    date_sub(col("end_date"), 3).alias("minus_3_days"),  # Subtract 3 days.
    add_months(col("start_date"), 2).alias("plus_2_months"),  # Add 2 months.
    datediff(col("end_date"), col("start_date")).alias("days_between"),  # Integer day difference.
    months_between(col("end_date"), col("start_date")).alias("months_between"),  # Fractional month difference.
    last_day(col("start_date")).alias("start_month_end"),  # Last day of start month.
    next_day(col("start_date"), "Mon").alias("next_monday")  # Next Monday after start date.
)  # End select.

# Show calculations.
print("=== Date Arithmetic and Differences ===")  # Print heading.
result_df.show(truncate=False)  # Display output.

# Expected Output:
# +--------+----------+----------+-----------+------------+-------------+------------+
# |customer|start_date|end_date  |plus_7_days|minus_3_days|plus_2_months|days_between|
# +--------+----------+----------+-----------+------------+-------------+------------+
# |Alice   |2026-05-01|2026-05-26|2026-05-08 |2026-05-23  |2026-07-01   |25          |
# +--------+----------+----------+-----------+------------+-------------+------------+

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: trunc, date_trunc, make_date, make_timestamp
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: trunc(), date_trunc(), make_date(), make_timestamp()
# ============================================================
# Real-world: Bucketing events to month/year and constructing dates from dimensions.

from pyspark.sql.functions import trunc, date_trunc, make_date, make_timestamp  # Import constructors and truncation helpers.

# Build data from dimensional parts.
parts_df = spark.createDataFrame(  # Create year/month/day/hour/minute/second components.
    [
        (2026, 5, 26, 14, 45, 30),  # One complete timestamp.
        (2025, 1, 1, 0, 0, 0),  # Beginning of year.
        (2024, 12, 31, 23, 59, 59),  # End of year.
    ],  # End rows.
    ["y", "m", "d", "hh", "mm", "ss"]  # Column names.
)  # End creation.

# Construct date and timestamp columns from pieces.
constructed_df = parts_df.withColumn("built_date", make_date(col("y"), col("m"), col("d")))  # Build DATE.
constructed_df = constructed_df.withColumn("built_ts", make_timestamp(col("y"), col("m"), col("d"), col("hh"), col("mm"), col("ss")))  # Build TIMESTAMP.

# Truncate to month, year, hour, and day buckets.
truncated_df = constructed_df.select(  # Select constructed and truncated values.
    col("built_date"),  # Keep built date.
    col("built_ts"),  # Keep built timestamp.
    trunc(col("built_date"), "month").alias("month_start"),  # First day of month.
    trunc(col("built_date"), "year").alias("year_start"),  # First day of year.
    date_trunc("day", col("built_ts")).alias("day_bucket"),  # Truncate timestamp to day.
    date_trunc("hour", col("built_ts")).alias("hour_bucket"),  # Truncate timestamp to hour.
    date_trunc("month", col("built_ts")).alias("ts_month_bucket")  # Truncate timestamp to month.
)  # End select.

# Show results.
print("=== Constructing and Truncating Dates/Timestamps ===")  # Print heading.
truncated_df.show(truncate=False)  # Display results.

# Expected Output:
# +----------+-------------------+-----------+----------+-------------------+-------------------+
# |built_date|built_ts           |month_start|year_start|day_bucket         |hour_bucket        |
# +----------+-------------------+-----------+----------+-------------------+-------------------+
# |2026-05-26|2026-05-26 14:45:30|2026-05-01 |2026-01-01|2026-05-26 00:00:00|2026-05-26 14:00:00|
# +----------+-------------------+-----------+----------+-------------------+-------------------+

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Unix time and timezone conversion
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Unix Time and Timezone Conversion
# ============================================================
# Real-world: Standardizing event timestamps from global systems.

from pyspark.sql.functions import (  # Import unix and timezone functions.
    unix_timestamp, from_unixtime, to_utc_timestamp, from_utc_timestamp
)  # End import list.

# Example local timestamps from a Europe/Berlin system.
tz_df = spark.createDataFrame(  # Build sample local timestamp strings.
    [
        (1, "2026-05-26 10:00:00"),  # Morning local time.
        (2, "2026-12-26 18:30:00"),  # Winter date example.
    ],  # End rows.
    ["event_id", "local_ts_str"]  # Column names.
)  # End creation.

# Parse local string timestamps.
tz_df = tz_df.withColumn("local_ts", to_timestamp(col("local_ts_str"), "yyyy-MM-dd HH:mm:ss"))  # Parse timestamp.

# Convert to Unix epoch seconds and back.
tz_df = tz_df.withColumn("epoch_seconds", unix_timestamp(col("local_ts")))  # Convert to epoch seconds.
tz_df = tz_df.withColumn("epoch_as_text", from_unixtime(col("epoch_seconds")))  # Convert epoch to text.

# Convert between local time and UTC.
tz_df = tz_df.withColumn("utc_ts", to_utc_timestamp(col("local_ts"), "Europe/Berlin"))  # Convert Berlin local time to UTC.
tz_df = tz_df.withColumn("new_york_ts", from_utc_timestamp(col("utc_ts"), "America/New_York"))  # Show same instant in New York.

time_df = tz_df.select(  # Select final view.
    col("event_id"),  # Keep id.
    col("local_ts"),  # Original local timestamp.
    col("epoch_seconds"),  # Epoch representation.
    col("epoch_as_text"),  # Epoch back to formatted text.
    col("utc_ts"),  # UTC instant.
    col("new_york_ts")  # Converted timezone.
)  # End select.

# Show timezone results.
print("=== Unix Time and Timezone Conversion ===")  # Print heading.
time_df.show(truncate=False)  # Display output.

# Expected Output:
# +--------+-------------------+-------------+-------------------+-------------------+-------------------+
# |event_id|local_ts           |epoch_seconds|epoch_as_text      |utc_ts             |new_york_ts        |
# +--------+-------------------+-------------+-------------------+-------------------+-------------------+
# |1       |2026-05-26 10:00:00|...          |2026-05-26 10:00:00|2026-05-26 08:00:00|2026-05-26 04:00:00|
# +--------+-------------------+-------------+-------------------+-------------------+-------------------+
# Note: Offset differs by daylight saving rules.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Calendar and cohort analysis
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Calendar and Cohort Analysis
# ============================================================
# Real-world: Subscription retention and monthly cohort reporting.

from pyspark.sql.functions import (  # Import cohort analysis helpers.
    count, countDistinct, min as _min, max as _max,  # Aggregate helpers.
    when, months_between, floor, concat_ws, lpad  # Cohort calculations.
)  # End import list.

# Build subscription events with signup and activity dates.
cohort_df = spark.createDataFrame(  # Create sample user lifecycle data.
    [
        (1, "2025-01-15", "2025-01-20"),  # Month 0 activity.
        (1, "2025-01-15", "2025-02-18"),  # Month 1 activity.
        (1, "2025-01-15", "2025-03-05"),  # Month 2 activity.
        (2, "2025-01-22", "2025-01-25"),  # Month 0 activity.
        (2, "2025-01-22", "2025-03-01"),  # Month 1+ activity.
        (3, "2025-02-05", "2025-02-28"),  # February cohort.
        (3, "2025-02-05", "2025-03-21"),  # Next month activity.
        (4, "2025-03-01", "2025-03-15"),  # March cohort.
    ],  # End rows.
    ["user_id", "signup_str", "activity_str"]  # Column names.
)  # End creation.

# Parse dates and compute cohort buckets.
cohort_df = cohort_df.withColumn("signup_date", to_date(col("signup_str"), "yyyy-MM-dd"))  # Parse signup date.
cohort_df = cohort_df.withColumn("activity_date", to_date(col("activity_str"), "yyyy-MM-dd"))  # Parse activity date.
cohort_df = cohort_df.withColumn("cohort_month", date_format(trunc(col("signup_date"), "month"), "yyyy-MM"))  # Cohort label.
cohort_df = cohort_df.withColumn("months_since_signup", floor(months_between(trunc(col("activity_date"), "month"), trunc(col("signup_date"), "month"))))  # Cohort age.

# Aggregate active users by cohort month and elapsed month.
cohort_summary = cohort_df.groupBy("cohort_month", "months_since_signup").agg(  # Aggregate retention counts.
    countDistinct("user_id").alias("active_users")  # Unique active users.
).orderBy("cohort_month", "months_since_signup")  # Sort for readability.

# Show cohort result.
print("=== Cohort Analysis ===")  # Print heading.
cohort_summary.show(truncate=False)  # Display summary.

# Pivot the cohort table for heatmap-style reporting.
print("=== Cohort Matrix ===")  # Print second heading.
cohort_summary.groupBy("cohort_month").pivot("months_since_signup", [0, 1, 2, 3]).sum("active_users").show(truncate=False)  # Pivot cohort ages.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: window() for time bucketing
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: window() for Time Bucketing
# ============================================================
# Real-world: Grouping clickstream or sensor events into 15-minute windows.

from pyspark.sql.functions import window, sum as _sum, avg, count  # Import window aggregation helpers.

# Build event data with timestamps and values.
events_df = spark.createDataFrame(  # Create sample event stream.
    [
        ("sensor_a", "2026-05-26 10:00:00", 10.0),  # Event 1.
        ("sensor_a", "2026-05-26 10:07:00", 15.0),  # Same 15-minute window.
        ("sensor_a", "2026-05-26 10:18:00", 12.0),  # Next 15-minute window.
        ("sensor_b", "2026-05-26 10:03:00", 20.0),  # Sensor B event.
        ("sensor_b", "2026-05-26 10:11:00", 18.0),  # Same 15-minute bucket.
        ("sensor_b", "2026-05-26 10:29:00", 25.0),  # Third bucket.
    ],  # End rows.
    ["sensor_id", "event_ts_str", "reading"]  # Column names.
)  # End creation.

# Parse timestamps.
events_df = events_df.withColumn("event_ts", to_timestamp(col("event_ts_str"), "yyyy-MM-dd HH:mm:ss"))  # Parse timestamp.

# Group into 15-minute windows by sensor.
windowed_df = events_df.groupBy(  # Start grouping.
    col("sensor_id"),  # Group by sensor.
    window(col("event_ts"), "15 minutes")  # Group into 15-minute windows.
).agg(  # Compute window metrics.
    count("*").alias("events"),  # Count events in each window.
    _sum("reading").alias("sum_reading"),  # Total reading value.
    avg("reading").alias("avg_reading")  # Average reading.
).orderBy("sensor_id", "window")  # Sort results.

# Show bucketed results.
print("=== Time Window Aggregation ===")  # Print heading.
windowed_df.show(truncate=False)  # Display window structure.

# Flatten window struct to explicit start/end columns.
print("=== Flattened Window Result ===")  # Print second heading.
windowed_df.select(  # Select explicit output columns.
    col("sensor_id"),  # Keep sensor.
    col("window.start").alias("window_start"),  # Extract window start.
    col("window.end").alias("window_end"),  # Extract window end.
    col("events"),  # Keep event count.
    col("sum_reading"),  # Keep sum.
    col("avg_reading")  # Keep average.
).show(truncate=False)  # Display flattened output.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production date quality toolkit
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Production Date Quality Toolkit
# ============================================================
# Real-world: Standardizing messy date inputs from multiple source systems.

from pyspark.sql.functions import (  # Import helpers for production cleaning.
    coalesce, when, isnan, regexp_replace, length, expr  # Quality helpers.
)  # End import list.

# Build messy inbound date strings from different systems.
messy_df = spark.createDataFrame(  # Create mixed-format raw data.
    [
        (1, "2026-05-26", "2026-05-26 10:00:00"),  # ISO format.
        (2, "26/05/2026", "26/05/2026 10:15:00"),  # Slash format.
        (3, "05-26-2026", "05-26-2026 11:30:45"),  # US dash format.
        (4, "bad_date", "not_a_ts"),  # Invalid format.
        (5, None, None),  # Null values.
    ],  # End rows.
    ["row_id", "raw_date", "raw_ts"]  # Column names.
)  # End creation.

# Parse with multiple fallback patterns using coalesce.
quality_df = messy_df.withColumn(  # Create standardized date.
    "standard_date",
    coalesce(  # Use first successful parse.
        to_date(col("raw_date"), "yyyy-MM-dd"),  # ISO format.
        to_date(col("raw_date"), "dd/MM/yyyy"),  # European slash format.
        to_date(col("raw_date"), "MM-dd-yyyy")  # US dash format.
    )  # End coalesce.
)  # End withColumn.

quality_df = quality_df.withColumn(  # Create standardized timestamp.
    "standard_ts",
    coalesce(  # Use first successful timestamp parse.
        to_timestamp(col("raw_ts"), "yyyy-MM-dd HH:mm:ss"),  # ISO timestamp.
        to_timestamp(col("raw_ts"), "dd/MM/yyyy HH:mm:ss"),  # Slash timestamp.
        to_timestamp(col("raw_ts"), "MM-dd-yyyy HH:mm:ss")  # US dash timestamp.
    )  # End coalesce.
)  # End withColumn.

quality_df = quality_df.withColumn(  # Flag invalid date parse.
    "date_parse_status",
    when(col("raw_date").isNull(), "missing")  # Missing raw date.
    .when(col("standard_date").isNull(), "invalid")  # Failed parse.
    .otherwise("valid")  # Successfully parsed.
)  # End status column.

quality_df = quality_df.withColumn(  # Flag invalid timestamp parse.
    "ts_parse_status",
    when(col("raw_ts").isNull(), "missing")  # Missing raw timestamp.
    .when(col("standard_ts").isNull(), "invalid")  # Failed timestamp parse.
    .otherwise("valid")  # Successfully parsed.
)  # End status column.

# Show cleaned and validated results.
print("=== Production Date Quality Toolkit ===")  # Print heading.
quality_df.show(truncate=False)  # Display standardized data.

# Build a mini quality report.
print("=== Quality Summary ===")  # Print second heading.
quality_df.groupBy("date_parse_status", "ts_parse_status").count().show(truncate=False)  # Show quality counts.

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Date Time Functions
# MAGIC
# MAGIC ### Mistake 1: Treating strings like dates
# MAGIC **Problem:** Comparing raw strings such as `"31.12.2025" > "01.01.2026"` gives lexicographic results, not calendar logic.  
# MAGIC **Fix:** Parse to `DATE` or `TIMESTAMP` first with `to_date()` or `to_timestamp()`.
# MAGIC
# MAGIC ### Mistake 2: Forgetting the format string
# MAGIC **Problem:** `to_date("26.05.2026")` without `"dd.MM.yyyy"` often returns NULL because Spark expects ISO-style input by default.  
# MAGIC **Fix:** Always specify the pattern when the input is not already `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss`.
# MAGIC
# MAGIC ### Mistake 3: Confusing `datediff` and `months_between`
# MAGIC **Problem:** `datediff()` returns integer days, while `months_between()` returns fractional months.  
# MAGIC **Fix:** Use `datediff` for day gaps and `months_between` for month-based tenure calculations.
# MAGIC
# MAGIC ### Mistake 4: Ignoring time zones
# MAGIC **Problem:** Comparing timestamps from Berlin, UTC, and New York as if they were all local clock time creates false delays and wrong daily buckets.  
# MAGIC **Fix:** Standardize to UTC first, then convert to local display zones only when needed.
# MAGIC
# MAGIC ### Mistake 5: Using `limit()` or `window()` without understanding time bucketing
# MAGIC **Problem:** Analysts sometimes think `window()` is a row window like SQL analytic windows. It is not. It creates time buckets.  
# MAGIC **Fix:** Use `window()` for event-time grouping, and use `pyspark.sql.window.Window` for ranking/running totals.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Date Time Function Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste
# MAGIC * Create a DataFrame with string dates and parse them using `to_date()`.
# MAGIC * Add `current_date()` and `current_timestamp()` columns.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC * Extract `year`, `month`, and `dayofmonth` from a parsed date.
# MAGIC * Format a timestamp using `date_format()`.
# MAGIC
# MAGIC ### Level 3 — Combine
# MAGIC * Parse two dates, calculate `datediff()`, and also compute `months_between()`.
# MAGIC * Build a `month_start` column with `trunc()` and a `month_end` column with `last_day()`.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC * Build a hotel booking dataset with `check_in` and `check_out`. Calculate stay length, booking month, and next cleaning date.
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC * Build a subscription dashboard with:
# MAGIC   * signup month
# MAGIC   * first activity date
# MAGIC   * last activity date
# MAGIC   * tenure in months
# MAGIC   * active/inactive flag based on recent activity
# MAGIC
# MAGIC ### Level 6 — Design
# MAGIC * Create a reusable function that standardizes mixed raw date formats into one clean `DATE` column and one parse-status column.
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC * Compare grouping by raw timestamp vs `date_trunc('hour', ts)` vs `window(ts, '1 hour')` on a larger synthetic dataset.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC * Test leap years, month-end arithmetic, daylight saving transitions, invalid dates, and NULL timestamps.
# MAGIC
# MAGIC ### Level 9 — Production
# MAGIC * Build a time-zone-safe event pipeline:
# MAGIC   * ingest local timestamps
# MAGIC   * convert to UTC
# MAGIC   * bucket into 15-minute windows
# MAGIC   * aggregate counts
# MAGIC   * output both UTC and business-local reporting fields
# MAGIC
# MAGIC ### Level 10 — Teach
# MAGIC * Create a one-page cheat sheet explaining when to use `to_date`, `to_timestamp`, `datediff`, `months_between`, `trunc`, `date_trunc`, and `window`.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import dayofmonth  # Import one extra helper for solutions.

# Create a small booking example.
booking_df = spark.createDataFrame(  # Build booking rows.
    [
        (1, "2026-06-01", "2026-06-05"),  # Four-night stay.
        (2, "2026-06-10", "2026-06-12"),  # Two-night stay.
        (3, "2026-07-01", "2026-07-10"),  # Nine-night stay.
    ],  # End rows.
    ["booking_id", "check_in_str", "check_out_str"]  # Column names.
)  # End creation.

# Parse booking dates.
booking_df = booking_df.withColumn("check_in", to_date(col("check_in_str"), "yyyy-MM-dd"))  # Parse check-in.
booking_df = booking_df.withColumn("check_out", to_date(col("check_out_str"), "yyyy-MM-dd"))  # Parse check-out.

# Build useful reporting columns.
solution_df = booking_df.select(  # Select final solution columns.
    col("booking_id"),  # Keep booking id.
    col("check_in"),  # Keep check-in date.
    col("check_out"),  # Keep check-out date.
    datediff(col("check_out"), col("check_in")).alias("stay_nights"),  # Compute stay length.
    date_format(col("check_in"), "yyyy-MM").alias("booking_month"),  # Booking month label.
    last_day(col("check_in")).alias("month_end"),  # Month-end date.
    next_day(col("check_out"), "Mon").alias("next_cleaning_monday")  # Next Monday after checkout.
)  # End select.

# Show booking solution output.
print("=== Booking Homework Solution ===")  # Print heading.
solution_df.show(truncate=False)  # Display booking result.

# Create a mixed raw-date standardization example.
raw_solution_df = spark.createDataFrame(  # Build mixed-format rows.
    [
        (1, "2026-05-26"),  # ISO format.
        (2, "26/05/2026"),  # Slash format.
        (3, "05-26-2026"),  # US format.
        (4, "bad"),  # Invalid value.
    ],  # End rows.
    ["row_id", "raw_date"]  # Column names.
)  # End creation.

# Standardize raw date values.
raw_solution_df = raw_solution_df.withColumn(  # Create cleaned date.
    "clean_date",
    coalesce(  # Use first successful parser.
        to_date(col("raw_date"), "yyyy-MM-dd"),  # ISO parser.
        to_date(col("raw_date"), "dd/MM/yyyy"),  # Slash parser.
        to_date(col("raw_date"), "MM-dd-yyyy")  # US parser.
    )  # End coalesce.
)  # End withColumn.

raw_solution_df = raw_solution_df.withColumn(  # Create parse status.
    "status",
    when(col("clean_date").isNull(), "invalid").otherwise("valid")  # Validity flag.
)  # End status column.

# Show standardization result.
print("=== Standardization Homework Solution ===")  # Print heading.
raw_solution_df.show(truncate=False)  # Display cleaned dates.

print("✅ All homework solutions complete!")  # Print completion message.