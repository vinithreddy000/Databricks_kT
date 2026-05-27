# Databricks notebook source
# DBTITLE 1,Header
# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # Notebook 27: Window Functions — Complete Guide
# MAGIC # Module: DataFrame Operations
# MAGIC # Language: Python (PySpark)
# MAGIC # Cluster: Standard DBR (Serverless compatible)
# MAGIC # Estimated time: 60 minutes
# MAGIC # ═══════════════════════════════════════════════════════

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Is This?
# MAGIC %md
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Real-World Analogy: Class Rankings
# MAGIC
# MAGIC Imagine a school report card:
# MAGIC - `groupBy` = "What's the average score per class?" (one row per class)
# MAGIC - `Window function` = "What's each student's RANK within their class?" (keeps ALL rows!)
# MAGIC
# MAGIC Window functions compute values **across a group of rows related to the current row** — without collapsing rows like groupBy does.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Three Families of Window Functions
# MAGIC
# MAGIC | Family | Functions | What They Do |
# MAGIC |--------|----------|--------------|
# MAGIC | **Ranking** | `row_number`, `rank`, `dense_rank`, `ntile`, `percent_rank`, `cume_dist` | Assign position/rank |
# MAGIC | **Navigation** | `lag`, `lead`, `first`, `last` | Look at previous/next rows |
# MAGIC | **Aggregate** | `sum`, `avg`, `count`, `min`, `max` over window | Running totals, moving averages |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Window Definition
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC window = Window.partitionBy("dept")  # Like GROUP BY but keeps all rows
# MAGIC                .orderBy("salary")    # Order within each partition
# MAGIC                .rowsBetween(-2, 0)   # Optional: frame (last 3 rows)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How It Works
# MAGIC %md
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ### Window vs GroupBy
# MAGIC
# MAGIC ```
# MAGIC groupBy("dept").avg("salary"):
# MAGIC   Input:  [Alice-Eng-95K, Bob-Eng-80K, Charlie-Mkt-70K]
# MAGIC   Output: [Eng-87.5K, Mkt-70K]    ← COLLAPSES rows!
# MAGIC
# MAGIC Window avg("salary").over(partitionBy("dept")):
# MAGIC   Input:  [Alice-Eng-95K, Bob-Eng-80K, Charlie-Mkt-70K]
# MAGIC   Output: [Alice-Eng-95K-avg:87.5K, Bob-Eng-80K-avg:87.5K, Charlie-Mkt-70K-avg:70K]
# MAGIC   ← KEEPS all rows, adds computed column!
# MAGIC ```
# MAGIC
# MAGIC ### Window Frame: rowsBetween vs rangeBetween
# MAGIC
# MAGIC ```
# MAGIC rowsBetween(start, end):  Counts by physical ROW position
# MAGIC   rowsBetween(-1, 1) = previous row, current row, next row
# MAGIC   rowsBetween(Window.unboundedPreceding, 0) = all rows up to current = running total
# MAGIC
# MAGIC rangeBetween(start, end): Counts by VALUE range
# MAGIC   rangeBetween(-7, 0) with orderBy(date) = all rows within 7 days before current
# MAGIC
# MAGIC Constants:
# MAGIC   Window.unboundedPreceding = from the very first row
# MAGIC   Window.unboundedFollowing = to the very last row
# MAGIC   Window.currentRow = 0 = current row
# MAGIC ```
# MAGIC
# MAGIC ### Ranking Functions Comparison
# MAGIC
# MAGIC ```
# MAGIC Scores: [100, 90, 90, 80, 70]
# MAGIC
# MAGIC row_number: 1, 2, 3, 4, 5   (always unique, arbitrary tiebreak)
# MAGIC rank:       1, 2, 2, 4, 5   (ties get same rank, skips next)
# MAGIC dense_rank: 1, 2, 2, 3, 4   (ties get same rank, no skip)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: Ranking functions
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 1: Ranking functions
# ═══════════════════════════════════════════════════════

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, ntile, percent_rank, cume_dist, col, desc

print("=== Ranking Functions ===")
print()

# Sample data: employees with salaries
data = [
    ("Alice", "Engineering", 110000),
    ("Bob", "Engineering", 95000),
    ("Charlie", "Engineering", 95000),  # Tie with Bob!
    ("Diana", "Engineering", 80000),
    ("Eve", "Marketing", 92000),
    ("Frank", "Marketing", 78000),
    ("Grace", "Marketing", 85000),
]
df = spark.createDataFrame(data, ["name", "dept", "salary"])

# --- Window: partition by dept, order by salary descending ---
window = Window.partitionBy("dept").orderBy(desc("salary"))

# --- All ranking functions ---
result = df.select(
    "name", "dept", "salary",
    row_number().over(window).alias("row_num"),      # Always unique (1,2,3,4)
    rank().over(window).alias("rank"),               # Ties same rank, skips (1,2,2,4)
    dense_rank().over(window).alias("dense_rank"),   # Ties same rank, no skip (1,2,2,3)
    ntile(2).over(window).alias("ntile_2"),          # Split into 2 groups
    percent_rank().over(window).alias("pct_rank"),   # 0.0 to 1.0 percentile
    cume_dist().over(window).alias("cume_dist"),     # Cumulative distribution
)

result.show()
print("--- Key observations (Engineering dept): ---")
print("  Bob & Charlie tied at 95K:")
print("    row_number: 2, 3 (arbitrary tiebreak)")
print("    rank: 2, 2 (both rank 2, next is 4)")
print("    dense_rank: 2, 2 (both rank 2, next is 3)")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: lag and lead (navigation)
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 2: lag() and lead()
# ═══════════════════════════════════════════════════════

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead, col

print("=== lag() and lead() — Look at Previous/Next Rows ===")
print()
print("lag(col, N)  = value from N rows BEFORE current")
print("lead(col, N) = value from N rows AFTER current")
print()

# Monthly revenue data
monthly = spark.createDataFrame([
    ("2024-01", 10000), ("2024-02", 12000), ("2024-03", 11500),
    ("2024-04", 13000), ("2024-05", 14500), ("2024-06", 13500),
], ["month", "revenue"])

# Window ordered by month
window = Window.orderBy("month")

# --- lag: previous month's revenue ---
result = monthly.select(
    "month",
    "revenue",
    lag("revenue", 1).over(window).alias("prev_month"),    # 1 row back
    lead("revenue", 1).over(window).alias("next_month"),   # 1 row forward
    lag("revenue", 1, 0).over(window).alias("prev_or_0"),  # Default 0 if no prev
)
result.show()

# --- Calculate month-over-month change ---
print("--- Month-over-Month Change ---")
monthly.select(
    "month",
    "revenue",
    lag("revenue", 1).over(window).alias("prev_revenue"),
    (col("revenue") - lag("revenue", 1).over(window)).alias("change"),  # Diff
    ((col("revenue") - lag("revenue", 1).over(window)) /
     lag("revenue", 1).over(window) * 100).alias("pct_change"),  # % change
).show()
print("  lag() is essential for time-series comparisons!")

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: Running total and moving average
# ═══════════════════════════════════════════════════════
# SECTION 3 — Beginner Example 3: Running total and moving average
# ═══════════════════════════════════════════════════════

from pyspark.sql.window import Window
from pyspark.sql.functions import sum as _sum, avg, col, round as _round

print("=== Running Total and Moving Average ===")
print()

# Daily sales data
sales = spark.createDataFrame([
    ("2024-01-01", 100), ("2024-01-02", 150), ("2024-01-03", 120),
    ("2024-01-04", 200), ("2024-01-05", 180), ("2024-01-06", 160),
    ("2024-01-07", 210),
], ["date", "amount"])

# --- Running total: sum of all rows up to current ---
print("--- 1. Running Total (cumulative sum) ---")
running_window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

sales.select(
    "date", "amount",
    _sum("amount").over(running_window).alias("running_total"),  # Cumulative sum
).show()
print("  rowsBetween(unboundedPreceding, currentRow) = all rows up to now")

# --- Moving average: average of last 3 days ---
print("--- 2. Moving Average (last 3 days) ---")
moving_window = Window.orderBy("date").rowsBetween(-2, 0)  # Current + 2 before = 3 rows

sales.select(
    "date", "amount",
    _round(avg("amount").over(moving_window), 1).alias("3_day_avg"),  # Avg of 3 rows
).show()
print("  rowsBetween(-2, 0) = current row + 2 previous = 3-day window")

# --- Running max ---
print("--- 3. Running Max (best day so far) ---")
from pyspark.sql.functions import max as _max
sales.select(
    "date", "amount",
    _max("amount").over(running_window).alias("best_so_far"),  # Max up to now
).show()

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: rowsBetween vs rangeBetween
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: rowsBetween vs rangeBetween
# ═══════════════════════════════════════════════════════

from pyspark.sql.window import Window
from pyspark.sql.functions import sum as _sum, col, count

print("=== rowsBetween vs rangeBetween ===")
print()
print("rowsBetween: counts by PHYSICAL row position")
print("rangeBetween: counts by VALUE range of the order column")
print()

# Data with a gap (no day 3!)
data = [
    (1, 100), (2, 200), (4, 300), (5, 400), (7, 500),  # Days 3, 6 missing!
]
df = spark.createDataFrame(data, ["day", "amount"])

# --- rowsBetween(-1, 1) = exactly 3 physical rows ---
print("--- rowsBetween(-1, 1): 3 physical rows (prev, current, next) ---")
row_win = Window.orderBy("day").rowsBetween(-1, 1)
df.select("day", "amount",
    _sum("amount").over(row_win).alias("sum_3_rows"),  # Sum of 3 physical rows
    count("amount").over(row_win).alias("count_rows"),  # Always 3 (or fewer at edges)
).show()

# --- rangeBetween(-1, 1) = days within value range ---
print("--- rangeBetween(-1, 1): days within ±1 of current day VALUE ---")
range_win = Window.orderBy("day").rangeBetween(-1, 1)
df.select("day", "amount",
    _sum("amount").over(range_win).alias("sum_range"),   # Sum of day±1
    count("amount").over(range_win).alias("count_range"),  # Varies with gaps!
).show()

print("--- Key difference ---")
print("  Day 4: rowsBetween(-1,1) includes rows at positions 2,3,4")
print("         rangeBetween(-1,1) includes days 3,4,5 (by VALUE)")
print("  With gaps in data, rangeBetween gives different results!")
print("  Use rangeBetween for time-based windows (e.g., last 7 days)")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: Year-over-Year comparison
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: Year-over-Year with lag()
# ═══════════════════════════════════════════════════════

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col, round as _round

print("=== Year-over-Year Comparison with lag() ===")
print()

# Quarterly revenue by region
revenue = spark.createDataFrame([
    ("US", "2022-Q1", 1000), ("US", "2022-Q2", 1200),
    ("US", "2022-Q3", 1100), ("US", "2022-Q4", 1400),
    ("US", "2023-Q1", 1150), ("US", "2023-Q2", 1350),
    ("US", "2023-Q3", 1250), ("US", "2023-Q4", 1600),
    ("EU", "2022-Q1", 800), ("EU", "2022-Q2", 900),
    ("EU", "2022-Q3", 850), ("EU", "2022-Q4", 1000),
    ("EU", "2023-Q1", 900), ("EU", "2023-Q2", 1050),
    ("EU", "2023-Q3", 980), ("EU", "2023-Q4", 1200),
], ["region", "quarter", "revenue"])

# Window: partition by region, order by quarter
window = Window.partitionBy("region").orderBy("quarter")

# --- YoY: compare with same quarter last year (lag 4 quarters) ---
result = revenue.select(
    "region", "quarter", "revenue",
    lag("revenue", 4).over(window).alias("same_q_last_year"),  # 4 quarters back
    _round(
        (col("revenue") - lag("revenue", 4).over(window)) /
        lag("revenue", 4).over(window) * 100, 1
    ).alias("yoy_pct"),  # YoY percentage change
)

print("--- Year-over-Year Revenue Comparison ---")
result.filter(col("same_q_last_year").isNotNull()).show()  # Filter out first year
print("  lag(4) = same quarter previous year")
print("  YoY% shows growth rate per quarter")

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: first, last, and dedup with window
# ═══════════════════════════════════════════════════════
# SECTION 4 — Intermediate: first(), last(), dedup with window
# ═══════════════════════════════════════════════════════

from pyspark.sql.window import Window
from pyspark.sql.functions import first, last, row_number, col, desc

print("=== first(), last(), and Dedup Pattern ===")
print()

# --- first() and last() over window ---
print("--- 1. first() and last() ---")
data = [
    ("Alice", "2024-01-01", 100), ("Alice", "2024-02-01", 150),
    ("Alice", "2024-03-01", 120), ("Bob", "2024-01-01", 200),
    ("Bob", "2024-02-01", 250),
]
df = spark.createDataFrame(data, ["name", "date", "score"])

window = Window.partitionBy("name").orderBy("date").rowsBetween(
    Window.unboundedPreceding, Window.unboundedFollowing  # Entire partition
)

df.select(
    "name", "date", "score",
    first("score").over(window).alias("first_score"),  # First score ever
    last("score").over(window).alias("last_score"),    # Latest score
).show()

# --- Dedup pattern: Keep latest per user ---
print("--- 2. Dedup: Keep LATEST row per user ---")
events = spark.createDataFrame([
    (1, "Alice", "v1@mail.com", "2024-01-01"),
    (1, "Alice", "v2@mail.com", "2024-06-15"),  # Latest!
    (2, "Bob", "bob@mail.com", "2024-03-20"),
    (2, "Bob", "bob_new@mail.com", "2024-09-01"),  # Latest!
], ["user_id", "name", "email", "updated_at"])

dedup_window = Window.partitionBy("user_id").orderBy(desc("updated_at"))  # Latest first

deduped = (
    events
    .withColumn("rn", row_number().over(dedup_window))  # Rank: 1 = latest
    .filter(col("rn") == 1)   # Keep only latest
    .drop("rn")               # Cleanup
)
deduped.show()
print("  THE most common Window pattern in production!")
print("  Use for: CDC dedup, keeping latest record, SCD processing")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Complex window analytics
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Complex window analytics
# ═══════════════════════════════════════════════════════

from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum as _sum, avg, count, dense_rank, desc, round as _round, percent_rank

print("=== Complex Window Analytics ===")
print()

# Sales data by rep, region, date
sales = spark.createDataFrame([
    ("Alice", "US", "2024-01", 5000), ("Alice", "US", "2024-02", 6000),
    ("Alice", "US", "2024-03", 5500), ("Bob", "US", "2024-01", 4000),
    ("Bob", "US", "2024-02", 4500), ("Bob", "US", "2024-03", 7000),
    ("Charlie", "EU", "2024-01", 3500), ("Charlie", "EU", "2024-02", 4000),
    ("Charlie", "EU", "2024-03", 4500), ("Diana", "EU", "2024-01", 3000),
    ("Diana", "EU", "2024-02", 3200), ("Diana", "EU", "2024-03", 3800),
], ["rep", "region", "month", "revenue"])

# --- Multiple window definitions ---
rep_window = Window.partitionBy("rep").orderBy("month")  # Per rep over time
region_window = Window.partitionBy("region")              # Per region (all months)
region_rank_window = Window.partitionBy("region").orderBy(desc("total_rev"))  # For ranking

# --- Running total per rep ---
print("--- Running total + rank per region ---")
result = (
    sales
    # Running total per rep
    .withColumn("running_total", _sum("revenue").over(rep_window))
    # Each rep's total revenue (for ranking)
    .withColumn("rep_total", _sum("revenue").over(Window.partitionBy("rep")))
    # Percentage of regional total
    .withColumn("region_total", _sum("revenue").over(region_window))
    .withColumn("pct_of_region", _round(col("rep_total") / col("region_total") * 100, 1))
)
result.select("rep", "region", "month", "revenue", "running_total", "pct_of_region").show()

# --- Rank reps within each region ---
print("--- Rep ranking within region ---")
rep_totals = sales.groupBy("rep", "region").agg(_sum("revenue").alias("total_rev"))
rep_totals.withColumn(
    "region_rank", dense_rank().over(Window.partitionBy("region").orderBy(desc("total_rev")))
).show()

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: Sessionization
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Sessionization with Window
# ═══════════════════════════════════════════════════════

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, when, sum as _sum, unix_timestamp, to_timestamp

print("=== Sessionization: Grouping Events into Sessions ===")
print()
print("A 'session' = consecutive events within 30 min of each other.")
print("New session starts when gap > 30 minutes.")
print()

# Clickstream data
clicks = spark.createDataFrame([
    ("user_1", "2024-01-01 10:00:00", "page_A"),
    ("user_1", "2024-01-01 10:05:00", "page_B"),  # 5 min gap (same session)
    ("user_1", "2024-01-01 10:20:00", "page_C"),  # 15 min gap (same session)
    ("user_1", "2024-01-01 14:00:00", "page_D"),  # 3.5 hr gap (NEW session!)
    ("user_1", "2024-01-01 14:10:00", "page_E"),  # 10 min gap (same session)
    ("user_2", "2024-01-01 09:00:00", "page_A"),
    ("user_2", "2024-01-01 09:45:00", "page_B"),  # 45 min gap (NEW session!)
], ["user_id", "event_time", "page"])

clicks = clicks.withColumn("event_time", to_timestamp("event_time"))  # Cast to timestamp

# --- Step 1: Calculate time gap from previous event ---
user_window = Window.partitionBy("user_id").orderBy("event_time")

clicks_with_gap = clicks.withColumn(
    "prev_time", lag("event_time", 1).over(user_window)  # Previous event time
).withColumn(
    "gap_seconds",
    unix_timestamp("event_time") - unix_timestamp("prev_time")  # Gap in seconds
)

# --- Step 2: Mark new session starts (gap > 30 min) ---
clicks_marked = clicks_with_gap.withColumn(
    "new_session",
    when((col("gap_seconds") > 1800) | col("gap_seconds").isNull(), 1)  # 1800s = 30 min
    .otherwise(0)
)

# --- Step 3: Assign session IDs (running sum of new_session flags) ---
result = clicks_marked.withColumn(
    "session_id",
    _sum("new_session").over(user_window)  # Cumulative sum = session counter
)

result.select("user_id", "event_time", "page", "gap_seconds", "new_session", "session_id").show(truncate=False)
print("  user_1: events 1-3 = session 1, events 4-5 = session 2")
print("  user_2: event 1 = session 1, event 2 = session 2 (45min gap)")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production window toolkit
# ═══════════════════════════════════════════════════════
# SECTION 5 — Advanced: Production window function toolkit
# ═══════════════════════════════════════════════════════

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, dense_rank, lag, lead, sum as _sum, avg, desc, first, last, count

print("=== Production Window Patterns Cheat Sheet ===")
print()

# Sample data
df = spark.createDataFrame([
    ("Alice","Eng",95000,"2024-01"),("Alice","Eng",98000,"2024-06"),
    ("Bob","Eng",80000,"2024-01"),("Charlie","Mkt",72000,"2024-01"),
    ("Diana","Mkt",85000,"2024-01"),("Eve","Mkt",78000,"2024-01"),
], ["name","dept","salary","date"])

# --- Pattern 1: Top-N per group ---
print("--- Pattern 1: Top 2 earners per dept ---")
w_rank = Window.partitionBy("dept").orderBy(desc("salary"))
df.withColumn("rank", dense_rank().over(w_rank)).filter(col("rank") <= 2).show()

# --- Pattern 2: Percentage of group total ---
print("--- Pattern 2: Salary as % of dept total ---")
w_dept = Window.partitionBy("dept")
df.select("name", "dept", "salary",
    (_sum("salary").over(w_dept)).alias("dept_total"),
    (col("salary") / _sum("salary").over(w_dept) * 100).alias("pct_of_dept")
).show()

# --- Pattern 3: Gap detection ---
print("--- Pattern 3: Time gap detection ---")
w_user = Window.partitionBy("name").orderBy("date")
df.select("name", "date", "salary",
    lag("date",1).over(w_user).alias("prev_date"),
    (col("salary") - lag("salary",1).over(w_user)).alias("salary_change")
).show()

print("--- Window Functions Summary ---")
print("  Ranking:    row_number, rank, dense_rank, ntile, percent_rank")
print("  Navigation: lag, lead, first, last")
print("  Aggregate:  sum, avg, count, min, max (with frame)")
print("  Frame:      rowsBetween, rangeBetween")
print("  Constants:  unboundedPreceding, currentRow, unboundedFollowing")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes
# MAGIC
# MAGIC ### Mistake #1: Forgetting orderBy in the window
# MAGIC **Problem:** `row_number()` without `orderBy` gives non-deterministic results (random ranking).  
# MAGIC **Fix:** Always specify `Window.partitionBy(...).orderBy(...)` for ranking functions.
# MAGIC
# MAGIC ### Mistake #2: Using wrong frame for running total
# MAGIC **Problem:** Default frame with orderBy is `rowsBetween(unboundedPreceding, currentRow)` for aggregate functions. Without orderBy, it's the entire partition.  
# MAGIC **Fix:** Be explicit: `.rowsBetween(Window.unboundedPreceding, Window.currentRow)` for running totals.
# MAGIC
# MAGIC ### Mistake #3: Confusing rowsBetween and rangeBetween
# MAGIC **Problem:** `rowsBetween(-7, 0)` means "7 physical rows back", not "7 days back".  
# MAGIC **Fix:** For time-based windows, use `rangeBetween` with numeric order columns (e.g., epoch seconds).
# MAGIC
# MAGIC ### Mistake #4: Using row_number() for ties when you need dense_rank()
# MAGIC **Problem:** `row_number()` arbitrarily breaks ties (one row gets rank 1, other gets 2).  
# MAGIC **Fix:** Use `dense_rank()` when tied values should get the same rank.
# MAGIC
# MAGIC ### Mistake #5: Window function without partition on large data
# MAGIC **Problem:** `Window.orderBy("date")` without `partitionBy` = entire dataset in one partition = OOM.  
# MAGIC **Fix:** Always include `partitionBy` unless you truly need a global window (rare).

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework (10 Levels)
# MAGIC
# MAGIC **Level 1 (Copy & Run):** Run the ranking example. Observe the difference between rank and dense_rank.
# MAGIC
# MAGIC **Level 2 (Tiny Change):** Use lag() to compute month-over-month revenue change.
# MAGIC
# MAGIC **Level 3 (Combine Two):** Create a running total AND a moving average (3-day) on the same DataFrame.
# MAGIC
# MAGIC **Level 4 (New Scenario):** Rank students by score within each class. Keep top 3 per class.
# MAGIC
# MAGIC **Level 5 (Mini Project):** Build a complete sales dashboard: running total, MoM change, rank within region.
# MAGIC
# MAGIC **Level 6 (Design First):** Design a sessionization algorithm. Explain how you'd use lag() and cumulative sum.
# MAGIC
# MAGIC **Level 7 (Optimize):** Rewrite a groupBy + self-join pattern using window functions instead.
# MAGIC
# MAGIC **Level 8 (Edge Cases):** What happens with NULLs in orderBy? With ties in row_number? Test and document.
# MAGIC
# MAGIC **Level 9 (Production):** Build a dedup-by-latest function using row_number + filter that handles multiple key columns.
# MAGIC
# MAGIC **Level 10 (Teach It):** Explain window functions to a colleague who only knows GROUP BY. Use the class ranking analogy.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ═══════════════════════════════════════════════════════
# HOMEWORK SOLUTIONS
# ═══════════════════════════════════════════════════════

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, dense_rank, lag, sum as _sum, avg, desc, round as _round

# Level 4: Top 3 students per class
print("=== Level 4: Top 3 per class ===")
students = spark.createDataFrame([
    ("Alice","Math",95),("Bob","Math",88),("Charlie","Math",92),
    ("Diana","Math",85),("Eve","Sci",90),("Frank","Sci",96),
    ("Grace","Sci",88),("Henry","Sci",92),
], ["name","class","score"])

w = Window.partitionBy("class").orderBy(desc("score"))
students.withColumn("rank", dense_rank().over(w)).filter(col("rank") <= 3).show()

# Level 5: Sales dashboard
print("\n=== Level 5: Sales Dashboard ===")
sales = spark.createDataFrame([
    ("US","2024-01",10000),("US","2024-02",12000),("US","2024-03",11000),
    ("EU","2024-01",8000),("EU","2024-02",9000),("EU","2024-03",9500),
], ["region","month","revenue"])

w_region = Window.partitionBy("region").orderBy("month")
w_running = Window.partitionBy("region").orderBy("month").rowsBetween(Window.unboundedPreceding, 0)
w_rank = Window.orderBy(desc("revenue"))  # Global rank

sales.select(
    "region", "month", "revenue",
    _sum("revenue").over(w_running).alias("running_total"),
    lag("revenue",1).over(w_region).alias("prev_month"),
    (col("revenue") - lag("revenue",1).over(w_region)).alias("mom_change"),
).show()

# Level 9: Production dedup
print("\n=== Level 9: Production Dedup ===")
def dedup_latest(df, key_cols, order_col):
    """Keep latest row per composite key."""
    w = Window.partitionBy(*key_cols).orderBy(desc(order_col))
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn")==1).drop("_rn")

dupes = spark.createDataFrame([
    (1,"A","v1","2024-01"),(1,"A","v2","2024-06"),  # Same key, different versions
    (2,"B","v1","2024-03"),
], ["id","cat","data","updated"])

dedup_latest(dupes, ["id","cat"], "updated").show()

print("\n\u2705 All homework solutions complete!")