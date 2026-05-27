# Databricks notebook source
# DBTITLE 1,NB_21 Header
# MAGIC %md
# MAGIC # NB_21 — Column Expressions: The Full Power
# MAGIC
# MAGIC **Module 4: DataFrame Operations** | Notebook 21 of 111
# MAGIC
# MAGIC **Topics Covered:**
# MAGIC - withColumn, withColumns (Spark 3.3+)
# MAGIC - drop, drop multiple columns
# MAGIC - Arithmetic operations (+, -, *, /, %, **)
# MAGIC - String concatenation with concat, concat_ws
# MAGIC - Conditional logic: when/otherwise, coalesce
# MAGIC - Type casting: cast(), astype()
# MAGIC - Complex type access: getItem(), getField()
# MAGIC - Struct manipulation: withField(), dropFields()
# MAGIC - String operations: substr, substring
# MAGIC - Bitwise operations: bitwiseAND, bitwiseOR, bitwiseXOR
# MAGIC - Column aliases and metadata
# MAGIC
# MAGIC **Difficulty:** ⭐⭐⭐ (Core DataFrame Skill)
# MAGIC
# MAGIC ---
# MAGIC *PySpark Databricks Curriculum — sin1hyd@bosch.com*

# COMMAND ----------

# DBTITLE 1,SECTION 1 — What Are Column Expressions?
# MAGIC %md
# MAGIC ## SECTION 1 — What Are Column Expressions? (Real-World Analogy)
# MAGIC
# MAGIC ### 🏭 The Spreadsheet Formula Engine
# MAGIC
# MAGIC Imagine Excel formulas — but running on millions of rows simultaneously:
# MAGIC
# MAGIC | Excel Formula | PySpark Column Expression | What It Does |
# MAGIC |---|---|---|
# MAGIC | `=A1*B1` | `col("price") * col("qty")` | Arithmetic between columns |
# MAGIC | `=IF(A1>100,"High","Low")` | `when(col("val")>100, "High").otherwise("Low")` | Conditional logic |
# MAGIC | `=UPPER(A1)` | `upper(col("name"))` | Transform values |
# MAGIC | `=LEFT(A1, 3)` | `col("code").substr(1, 3)` | Extract part of string |
# MAGIC | `=A1&"-"&B1` | `concat(col("a"), lit("-"), col("b"))` | Combine values |
# MAGIC | `=INT(A1)` | `col("str_val").cast("int")` | Convert types |
# MAGIC
# MAGIC ### Key Insight
# MAGIC A **Column Expression** is NOT data — it's a **recipe** that tells Spark how to compute a value for each row. It only executes when an action is triggered.
# MAGIC
# MAGIC ### The Column Object
# MAGIC - `col("name")` — creates a Column reference
# MAGIC - `df["name"]` — same thing (DataFrame accessor)
# MAGIC - `df.name` — same thing (attribute accessor, breaks if name has spaces)
# MAGIC - `F.lit(42)` — creates a literal Column (same value every row)
# MAGIC
# MAGIC Every transformation (filter, select, withColumn) takes Column expressions as input.

# COMMAND ----------

# DBTITLE 1,SECTION 2 — How Column Expressions Work
# MAGIC %md
# MAGIC ## SECTION 2 — How Column Expressions Work (Internal Mechanics)
# MAGIC
# MAGIC ### The Expression Tree
# MAGIC
# MAGIC ```
# MAGIC col("price") * col("qty") + lit(10)
# MAGIC
# MAGIC Becomes an expression tree:
# MAGIC             [Add]
# MAGIC            /     \
# MAGIC      [Multiply]  [Literal: 10]
# MAGIC       /     \
# MAGIC [Column:price] [Column:qty]
# MAGIC
# MAGIC Catalyst Optimizer transforms this into efficient JVM bytecode.
# MAGIC ```
# MAGIC
# MAGIC ### Column Operations Categories
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────┐
# MAGIC │           COLUMN EXPRESSION TYPES                  │
# MAGIC ├──────────────────┬──────────────────┬────────────────┤
# MAGIC │  ARITHMETIC      │  COMPARISON      │  LOGICAL       │
# MAGIC │  + - * / %       │  == != > < >= <= │  & | ~ ^       │
# MAGIC │  ** (power)      │  between()      │  isNull()      │
# MAGIC │  abs(), round()  │  isin()         │  isNotNull()   │
# MAGIC ├──────────────────┼──────────────────┼────────────────┤
# MAGIC │  TYPE CAST       │  STRING OPS      │  COMPLEX TYPE  │
# MAGIC │  cast("int")     │  substr(p, l)   │  getItem(idx)  │
# MAGIC │  cast("double")  │  contains()     │  getField(nm)  │
# MAGIC │  cast("date")    │  startswith()   │  withField()   │
# MAGIC │  cast("string")  │  endswith()     │  dropFields()  │
# MAGIC ├──────────────────┼──────────────────┼────────────────┤
# MAGIC │  BITWISE         │  ALIAS           │  SPECIAL       │
# MAGIC │  bitwiseAND()   │  alias("name")  │  over(window)  │
# MAGIC │  bitwiseOR()    │  name()         │  asc() / desc()│
# MAGIC │  bitwiseXOR()   │                  │  like/rlike    │
# MAGIC └──────────────────┴──────────────────┴────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### withColumn vs select
# MAGIC ```
# MAGIC withColumn:  Adds/replaces ONE column, keeps all others
# MAGIC   df.withColumn("new", expr)  →  [all_old_cols + new]
# MAGIC
# MAGIC select:  Choose exactly which columns to keep
# MAGIC   df.select("a", "b", expr.alias("c"))  →  [a, b, c]
# MAGIC
# MAGIC Performance: select is faster for multiple new columns
# MAGIC   (withColumn chain = deep plan, select = flat plan)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 1: withColumn and Arithmetic
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 1: withColumn and Arithmetic
# ============================================================
# Real-world: Adding calculated columns to a sales dataset

from pyspark.sql import SparkSession  # Import SparkSession
from pyspark.sql.functions import col, lit, round as spark_round  # Import functions

spark = SparkSession.builder.getOrCreate()  # Get existing session

# Create sales data
sales_data = [
    ("Laptop", 999.99, 3, 0.10),    # product, price, qty, discount_rate
    ("Phone", 699.50, 5, 0.05),
    ("Tablet", 449.00, 2, 0.15),
    ("Monitor", 329.99, 4, 0.00),
    ("Keyboard", 79.99, 10, 0.20),
]

df = spark.createDataFrame(sales_data, ["product", "price", "qty", "discount_rate"])

print("=== Arithmetic Column Expressions ===")
print("\n--- Original Data ---")
df.show()  # Show original

# Addition: col + col
df1 = df.withColumn("gross_total", col("price") * col("qty"))  # Multiply price × qty

# Subtraction with formula
df2 = df1.withColumn(
    "discount_amount",
    spark_round(col("price") * col("qty") * col("discount_rate"), 2)  # price × qty × rate
)

# More arithmetic
df3 = df2.withColumn(
    "net_total",
    spark_round(col("gross_total") - col("discount_amount"), 2)  # Gross - discount
)

# Division
df4 = df3.withColumn(
    "unit_net_price",
    spark_round(col("net_total") / col("qty"), 2)  # Net total / quantity
)

# Modulo (remainder)
df5 = df4.withColumn(
    "qty_mod_3",
    col("qty") % lit(3)  # Remainder when dividing qty by 3
)

print("--- With All Arithmetic Columns ---")
df5.select("product", "gross_total", "discount_amount", "net_total", "unit_net_price", "qty_mod_3").show()

# Literal values with lit()
df_with_lit = df.withColumn("currency", lit("USD"))  # Constant value for all rows
df_with_lit = df_with_lit.withColumn("tax_rate", lit(0.08))  # 8% tax rate
df_with_lit = df_with_lit.withColumn(
    "tax_amount", 
    spark_round(col("price") * col("qty") * col("tax_rate"), 2)  # Calculated tax
)

print("--- With Literal Columns ---")
df_with_lit.select("product", "currency", "tax_rate", "tax_amount").show()

# Expected Output:
# +--------+-----------+---------------+---------+--------------+---------+
# |product |gross_total|discount_amount|net_total|unit_net_price|qty_mod_3|
# +--------+-----------+---------------+---------+--------------+---------+
# |Laptop  |2999.97    |299.997        |2700.0   |900.0         |0        |
# |Phone   |3497.5     |174.875        |3322.63  |664.53        |2        |
# +--------+-----------+---------------+---------+--------------+---------+

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 2: Drop, Cast, and when/otherwise
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 2: Drop, Cast, and when/otherwise
# ============================================================
# Real-world: Cleaning and transforming a raw CSV import

from pyspark.sql.functions import col, when, lit, coalesce
from pyspark.sql.types import IntegerType, DoubleType, DateType

# Simulate raw CSV data (all strings)
raw_data = [
    ("1", "Alice", "28", "85000.50", "2024-01-15", "engineering"),
    ("2", "Bob", "35", "72000.00", "2023-06-01", "marketing"),
    ("3", "Charlie", "42", "95000.75", "2022-03-20", "engineering"),
    ("4", "Diana", "31", None, "2024-08-10", "sales"),
    ("5", "Eve", "55", "120000.00", "2020-01-05", "engineering"),
]

df = spark.createDataFrame(raw_data, ["id_str", "name", "age_str", "salary_str", "start_date_str", "dept"])

print("=== Type Casting with cast() ===")
print("\n--- Original Schema (all strings) ---")
df.printSchema()  # Show string types

# Cast columns to correct types
df_typed = df \
    .withColumn("id", col("id_str").cast(IntegerType())) \
    .withColumn("age", col("age_str").cast("int")) \
    .withColumn("salary", col("salary_str").cast("double")) \
    .withColumn("start_date", col("start_date_str").cast("date"))

print("--- After Casting ---")
df_typed.printSchema()  # Show correct types

# Drop the original string columns (no longer needed)
df_clean = df_typed.drop("id_str", "age_str", "salary_str", "start_date_str")

print("--- After Dropping Original Columns ---")
df_clean.printSchema()  # Cleaner schema
df_clean.show()  # Show data

# when/otherwise — conditional column logic
print("\n=== when/otherwise (Conditional Logic) ===")

df_enriched = df_clean \
    .withColumn(
        "salary_band",  # Create salary band category
        when(col("salary") >= 100000, "Senior")      # >= 100K
        .when(col("salary") >= 80000, "Mid")          # >= 80K
        .when(col("salary") >= 60000, "Junior")       # >= 60K
        .otherwise("Entry")                            # < 60K
    ) \
    .withColumn(
        "salary_filled",  # Handle NULL salary
        coalesce(col("salary"), lit(50000.0))  # Use 50K default if NULL
    ) \
    .withColumn(
        "is_senior",  # Boolean column
        when(col("age") >= 40, True).otherwise(False)  # True if age >= 40
    )

df_enriched.select("name", "age", "salary", "salary_band", "salary_filled", "is_senior").show()

# Expected Output:
# +-------+---+---------+-----------+-------------+---------+
# |name   |age|salary   |salary_band|salary_filled|is_senior|
# +-------+---+---------+-----------+-------------+---------+
# |Alice  |28 |85000.5  |Mid        |85000.5      |false    |
# |Bob    |35 |72000.0  |Junior     |72000.0      |false    |
# |Charlie|42 |95000.75 |Mid        |95000.75     |true     |
# |Diana  |31 |null     |Entry      |50000.0      |false    |
# |Eve    |55 |120000.0 |Senior     |120000.0     |true     |
# +-------+---+---------+-----------+-------------+---------+

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Beginner Example 3: substr, concat, and Column aliases
# ============================================================
# SECTION 3 — BEGINNER EXAMPLE 3: substr, concat, and Column aliases
# ============================================================
# Real-world: Building composite keys and extracting code parts

from pyspark.sql.functions import (
    col, concat, concat_ws, lit, substring, length, upper, lower, initcap
)

# Employee data with codes
emp_data = [
    ("EMP-2024-00142", "John", "Doe", "Engineering", "NYC"),
    ("EMP-2023-00078", "Jane", "Smith", "Marketing", "LAX"),
    ("EMP-2024-00256", "Bob", "Williams", "Sales", "CHI"),
    ("EMP-2022-00001", "Alice", "Johnson", "Engineering", "NYC"),
]

df = spark.createDataFrame(emp_data, ["emp_code", "first_name", "last_name", "dept", "office"])

# substr(startPos, length) — extract part of string (1-based!)
print("=== substr() — Extract Parts of Strings ===")
df_parts = df.select(
    col("emp_code"),  # Original
    col("emp_code").substr(1, 3).alias("prefix"),       # "EMP" (pos 1, len 3)
    col("emp_code").substr(5, 4).alias("year"),         # "2024" (pos 5, len 4)
    col("emp_code").substr(10, 5).alias("seq_num"),     # "00142" (pos 10, len 5)
)
df_parts.show(truncate=False)  # Show extracted parts

# concat and concat_ws — combine columns
print("=== concat() and concat_ws() ===")
df_combined = df.select(
    # concat — simple join (NULL-propagating!)
    concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
    
    # concat_ws — join with separator (NULL-safe)
    concat_ws("-", col("dept"), col("office")).alias("dept_office"),
    
    # Build a composite key
    concat_ws("_",
        upper(col("office")),           # Uppercase office code
        lower(col("dept")),            # Lowercase department
        col("emp_code").substr(10, 5)  # Sequence number
    ).alias("composite_key"),
    
    # String length
    length(col("emp_code")).alias("code_length"),  # 14 characters
)
df_combined.show(truncate=False)  # Show combinations

# Column aliases — multiple ways
print("=== Column Aliases ===")
df_aliases = df.select(
    col("first_name").alias("given_name"),         # .alias() — most common
    col("last_name").name("family_name"),           # .name() — same as alias
    (col("first_name")).cast("string").alias("fn"), # Chain with cast
    initcap(concat_ws(" ", col("first_name"), col("last_name"))).alias("display_name"),
)
df_aliases.show(truncate=False)  # Show aliased columns

# Expected Output (df_parts):
# +--------------+------+----+-------+
# |emp_code      |prefix|year|seq_num|
# +--------------+------+----+-------+
# |EMP-2024-00142|EMP   |2024|00142  |
# |EMP-2023-00078|EMP   |2023|00078  |
# |EMP-2024-00256|EMP   |2024|00256  |
# |EMP-2022-00001|EMP   |2022|00001  |
# +--------------+------+----+-------+

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 1: getItem and getField
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 1: getItem() and getField()
# ============================================================
# Real-world: Accessing elements in arrays, maps, and structs

from pyspark.sql.functions import (
    col, array, create_map, struct, lit, explode, size
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, MapType
)

# === getItem() for Arrays ===
print("=== getItem() — Access Array Elements ===")

array_data = [
    ("Alice", ["Python", "Spark", "SQL"]),
    ("Bob", ["Java", "Scala"]),
    ("Charlie", ["Python", "R", "Julia", "Rust"]),
]

df_arr = spark.createDataFrame(array_data, ["name", "skills"])
df_arr.show(truncate=False)  # Show array column

# Access by index (0-based for getItem!)
df_arr.select(
    col("name"),
    col("skills").getItem(0).alias("first_skill"),   # First element
    col("skills").getItem(1).alias("second_skill"),  # Second element (NULL if missing)
    col("skills")[2].alias("third_skill"),            # Alternative syntax with []
    size(col("skills")).alias("num_skills"),          # Array length
).show(truncate=False)

# === getItem() for Maps ===
print("\n=== getItem() — Access Map Values ===")

map_data = [
    ("Product A", {"color": "red", "size": "large", "weight": "2kg"}),
    ("Product B", {"color": "blue", "size": "small"}),
    ("Product C", {"color": "green", "size": "medium", "material": "wood"}),
]

df_map = spark.createDataFrame(map_data, ["product", "attributes"])
df_map.show(truncate=False)  # Show map column

# Access map values by key
df_map.select(
    col("product"),
    col("attributes").getItem("color").alias("color"),     # Get "color" key
    col("attributes").getItem("size").alias("size"),       # Get "size" key
    col("attributes")["weight"].alias("weight"),           # Alternative syntax
    col("attributes").getItem("missing").alias("missing"), # NULL for missing key
).show(truncate=False)

# === getField() for Structs ===
print("\n=== getField() — Access Struct Fields ===")

# Create DataFrame with struct column
struct_data = [
    ("Order1", ("John Doe", "123 Main St", "NYC")),
    ("Order2", ("Jane Smith", "456 Oak Ave", "LAX")),
    ("Order3", ("Bob Williams", "789 Pine Rd", "CHI")),
]

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer", StructType([
        StructField("name", StringType()),
        StructField("address", StringType()),
        StructField("city", StringType()),
    ]))
])

df_struct = spark.createDataFrame(struct_data, schema)
df_struct.printSchema()  # Show nested schema
df_struct.show(truncate=False)  # Show data

# Access struct fields
df_struct.select(
    col("order_id"),
    col("customer").getField("name").alias("cust_name"),       # getField for structs
    col("customer.address").alias("cust_address"),              # Dot notation (same thing)
    col("customer")["city"].alias("cust_city"),                 # Bracket notation
).show(truncate=False)

# Expected Output:
# +--------+------------+----------+-----------+----------+
# |name    |first_skill |second_skill|third_skill|num_skills|
# +--------+------------+----------+-----------+----------+
# |Alice   |Python      |Spark      |SQL        |3         |
# |Bob     |Java        |Scala      |null       |2         |
# |Charlie |Python      |R          |Julia      |4         |
# +--------+------------+----------+-----------+----------+

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 2: withField and dropFields
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 2: withField() and dropFields()
# ============================================================
# Real-world: Modifying nested struct fields without flattening
# Available in Spark 3.1+

from pyspark.sql.functions import col, struct, lit, upper, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create data with nested struct
data = [
    (1, ("john doe", "123 main st", "nyc", 10001)),
    (2, ("jane smith", "456 oak ave", "lax", 90001)),
    (3, ("bob williams", "789 pine rd", "chi", 60601)),
]

schema = StructType([
    StructField("id", IntegerType()),
    StructField("address", StructType([
        StructField("name", StringType()),
        StructField("street", StringType()),
        StructField("city", StringType()),
        StructField("zip", IntegerType()),
    ]))
])

df = spark.createDataFrame(data, schema)
print("=== Original Struct ===")
df.printSchema()  # Show nested schema
df.show(truncate=False)  # Show data

# withField() — modify a field INSIDE a struct without flattening
print("\n=== withField() — Modify Nested Fields In-Place ===")

df_modified = df \
    .withColumn(
        "address",  # Target the struct column
        col("address").withField("city", upper(col("address.city")))  # Uppercase city
    ) \
    .withColumn(
        "address",
        col("address").withField("name", initcap(col("address.name")))  # Proper case name
    ) \
    .withColumn(
        "address",
        col("address").withField("state", lit("US"))  # Add new field to struct!
    )

print("After withField (city uppercased, name initcapped, state added):")
df_modified.printSchema()  # Note: state field added
df_modified.show(truncate=False)

# dropFields() — remove field(s) from a struct
print("\n=== dropFields() — Remove Nested Fields ===")

df_dropped = df_modified.withColumn(
    "address",
    col("address").dropFields("zip", "state")  # Remove zip and state from struct
)

print("After dropFields (zip and state removed):")
df_dropped.printSchema()  # zip and state gone
df_dropped.show(truncate=False)

# Combine withField in select for multiple modifications
print("\n=== Combining withField in select ===")
result = df.select(
    col("id"),
    col("address")
        .withField("city", upper(col("address.city")))          # Uppercase city
        .withField("full_address",                               # New computed field
            concat(col("address.street"), lit(", "), col("address.city"))
        )
        .dropFields("zip")                                       # Remove zip
        .alias("address")
)

result.printSchema()  # Show final schema
result.show(truncate=False)

# Expected Output:
# After withField:
# root
#  |-- id: integer
#  |-- address: struct
#  |    |-- name: string
#  |    |-- street: string
#  |    |-- city: string  (now UPPERCASE)
#  |    |-- zip: integer
#  |    |-- state: string  (NEW field)

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Intermediate Example 3: Bitwise Operations
# ============================================================
# SECTION 4 — INTERMEDIATE EXAMPLE 3: Bitwise Operations
# ============================================================
# Real-world: Permission flags, feature toggles, IoT sensor status bits

from pyspark.sql.functions import (
    col, lit, expr, bin as spark_bin, hex as spark_hex,
    when, concat_ws
)

# === Bitwise Operations for Permission Systems ===
print("=== Bitwise Operations ===")
print()
print("Permission bits: READ=1(001), WRITE=2(010), EXEC=4(100)")
print("Combined: READ+WRITE=3(011), ALL=7(111), NONE=0(000)")
print()

# User permissions stored as integer flags
perms_data = [
    ("admin", 7),     # 111 = READ + WRITE + EXEC
    ("editor", 3),    # 011 = READ + WRITE
    ("viewer", 1),    # 001 = READ only
    ("executor", 5),  # 101 = READ + EXEC
    ("none", 0),      # 000 = no permissions
]

df = spark.createDataFrame(perms_data, ["role", "permission_flags"])

# Bitwise AND — check if specific bit is set
READ = 1    # Bit 0: 001
WRITE = 2   # Bit 1: 010
EXEC = 4    # Bit 2: 100

df_perms = df.select(
    col("role"),
    col("permission_flags"),
    
    # bitwiseAND — check individual permissions
    (col("permission_flags").bitwiseAND(lit(READ)) > 0).alias("can_read"),    # Has READ?
    (col("permission_flags").bitwiseAND(lit(WRITE)) > 0).alias("can_write"),  # Has WRITE?
    (col("permission_flags").bitwiseAND(lit(EXEC)) > 0).alias("can_exec"),    # Has EXEC?
    
    # Show binary representation
    expr("lpad(bin(permission_flags), 3, '0')").alias("binary"),  # "111", "011", etc.
)

df_perms.show(truncate=False)  # Show permission breakdown

# Bitwise OR — grant additional permission
print("\n=== bitwiseOR — Grant Permission ===")
df_granted = df.select(
    col("role"),
    col("permission_flags"),
    col("permission_flags").bitwiseOR(lit(WRITE)).alias("after_grant_write"),  # Add WRITE
    expr("lpad(bin(permission_flags | 2), 3, '0')").alias("binary_after"),  # Show binary
)
df_granted.show(truncate=False)  # Show after granting write

# Bitwise XOR — toggle permission
print("\n=== bitwiseXOR — Toggle Permission ===")
df_toggled = df.select(
    col("role"),
    col("permission_flags"),
    col("permission_flags").bitwiseXOR(lit(EXEC)).alias("after_toggle_exec"),  # Toggle EXEC
    expr("lpad(bin(permission_flags ^ 4), 3, '0')").alias("binary_after"),
)
df_toggled.show(truncate=False)  # EXEC toggled: on→off, off→on

# Practical: IoT sensor status decoding
print("\n=== Practical: IoT Sensor Status Decoding ===")
sensor_data = [
    ("sensor_01", 0b11010110),  # Binary literal: 214
    ("sensor_02", 0b00000001),  # Just power on
    ("sensor_03", 0b11111111),  # All flags set
    ("sensor_04", 0b10000000),  # Error flag only
]

# Bit layout: [7:error][6:warning][5:active][4:calibrated][3:unused][2:wifi][1:battery][0:power]
df_sensor = spark.createDataFrame(sensor_data, ["sensor_id", "status"])

df_decoded = df_sensor.select(
    col("sensor_id"),
    col("status"),
    expr("lpad(bin(status), 8, '0')").alias("binary"),
    (col("status").bitwiseAND(lit(1)) > 0).alias("power_on"),       # Bit 0
    (col("status").bitwiseAND(lit(2)) > 0).alias("battery_ok"),     # Bit 1
    (col("status").bitwiseAND(lit(4)) > 0).alias("wifi_connected"), # Bit 2
    (col("status").bitwiseAND(lit(32)) > 0).alias("active"),        # Bit 5
    (col("status").bitwiseAND(lit(128)) > 0).alias("has_error"),    # Bit 7
)

df_decoded.show(truncate=False)  # Show decoded sensor status

# Expected Output (df_perms):
# +--------+----------------+--------+---------+--------+------+
# |role    |permission_flags|can_read|can_write|can_exec|binary|
# +--------+----------------+--------+---------+--------+------+
# |admin   |7               |true    |true     |true    |111   |
# |editor  |3               |true    |true     |false   |011   |
# |viewer  |1               |true    |false    |false   |001   |
# |executor|5               |true    |false    |true    |101   |
# |none    |0               |false   |false    |false   |000   |
# +--------+----------------+--------+---------+--------+------+

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 1: Column Expression Composition
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 1: Column Expression Composition
# ============================================================
# Real-world: Building complex business rules as composable expressions

from pyspark.sql.functions import (
    col, when, lit, coalesce, greatest, least,
    abs as spark_abs, round as spark_round, expr,
    datediff, current_date, months_between, year
)

# Insurance pricing data
insurance_data = [
    ("Alice", 28, "NYC", 650, 0, "2020-01-15", "standard"),
    ("Bob", 45, "rural", 720, 2, "2018-06-01", "premium"),
    ("Charlie", 19, "suburb", 580, 1, "2024-03-20", "basic"),
    ("Diana", 62, "NYC", 800, 0, "2015-12-10", "premium"),
    ("Eve", 35, "suburb", None, 3, "2022-07-05", "standard"),
]

df = spark.createDataFrame(
    insurance_data, 
    ["name", "age", "location", "credit_score", "claims", "start_date", "tier"]
)

# === Building Complex Expressions Step by Step ===
print("=== Complex Business Rules as Column Expressions ===")

# Rule 1: Age risk factor (young and old = higher risk)
age_factor = (
    when(col("age") < 25, lit(1.5))          # Young drivers: 50% surcharge
    .when(col("age") > 60, lit(1.3))          # Elderly: 30% surcharge
    .when(col("age").between(25, 35), lit(0.9))  # Sweet spot: 10% discount
    .otherwise(lit(1.0))                       # Standard rate
)

# Rule 2: Location factor
location_factor = (
    when(col("location") == "NYC", lit(1.4))   # Urban: expensive
    .when(col("location") == "suburb", lit(1.1))  # Suburb: slightly more
    .otherwise(lit(0.8))                        # Rural: cheapest
)

# Rule 3: Credit score factor (handle NULL)
credit_factor = (
    when(coalesce(col("credit_score"), lit(0)) >= 750, lit(0.85))  # Excellent
    .when(coalesce(col("credit_score"), lit(0)) >= 650, lit(1.0))  # Good
    .when(coalesce(col("credit_score"), lit(0)) >= 550, lit(1.2))  # Fair
    .otherwise(lit(1.5))  # Poor or missing
)

# Rule 4: Claims history factor
claims_factor = (
    when(col("claims") == 0, lit(0.9))   # No claims: discount
    .when(col("claims") == 1, lit(1.0))  # One claim: standard
    .when(col("claims") == 2, lit(1.3))  # Two claims: surcharge
    .otherwise(lit(1.6))                  # 3+: high risk
)

# Rule 5: Loyalty discount (years as customer)
loyalty_discount = (
    when(datediff(current_date(), col("start_date").cast("date")) > 1825, lit(0.9))  # 5+ years
    .when(datediff(current_date(), col("start_date").cast("date")) > 730, lit(0.95))  # 2+ years
    .otherwise(lit(1.0))  # New customer
)

# Base price by tier
base_price = (
    when(col("tier") == "premium", lit(200.0))
    .when(col("tier") == "standard", lit(150.0))
    .otherwise(lit(100.0))  # basic
)

# === Compose all factors into final price ===
df_priced = df.select(
    col("name"),
    col("age"),
    col("location"),
    col("credit_score"),
    col("claims"),
    col("tier"),
    spark_round(age_factor, 2).alias("age_factor"),
    spark_round(location_factor, 2).alias("loc_factor"),
    spark_round(credit_factor, 2).alias("credit_factor"),
    spark_round(claims_factor, 2).alias("claims_factor"),
    spark_round(loyalty_discount, 2).alias("loyalty_disc"),
    spark_round(
        base_price * age_factor * location_factor * credit_factor * claims_factor * loyalty_discount,
        2
    ).alias("monthly_premium"),
)

df_priced.show(truncate=False)  # Show all factors and final price

# Expected: complex multi-factor pricing computed entirely with Column expressions
# No UDFs needed! Everything runs in Tungsten-optimized JVM.

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 2: withColumns (Spark 3.3+) and Performance
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 2: withColumns (Spark 3.3+) & Performance
# ============================================================
# Real-world: Adding many columns efficiently in production pipelines

import time
from pyspark.sql.functions import (
    col, lit, upper, lower, length, when, expr, 
    current_timestamp, date_format, hash as spark_hash
)

# Generate test data
df = spark.range(100000).select(  # 100K rows
    col("id"),
    expr("concat('user_', lpad(cast(id as string), 5, '0'))").alias("username"),
    expr("CASE WHEN id % 3 = 0 THEN 'admin' WHEN id % 3 = 1 THEN 'editor' ELSE 'viewer' END").alias("role"),
    expr("cast(rand() * 100000 as decimal(10,2))").alias("salary"),
)

print("=== Performance: withColumn chain vs select ===")
print(f"DataFrame: {df.count():,} rows")
print()

# METHOD 1: Chained withColumn (creates deep logical plan)
start = time.time()
df_chained = df \
    .withColumn("username_upper", upper(col("username"))) \
    .withColumn("username_lower", lower(col("username"))) \
    .withColumn("username_len", length(col("username"))) \
    .withColumn("is_admin", when(col("role") == "admin", True).otherwise(False)) \
    .withColumn("salary_band", when(col("salary") > 75000, "high").otherwise("standard")) \
    .withColumn("row_hash", spark_hash(col("username"), col("role"))) \
    .withColumn("processed_at", current_timestamp())

# Force evaluation
df_chained.write.format("noop").mode("overwrite").save()  # Trigger execution
time_chained = time.time() - start
print(f"Method 1 (chained withColumn): {time_chained:.2f}s")

# METHOD 2: Single select (flat logical plan)
start = time.time()
df_select = df.select(
    "*",  # Keep all original columns
    upper(col("username")).alias("username_upper"),
    lower(col("username")).alias("username_lower"),
    length(col("username")).alias("username_len"),
    when(col("role") == "admin", True).otherwise(False).alias("is_admin"),
    when(col("salary") > 75000, "high").otherwise("standard").alias("salary_band"),
    spark_hash(col("username"), col("role")).alias("row_hash"),
    current_timestamp().alias("processed_at"),
)

df_select.write.format("noop").mode("overwrite").save()  # Trigger execution
time_select = time.time() - start
print(f"Method 2 (single select):       {time_select:.2f}s")

# METHOD 3: withColumns (Spark 3.3+) — best of both worlds
start = time.time()
try:
    df_withcols = df.withColumns({
        "username_upper": upper(col("username")),
        "username_lower": lower(col("username")),
        "username_len": length(col("username")),
        "is_admin": when(col("role") == "admin", True).otherwise(False),
        "salary_band": when(col("salary") > 75000, "high").otherwise("standard"),
        "row_hash": spark_hash(col("username"), col("role")),
        "processed_at": current_timestamp(),
    })
    df_withcols.write.format("noop").mode("overwrite").save()
    time_withcols = time.time() - start
    print(f"Method 3 (withColumns dict):    {time_withcols:.2f}s")
except AttributeError:
    print("Method 3: withColumns not available (requires Spark 3.3+)")

print("\n--- Results are identical, but plan depth differs ---")
print(f"Chained withColumn plan depth: ~7 Project nodes")
print(f"Single select plan depth: 1 Project node")
print(f"\nBest practice: Use select() or withColumns() for 3+ new columns")

# Show the result
df_select.show(5, truncate=False)  # Show sample rows

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Advanced Example 3: Production Column Expression Library
# ============================================================
# SECTION 5 — ADVANCED EXAMPLE 3: Production Column Expression Library
# ============================================================
# Real-world: Building reusable column expression functions for team use

from pyspark.sql.functions import (
    col, when, lit, coalesce, trim, lower, upper, regexp_replace,
    length, concat_ws, current_timestamp, date_format, md5, sha2,
    expr, initcap, round as spark_round
)
from pyspark.sql import Column  # Type hint for expressions

# === Reusable Column Expression Library ===

def clean_string(c: Column) -> Column:
    """Clean a string column: trim, collapse spaces, handle NULL."""
    return when(
        trim(regexp_replace(c, r"\s+", " ")) == "",  # Empty after cleaning?
        lit(None)                                       # Return NULL
    ).otherwise(
        trim(regexp_replace(c, r"\s+", " "))           # Trimmed, single spaces
    )

def standardize_name(c: Column) -> Column:
    """Standardize a name: clean + proper case."""
    return initcap(clean_string(c))  # Clean then capitalize each word

def mask_pii(c: Column, show_last: int = 4) -> Column:
    """Mask PII: show only last N characters."""
    return when(
        c.isNull(), lit(None)  # NULL stays NULL
    ).otherwise(
        concat_ws("",
            expr(f"repeat('*', greatest(length({c._jc.toString()}) - {show_last}, 0))"),
            expr(f"right({c._jc.toString()}, {show_last})")
        )
    )

def null_safe_divide(numerator: Column, denominator: Column, default: float = 0.0) -> Column:
    """Safe division: returns default when denominator is 0 or NULL."""
    return when(
        (denominator.isNull()) | (denominator == 0),  # Unsafe denominator
        lit(default)                                    # Return default
    ).otherwise(
        spark_round(numerator / denominator, 4)        # Safe division
    )

def categorize(c: Column, bins: list, labels: list) -> Column:
    """Bin a numeric column into categories.
    bins: [0, 25, 50, 75, 100] 
    labels: ["low", "medium", "high", "very_high"]
    """
    result = lit(labels[-1])  # Default: last label
    # Build from last to first (when chain)
    for i in range(len(bins) - 2, -1, -1):  # Reverse order
        result = when(c < bins[i + 1], lit(labels[i])).otherwise(result)
    return result

def add_audit_columns(df):
    """Add standard audit columns to any DataFrame."""
    return df.select(
        "*",  # Keep all existing columns
        current_timestamp().alias("_loaded_at"),                     # Load timestamp
        date_format(current_timestamp(), "yyyyMMdd").alias("_partition_date"),  # Partition key
        md5(concat_ws("|", *[col(c) for c in df.columns])).alias("_row_hash"),  # Row hash
        lit("v2.1").alias("_pipeline_version"),                      # Version tracking
    )

# === Apply the library ===
print("=== Production Column Expression Library Demo ===")

test_data = [
    ("  JOHN   DOE  ", "5551234567", 85000, 40, "engineering"),
    ("jane smith", "5559876543", 72000, 35, "marketing"),
    ("  ", "5552468135", 95000, 0, "engineering"),  # Empty name
    (None, "5553698521", None, 50, "sales"),          # NULL name and salary
    ("BOB WILLIAMS III", "5551112222", 120000, 25, "executive"),
]

df = spark.createDataFrame(test_data, ["name", "phone", "salary", "hours", "dept"])

# Apply library functions
result = df.select(
    standardize_name(col("name")).alias("clean_name"),       # Clean + proper case
    mask_pii(col("phone"), show_last=4).alias("phone_masked"),  # Mask phone
    col("salary"),
    null_safe_divide(col("salary"), col("hours")).alias("hourly_rate"),  # Safe division
    categorize(
        col("salary"),
        bins=[0, 60000, 80000, 100000, 200000],
        labels=["entry", "mid", "senior", "executive"]
    ).alias("level"),
)

result.show(truncate=False)  # Show library results

# Add audit columns
audited = add_audit_columns(result)
print("\n--- With Audit Columns ---")
audited.printSchema()  # Show schema with audit fields
audited.show(2, truncate=False)  # Show sample

print("\n✅ Column Expression Library ready for production use!")
print("Tip: Put these in a shared Python module for team reuse.")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Common Mistakes
# MAGIC %md
# MAGIC ## SECTION 6 — Common Mistakes with Column Expressions
# MAGIC
# MAGIC ### ❌ Mistake 1: Chaining 50+ withColumn calls
# MAGIC ```python
# MAGIC # WRONG — Creates deeply nested logical plan, slow compile time
# MAGIC for c in columns:
# MAGIC     df = df.withColumn(f"{c}_clean", trim(col(c)))  # N Project nodes!
# MAGIC
# MAGIC # CORRECT — Use single select (1 Project node)
# MAGIC df = df.select("*", *[trim(col(c)).alias(f"{c}_clean") for c in columns])
# MAGIC
# MAGIC # OR (Spark 3.3+) — Use withColumns
# MAGIC df = df.withColumns({f"{c}_clean": trim(col(c)) for c in columns})
# MAGIC ```
# MAGIC **Why:** Each withColumn adds a Project node to the logical plan. 50+ = StackOverflow or 30+ second plan compilation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 2: Using 0-based indexing with substr
# MAGIC ```python
# MAGIC # WRONG — PySpark substr is 1-based (SQL convention)
# MAGIC df.select(col("code").substr(0, 3))  # Returns empty or unexpected!
# MAGIC
# MAGIC # CORRECT — Start at 1
# MAGIC df.select(col("code").substr(1, 3))  # First 3 characters
# MAGIC ```
# MAGIC **Why:** PySpark follows SQL standard (1-based). But getItem() for arrays is 0-based! This inconsistency trips everyone up.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 3: Confusing getItem vs getField
# MAGIC ```python
# MAGIC # For Arrays and Maps — use getItem (or [] bracket syntax)
# MAGIC col("my_array").getItem(0)      # Array element at index 0
# MAGIC col("my_map").getItem("key")    # Map value for "key"
# MAGIC col("my_array")[0]              # Same as getItem(0)
# MAGIC
# MAGIC # For Structs — use getField (or dot notation)
# MAGIC col("my_struct").getField("field_name")  # Struct field
# MAGIC col("my_struct.field_name")              # Same with dot notation
# MAGIC col("my_struct")["field_name"]           # Also works
# MAGIC
# MAGIC # WRONG — getField on an array
# MAGIC col("my_array").getField(0)  # Error! Use getItem for arrays.
# MAGIC ```
# MAGIC **Why:** Different complex types need different accessors. Arrays/Maps are indexed; Structs are named.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 4: Forgetting that withField modifies a copy
# MAGIC ```python
# MAGIC # WRONG — Expecting in-place modification
# MAGIC col("address").withField("city", upper(col("address.city")))  # Returns new Column!
# MAGIC # Must assign back:
# MAGIC
# MAGIC # CORRECT
# MAGIC df = df.withColumn("address", col("address").withField("city", upper(col("address.city"))))
# MAGIC ```
# MAGIC **Why:** All Column operations return NEW Column objects. DataFrames are immutable.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 5: Division without NULL/zero protection
# MAGIC ```python
# MAGIC # WRONG — Will produce NULL or Infinity
# MAGIC df.withColumn("rate", col("revenue") / col("hours"))  # Division by zero!
# MAGIC
# MAGIC # CORRECT — Guard against zero and NULL
# MAGIC df.withColumn("rate",
# MAGIC     when((col("hours").isNull()) | (col("hours") == 0), lit(0.0))
# MAGIC     .otherwise(col("revenue") / col("hours"))
# MAGIC )
# MAGIC ```
# MAGIC **Why:** Spark returns NULL for division by zero in some contexts, infinity in others. Always guard.

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework
# MAGIC %md
# MAGIC ## SECTION 7 — Homework: 10 Levels of Column Expression Mastery
# MAGIC
# MAGIC ### Level 1 — Copy-Paste (Run these exactly)
# MAGIC 1. Create a DataFrame with price and quantity. Add a `total` column using arithmetic.
# MAGIC 2. Use `cast()` to convert string columns to int and double.
# MAGIC
# MAGIC ### Level 2 — Tiny Change
# MAGIC 3. Modify the pricing example to add a 8.5% tax column.
# MAGIC 4. Change the credit score factor to use 4 bands instead of 3.
# MAGIC
# MAGIC ### Level 3 — Combine Two Concepts
# MAGIC 5. Build a DataFrame with nested structs. Use `withField()` to uppercase a nested field, then `dropFields()` to remove another.
# MAGIC 6. Combine `when/otherwise` with `cast()` to create a bucketed column from a string-typed numeric.
# MAGIC
# MAGIC ### Level 4 — New Scenario
# MAGIC 7. Create a permissions system where users have role flags stored as integers. Decode them using `bitwiseAND` to check access to: dashboard (1), reports (2), admin_panel (4), settings (8), billing (16).
# MAGIC
# MAGIC ### Level 5 — Mini Project
# MAGIC 8. Build a complete customer scoring pipeline using only Column expressions:
# MAGIC    - Input: age, income, credit_history_years, num_products, has_default (boolean)
# MAGIC    - Output: risk_score (0-100), risk_category, approval_recommendation
# MAGIC
# MAGIC ### Level 6 — Design Your Own
# MAGIC 9. Create a reusable Column expression library for your domain (e.g., finance: calculate interest, amortization; healthcare: BMI, risk scores; e-commerce: discount tiers, shipping cost).
# MAGIC
# MAGIC ### Level 7 — Optimize
# MAGIC 10. Create a DataFrame with 1M rows and 20 columns. Compare:
# MAGIC     - Adding 10 computed columns via chained `withColumn` vs single `select` vs `withColumns`
# MAGIC     - Measure compilation time and execution time separately.
# MAGIC     - Check `.explain()` output to see plan depth differences.
# MAGIC
# MAGIC ### Level 8 — Edge Cases
# MAGIC 11. Handle these edge cases:
# MAGIC     - `substr()` with position beyond string length (returns empty string)
# MAGIC     - `getItem()` with index beyond array size (returns NULL)
# MAGIC     - `withField()` adding field that already exists (overwrites)
# MAGIC     - Arithmetic with NULL operands (result is NULL)
# MAGIC     - Bitwise operations on negative numbers
# MAGIC
# MAGIC ### Level 9 — Production Ready
# MAGIC 12. Build a `ColumnExpressionValidator` class that:
# MAGIC     - Takes a DataFrame and a list of column expressions
# MAGIC     - Validates types are compatible before execution
# MAGIC     - Reports which expressions would produce NULLs
# MAGIC     - Suggests fixes for common type mismatches
# MAGIC     - Generates a data quality report
# MAGIC
# MAGIC ### Level 10 — Teach Someone
# MAGIC 13. Create a visual decision tree for "Which column method should I use?":
# MAGIC     - Accessing nested data? → getItem vs getField vs dot notation
# MAGIC     - Adding columns? → withColumn vs select vs withColumns
# MAGIC     - Conditional logic? → when vs coalesce vs case (SQL)
# MAGIC     - Type conversion? → cast vs explicit functions (to_date, etc.)

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Homework Solutions
# ============================================================
# SECTION 7 — HOMEWORK SOLUTIONS
# ============================================================

from pyspark.sql.functions import *  # Import all for solutions
from pyspark.sql.types import *

# --- Level 1: Arithmetic and Casting ---
print("=== Level 1: Arithmetic ===")
orders = spark.createDataFrame([
    ("Widget", "10", "25.50"),
    ("Gadget", "5", "99.99"),
    ("Doohickey", "20", "5.75"),
], ["product", "qty_str", "price_str"])

# Cast strings to numeric types
orders_typed = orders \
    .withColumn("qty", col("qty_str").cast(IntegerType())) \
    .withColumn("price", col("price_str").cast(DoubleType())) \
    .drop("qty_str", "price_str")  # Remove string originals

# Add arithmetic columns
orders_calc = orders_typed \
    .withColumn("subtotal", round(col("price") * col("qty"), 2)) \
    .withColumn("tax", round(col("price") * col("qty") * 0.085, 2)) \
    .withColumn("total", round(col("price") * col("qty") * 1.085, 2))

orders_calc.show(truncate=False)

# --- Level 5: Customer Scoring Pipeline ---
print("\n=== Level 5: Customer Scoring ===")
customers = spark.createDataFrame([
    (25, 45000, 2, 1, False),
    (45, 120000, 15, 5, False),
    (60, 80000, 30, 3, True),
    (35, 95000, 8, 2, False),
    (22, 30000, 0, 1, True),
], ["age", "income", "credit_years", "num_products", "has_default"])

# Scoring formula using only column expressions
age_score = when(col("age").between(30, 55), lit(20)).when(col("age").between(25, 65), lit(15)).otherwise(lit(5))
income_score = when(col("income") > 100000, lit(25)).when(col("income") > 60000, lit(20)).when(col("income") > 40000, lit(10)).otherwise(lit(5))
credit_score = least(lit(25), col("credit_years") * 2)  # 2 points per year, max 25
product_score = least(lit(15), col("num_products") * 3)  # 3 points per product, max 15
default_penalty = when(col("has_default"), lit(-20)).otherwise(lit(0))  # -20 for defaults

total_score = age_score + income_score + credit_score + product_score + default_penalty

scored = customers.select(
    "*",
    greatest(lit(0), least(lit(100), total_score)).alias("risk_score"),  # Clamp 0-100
    when(total_score >= 70, "low_risk")
        .when(total_score >= 45, "medium_risk")
        .otherwise("high_risk").alias("risk_category"),
    when(total_score >= 50, "APPROVE")
        .when(total_score >= 30, "REVIEW")
        .otherwise("DECLINE").alias("recommendation"),
)

scored.show(truncate=False)

# --- Level 7: Performance Comparison ---
print("\n=== Level 7: Performance Test ===")
import time

big_df = spark.range(1000000).select(
    col("id"),
    expr("concat('val_', id)").alias("text"),
    expr("rand() * 100").alias("num")
)

# Chained withColumn
start = time.time()
chain_df = big_df
for i in range(10):
    chain_df = chain_df.withColumn(f"computed_{i}", col("num") + lit(i))
chain_df.write.format("noop").mode("overwrite").save()
print(f"Chained (10 withColumn): {time.time()-start:.3f}s")

# Single select
start = time.time()
select_df = big_df.select("*", *[( col("num") + lit(i)).alias(f"computed_{i}") for i in range(10)])
select_df.write.format("noop").mode("overwrite").save()
print(f"Single select:           {time.time()-start:.3f}s")

print("\n✅ All homework solutions complete!")